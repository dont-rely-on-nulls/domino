module type Action = sig
  (* Names of the attributes & The tuples *)
  type tuples = string list * bytes list list

  val execute : tuples -> int
end

module Scan = struct
  open Disk

  let execute (commit : Executor.commit) locations (entity : string) =
    (*Executor.read commit locations ~filename:("schema~" ^ entity) |> function
    | Some binary -> (
        let relation =
          Result.map_error (fun _ ->
              "Failed to deserialize relation from binary format.")
          @@ Data_encoding.Binary.of_bytes Schema.Protocol.relation_encoding
               (Bytes.concat Bytes.empty binary)
        in
        match relation with
        | Ok relation ->
            let _attrs =
              List.map
                (fun (a : Schema.Protocol.attribute) ->
                  relation.name ^ "/" ^ a.name)
                relation.attributes
            in
            Executor.read commit locations ~filename:("schema~" ^ entity)
        | Error err -> failwith err)
    | None -> failwith ""
    *)
    let entities :
        (string * Executor.relational_type * string) list Executor.IntMap.t =
      Executor.StringMap.find_opt entity commit.references |> function
      | Some entities -> entities
      | None -> Executor.IntMap.empty
    in

    let content : (int64 * (string * Executor.relational_literal) list) list =
      Executor.IntMap.fold
        (fun key elems acc ->
          ( key,
            List.map
              (fun (location, type', attribute_name) ->
                ( attribute_name,
                  Executor.read_location ~hash:location locations type' ))
              elems )
          :: acc)
        entities []
    in
    content
end

module Join = struct
  open Disk

  module NestedLoop = struct
    let execute
        (left : (int64 * (string * Executor.relational_literal) list) list)
        (right : (int64 * (string * Executor.relational_literal) list) list)
        (join_clause :
          (int64 * (string * Executor.relational_literal) list)
          * (int64 * (string * Executor.relational_literal) list) ->
          bool) =
      let result = ref [] in
      List.iter
        (fun left_tuple ->
          List.iter
            (fun right_tuple ->
              if join_clause (left_tuple, right_tuple) then
                result := (left_tuple, right_tuple) :: !result)
            right)
        left;
      !result
  end

  (* open Disk
     let execute (left_attributes: bytes list) (right_attributes: bytes list) locations =
       (* Redundant operations that are already in the locations, but this is easier to implement now *)
       (* Fix this when we persist the indexes *)
       let left_computed_hashes: (int * string) list = List.mapi (fun index elem -> (index, Interop.Sha256.compute_hash elem)) left_attributes in
       let right_computed_hashes: (int * string) list = List.mapi (fun index elem -> (index, Interop.Sha256.compute_hash elem)) right_attributes in
       let references_locations_right = List.filter_map (fun (index, rhash) -> Option.map (fun (elem: Executor.Location.t) -> index, rhash, elem.references)
                                                                               @@ Executor.StringMap.find_opt rhash locations) right_computed_hashes in
       let indexes =
         List.filter_map (fun (lindex, lhash) -> Option.map (fun (rindex, rhash, _) -> lindex, rindex, lhash, rhash)
                                                 @@ List.find_opt (fun (_rindex, _rhash, references) -> List.exists ((=) lhash) references) references_locations_right) left_computed_hashes
       in List.map (fun (lindex, rindex, _, _) -> List.nth left_attributes lindex, List.nth right_attributes rindex) indexes *)
  open Disk

  let natural commit locations left_relation right_relation =
    let left_scan = Scan.execute commit locations left_relation in
    let right_scan = Scan.execute commit locations right_relation in
    let common_attributes =
      let left_attributes =
        commit.schema |> Executor.StringMap.find left_relation
      in
      let right_attributes =
        commit.schema |> Executor.StringMap.find right_relation
      in
      List.filter_map
        (fun left_attr ->
          if List.mem left_attr right_attributes then Some (fst left_attr)
          else None)
        left_attributes
    in
    let join_clause =
     fun ((_entity_id_1, left_tuple), (_entity_id_2, right_tuple)) : bool ->
      (* Check only the key name for now, not subtype, so literals must be equal for it to work *)
      let left_tuple =
        Executor.StringMap.filter (fun key _ -> List.mem key common_attributes)
        @@ Executor.StringMap.of_list left_tuple
      in
      let right_tuple =
        Executor.StringMap.filter (fun key _ -> List.mem key common_attributes)
        @@ Executor.StringMap.of_list right_tuple
      in
      left_tuple = right_tuple
    in
    NestedLoop.execute left_scan right_scan join_clause

  let nest commit locations (relation: string) =
    let parent_scan = Scan.execute commit locations relation in
    let nested_attributes =
      Executor.StringMap.find relation commit.schema
      |> List.filter (fun (_, (elem : Executor.relational_type)) ->
             match elem with Executor.Relation _ -> true | _ -> false)
    in
    let nested_scans =
      List.concat_map
        (fun (nested_relation_attr, _) ->
          Scan.execute commit locations nested_relation_attr)
        nested_attributes
    in
    List.map
      (fun (entity_id, x) ->
        ( entity_id,
          List.map
            (fun (name_attr, item) ->
              match item with
              | Executor.LRelation (nested_entity_id, nested_name_attr) ->
                  ( name_attr,
                    Executor.LRelationNested
                      ( nested_entity_id,
                        nested_name_attr,
                        List.find_map
                          (fun (iter_entity_id, iter_attrs) ->
                            if iter_entity_id = nested_entity_id then
                              Some iter_attrs
                            else None)
                          nested_scans
                        |> Option.to_list |> List.concat ) )
              | _ -> (name_attr, item))
            x ))
      parent_scan
end

module Filter = struct end

module Executor = struct
  open Disk
  open Disk.Executor
  let perform (commit : Executor.commit) (locations : Executor.locations) (command : Command.t) =
    match command with
    | State ->
       Ok ((commit, locations), Command.Schema (StringMap.to_list commit.schema))
    | Nest {relation_name;} ->
       let results = Join.nest commit locations relation_name in
       Ok ((commit, locations), Command.Read results)
    | NaturalJoin {left_relation = _; right_relation = _} ->
       Ok ((commit, locations), Nothing)
    | SpecifyRelation {relation_name; attributes} ->
       (* If it already exists under the relation name key, it will be replaced *)
       Ok (({ commit with
              schema = commit.schema
                       |> Executor.StringMap.add relation_name attributes}, locations), Nothing)
    | SequentialRead { relation_name } ->
        let relation_name = List.hd @@ String.split_on_char '/' relation_name in
        let entities : (string * Executor.relational_type * string) list Executor.IntMap.t =
          print_endline relation_name;
          Executor.StringMap.find_opt relation_name commit.references
          |> function
          | Some entities -> entities
          | None -> Executor.IntMap.empty
        in

        let content: (int64 * (string*Executor.relational_literal) list) list =
          Executor.IntMap.fold
            (fun key elems acc ->
              (key,
                List.map
                  (fun (location, type', attribute_name) ->
                    (attribute_name, Executor.read_location ~hash:location locations type'))
                  elems)
              :: acc)
            entities []
        in
        
        Ok ((commit, locations), Read content)
end
