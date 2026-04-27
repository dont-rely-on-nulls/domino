(** Branch registry: named pointers to database tip hashes, plus HEAD tracking.
    Stored in the Physical backend under reserved key prefixes:
    - "branch:<name>" → serialized { name; tip }
    - "branch_names"  → serialized string list (index for enumeration)
    - "head"          → branch name as raw bytes *)

type t = { name : string; tip : Conventions.Hash.t }
type head = string
type error = string

let branch_key name = "branch:" ^ name
let head_key = "head"
let names_key = "branch_names"

module Make (Storage : Physical.S with type error = string) = struct
  let load_names storage =
    match Storage.load_raw storage names_key with
    | Ok (Some bytes) -> (Marshal.from_bytes bytes 0 : string list)
    | _ -> []

  let store_names storage names =
    let bytes = Marshal.to_bytes names [] in
    ignore (Storage.store_raw storage names_key bytes)

  let create storage ~name ~tip =
    let record = { name; tip } in
    let bytes = Marshal.to_bytes record [] in
    match Storage.store_raw storage (branch_key name) bytes with
    | Error e -> Error e
    | Ok () ->
        let names = load_names storage in
        if not (List.mem name names) then store_names storage (name :: names);
        Ok ()

  let checkout storage branch_name =
    let bytes = Bytes.of_string branch_name in
    Storage.store_raw storage head_key bytes

  let get_head storage =
    match Storage.load_raw storage head_key with
    | Ok (Some bytes) -> Ok (Some (Bytes.to_string bytes))
    | Ok None -> Ok None
    | Error e -> Error e

  let get_tip storage name =
    match Storage.load_raw storage (branch_key name) with
    | Ok (Some bytes) ->
        let record : t = Marshal.from_bytes bytes 0 in
        Ok (Some record.tip)
    | Ok None -> Ok None
    | Error e -> Error e

  let update_tip storage ~name ~tip =
    match Storage.load_raw storage (branch_key name) with
    | Ok (Some _) ->
        let record = { name; tip } in
        let bytes = Marshal.to_bytes record [] in
        Storage.store_raw storage (branch_key name) bytes
    | Ok None -> Error ("Branch not found: " ^ name)
    | Error e -> Error e

  let list storage =
    let names = load_names storage in
    let branches =
      List.filter_map
        (fun name ->
          match get_tip storage name with
          | Ok (Some tip) -> Some (name, tip)
          | _ -> None)
        names
    in
    Ok branches

  let branch_relation storage : Relation.t =
    let schema =
      Schema.empty |> Schema.add "name" "string" |> Schema.add "hash" "string"
    in
    let mk v = { Attribute.value = Obj.repr v } in
    let rec generator position =
      match position with
      | None -> Generator.Error "random access not supported for public:branch"
      | Some pos -> (
          match list storage with
          | Error _ -> Generator.Done
          | Ok branches ->
              if pos >= List.length branches then Generator.Done
              else
                let name, tip = List.nth branches pos in
                let short = String.sub tip 0 (min 8 (String.length tip)) in
                let attributes =
                  Tuple.AttributeMap.of_list
                    [ ("name", mk name); ("hash", mk short) ]
                in
                Generator.Value
                  ( Tuple.make_materialized
                      ~relation:Prelude.Catalog.branch_rel_name ~attributes,
                    generator ))
    in
    Relation.make ~hash:None ~name:Prelude.Catalog.branch_rel_name ~schema
      ~tree:None ~constraints:None
      ~cardinality:Conventions.Cardinality.ConstrainedFinite
      ~generator:(Some generator)
      ~membership_criteria:(fun _ _ -> true)
      ~provenance:(Relation.Provenance.Base Prelude.Catalog.branch_rel_name)
      ~lineage:(Relation.Lineage.Base Prelude.Catalog.branch_rel_name)

  let head_relation storage : Relation.t =
    let schema = Schema.empty |> Schema.add "branch" "string" in
    let mk v = { Attribute.value = Obj.repr v } in
    let generator position =
      match position with
      | None -> Generator.Error "random access not supported for public:head"
      | Some 0 -> (
          match get_head storage with
          | Ok None | Error _ -> Generator.Done
          | Ok (Some branch_name) ->
              let attributes =
                Tuple.AttributeMap.of_list [ ("branch", mk branch_name) ]
              in
              Generator.Value
                ( Tuple.make_materialized
                    ~relation:Prelude.Catalog.head_rel_name ~attributes,
                  fun _ -> Generator.Done ))
      | Some _ -> Generator.Done
    in
    Relation.make ~hash:None ~name:Prelude.Catalog.head_rel_name ~schema
      ~tree:None ~constraints:None
      ~cardinality:Conventions.Cardinality.ConstrainedFinite
      ~generator:(Some generator)
      ~membership_criteria:(fun _ _ -> true)
      ~provenance:(Relation.Provenance.Base Prelude.Catalog.head_rel_name)
      ~lineage:(Relation.Lineage.Base Prelude.Catalog.head_rel_name)
end
