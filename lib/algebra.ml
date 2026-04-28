type error = StorageError of string | GeneratorError of string

module Make (Storage : Management.Physical.S) = struct
  module Ops = Manipulation.Make (Storage)

  type storage = Storage.t

  (* Internal generator utilities *)

  let rec list_generator : Tuple.t list -> Generator.t = function
    | [] -> fun _pos -> Generator.Done
    | t :: rest -> fun _pos -> Generator.Value (t, list_generator rest)

  let drain gen =
    let rec go gen pos acc =
      match gen (Some pos) with
      | Generator.Done -> Ok (List.rev acc)
      | Generator.Error e -> Error (GeneratorError e)
      | Generator.Value (t, next) -> go next (pos + 1) (t :: acc)
    in
    go gen 0 []

  let to_generator (storage : storage) (rel : Relation.t) : Generator.t =
    match rel.Relation.generator with
    | Some gen -> gen
    | None -> (
        (* TODO: Avoid materializing all tuple hashes/tuples at once.
         Current implementation calls tuple_hashes rel which loads all hashes
         into memory, then load_tuples which loads all tuples. This defeats
         lazy evaluation and scales badly (fails for billions of tuples).
         Replace with paginated tuple_hashes or Merkle streaming to load
         hashes/tuples on demand in chunks. *)
        let hashes = Ops.tuple_hashes rel in
        match Ops.load_tuples storage hashes with
        | Error _ ->
            fun _pos -> Generator.Error ("Failed to load: " ^ rel.Relation.name)
        | Ok ts -> list_generator (List.map (fun t -> Tuple.Materialized t) ts))

  let of_generator ~name ~schema ?constraints ?cardinality gen =
    let cardinality =
      match cardinality with
      | Some c -> c
      | None -> Conventions.Cardinality.AlephZero
    in
    Relation.make ~hash:None ~name ~schema ~tree:None ~constraints ~cardinality
      ~generator:(Some gen)
      ~membership_criteria:(fun _ _ -> true)
      ~provenance:Relation.Provenance.Undefined
      ~lineage:(Relation.Lineage.Base "derived") ()

  let const_relation (pairs : (string * Conventions.AbstractValue.t) list) :
      Relation.t =
    let attrs =
      List.fold_left
        (fun acc (k, v) -> Tuple.AttributeMap.add k { Attribute.value = v } acc)
        Tuple.AttributeMap.empty pairs
    in
    let tuple =
      Tuple.Materialized { Tuple.relation = "const"; attributes = attrs }
    in
    let schema = List.map (fun (k, _) -> (k, "abstract")) pairs in
    of_generator ~name:"const" ~schema (list_generator [ tuple ])

  (* Operators *)

  let select_fn storage predicate rel =
    let gen = to_generator storage rel in
    let lazy_gen =
      let rec go g pos =
        match g pos with
        | Generator.Done -> Generator.Done
        | Generator.Error e -> Generator.Error e
        | Generator.Value (t, next) ->
            if predicate t then Generator.Value (t, go next)
            else go next (Option.map (( + ) 1) pos)
      in
      fun pos -> go gen pos
    in
    Ok
      (of_generator
         ~name:("σ_" ^ rel.Relation.name)
         ~schema:rel.Relation.schema ?constraints:rel.Relation.constraints
         lazy_gen)

  let project storage (attrs : string list) rel =
    let gen = to_generator storage rel in
    let project_tuple = function
      | Tuple.Materialized t ->
          let attrs' =
            List.fold_left
              (fun acc k ->
                match Tuple.AttributeMap.find_opt k t.Tuple.attributes with
                | None -> acc
                | Some v -> Tuple.AttributeMap.add k v acc)
              Tuple.AttributeMap.empty attrs
          in
          Tuple.Materialized { t with attributes = attrs' }
      | other -> other
    in
    let new_schema =
      List.filter (fun (n, _) -> List.mem n attrs) rel.Relation.schema
    in
    let lazy_gen =
      let rec go g pos =
        match g pos with
        | Generator.Done -> Generator.Done
        | Generator.Error e -> Generator.Error e
        | Generator.Value (t, next) -> Generator.Value (project_tuple t, go next)
      in
      fun pos -> go gen pos
    in
    let filtered_constraints =
      match rel.Relation.constraints with
      | None | Some [] -> None
      | Some cs -> (
          let kept =
            List.filter_map
              (fun (name, c) ->
                match Constraint.filter_by_attrs attrs c with
                | Some c' -> Some (name, c')
                | None -> None)
              cs
          in
          match kept with [] -> None | _ -> Some kept)
    in
    Ok
      (of_generator
         ~name:("π_" ^ rel.Relation.name)
         ~schema:new_schema ?constraints:filtered_constraints lazy_gen)

  let rename storage (renames : (string * string) list) rel =
    let rename_key k =
      match List.assoc_opt k renames with Some k' -> k' | None -> k
    in
    let rename_tuple = function
      | Tuple.Materialized t ->
          let attrs' =
            Tuple.AttributeMap.fold
              (fun k v acc -> Tuple.AttributeMap.add (rename_key k) v acc)
              t.Tuple.attributes Tuple.AttributeMap.empty
          in
          Tuple.Materialized { t with attributes = attrs' }
      | other -> other
    in
    let gen = to_generator storage rel in
    let new_schema =
      List.map (fun (n, d) -> (rename_key n, d)) rel.Relation.schema
    in
    let lazy_gen =
      let rec go g pos =
        match g pos with
        | Generator.Done -> Generator.Done
        | Generator.Error e -> Generator.Error e
        | Generator.Value (t, next) -> Generator.Value (rename_tuple t, go next)
      in
      fun pos -> go gen pos
    in
    let renamed_constraints =
      match rel.Relation.constraints with
      | None | Some [] -> None
      | Some cs ->
          Some
            (List.map
               (fun (name, c) -> (name, Constraint.rename_vars renames c))
               cs)
    in
    Ok
      (of_generator
         ~name:("ρ_" ^ rel.Relation.name)
         ~schema:new_schema ?constraints:renamed_constraints lazy_gen)

  let equijoin storage (attrs : string list) left right =
    let get_vals attr_list = function
      | Tuple.Materialized t ->
          List.map
            (fun a -> Tuple.AttributeMap.find_opt a t.Tuple.attributes)
            attr_list
      | _ -> List.map (fun _ -> None) attr_list
    in
    let merge lt rt =
      match (lt, rt) with
      | Tuple.Materialized l, Tuple.Materialized r ->
          Tuple.Materialized
            {
              l with
              attributes =
                Tuple.AttributeMap.union
                  (fun _ a _ -> Some a)
                  l.Tuple.attributes r.Tuple.attributes;
            }
      | _ -> lt
    in
    let merged_schema =
      left.Relation.schema
      @ List.filter
          (fun (n, _) ->
            (not (List.mem n attrs))
            && not (List.exists (fun (m, _) -> m = n) left.Relation.schema))
          right.Relation.schema
    in
    let merged_constraints =
      match (left.Relation.constraints, right.Relation.constraints) with
      | None, None -> None
      | Some cs, None | None, Some cs -> Some cs
      | Some cs1, Some cs2 -> Some (Constraint.merge cs1 cs2)
    in
    let name = "⋈_" ^ left.Relation.name ^ "_" ^ right.Relation.name in
    let rec chain_with_cont g cont pos =
      match g pos with
      | Generator.Done -> cont pos
      | Generator.Value (t, next) ->
          Generator.Value (t, fun p -> chain_with_cont next cont p)
      | Generator.Error e -> Generator.Error e
    in
    let make_join_gen left_gen right_tuples =
      let rec from_left lgen lpos =
       fun pos ->
        match lgen (Some lpos) with
        | Generator.Done -> Generator.Done
        | Generator.Error e -> Generator.Error e
        | Generator.Value (lt, next_left) -> (
            let lv = get_vals attrs lt in
            let matches =
              List.filter_map
                (fun rt ->
                  if get_vals attrs rt = lv then Some (merge lt rt) else None)
                right_tuples
            in
            match matches with
            | [] -> from_left next_left (lpos + 1) pos
            | _ ->
                chain_with_cont (list_generator matches)
                  (from_left next_left (lpos + 1))
                  pos)
      in
      from_left left_gen 0
    in
    let bindings_of_tuple attrs_list = function
      | Tuple.Materialized m ->
          List.filter_map
            (fun attr ->
              match Tuple.AttributeMap.find_opt attr m.Tuple.attributes with
              | Some a -> Some (attr, a.Attribute.value)
              | None -> None)
            attrs_list
      | _ -> []
    in
    match right.Relation.producer with
    | Some right_producer when attrs <> [] ->
        (* Right is a function predicate: for each left tuple, call the producer
           with that tuple's join-attribute bindings to obtain matching right rows. *)
        let left_gen = to_generator storage left in
        let make_right_producer_join_gen lgen =
          let rec from_left lgen lpos =
           fun _pos ->
            match lgen (Some lpos) with
            | Generator.Done -> Generator.Done
            | Generator.Error e -> Generator.Error e
            | Generator.Value (lt, next_left) -> (
                let bindings = bindings_of_tuple attrs lt in
                match drain (right_producer bindings) with
                | Error (GeneratorError e | StorageError e) -> Generator.Error e
                | Ok right_tuples -> (
                    let lv = get_vals attrs lt in
                    let matches =
                      List.filter_map
                        (fun rt ->
                          if get_vals attrs rt = lv then Some (merge lt rt)
                          else None)
                        right_tuples
                    in
                    match matches with
                    | [] -> from_left next_left (lpos + 1) _pos
                    | _ ->
                        chain_with_cont (list_generator matches)
                          (from_left next_left (lpos + 1))
                          _pos))
          in
          from_left lgen 0
        in
        Ok
          (of_generator ~name ~schema:merged_schema
             ?constraints:merged_constraints
             (make_right_producer_join_gen left_gen))
    | _ -> (
        let right_gen = to_generator storage right in
        match drain right_gen with
        | Error e -> Error e
        | Ok right_tuples ->
            let left_gen_result =
              match left.Relation.producer with
              | Some left_producer when attrs <> [] ->
                  (* Left is a function predicate: collect all left tuples by
                     calling the producer once per unique binding set from right. *)
                  let unique_bindings =
                    right_tuples
                    |> List.filter_map (fun rt ->
                        let b = bindings_of_tuple attrs rt in
                        if List.length b = List.length attrs then Some b
                        else None)
                    |> List.sort_uniq compare
                  in
                  List.fold_left
                    (fun acc bindings ->
                      Result.bind acc (fun ts ->
                          match drain (left_producer bindings) with
                          | Ok new_ts -> Ok (ts @ new_ts)
                          | Error e -> Error e))
                    (Ok []) unique_bindings
                  |> Result.map list_generator
              | _ -> Ok (to_generator storage left)
            in
            Result.bind left_gen_result (fun left_gen ->
                Ok
                  (of_generator ~name ~schema:merged_schema
                     ?constraints:merged_constraints
                     (make_join_gen left_gen right_tuples))))

  let union storage rel1 rel2 =
    let gen1 = to_generator storage rel1 in
    let gen2 = to_generator storage rel2 in
    let chain_gen =
      let rec go g1 g2 pos =
        match g1 pos with
        | Generator.Done -> g2 pos
        | Generator.Error _ -> g2 pos
        | Generator.Value (t, next) -> Generator.Value (t, fun p -> go next g2 p)
      in
      fun pos -> go gen1 gen2 pos
    in
    let name = "∪_" ^ rel1.Relation.name ^ "_" ^ rel2.Relation.name in
    (* Conservative: drop constraints since they only hold if both inputs agree *)
    Ok (of_generator ~name ~schema:rel1.Relation.schema chain_gen)

  let attrs_equal (m1 : Attribute.materialized Tuple.AttributeMap.t)
      (m2 : Attribute.materialized Tuple.AttributeMap.t) =
    Tuple.AttributeMap.equal
      (fun a b -> Stdlib.( = ) a.Attribute.value b.Attribute.value)
      m1 m2

  let diff storage rel1 rel2 =
    let right_gen = to_generator storage rel2 in
    match drain right_gen with
    | Error e -> Error e
    | Ok right_tuples ->
        let right_mats =
          List.filter_map
            (function Tuple.Materialized t -> Some t | _ -> None)
            right_tuples
        in
        let left_gen = to_generator storage rel1 in
        let not_in_right = function
          | Tuple.Materialized t ->
              not
                (List.exists
                   (fun r -> attrs_equal t.Tuple.attributes r.Tuple.attributes)
                   right_mats)
          | _ -> true
        in
        let lazy_gen =
          let rec go g pos =
            match g pos with
            | Generator.Done -> Generator.Done
            | Generator.Error e -> Generator.Error e
            | Generator.Value (t, next) ->
                if not_in_right t then Generator.Value (t, go next)
                else go next (Option.map (( + ) 1) pos)
          in
          fun pos -> go left_gen pos
        in
        let name = "−_" ^ rel1.Relation.name ^ "_" ^ rel2.Relation.name in
        Ok
          (of_generator ~name ~schema:rel1.Relation.schema
             ?constraints:rel1.Relation.constraints lazy_gen)

  let take storage n rel =
    let gen = to_generator storage rel in
    let lazy_gen =
      let rec go g count pos =
        if count = 0 then Generator.Done
        else
          match g pos with
          | Generator.Done -> Generator.Done
          | Generator.Error e -> Generator.Error e
          | Generator.Value (t, next) -> Generator.Value (t, go next (count - 1))
      in
      fun pos -> go gen n pos
    in
    Ok
      (of_generator
         ~name:("τ_" ^ rel.Relation.name)
         ~schema:rel.Relation.schema ?constraints:rel.Relation.constraints
         ~cardinality:(Conventions.Cardinality.Finite n) lazy_gen)

  let materialize storage rel : (Tuple.materialized list, error) Result.t =
    match drain (to_generator storage rel) with
    | Error e -> Error e
    | Ok tuples ->
        Ok
          (List.filter_map
             (function Tuple.Materialized t -> Some t | _ -> None)
             tuples)
end

module Memory = Make (Management.Physical.Memory)
