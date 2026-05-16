module Make (NT : Nt.S) = struct
  module DrlExec = Drl.Executor.Make (NT)

  type error =
    | ParseError of string
    | NtError of Nt.error
    | RelationNotFound of string
    | DrlError of DrlExec.error

  let sexp_of_error e =
    let open Sexplib.Sexp in
    match e with
    | ParseError s -> List [ Atom "parse-error"; Atom s ]
    | NtError e -> List [ Atom "nt-error"; Atom (Nt.string_of_error e) ]
    | RelationNotFound s -> List [ Atom "relation-not-found"; Atom s ]
    | DrlError e -> List [ Atom "drl-error"; DrlExec.sexp_of_error e ]

  let ( let* ) = Result.bind
  let wrap_nt r = Result.map_error (fun e -> NtError e) r

  let get_rel db name =
    match NT.get_relation db name with
    | Some r -> Ok r
    | None -> Error (RelationNotFound name)

  let retarget target (t : Tuple.materialized) =
    { t with Tuple.relation = target }

  let build_tuple ~relation (attributes : Ast.attr_value list) : Tuple.materialized =
    let attr_map =
      List.fold_left
        (fun acc (name, value) ->
          Tuple.AttributeMap.add name
            { Attribute.value = Drl.Ast.value_to_abstract value }
            acc)
        Tuple.AttributeMap.empty attributes
    in
    { Tuple.relation; attributes = attr_map }

  (* Compile a DRL query and drain ALL tuples via the VM.
     Used internally by mutation operations that need to inspect query results. *)
  let drain_query db query =
    match DrlExec.compile (DrlExec.make_resolver db#name) query with
    | Error e -> Error (DrlError e)
    | Ok plan ->
        let* stream =
          NT.execute_query plan ~rel_name:"dml_drain"
          |> wrap_nt
        in
        let rec drain acc =
          match NT.stream_next stream with
          | Error _ -> List.rev acc
          | Ok None -> List.rev acc
          | Ok (Some t) -> drain (t :: acc)
        in
        let tuples = drain [] in
        ignore (NT.stream_close stream);
        Ok tuples

  (* In-memory semijoin: return target tuples whose common-attribute values
     match at least one predicate tuple.  Used by DeleteWhere until the VM
     has a native filter/equijoin operator. *)
  let attr_val_eq a b =
    Stdlib.( = ) a.Attribute.value b.Attribute.value

  let tuple_matches_pred common (target_t : Tuple.materialized)
      (pred_t : Tuple.materialized) =
    List.for_all (fun attr ->
      match
        ( Tuple.AttributeMap.find_opt attr target_t.Tuple.attributes,
          Tuple.AttributeMap.find_opt attr pred_t.Tuple.attributes )
      with
      | Some a, Some b -> attr_val_eq a b
      | _ -> false)
      common

  let semijoin common target_tuples pred_tuples =
    List.filter
      (fun t -> List.exists (tuple_matches_pred common t) pred_tuples)
      target_tuples

  let execute (bh : Nt.branch_handle) (db : Management.Multigroup.multigroup)
      (stmt : Ast.statement) :
      (Nt.branch_handle * Management.Multigroup.multigroup, error) result =
    match stmt with
    | Ast.InsertTuple { relation; attributes } ->
        let* rel = get_rel db relation in
        let tuple = build_tuple ~relation:rel#name attributes in
        let* _ =
          NT.create_tuple ~branch_name:db#name ~rel_name:rel#name tuple
          |> wrap_nt
        in
        Ok (bh, db)
    | Ast.InsertTuples { relation; tuples } ->
        let* rel = get_rel db relation in
        let tuple_list = List.map (build_tuple ~relation:rel#name) tuples in
        let* _ =
          NT.create_tuples ~branch_name:db#name ~rel_name:rel#name tuple_list
          |> wrap_nt
        in
        Ok (bh, db)
    | Ast.DeleteTuple { relation; attributes } ->
        let* rel = get_rel db relation in
        let tuple = build_tuple ~relation:rel#name attributes in
        let hash = Hashing.hash_tuple tuple in
        let* () =
          NT.retract_tuple ~branch_name:db#name ~rel_name:rel#name hash
          |> wrap_nt
        in
        Ok (bh, db)
    | Ast.Assign { target; body } ->
        let* rel = get_rel db target in
        let* tuples = drain_query db body in
        let* bh, db =
          NT.clear_relation bh db (rel :> Relation.relation) |> wrap_nt
        in
        let* _ =
          NT.create_tuples ~branch_name:db#name ~rel_name:rel#name
            (List.map (retarget rel#name) tuples)
          |> wrap_nt
        in
        Ok (bh, db)
    | Ast.InsertFrom { target; source } ->
        let* rel = get_rel db target in
        let* tuples = drain_query db source in
        let* _ =
          NT.create_tuples ~branch_name:db#name ~rel_name:rel#name
            (List.map (retarget rel#name) tuples)
          |> wrap_nt
        in
        Ok (bh, db)
    | Ast.DeleteWhere { target; predicate } ->
        let* rel = get_rel db target in
        let* target_tuples = drain_query db (Drl.Ast.Base rel#name) in
        let* pred_tuples   = drain_query db predicate in
        (* Derive common attribute names from the actual result sets. *)
        let attr_names_of = function
          | [] -> []
          | t :: _ ->
              List.map fst (Tuple.AttributeMap.bindings t.Tuple.attributes)
        in
        let target_attrs = attr_names_of target_tuples in
        let pred_attrs   = attr_names_of pred_tuples in
        let common = List.filter (fun n -> List.mem n pred_attrs) target_attrs in
        let to_delete = semijoin common target_tuples pred_tuples in
        List.fold_left
          (fun acc t ->
            let* bh, db = acc in
            let hash = Hashing.hash_tuple (retarget rel#name t) in
            let* () =
              NT.retract_tuple ~branch_name:db#name ~rel_name:rel#name hash
              |> wrap_nt
            in
            Ok (bh, db))
          (Ok (bh, db)) to_delete
end

module Memory = Make (Nt.Memory)
