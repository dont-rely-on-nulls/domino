module Make (NT : Nt.S) = struct
  module DrlExec = Drl.Executor.Make (NT)

  type error =
    | ParseError of string
    | NtError of Nt.error
    | RelationNotFound of string
    | AlgebraError of Algebra.error

  let sexp_of_error e =
    let open Sexplib.Sexp in
    match e with
    | ParseError s -> List [ Atom "parse-error"; Atom s ]
    | NtError e -> List [ Atom "nt-error"; Atom (Nt.string_of_error e) ]
    | RelationNotFound s -> List [ Atom "relation-not-found"; Atom s ]
    | AlgebraError (Algebra.StorageError s) ->
        List [ Atom "storage-error"; Atom s ]
    | AlgebraError (Algebra.GeneratorError s) ->
        List [ Atom "generator-error"; Atom s ]

  let ( let* ) = Result.bind
  let wrap_nt r = Result.map_error (fun e -> NtError e) r
  let wrap_alg e = AlgebraError e

  let get_rel db name =
    match NT.get_relation db name with
    | Some r -> Ok r
    | None -> Error (RelationNotFound name)

  let retarget target (t : Tuple.materialized) =
    { t with Tuple.relation = target }

  let build_tuple ~relation (attributes : Ast.attr_value list) :
      Tuple.materialized =
    let attr_map =
      List.fold_left
        (fun acc (name, value) ->
          Tuple.AttributeMap.add name
            { Attribute.value = Drl.Ast.value_to_abstract value }
            acc)
        Tuple.AttributeMap.empty attributes
    in
    { Tuple.relation; attributes = attr_map }

  let eval_query bh db query =
    match DrlExec.execute bh db query with
    | Ok rel -> Ok rel
    | Error (DrlExec.ParseError s) -> Error (ParseError s)
    | Error (DrlExec.RelationNotFound s) -> Error (RelationNotFound s)
    | Error (DrlExec.AlgebraError e) -> Error (AlgebraError e)

  let materialize_tuples rel =
    Result.map_error wrap_alg (Algebra.materialize rel)

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
        let* result_rel = eval_query bh db body in
        let* tuples = materialize_tuples result_rel in
        let* bh, db = NT.clear_relation bh db (rel :> Relation.relation) |> wrap_nt in
        let* _ =
          NT.create_tuples ~branch_name:db#name ~rel_name:rel#name
            (List.map (retarget rel#name) tuples)
          |> wrap_nt
        in
        Ok (bh, db)
    | Ast.InsertFrom { target; source } ->
        let* rel = get_rel db target in
        let* result_rel = eval_query bh db source in
        let* tuples = materialize_tuples result_rel in
        let* _ =
          NT.create_tuples ~branch_name:db#name ~rel_name:rel#name
            (List.map (retarget rel#name) tuples)
          |> wrap_nt
        in
        Ok (bh, db)
    | Ast.DeleteWhere { target; predicate } ->
        let* rel = get_rel db target in
        let* pred_rel = eval_query bh db predicate in
        let common =
          List.filter_map
            (fun (n, _) ->
              if List.exists (fun (m, _) -> m = n) pred_rel#schema then Some n
              else None)
            rel#schema
        in
        let* joined =
          Algebra.equijoin common rel pred_rel |> Result.map_error wrap_alg
        in
        let* to_delete =
          Algebra.project (List.map fst rel#schema) joined
          |> Result.map_error wrap_alg
        in
        let* tuples = materialize_tuples to_delete in
        List.fold_left
          (fun acc t ->
            let* bh, db = acc in
            let hash = Hashing.hash_tuple (retarget rel#name t) in
            let* () =
              NT.retract_tuple ~branch_name:db#name ~rel_name:rel#name hash
              |> wrap_nt
            in
            Ok (bh, db))
          (Ok (bh, db)) tuples
end

module Memory = Make (Nt.Memory)
