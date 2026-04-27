module Make (Storage : Management.Physical.S) = struct
  module Ops = Manipulation.Make (Storage)
  module Alg = Algebra.Make (Storage)
  module DrlExec = Drl.Executor.Make (Storage)

  type error =
    | ParseError of string
    | ManipulationError of Error.t
    | RelationNotFound of string
    | AlgebraError of Algebra.error

  let sexp_of_error e =
    let open Sexplib.Sexp in
    match e with
    | ParseError s                            -> List [Atom "parse-error";        Atom s]
    | ManipulationError e                     -> Error.sexp_of_error e
    | RelationNotFound s                      -> List [Atom "relation-not-found"; Atom s]
    | AlgebraError (Algebra.StorageError s)   -> List [Atom "storage-error";      Atom s]
    | AlgebraError (Algebra.GeneratorError s) -> List [Atom "generator-error";    Atom s]

  let ( let* ) = Result.bind
  let wrap_manip r = Result.map_error (fun e -> ManipulationError e) r
  let wrap_alg e = AlgebraError e

  let get_rel db name =
    match Ops.get_relation db ~name with
    | Some r -> Ok r
    | None -> Error (RelationNotFound name)

  let retarget target (t : Tuple.materialized) =
    { t with Tuple.relation = target }

  (** Build a materialized tuple from a list of (attr_name, ast_value) pairs *)
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

  let eval_query storage db query =
    match DrlExec.execute storage db query with
    | Ok rel -> Ok rel
    | Error (DrlExec.ParseError s) -> Error (ParseError s)
    | Error (DrlExec.RelationNotFound s) -> Error (RelationNotFound s)
    | Error (DrlExec.AlgebraError e) -> Error (AlgebraError e)

  let materialize_tuples storage rel =
    Result.map_error wrap_alg (Alg.materialize storage rel)

  let execute (storage : Storage.t) (db : Management.Database.t)
      (stmt : Ast.statement) : (Management.Database.t, error) result =
    match stmt with
    | Ast.InsertTuple { relation; attributes } ->
        let* rel = get_rel db relation in
        let tuple = build_tuple ~relation:rel.Relation.name attributes in
        let* db, _, _ = Ops.create_tuple storage db rel tuple |> wrap_manip in
        Ok db
    | Ast.InsertTuples { relation; tuples } ->
        let* rel = get_rel db relation in
        let tuple_list =
          List.map (build_tuple ~relation:rel.Relation.name) tuples
        in
        let* db, _, _ =
          Ops.create_tuples storage db rel tuple_list |> wrap_manip
        in
        Ok db
    | Ast.DeleteTuple { relation; attributes } ->
        let* rel = get_rel db relation in
        let tuple = build_tuple ~relation:rel.Relation.name attributes in
        let tuple_hash = Hashing.hash_tuple tuple in
        let* db, _ =
          Ops.retract_tuple storage db rel ~tuple_hash |> wrap_manip
        in
        Ok db
    | Ast.Assign { target; body } ->
        let* rel = get_rel db target in
        let* result_rel = eval_query storage db body in
        let* tuples = materialize_tuples storage result_rel in
        let* db, rel = Ops.clear_relation storage db rel |> wrap_manip in
        let* db, _, _ =
          Ops.create_tuples storage db rel
            (List.map (retarget rel.Relation.name) tuples)
          |> wrap_manip
        in
        Ok db
    | Ast.InsertFrom { target; source } ->
        let* rel = get_rel db target in
        let* result_rel = eval_query storage db source in
        let* tuples = materialize_tuples storage result_rel in
        let* db, _, _ =
          Ops.create_tuples storage db rel
            (List.map (retarget rel.Relation.name) tuples)
          |> wrap_manip
        in
        Ok db
    | Ast.DeleteWhere { target; predicate } ->
        let* rel = get_rel db target in
        let* pred_rel = eval_query storage db predicate in
        let common =
          List.filter_map
            (fun (n, _) ->
              if List.exists (fun (m, _) -> m = n) pred_rel.Relation.schema then
                Some n
              else None)
            rel.Relation.schema
        in
        let* joined =
          Alg.equijoin storage common rel pred_rel |> Result.map_error wrap_alg
        in
        let* to_delete =
          Alg.project storage (List.map fst rel.Relation.schema) joined
          |> Result.map_error wrap_alg
        in
        let* tuples = materialize_tuples storage to_delete in
        List.fold_left
          (fun acc t ->
            let* db = acc in
            let* rel = get_rel db target in
            let tuple_hash = Hashing.hash_tuple (retarget rel.Relation.name t) in
            let* db, _ =
              Ops.retract_tuple storage db rel ~tuple_hash |> wrap_manip
            in
            Ok db)
          (Ok db) tuples
end

module Memory = Make (Management.Physical.Memory)
