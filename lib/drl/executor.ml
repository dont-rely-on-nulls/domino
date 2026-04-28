module Make (Storage : Management.Physical.S) = struct
  module Alg = Algebra.Make (Storage)
  module Ops = Manipulation.Make (Storage)

  type error =
    | ParseError of string
    | RelationNotFound of string
    | AlgebraError of Algebra.error

  let sexp_of_error e =
    let open Sexplib.Sexp in
    match e with
    | ParseError s -> List [ Atom "parse-error"; Atom s ]
    | RelationNotFound s -> List [ Atom "relation-not-found"; Atom s ]
    | AlgebraError (Algebra.StorageError s) ->
        List [ Atom "storage-error"; Atom s ]
    | AlgebraError (Algebra.GeneratorError s) ->
        List [ Atom "generator-error"; Atom s ]

  let wrap = Result.map_error (fun e -> AlgebraError e)
  let ast_value_to_abstract = Ast.value_to_abstract

  let select_semijoin storage source filter =
    let common =
      List.filter_map
        (fun (n, _) ->
          if List.exists (fun (m, _) -> m = n) filter.Relation.schema then
            Some n
          else None)
        source.Relation.schema
    in
    let source_attrs = List.map fst source.Relation.schema in
    Result.bind
      (wrap (Alg.equijoin storage common source filter))
      (fun joined -> wrap (Alg.project storage source_attrs joined))

  let rec execute (storage : Storage.t) (db : Management.Database.t)
      (q : Ast.query) : (Relation.t, error) Result.t =
    let ( >>= ) = Result.bind in
    match q with
    | Ast.Base name -> (
        match Ops.get_relation db ~name with
        | None -> Error (RelationNotFound name)
        | Some rel -> Ok rel)
    | Ast.Const pairs ->
        Ok
          (Alg.const_relation
             (List.map (fun (k, v) -> (k, ast_value_to_abstract v)) pairs))
    | Ast.Select (filter_q, source_q) ->
        execute storage db filter_q >>= fun filter ->
        execute storage db source_q >>= fun source ->
        select_semijoin storage source filter
    | Ast.Join (attrs, q1, q2) ->
        execute storage db q1 >>= fun r1 ->
        execute storage db q2 >>= fun r2 ->
        wrap (Alg.equijoin storage attrs r1 r2)
    | Ast.Project (attrs, q) ->
        execute storage db q >>= fun rel -> wrap (Alg.project storage attrs rel)
    | Ast.Rename (renames, q) ->
        execute storage db q >>= fun rel ->
        wrap (Alg.rename storage renames rel)
    | Ast.Cartesian (q1, q2) ->
        execute storage db q1 >>= fun r1 ->
        execute storage db q2 >>= fun r2 -> wrap (Alg.equijoin storage [] r1 r2)
    | Ast.Union (q1, q2) ->
        execute storage db q1 >>= fun r1 ->
        execute storage db q2 >>= fun r2 -> wrap (Alg.union storage r1 r2)
    | Ast.Diff (q1, q2) ->
        execute storage db q1 >>= fun r1 ->
        execute storage db q2 >>= fun r2 -> wrap (Alg.diff storage r1 r2)
    | Ast.Take (n, q) ->
        execute storage db q >>= fun rel -> wrap (Alg.take storage n rel)
end

module Memory = Make (Management.Physical.Memory)
