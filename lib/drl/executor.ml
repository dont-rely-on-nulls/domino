module Make (NT : Nt.S) = struct
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

  let select_semijoin source filter =
    let common =
      List.filter_map
        (fun (n, _) ->
          if List.exists (fun (m, _) -> m = n) filter#schema then Some n
          else None)
        source#schema
    in
    let source_attrs = List.map fst source#schema in
    Result.bind
      (wrap (Algebra.equijoin common source filter))
      (fun joined -> wrap (Algebra.project source_attrs joined))

  let rec execute (bh : Nt.branch_handle) (db : Management.Multigroup.multigroup)
      (q : Ast.query) : (Relation.ephemeral, error) Result.t =
    let ( >>= ) = Result.bind in
    ignore bh;
    match q with
    | Ast.Base name -> (
        match NT.get_relation db name with
        | None -> Error (RelationNotFound name)
        | Some rel -> Ok rel)
    | Ast.Const pairs ->
        Ok
          (Algebra.const_relation
             (List.map (fun (k, v) -> (k, ast_value_to_abstract v)) pairs))
    | Ast.Select (filter_q, source_q) ->
        execute bh db filter_q >>= fun filter ->
        execute bh db source_q >>= fun source ->
        select_semijoin source filter
    | Ast.Join (attrs, q1, q2) ->
        execute bh db q1 >>= fun r1 ->
        execute bh db q2 >>= fun r2 ->
        wrap (Algebra.equijoin attrs r1 r2)
    | Ast.Project (attrs, q) ->
        execute bh db q >>= fun rel -> wrap (Algebra.project attrs rel)
    | Ast.Rename (renames, q) ->
        execute bh db q >>= fun rel ->
        wrap (Algebra.rename renames rel)
    | Ast.Cartesian (q1, q2) ->
        execute bh db q1 >>= fun r1 ->
        execute bh db q2 >>= fun r2 -> wrap (Algebra.equijoin [] r1 r2)
    | Ast.Union (q1, q2) ->
        execute bh db q1 >>= fun r1 ->
        execute bh db q2 >>= fun r2 -> wrap (Algebra.union r1 r2)
    | Ast.Diff (q1, q2) ->
        execute bh db q1 >>= fun r1 ->
        execute bh db q2 >>= fun r2 -> wrap (Algebra.diff r1 r2)
    | Ast.Take (n, q) ->
        execute bh db q >>= fun rel -> wrap (Algebra.take n rel)
end
