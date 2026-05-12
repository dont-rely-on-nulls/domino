module Make (NT : Nt.S) = struct
  type error =
    | ParseError of string
    | RelationNotFound of string
    | NtError of Nt.error
    | UnsupportedOperator of string

  let sexp_of_error e =
    let open Sexplib.Sexp in
    match e with
    | ParseError s -> List [ Atom "parse-error"; Atom s ]
    | RelationNotFound s -> List [ Atom "relation-not-found"; Atom s ]
    | NtError e -> List [ Atom "nt-error"; Atom (Nt.string_of_error e) ]
    | UnsupportedOperator s -> List [ Atom "unsupported-operator"; Atom s ]

  let ( let* ) = Result.bind

  let relation_path (db : Management.Multigroup.multigroup) (name : string) =
    "/system/branches/" ^ db#name ^ "/relations/" ^ name

  (* Compile a DRL AST query to a Tarski VM plan node tree.
     Only Base (SCAN), Join (JOIN), and Take (TAKE) are supported for now. *)
  let rec compile (db : Management.Multigroup.multigroup) (q : Ast.query) :
      (Nt.plan_node, error) result =
    match q with
    | Ast.Base name ->
        Ok (Nt.Scan { path = relation_path db name; args = [] })
    | Ast.Join (on_attrs, q1, q2) ->
        let* p1 = compile db q1 in
        let* p2 = compile db q2 in
        Ok (Nt.Join { left = p1; right = p2; on_attrs })
    | Ast.Take (n, q) ->
        let* p = compile db q in
        Ok (Nt.Take { limit = n; source = p })
    | Ast.Const _ ->
        Error (UnsupportedOperator
          "Const: literal relations not yet reachable by VM; use Base")
    | _ ->
        Error (UnsupportedOperator
          "Select/Project/Rename/Union/Diff/Cartesian require VM operators not yet implemented")

  let page_limit = 16

  (* Execute a compiled plan: submit to VM, drain up to page_limit tuples,
     return a Cursor result.  The VM cursor is closed after draining. *)
  let execute (bh : Nt.branch_handle) (db : Management.Multigroup.multigroup)
      (q : Ast.query) : (Sublanguage_types.result, error) result =
    let* plan = compile db q in
    let rel_name = "query_result" in
    let* stream =
      NT.execute_query bh plan ~rel_name
      |> Result.map_error (fun e -> NtError e)
    in
    let rec drain acc count =
      if count >= page_limit then (List.rev acc, true)
      else
        match NT.stream_next stream with
        | Error _    -> (List.rev acc, false)
        | Ok None    -> (List.rev acc, false)
        | Ok (Some t) -> drain (t :: acc) (count + 1)
    in
    let (rows, has_more) = drain [] 0 in
    ignore (NT.stream_close stream);
    Ok (Sublanguage_types.Cursor { cursor_id = "0"; rows; has_more })
end
