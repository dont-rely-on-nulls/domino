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

  (* Constructs a resolver for a single branch: default to the given branch
     name, override with ~branch_name for cross-branch references. *)
  let make_resolver (default_branch : string)
      ?(branch_name = default_branch) (rel_name : string) : string =
    "/system/branches/" ^ branch_name ^ "/relations/" ^ rel_name

  (* Compile a DRL AST query to a Tarski VM plan node tree.
     [resolve] maps a relation name (optionally branch-qualified) to its
     full RNT path.  Pass the branch's [relation_path] method, or a custom
     resolver for cross-branch queries.
     Only Base (SCAN), Join (JOIN), and Take (TAKE) are supported for now. *)
  let rec compile
      (resolve : ?branch_name:string -> string -> string)
      (q : Ast.query) : (Nt.plan_node, error) result =
    match q with
    | Ast.Base name ->
        Ok (Nt.Scan { path = resolve name; args = [] })
    | Ast.Join (on_attrs, q1, q2) ->
        let* p1 = compile resolve q1 in
        let* p2 = compile resolve q2 in
        Ok (Nt.Join { left = p1; right = p2; on_attrs })
    | Ast.Take (n, q) ->
        let* p = compile resolve q in
        Ok (Nt.Take { limit = n; source = p })
    | Ast.Const _ ->
        Error (UnsupportedOperator
          "Const: literal relations not yet reachable by VM; use Base")
    | _ ->
        Error (UnsupportedOperator
          "Select/Project/Rename/Union/Diff/Cartesian require VM operators not yet implemented")

  let page_limit = 16

  (* Execute a compiled plan: submit to VM, drain up to page_limit tuples,
     return a Cursor result.  The VM cursor is closed after draining.

     [resolve] is the path resolver — pass [branch#relation_path] for
     single-branch queries, or a custom fn for cross-branch joins. *)
  let execute
      (resolve : ?branch_name:string -> string -> string)
      (q : Ast.query) : (Sublanguage_types.result, error) result =
    let* plan = compile resolve q in
    let rel_name = "query_result" in
    let* stream =
      NT.execute_query plan ~rel_name
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
