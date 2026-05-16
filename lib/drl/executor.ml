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

  (* Resolver for a single branch: [None] → default branch, [Some b] → explicit. *)
  let make_resolver (default_branch : string) (branch_override : string option)
      (rel_name : string) : string =
    let branch = Option.value branch_override ~default:default_branch in
    "/system/branches/" ^ branch ^ "/relations/" ^ rel_name

  (* Compile a DRL AST query to a Tarski VM plan node tree.
     [resolve] maps (branch_override, rel_name) → full RNT path, enabling
     cross-multigroup reads without scope restriction. *)
  let rec compile
      (resolve : string option -> string -> string)
      (q : Ast.query) : (Nt.plan_node, error) result =
    match q with
    | Ast.Base name ->
        Ok (Nt.Scan { path = resolve None name; args = [] })
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

  let execute (ctx : Sublanguage_context.t) (q : Ast.query) :
      (Sublanguage_types.result, error) result =
    let* plan = compile ctx.resolve q in
    let rel_name = "query_result" in
    let* stream =
      NT.execute_query plan ~rel_name
      |> Result.map_error (fun e -> NtError e)
    in
    let rec drain acc count =
      if count >= page_limit then (List.rev acc, true)
      else
        match NT.stream_next stream with
        | Error _     -> (List.rev acc, false)
        | Ok None     -> (List.rev acc, false)
        | Ok (Some t) -> drain (t :: acc) (count + 1)
    in
    let (rows, has_more) = drain [] 0 in
    ignore (NT.stream_close stream);
    Ok (Sublanguage_types.Cursor { cursor_id = "0"; rows; has_more })
end
