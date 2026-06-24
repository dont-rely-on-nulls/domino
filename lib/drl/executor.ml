module Make (NT : Nt.S) = struct
  module Error = struct
    open Condition

    let parse_error error = condition "parse-error" error empty

    let relation_not_found name =
      condition "relation-not-found" "Relation not found" ("name" |=| of_string name)

    let unsupported_operator msg = condition "unsupported-operator" msg empty
  end

  let ( let* ) = Result.bind

  (* Compile a DRL AST query to a Tarski VM plan node tree.
     A qualified base reference ([<mg>:<rel>]) resolves through [ctx.resolve]
     to its stored-relation path; an unqualified name resolves to the
     session-scoped ephemeral binding created by DML [Define]. *)
  let rec compile (ctx : Sublanguage_context.t) (q : Ast.query) : (Nt.plan_node, Condition.t) result
      =
    match q with
    | Ast.Base name -> (
      match Qualified_name.try_parse name with
      | Error _ -> Ok (Nt.Scan {path= Sublanguage_context.ephemeral_path ctx name; args= []})
      | Ok fqn -> Ok (Nt.Scan {path= ctx.resolve fqn; args= []}) )
    | Ast.Join (on_attrs, q1, q2) ->
        let* p1 = compile ctx q1 in
        let* p2 = compile ctx q2 in
        Ok (Nt.Join {left= p1; right= p2; on_attrs})
    | Ast.Take (n, q) ->
        let* p = compile ctx q in
        Ok (Nt.Take {limit= n; source= p})
    | Ast.Project (attrs, q) ->
        let* p = compile ctx q in
        Ok (Nt.Project {attrs; source= p})
    | Ast.Const _ ->
        Error
          (Error.unsupported_operator "Const: literal relations not yet reachable by VM; use Base")
    | _ ->
        Error
          (Error.unsupported_operator
             "Select/Rename/Union/Diff/Cartesian require VM operators not yet implemented" )

  let page_limit = 16

  let execute (ctx : Sublanguage_context.t) (q : Ast.query) :
      (Sublanguage_types.result, Condition.t) result =
    let* plan = compile ctx q in
    let rel_name = "query_result" in
    let* stream = NT.execute_query plan ~rel_name in
    let rec drain acc count =
      if count >= page_limit then List.rev acc, true
      else
        match NT.stream_next stream with
        | Error _ -> List.rev acc, false
        | Ok None -> List.rev acc, false
        | Ok (Some t) -> drain (t :: acc) (count + 1)
    in
    let rows, has_more = drain [] 0 in
    ignore (NT.stream_close stream);
    Ok (Sublanguage_types.Cursor {cursor_id= "0"; rows; has_more})
end
