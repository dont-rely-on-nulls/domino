module Make (NT : Nt.S) = struct
  let execute (ctx : Sublanguage_context.t) (stmt : Ast.statement) :
      (Sublanguage_types.result, Condition.t) result =
    match stmt with
    | Ast.Use branch_name ->
        Result.map (fun delta -> Sublanguage_types.Transition delta) (ctx.switch_branch branch_name)

  let _ = (module NT : Nt.S)
end
