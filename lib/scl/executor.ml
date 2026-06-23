let default_batch = 50

type cursor_batch = {cursor_id: string; rows: Tuple.materialized list; has_more: bool}

type exec_result = Batch of cursor_batch | Closed of Sublanguage_types.transition_delta

module Make (NT : Nt.S) = struct
  module DrlExec = Drl.Executor.Make (NT)

  module Error = struct
    open Condition

    (* TODO: more structure *)
    let cursor_error msg = condition "cursor-error" msg empty
  end

  let ( let* ) = Result.bind

  let execute (ctx : Sublanguage_context.t) (stmt : Ast.statement) :
      (exec_result, Condition.t) result =
    ignore default_batch ;
    match stmt with
    | Ast.Begin _ ->
        Error (Error.cursor_error "cursors not yet implemented")
    | Ast.Fetch _ ->
        Error (Error.cursor_error "cursors not yet implemented")
    | Ast.Close _ ->
        let* () = Ok () in
        Ok (Closed ctx.branch#multigroups)
end

module Memory = Make (Nt.Memory)
