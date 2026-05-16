let default_batch = 50

type cursor_batch = {
  cursor_id : string;
  rows : Tuple.materialized list;
  has_more : bool;
}

type exec_result = Batch of cursor_batch | Closed of Management.Multigroup.multigroup

module Make (NT : Nt.S) = struct
  module DrlExec = Drl.Executor.Make (NT)

  type error =
    | ParseError of string
    | QueryError of DrlExec.error
    | CursorError of string

  let sexp_of_error e =
    let open Sexplib.Sexp in
    match e with
    | ParseError s -> List [ Atom "parse-error"; Atom s ]
    | QueryError e -> DrlExec.sexp_of_error e
    | CursorError s -> List [ Atom "cursor-error"; Atom s ]

  let ( let* ) = Result.bind

  let execute (ctx : Sublanguage_context.t) (stmt : Ast.statement) :
      (exec_result, error) result =
    ignore default_batch;
    match stmt with
    | Ast.Begin _ ->
        Error (CursorError "cursors not yet implemented")
    | Ast.Fetch _ ->
        Error (CursorError "cursors not yet implemented")
    | Ast.Close _ ->
        let* () = Ok () in
        Ok (Closed ctx.schema_cache)
end

module Memory = Make (Nt.Memory)
