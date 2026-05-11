let default_batch = 50

type cursor_batch = {
  cursor_id : string;
  rows : Tuple.materialized list;
  has_more : bool;
}

type exec_result = Batch of cursor_batch | Closed of Management.Multigroup.multigroup

module Make (Storage : Management.Physical.S) = struct
  module DrlExec = Drl.Executor.Make (Storage)
  module Alg = Algebra.Make (Storage)

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

  let execute (_storage : Storage.t) (db : Management.Multigroup.multigroup)
      (stmt : Ast.statement) : (exec_result, error) result =
    ignore default_batch;
    match stmt with
    | Ast.Begin _ ->
        Error (CursorError "cursors not yet implemented")
    | Ast.Fetch _ ->
        Error (CursorError "cursors not yet implemented")
    | Ast.Close _ ->
        let* () = Ok () in
        Ok (Closed db)
end

module Memory = Make (Management.Physical.Memory)
