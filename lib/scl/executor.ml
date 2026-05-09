let default_batch = 50

type cursor_batch = {
  cursor_id : string;
  rows : Tuple.materialized list;
  has_more : bool;
}

type exec_result = Batch of cursor_batch | Closed of Management.Database.t

let sessions : Session.t option ref = ref None
let set_sessions s = sessions := Some s

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

  let get_sessions () =
    match !sessions with
    | Some s -> Ok s
    | None ->
        Error
          (CursorError
             "The session registry has not been initialised; ensure \
              `set_sessions` is called at startup.")

  let ( let* ) = Result.bind

  let execute (storage : Storage.t) (db : Management.Database.t)
      (stmt : Ast.statement) : (exec_result, error) result =
    match stmt with
    | Ast.Begin { query; limit } ->
        let* reg = get_sessions () in
        let* rel =
          DrlExec.execute storage db query
          |> Result.map_error (fun e -> QueryError e)
        in
        let gen = Alg.to_generator storage rel in
        let query_str = Drl.Parser.to_string query in
        let cur = Session.register reg ~db ~query:query_str ~generator:gen in
        let batch = Option.value ~default:default_batch limit in
        let* rows, has_more =
          Session.fetch reg ~id:cur.Session.id ~limit:batch
          |> Result.map_error (fun s -> CursorError s)
        in
        Ok (Batch { cursor_id = cur.Session.id; rows; has_more })
    | Ast.Fetch { cursor; limit } ->
        let* reg = get_sessions () in
        let batch = Option.value ~default:default_batch limit in
        let* rows, has_more =
          Session.fetch reg ~id:cursor ~limit:batch
          |> Result.map_error (fun s -> CursorError s)
        in
        Ok (Batch { cursor_id = cursor; rows; has_more })
    | Ast.Close { cursor } ->
        let* reg = get_sessions () in
        Session.close reg ~id:cursor;
        Ok (Closed db)
end
