module Make (Storage : Management.Physical.S) = struct
  module Exec = Executor.Make (Storage)

  type configuration = unit
  type storage = Storage.t
  type ast = Ast.statement
  type error = Exec.error

  let name = "scl"
  let parse _ = Ok ()

  let parse_sexp sexp =
    match Parser.of_sexp sexp with
    | Ok r -> Ok r
    | Error (Parser.ParseError s) -> Error (Exec.ParseError s)

  let execute storage db ast =
    Exec.execute storage db ast
    |> Result.map (function
      | Executor.Batch { cursor_id; rows; has_more } ->
          Sublanguage_types.Cursor { cursor_id; rows; has_more }
      | Executor.Closed db -> Sublanguage_types.Transition db)

  let sexp_of_error = Exec.sexp_of_error
end

module Memory = Make (Management.Physical.Memory)
