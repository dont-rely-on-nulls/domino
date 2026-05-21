module Make (NT : Nt.S) = struct
  module Exec = Executor.Make (NT)

  type configuration = unit
  type ast = Ast.statement

  let name = "scl"
  let parse _ = Ok ()

  let parse_sexp sexp = Parser.of_sexp sexp

  let execute ctx ast =
    Exec.execute ctx ast
    |> Result.map (function
         | Executor.Batch { cursor_id; rows; has_more } ->
             Sublanguage_types.Cursor { cursor_id; rows; has_more }
         | Executor.Closed delta -> Sublanguage_types.Transition delta)
end
