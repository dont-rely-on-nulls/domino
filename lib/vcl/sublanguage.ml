module Make (NT : Nt.S) = struct
  module Error = struct
    open Condition

    let unrecognized_command expr =
      condition "unrecognized-command" "Unrecognized command" ("expression" |=| of_sexp expr)
  end

  module Exec = Executor.Make (NT)

  type configuration = unit

  type ast = Ast.statement

  let name = "vcl"

  let parse _ = Ok ()

  let parse_sexp = function
    | Sexplib.Sexp.(List [Atom "use"; Atom branch]) ->
        Ok (Ast.Use branch)
    | sexp ->
        Error (Error.unrecognized_command sexp)

  let execute = Exec.execute
end
