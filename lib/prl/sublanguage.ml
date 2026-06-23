module Make (NT : Nt.S) = struct
  module Exec = Executor.Make (NT)

  type configuration = unit

  type ast = Ast.statement

  let name = "prl"

  let parse _ = Ok ()

  let parse_sexp sexp = Parser.of_sexp sexp

  let execute = Exec.execute
end
