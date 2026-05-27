module Make (NT : Nt.S) = struct
  module Exec = Executor.Make (NT)

  type configuration = unit
  type ast = Ast.query

  let name = "drl"
  let parse _ = Ok ()

  let parse_sexp sexp = Parser.of_sexp sexp

  let execute ctx ast = Exec.execute ctx ast
end
