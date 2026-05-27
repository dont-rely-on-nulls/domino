module Make (NT : Nt.S) = struct
  module Exec = Executor.Make (NT)

  type configuration = unit
  type ast = Ast.statement

  let name = "dml"
  let parse _ = Ok ()

  let parse_sexp sexp = Parser.of_sexp sexp

  let execute ctx ast =
    match Exec.execute ctx ast with
    | Ok delta -> Ok (Sublanguage_types.Transition delta)
    | Error e -> Error e
end
