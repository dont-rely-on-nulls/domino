module Make (NT : Nt.S) = struct
  module Exec = Executor.Make (NT)

  type configuration = unit
  type ast = Ast.statement
  type error = Exec.error

  let name = "dml"
  let parse _ = Ok ()

  let parse_sexp sexp =
    match Parser.of_sexp sexp with
    | Ok r -> Ok r
    | Error (Parser.ParseError s) -> Error (Exec.ParseError s)

  let execute ctx ast =
    match Exec.execute ctx ast with
    | Ok new_cache -> Ok (Sublanguage_types.Transition new_cache)
    | Error e -> Error e

  let sexp_of_error = Exec.sexp_of_error
end
