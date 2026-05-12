module Make (NT : Nt.S) = struct
  module Exec = Executor.Make (NT)

  type configuration = unit
  type ast = Ast.query
  type error = Exec.error

  let name = "drl"
  let parse _ = Ok ()

  let parse_sexp sexp =
    match Parser.of_sexp sexp with
    | Ok r -> Ok r
    | Error (Parser.ParseError s) -> Error (Exec.ParseError s)

  let execute bh db ast =
    match Exec.execute bh db ast with
    | Ok rel -> Ok (Sublanguage_types.Query (rel :> Relation.relation))
    | Error e -> Error e

  let sexp_of_error = Exec.sexp_of_error
end
