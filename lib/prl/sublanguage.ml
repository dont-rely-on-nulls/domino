module Make (Storage : Management.Physical.S) = struct
  module Exec = Executor.Make (Storage)

  type configuration = unit
  type storage = Storage.t
  type ast = Ast.statement
  type error = Exec.error

  let name = "prl"
  let parse _ = Ok ()

  let parse_sexp sexp =
    match Parser.of_sexp sexp with
    | Ok r -> Ok r
    | Error (Parser.ParseError s) -> Error (Exec.ParseError s)

  let execute storage db ast =
    match Exec.execute storage db ast with
    | Ok (Relation rel) -> Ok (Sublanguage_types.Query rel)
    | Ok LibraryLoad -> Ok (Sublanguage_types.Transition db)
    | Error e -> Error e
    
  let sexp_of_error = Exec.sexp_of_error
end

module Memory = Make (Management.Physical.Memory)
