module Make (Storage : Management.Physical.S with type error = string) = struct
  module Exec = Executor.Make (Storage)

  type configuration = unit
  type storage = Storage.t
  type ast = Ast.statement
  type error = Exec.error

  let name = "dcl"
  let parse _ = Ok ()

  let parse_sexp sexp =
    match Parser.of_sexp sexp with
    | Ok r -> Ok r
    | Error (Parser.ParseError s) -> Error (Exec.ParseError s)

  let execute storage db ast =
    match Exec.execute storage db ast with
    | Ok (Exec.DbResult (db, msg)) -> Ok (Sublanguage_types.Transition db) 
    | Ok (Exec.Switch multigroup) -> Ok (Sublanguage_types.SessionSwitch multigroup)
    | Ok (Exec.NewMultigroup name) -> Ok (Sublanguage_types.CreateMultigroup name)
    | Error e -> Error e
    [@@warning "-27"]

  let sexp_of_error = Exec.sexp_of_error
end

module Memory = Make (Management.Physical.Memory)
