module Make (NT : Nt.S) = struct
  module Exec = Executor.Make (NT)

  type configuration = unit
  type ast = Ast.statement
  type error = Exec.error

  let name = "vcl"
  let parse _ = Ok ()

  let parse_sexp = function
    | Sexplib.Sexp.(List [ Atom "use"; Atom branch ]) -> Ok (Ast.Use branch)
    | sexp ->
        Error (Exec.ParseError ("unrecognized vcl command: " ^ Sexplib.Sexp.to_string sexp))

  let execute = Exec.execute

  let sexp_of_error = Exec.sexp_of_error
end
