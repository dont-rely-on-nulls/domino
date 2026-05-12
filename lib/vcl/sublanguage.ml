module Make (NT : Nt.S) = struct
  type configuration = unit
  type ast = Ast.statement
  type error = ParseError of string

  let name = "vcl"
  let parse _ = Ok ()

  let parse_sexp = function
    | Sexplib.Sexp.(List [ Atom "use"; Atom branch ]) -> Ok (Ast.Use branch)
    | sexp ->
        Error (ParseError ("unrecognized vcl command: " ^ Sexplib.Sexp.to_string sexp))

  let execute _bh _db (Ast.Use branch_name) =
    Ok (Sublanguage_types.SessionSwitch branch_name)

  let sexp_of_error (ParseError s) =
    Sexplib.Sexp.(List [ Atom "vcl-error"; Atom s ])
end
