type error = ParseError of string

let of_sexp sexp =
  let sexp =
    match sexp with Sexplib.Sexp.List [ atom ] -> atom | other -> other
  in
  match Ast.statement_of_sexp sexp with
  | stmt -> Ok stmt
  | exception exn -> Error (ParseError (Printexc.to_string exn))

let of_string s = of_sexp (Sexplib.Sexp.of_string s)
let to_string stmt = Ast.sexp_of_statement stmt |> Sexplib.Sexp.to_string_hum
