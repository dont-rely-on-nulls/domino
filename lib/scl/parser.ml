module Error = struct
  open Condition

  let parse_error msg = condition "parse-error" msg empty
end

let validate = function
  | (Ast.Begin {limit= Some limit; _} | Ast.Fetch {limit= Some limit; _}) when limit <= 0 ->
      Error (Error.parse_error "The provision of `limit` is expected to be positive.")
  | stmt -> Ok stmt

let of_sexp sexp =
  match Ast.statement_of_sexp sexp with
  | exception exn -> Error (Error.parse_error (Printexc.to_string exn))
  | stmt -> validate stmt

let of_string s =
  match Sexplib.Sexp.of_string s |> Ast.statement_of_sexp with
  | exception exn -> Error (Error.parse_error (Printexc.to_string exn))
  | stmt -> validate stmt

let to_string stmt = Ast.sexp_of_statement stmt |> Sexplib.Sexp.to_string_hum
