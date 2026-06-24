module Error = struct
  open Condition

  let parse_error msg = condition "parse-error" msg empty
end

let of_sexp sexp =
  match Ast.query_of_sexp sexp with
  | q -> Ok q
  | exception exn -> Error (Error.parse_error (Printexc.to_string exn))

let of_string s =
  match Sexplib.Sexp.of_string s |> Ast.query_of_sexp with
  | q -> Ok q
  | exception exn -> Error (Error.parse_error (Printexc.to_string exn))

let to_string q = Ast.sexp_of_query q |> Sexplib.Sexp.to_string_hum
