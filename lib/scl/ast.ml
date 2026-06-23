open Sexplib.Std

type statement =
  | Begin of {query: Drl.Ast.query; limit: int option}
  | Fetch of {cursor: string; limit: int option}
  | Close of {cursor: string}
[@@deriving sexp]
