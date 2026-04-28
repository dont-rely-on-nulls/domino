open Sexplib.Std

type function_predicate = {
  name : string;
  schema : (string * string) list;
  symbol : string;
  purity : Conventions.Purity.t;
  cardinality : Conventions.Cardinality.t;
}
[@@deriving sexp]

type statement =
  | LoadLibrary of string
  | DefineFunctionPredicate of function_predicate
  | ListFunctionPredicates
[@@deriving sexp]
