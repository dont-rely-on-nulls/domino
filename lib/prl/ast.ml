open Sexplib.Std

type purity = Pure | Io [@@deriving sexp]

type cardinality_spec =
  | Finite of int
  | AlephZero
  | Continuum
  | ConstrainedFinite
[@@deriving sexp]

type function_predicate = {
  name : string;
  schema : (string * string) list;
  symbol : string;
  purity : purity;
  cardinality : cardinality_spec;
}
[@@deriving sexp]

type statement =
  | LoadLibrary of string
  | DefineFunctionPredicate of function_predicate
  | RetractFunctionPredicate of string
  | ListFunctionPredicates
[@@deriving sexp]
