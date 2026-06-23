open Sexplib.Std

type cardinality_spec = Finite of int | AlephZero | Continuum | ConstrainedFinite
[@@deriving sexp]

type statement =
  | CreateMultigroup of string
  | CreateRelation of {name: string; schema: (string * string) list}
  | RetractRelation of string
  | ClearRelation of string
  | RegisterDomain of {name: string; cardinality: cardinality_spec}
[@@deriving sexp]
