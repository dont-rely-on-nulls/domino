open Sexplib.Std

(** Literal values for Const nodes *)
type value = Int of int | Float of float | Str of string | Bool of bool [@@deriving sexp]

(** Convert an AST value to an AbstractValue.t (Obj.t) via Obj.repr *)
let value_to_abstract : value -> Conventions.AbstractValue.t = function
  | Int n -> Obj.repr n
  | Float f -> Obj.repr f
  | Str s -> Obj.repr s
  | Bool b -> Obj.repr b

type direction = Asc | Desc [@@deriving sexp]

type query =
  | Base of string  (** base relation by name *)
  | Const of (string * value) list  (** constant single-tuple relation *)
  | Select of query * query  (** σ semijoin: (filter, source) *)
  | Join of string list * query * query  (** ⋈ natural equijoin on named attrs *)
  | Cartesian of query * query  (** × Cartesian product *)
  | Project of string list * query  (** π restrict columns *)
  | Rename of (string * string) list * query  (** ρ rename (old,new) pairs *)
  | Union of query * query  (** ∪ — compatible schemas assumed *)
  | Diff of query * query  (** − *)
  | Take of int * query  (** τ *)
[@@deriving sexp]
