module Tree = Merkle
(** Use the Merkle module for tuple hash storage *)

module RelationConstraint = struct
  type name = string
  type t = (name * Constraint.t) list
end

module Provenance = struct
  type t =
    | Undefined
    | Base of Conventions.Name.t
    | Join of t * t
    | Select of t * (Tuple.t -> bool)
    | Project of t * string list
    | Take of t * int
end

module Lineage = struct
  type t =
    | Base of Conventions.Name.t
    | Select of (string -> bool) * t
    | Project of string list * t
    | Join of string * t * t
    | ThetaJoin of (string -> string -> bool) * t * t
    | Sort of (string -> string -> int) * t
    | Take of int * t
    | Aggregate of string * string * t
end

type producer = (string * Conventions.AbstractValue.t) list -> Generator.t

type t = {
  hash : Conventions.Hash.t option;
  name : Conventions.Name.t;
  schema : Schema.t;
  tree : Tree.t option;
  constraints : RelationConstraint.t option;
  cardinality : Conventions.Cardinality.t;
  generator : Generator.t option;
  producer : producer option;
  membership_criteria : (string -> Tree.t option) -> Tuple.t -> bool;
  provenance : Provenance.t;
  lineage : Lineage.t;
}

let make ?(producer = None) ~hash ~name ~schema ~tree ~constraints ~cardinality ~generator ~membership_criteria ~provenance ~lineage () =
  {
    hash;
    name;
    schema;
    tree;
    constraints;
    cardinality;
    generator;
    producer;
    membership_criteria;
    provenance;
    lineage;
  }
