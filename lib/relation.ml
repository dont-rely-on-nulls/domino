(** Relation: a lightweight versioned root describing a named collection of
    tuples. Physical storage owns the tuple B+ tree; this object holds only
    metadata and a tree pointer.

    Class hierarchy:
    - [relation]           — base, storable, has membership_criteria/provenance/lineage
    - [ephemeral_relation] — adds a generator (query-time computed, not stored)
    - [pseudo_relation]    — adds a producer (system-defined, not stored)
    - [domain]             — adds a generator (system or user defined, not necessarily stored) *)

module RelationConstraint = struct
  type name = string
  type t = (name * Constraint.t) list
end

module Provenance = struct
  (** Tracks which base relation each attribute originates from.
      Lives on the relation. *)
  type source = {
    relation : Conventions.Name.t;
    attribute : string;
  }

  type t = source list BatMap.String.t
  (** Maps attribute name -> list of sources (multiple after joins) *)

  let base ~relation ~attributes : t =
    List.fold_left
      (fun acc attr ->
        BatMap.String.add attr [{ relation; attribute = attr }] acc)
      BatMap.String.empty attributes

  let empty : t = BatMap.String.empty
end

module Lineage = struct
  (** Tracks which relational algebra operations produced a tuple.
      Lives on the tuple, not the relation. *)
  type t =
    | Base of Conventions.Name.t
    | Select of t
    | Project of Conventions.Name.t list * t
    | Join of t * t
    | ThetaJoin of t * t
    | Sort of t
    | Take of t
end

type producer = (string * Conventions.AbstractValue.t) list -> Generator.t

class relation ~name:(init_name : Conventions.Name.t) ~schema:(init_schema : Schema.t)
  ~constraints:(init_constraints : RelationConstraint.t option)
  ~cardinality:(init_cardinality : Conventions.Cardinality.t)
  ~membership_criteria:(init_membership : (string -> Bplustree.String.t option) -> Tuple.t -> bool)
  ~provenance:(init_provenance : Provenance.t)
  =
  let init_hash =
    Conventions.Hash.hash_text
      (init_name ^ "|" ^ Schema.to_string init_schema ^ "|empty")
  in
  object (self)
    val name : Conventions.Name.t = init_name
    val hash : Conventions.Hash.t = init_hash
    val schema : Schema.t = init_schema
    val tree_pointer : Conventions.Hash.t option = None
    val constraints : RelationConstraint.t option = init_constraints
    val cardinality : Conventions.Cardinality.t = init_cardinality
    val membership_criteria = init_membership
    val provenance : Provenance.t = init_provenance
    val timestamp : float = Unix.gettimeofday ()

    method name = name
    method hash = hash
    method schema = schema
    method tree_pointer = tree_pointer
    method constraints = constraints
    method cardinality = cardinality
    method membership_criteria = membership_criteria
    method provenance = provenance
    method timestamp = timestamp

    method private compute_hash tp =
      let tree_hash = match tp with Some h -> h | None -> "empty" in
      Conventions.Hash.hash_text
        (name ^ "|" ^ Schema.to_string schema ^ "|" ^ tree_hash)

    method private advance tp =
      {< hash = self#compute_hash tp;
         tree_pointer = tp;
         timestamp = Unix.gettimeofday () >}

    method set_tree_pointer tp = self#advance tp
    method set_cardinality c = {< cardinality = c >}
    method set_constraints c = {< constraints = c >}
  end

class ephemeral_relation ~name ~schema ~constraints ~cardinality
  ~membership_criteria ~provenance
  ~generator:(init_generator : Generator.t) =
  object
    inherit relation ~name ~schema ~constraints ~cardinality
      ~membership_criteria ~provenance

    val generator : Generator.t = init_generator

    method generator = generator
  end

class pseudo_relation ~name ~schema ~constraints ~cardinality
  ~membership_criteria ~provenance
  ~producer:(init_producer : producer) =
  object
    inherit relation ~name ~schema ~constraints ~cardinality
      ~membership_criteria ~provenance

    val producer : producer = init_producer

    method producer = producer
  end

class domain ~name ~schema ~constraints ~cardinality
  ~membership_criteria ~provenance
  ~generator:(init_generator : Generator.t)
  ~compare:(init_compare : Conventions.AbstractValue.t -> Conventions.AbstractValue.t -> int) =
  object
    inherit ephemeral_relation ~name ~schema ~constraints ~cardinality
      ~membership_criteria ~provenance ~generator:init_generator

    val compare_fn = init_compare

    method compare = compare_fn
  end
