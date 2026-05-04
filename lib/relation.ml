(** Relation: a lightweight versioned root describing a named collection of
    tuples. Physical storage owns the tuple B+ tree; this object holds only
    metadata and a tree pointer.

    Class hierarchy:
    - [relation] — base, storable, has membership_criteria/provenance/lineage
    - [ephemeral_relation] — adds a generator (query-time computed, not stored)
    - [pseudo_relation] — adds a producer (system-defined, not stored)
    - [domain] — adds a generator (system or user defined, not necessarily
      stored) *)

module RelationConstraint = struct
  type name = string
  type t = (name * Constraint.t) list
end

module Lineage = struct
  (** Tracks which relational algebra operations produced the tuples. Lives in
      the relation, but refers to the tuple. *)
  type t =
    | Base of Conventions.Name.t
    | Select of t
    | Project of Conventions.Name.t list * t
    | Join of t * t
    | ThetaJoin of t * t
    | Sort of t
    | Take of t
end

module Provenance = struct
  type source = { relation : Conventions.Name.t; attribute : string }
  (** Tracks which base relation each attribute originates from. Lives on the
      relation and refers to the `entity group`. *)

  type t = source list BatMap.String.t
  (** Maps attribute name -> list of sources (multiple after joins) *)

  let base ~relation ~attributes : t =
    List.fold_left
      (fun acc attr ->
        BatMap.String.add attr [ { relation; attribute = attr } ] acc)
      BatMap.String.empty attributes

  let empty : t = BatMap.String.empty
end

type producer = (string * Conventions.AbstractValue.t) list -> Generator.t

class relation ~name:(init_name : Conventions.Name.t)
  ~schema:(init_schema : Schema.t)
  ~constraints:(init_constraints : RelationConstraint.t option)
  ~cardinality:(init_cardinality : Conventions.Cardinality.t)
  ~membership_criteria:
    (init_membership : Tuple.t -> bool)
  ~lineage:(init_lineage : Lineage.t option)
  ~provenance:(init_provenance : Provenance.t option) =
  (* TODO: On initialization, we assume that no tree is present.
     As this might not be the case in the future, consider adding a check in callers. *)
  let init_hash =
    Conventions.Hash.hash_text (init_name ^ Schema.to_string init_schema)
  in
  object (self)
    val name : Conventions.Name.t = init_name
    val hash : Conventions.Hash.t = init_hash
    val schema : Schema.t = init_schema

    (* TODO: Maybe remove these from the relation class and push it downwards
       to another "stored" relation class, treating this as abstract. *)
    val tree_pointer : Conventions.Hash.t option = None
    val constraints : RelationConstraint.t option = init_constraints
    val cardinality : Conventions.Cardinality.t = init_cardinality
    val membership_criteria = init_membership
    val timestamp : float = Unix.gettimeofday ()

    val lineage : Lineage.t =
      Option.value init_lineage ~default:(Lineage.Base init_name)

    val provenance : Provenance.t =
      Option.value init_provenance
        ~default:
          (Provenance.base ~relation:init_name
             ~attributes:(Schema.attributes init_schema))

    method name = name
    method hash = hash
    method schema = schema
    method tree_pointer = tree_pointer
    method constraints = constraints
    method cardinality = cardinality
    method membership_criteria = membership_criteria
    method timestamp = timestamp
    method lineage = lineage
    method provenance = provenance

    method private compute_hash tp =
      let tree_hash = match tp with Some h -> h | None -> String.empty in
      Conventions.Hash.hash_text (name ^ Schema.to_string schema ^ tree_hash)

    method private advance tp =
      {<hash = self#compute_hash tp
       ; tree_pointer = tp
       ; timestamp = Unix.gettimeofday ()>}

    method set_tree_pointer tp = self#advance tp
    method set_cardinality c = {<cardinality = c>}
    method set_constraints c = {<constraints = c>}
  end

class ephemeral_relation ~name ~schema ~constraints ~cardinality
  ~membership_criteria ~lineage ~provenance
  ~generator:(init_generator : Generator.t) =
  object
    inherit
      relation
        ~name ~schema ~constraints ~cardinality ~membership_criteria ~lineage
          ~provenance

    val generator : Generator.t = init_generator
    method generator = generator
  end

(* Relations that are in fact procedural underneath. Used for linked alien calls, planned for the VM. *)
class pseudo_relation ~name ~schema ~constraints ~cardinality
  ~membership_criteria ~lineage ~provenance ~producer:(init_producer : producer)
  =
  object
    inherit
      relation
        ~name ~schema ~constraints ~cardinality ~membership_criteria ~lineage
          ~provenance

    val producer : producer = init_producer
    method producer = producer
  end

class domain ~name ~schema ~constraints ~cardinality ~membership_criteria
  ~lineage ~provenance ~generator:(init_generator : Generator.t) =
  object
    inherit
      ephemeral_relation
        ~name ~schema ~constraints ~cardinality ~membership_criteria ~lineage
          ~provenance ~generator:init_generator
  end
