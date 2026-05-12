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

  type t =
    | Undefined
    | Base of Conventions.Name.t
    | Sources of source list BatMap.String.t
  (** Maps attribute name -> list of sources (multiple after joins) *)

  let base ~relation ~attributes : t =
    Sources
      (List.fold_left
         (fun acc attr ->
           BatMap.String.add attr [ { relation; attribute = attr } ] acc)
         BatMap.String.empty attributes)

  let empty : t = Sources BatMap.String.empty

  let undefined : t = Undefined
end

type producer = (string * Conventions.AbstractValue.t) list -> Generator.t

type kind = [ `Stored | `Ephemeral | `Pseudo | `Domain ]

class virtual relation = object (_self)
  method virtual kind : kind
  method virtual name : Conventions.Name.t
  method virtual schema : Schema.t
  method virtual hash : Conventions.Hash.t
  method virtual constraints : RelationConstraint.t option
  method virtual cardinality : Conventions.Cardinality.t
  method virtual lineage : Lineage.t
  method virtual provenance : Provenance.t
  method virtual membership_criteria : Tuple.t -> bool
  method virtual timestamp : float
  method virtual serialize : unit -> Storable.Relation.t
end

class stored
  ~name:(init_name : Conventions.Name.t)
  ~schema:(init_schema : Schema.t)
  ~constraints:(init_constraints : RelationConstraint.t option)
  ~cardinality:(init_cardinality : Conventions.Cardinality.t)
  ~membership_criteria:(init_membership : Tuple.t -> bool)
  ~lineage:(init_lineage : Lineage.t option)
  ~provenance:(init_provenance : Provenance.t option) =
  let init_hash =
    Conventions.Hash.hash_text (init_name ^ Schema.to_string init_schema)
  in
  object (self)
    inherit relation

    val name = init_name
    val hash = init_hash
    val schema = init_schema
    val tree_pointer : Conventions.Hash.t option = None
    val constraints = init_constraints
    val cardinality = init_cardinality
    val membership_criteria = init_membership
    val timestamp = Unix.gettimeofday ()
    val lineage = Option.value init_lineage ~default:(Lineage.Base init_name)
    val provenance = Option.value init_provenance
        ~default:(Provenance.base ~relation:init_name ~attributes:(Schema.attributes init_schema))

    method name = name
    method kind = `Stored
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
      let tree_hash = match tp with Some h -> h | None -> "" in
      Conventions.Hash.hash_text (name ^ Schema.to_string schema ^ tree_hash)

    method private advance tp =
      {< hash = self#compute_hash tp;
         tree_pointer = tp;
         timestamp = Unix.gettimeofday () >}

    method set_tree_pointer tp = self#advance tp
    method set_cardinality c = {< cardinality = c >}
    method set_constraints c = {< constraints = c >}
    method serialize () =
      { Storable.Relation.name;
        schema;
        tree_pointer = Option.value tree_pointer ~default:"" }
end

class ephemeral 
  ~name:(init_name : Conventions.Name.t)
  ~schema 
  ~constraints
  ~cardinality ~membership_criteria 
  ~lineage 
  ~provenance
  ~generator:(init_generator : Generator.t) =
  object
    inherit relation
    val name : Conventions.Name.t = init_name
    val schema : Schema.t = schema
    val generator : Generator.t = init_generator
    val constraints = constraints
    val cardinality = cardinality
    val membership_criteria = membership_criteria
    val lineage = Option.value lineage ~default:(Lineage.Base init_name)
    val provenance = Option.value provenance ~default:(Provenance.Base init_name)
    val timestamp = Unix.gettimeofday ()

    method name = name
    method kind = `Ephemeral
    method schema = schema
    method generator = generator
    method hash = Conventions.Hash.hash_text (name ^ "ephemeral")
    method constraints = constraints
    method cardinality = cardinality
    method membership_criteria = membership_criteria
    method lineage = lineage
    method provenance = provenance
    method timestamp = timestamp

    method serialize () = failwith "Ephemeral relations cannot be serialized directly"
end

class pseudo
  ~name:(init_name : Conventions.Name.t) ~schema ~constraints ~cardinality ~membership_criteria ~lineage ~provenance 
  ~producer:(init_producer : producer) =
  object
    inherit relation
    val name = init_name
    val schema = schema
    val producer = init_producer
    val constraints = constraints
    val cardinality = cardinality
    val membership_criteria = membership_criteria
    val lineage = Option.value lineage ~default:(Lineage.Base init_name)
    val provenance = provenance
    val timestamp = Unix.gettimeofday ()

    method name = name
    method kind = `Pseudo
    method schema = schema
    method producer = producer
    method as_generator (bindings : (string * Conventions.AbstractValue.t) list) : Generator.t = producer bindings
    method hash = Conventions.Hash.hash_text (name ^ "pseudo")
    method constraints = constraints
    method cardinality = cardinality
    method membership_criteria = membership_criteria
    method lineage = lineage
    method provenance = provenance
    method timestamp = timestamp

    (* TODO: Remove this from the virtual definition *)
    method serialize () = failwith "Pseudo relations are procedural"
end

let deserialize_stored (_bytes : bytes) : (stored, string) result =
  try
    let stored = Storable.Relation.of_bytes _bytes in
    let membership_criteria =
      Constraint.build_membership_criteria ~_name:stored.name ~schema:stored.schema
    in
    Ok
      (new stored ~name:stored.name ~schema:stored.schema ~constraints:None
         ~cardinality:Conventions.Cardinality.ConstrainedFinite
         ~membership_criteria ~lineage:None ~provenance:None)
  with Invalid_argument message -> Error message

class domain ~name ~schema ~constraints ~cardinality ~membership_criteria
  ~lineage ~provenance ~generator =
  object
    inherit ephemeral
        ~name ~schema ~constraints ~cardinality 
        ~membership_criteria ~lineage
        ~provenance ~generator

    method! kind = `Domain
  end
