(** Database manipulation operations.

    This module implements the core operations for the relational engine:
    - Database creation
    - Relation creation and deletion
    - Tuple insertion and deletion
    - Query operations

    All operations are immutable: they return new database/relation states
    rather than modifying existing ones. The storage layer handles persistence.

    This is a functor that takes a storage backend, ensuring all tuple data is
    properly persisted to the content-addressed store. *)

(** Build membership criteria from schema (validates type membership). Returns a
    function that takes a tree-lookup function and a tuple, checking schema
    conformance and tree membership at call time. *)
let build_membership_criteria 
  ~_name
  ~schema :
    (*(string -> Merkle.t option) ->*) Tuple.t -> bool =
  let value_conforms_to_domain domain_name (value : Conventions.AbstractValue.t)
      =
    match domain_name with
    | "integer" -> Obj.is_int value
    | "natural" -> Obj.is_int value && (Obj.obj value : int) >= 0
    | "string" -> Obj.tag value = Obj.string_tag
    | _ ->
        (* Unknown domain: could be a relation-typed attribute (nested tuple)
           or a user-defined domain. Accept for now; full validation would
           require looking up the target relation's schema. *)
        true
  in
  let n_expected = List.length schema in
  (* fun tree_of tuple -> *)
  fun tuple ->
    match tuple with
    | Tuple.NonMaterialized _nm ->
        (* begin match tree_of name with
        | None -> false
        | Some t ->
            Merkle.member nm.hash t
            && Tuple.AttributeMap.cardinal nm.attributes = n_expected
            && List.for_all
                 (fun (attr_name, _) ->
                   Tuple.AttributeMap.mem attr_name nm.attributes)
                 schema
        end *)
        false
    | Tuple.Materialized m ->
        Tuple.AttributeMap.cardinal m.attributes = n_expected
        && List.for_all
             (fun (attr_name, domain_name) ->
               match Tuple.AttributeMap.find_opt attr_name m.attributes with
               | None -> false
               | Some attr ->
                   value_conforms_to_domain domain_name attr.Attribute.value)
             schema

(* let tree_of_db (db : Management.Database.t) name = *)
  (* Option.bind (Management.Database.get_relation db name) (fun r -> *)
      (* r.Relation.tree) *)

module Constraint = struct
  include Constraint

  (** Mutation direction for cascade constraint checking *)
  type mutation = Insert | Delete

  type mutation_context = {
    target_relation : string;
    transition : (string * Conventions.AbstractValue.t) list;
    kind : mutation;
  }
  (** Context for a single tuple mutation, used by cascade checking *)
end

(** Functor to create manipulation operations with a storage backend *)
module Make (Storage : Management.Physical.S) = struct
  type storage = Storage.t
  type error = Error.t

  let of_string_error s = Error.StorageError s
  let ( let* ) = Result.bind
  let normalize_name n = Qualified_name.(parse n |> to_key)

  let get_relation _db _name = failwith "NOT IMPLEMENTED"
  let create_database _storage _name = failwith "NOT IMPLEMENTED"
  let create_relation _storage _db _name _schema = failwith "NOT IMPLEMENTED"
  let create_tuple _storage _db _ll_rel _tuple = failwith "NOT IMPLEMENTED"
  let [@warning "-27"] register_constraint storage db ~constraint_name ~relation_name ~body = failwith "NOT IMPLEMENTED"
  let [@warning "-27"] create_tuples storage db rel tuples = failwith "NOT IMPLEMENTED"
  let [@warning "-27"] create_immutable_relation storage db ~name ~schema ~generator
                                ~membership_criteria 
                                ~cardinality
                                ~producer = failwith "NOT IMPLEMENTED"
  let retract_relation _storage _db _name = failwith "NOT IMPLEMENTED"
  let retract_tuple _storage _db _mg_rel _tuple_hash = failwith "NOT IMPLEMENTED"
  let clear_relation _storage _db _rel = failwith "NOT IMPLEMENTED"
  let register_domain _storage _db _domain = failwith "NOT IMPLEMENTED"
  (* let tuple_hashes _rel = failwith "NOT IMPLEMENTED" *)
  (* let load_tuples _storage _hashes = failwith "NOT IMPLEMENTED" *)
  let tuple_hashes = failwith "NOT IMPLEMENTED"
  let load_tuples _storage _rel_hash = failwith "NOT IMPLEMENTED"

  let rec normalize_constraint_body c =
    match c with
    | Constraint.MemberOf { target; binding } ->
        Constraint.MemberOf { target = normalize_name target; binding }
    | Constraint.Not { body; universe } ->
        Constraint.Not
          {
            body = normalize_constraint_body body;
            universe = normalize_name universe;
          }
    | Constraint.And cs ->
        Constraint.And (List.map normalize_constraint_body cs)
    | Constraint.Or cs -> Constraint.Or (List.map normalize_constraint_body cs)
    | Constraint.Exists { variable; quantifier; body } ->
        Constraint.Exists
          {
            variable;
            quantifier = normalize_name quantifier;
            body = normalize_constraint_body body;
          }
    | Constraint.Forall { variable; quantifier; body } ->
        Constraint.Forall
          {
            variable;
            quantifier = normalize_name quantifier;
            body = normalize_constraint_body body;
          }
    | Constraint.Eq _ as eq -> eq

  let fold_result (f : 'a -> 'b -> ('a, error) Result.t) (init : 'a)
      (xs : 'b list) : ('a, error) Result.t =
    List.fold_left
      (fun acc x ->
        let* a = acc in
        f a x)
      (Ok init) xs

  (* State Persistence - Store relation and database states *)

  (** Store a relation state to storage *)
  let store_relation (storage : storage) (relation : Relation.relation) :
      (unit, error) Result.t =
      match
        Storage.store_raw storage relation#hash (Storable.Relation.to_bytes @@ relation#serialize ())
      with
      | Error _ -> Error (Error.StorageError "Failed to store relation")
      | Ok () -> Ok ()

  (** Store a database state to storage *)
  let store_database (storage : storage) (db : Management.Database.database) :
      (unit, error) Result.t =
      (* Extract relation hashes from the actual relations *)
      match
        Storage.store_raw storage db#hash (Storable.Database.to_bytes @@ db#serialize ())
      with
      | Error _ -> Error (Error.StorageError "Failed to store database")
      | Ok () -> Ok ()

  (** Load a relation state from storage by hash *)
  let load_relation (storage : storage) (rel_hash : Conventions.Hash.t) : (Relation.stored option, error) Result.t =
    match Storage.load_raw storage rel_hash with
    | Error _ -> Error (Error.StorageError "Failed to load relation")
    | Ok None -> Ok None
    | Ok (Some bytes) ->
        let _stored = Storable.Relation.of_bytes bytes in
        let name = failwith "NOT IMPLEMENTED" in
        let schema = failwith "NOT IMPLEMENTED" in
        let constraints = failwith "NOT IMPLEMENTED" in
        let cardinality = failwith "NOT IMPLEMENTED" in
        let membership_criteria = build_membership_criteria ~_name:name ~schema in
        let relation =
          new Relation.stored
            ~name
            ~schema
            ~constraints
            ~cardinality
            ~membership_criteria
            ~provenance:None
            ~lineage:None
        in
        Ok (Some relation)

  (** Load a database state from storage by hash. Note: This loads the database
      structure with relation hashes, but the relations themselves need to be
      loaded separately. *)
  let load_database (storage : storage) (db_hash : Conventions.Hash.t) :
      (Management.Database.database option, error) Result.t =
    match Storage.load_raw storage db_hash with
    | Error _ -> Error (Error.StorageError "Failed to load database")
    | Ok None -> Ok None
    | Ok (Some bytes) ->
        (* TODO: Properly load stuff here and modify the database init method to add the relation hash and etc *)
        let _stored = Storable.Database.of_bytes bytes in
        let name = failwith "NOT IMPLEMENTED" in
        let database = new Management.Database.database ~name in
        Ok (Some database)

  (* Tuple Operations - Storage Integrated *)

  (** Store each attribute value and return map of attr_name -> attr_hash *)
  let store_attributes (_storage : storage) (_tuple : Tuple.materialized) :
      ((string * Conventions.Hash.t) list, error) Result.t = failwith "NOT IMPLEMENTED"

  (** Load a tuple from storage by its hash *)
  let load_tuple (storage : storage) (tuple_hash : Conventions.Hash.t) :
      (Tuple.materialized option, error) Result.t =
    match Storage.load_raw storage tuple_hash with
    | Error _ -> Error (Error.StorageError "Failed to load tuple")
    | Ok None -> Ok None
    | Ok (Some tuple_bytes) ->
        let _stored = Storable.Tuple.of_bytes tuple_bytes in
        let attributes = failwith "NOT IMPLEMENTED" in
        let relation = failwith "NOT IMPLEMENTED" in
        let rec load_attrs acc = function
          | [] -> Ok (List.rev acc)
          | (name, attr_hash) :: rest -> (
              match Storage.load_raw storage attr_hash with
              | Error _ ->
                  Error
                    (Error.StorageError ("Failed to load attribute: " ^ name))
              | Ok None ->
                  Error (Error.StorageError ("Attribute not found: " ^ name))
              | Ok (Some value_bytes) ->
                  let value : Conventions.AbstractValue.t =
                    Marshal.from_bytes value_bytes 0
                  in
                  let attr : Attribute.materialized = { value } in
                  load_attrs ((name, attr) :: acc) rest)
        in
        let* attrs = load_attrs [] attributes in
        let attributes =
          List.fold_left
            (fun m (k, v) -> Tuple.AttributeMap.add k v m)
            Tuple.AttributeMap.empty attrs
        in
        Ok (Some { Tuple.relation = relation; attributes })

end
