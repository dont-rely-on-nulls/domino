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

module RelationMap = Management.Database.RelationMap

(** Build membership criteria from schema (validates type membership). Returns a
    function that takes a tree-lookup function and a tuple, checking schema
    conformance and tree membership at call time. *)
let build_membership_criteria ~name schema :
    (string -> Merkle.t option) -> Tuple.t -> bool =
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
  fun tree_of tuple ->
    match tuple with
    | Tuple.NonMaterialized nm -> (
        match tree_of name with
        | None -> false
        | Some t ->
            Merkle.member nm.hash t
            && Tuple.AttributeMap.cardinal nm.attributes = n_expected
            && List.for_all
                 (fun (attr_name, _) ->
                   Tuple.AttributeMap.mem attr_name nm.attributes)
                 schema)
    | Tuple.Materialized m ->
        Tuple.AttributeMap.cardinal m.attributes = n_expected
        && List.for_all
             (fun (attr_name, domain_name) ->
               match Tuple.AttributeMap.find_opt attr_name m.attributes with
               | None -> false
               | Some attr ->
                   value_conforms_to_domain domain_name attr.Attribute.value)
             schema

let tree_of_db (db : Management.Database.t) name =
  Option.bind (Management.Database.get_relation db name) (fun r ->
      r.Relation.tree)

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
    | Constraint.And cs -> Constraint.And (List.map normalize_constraint_body cs)
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

  let fold_result (f : 'a -> 'b -> ('a, error) Result.t) (init : 'a)
      (xs : 'b list) : ('a, error) Result.t =
    List.fold_left
      (fun acc x ->
        let* a = acc in
        f a x)
      (Ok init) xs

  (** Build an eval_context closed over a specific (storage, db) snapshot. All
      relation lookups resolve against this exact database version. *)
  let build_eval_context (storage : storage) (db : Management.Database.t) :
      Constraint.eval_context =
    let load_value attr_hash =
      match Storage.load_raw storage attr_hash with
      | Error _ | Ok None -> None
      | Ok (Some vbytes) ->
          Some (Marshal.from_bytes vbytes 0 : Conventions.AbstractValue.t)
    in
    let rec load_attrs acc = function
      | [] -> Some (List.rev acc)
      | (name, attr_hash) :: rest -> (
          match load_value attr_hash with
          | None -> None
          | Some value -> load_attrs ((name, value) :: acc) rest)
    in
    let load_tuple hash =
      match Storage.load_raw storage hash with
      | Error _ | Ok None -> None
      | Ok (Some bytes) ->
          load_attrs []
            (Storable.Tuple.of_bytes bytes).Storable.Tuple.attributes
    in
    let rec load_all acc = function
      | [] -> Some (List.rev acc)
      | h :: rest -> (
          match load_tuple h with
          | None -> None
          | Some pairs -> load_all (pairs :: acc) rest)
    in
    let check_membership rel_name bound_pairs =
      match Management.Database.get_relation db rel_name with
      | None -> false
      | Some rel ->
          let attributes =
            List.fold_left
              (fun acc (k, v) ->
                Tuple.AttributeMap.add k { Attribute.value = v } acc)
              Tuple.AttributeMap.empty bound_pairs
          in
          let tuple =
            Tuple.Materialized { Tuple.relation = rel_name; attributes }
          in
          rel.membership_criteria (tree_of_db db) tuple
    in
    let iterate_finite rel_name =
      match Management.Database.get_relation db rel_name with
      | None -> None
      | Some rel -> (
          match rel.cardinality with
          | Conventions.Cardinality.Finite _
          | Conventions.Cardinality.ConstrainedFinite ->
              let hashes =
                match rel.tree with None -> [] | Some tree -> Merkle.keys tree
              in
              load_all [] hashes
          | _ -> None)
    in
    { Constraint.check_membership; iterate_finite }

  (* State Persistence - Store relation and database states *)

  (** Store a relation state to storage *)
  let store_relation (storage : storage) (relation : Relation.t) :
      (unit, error) Result.t =
    match relation.hash with
    | None -> Error (Error.StorageError "Relation has no hash")
    | Some rel_hash -> (
        let tree_keys =
          match relation.tree with None -> [] | Some tree -> Merkle.keys tree
        in
        let stored : Storable.Relation.t =
          {
            name = relation.name;
            schema = relation.schema;
            tree_keys;
            cardinality = relation.cardinality;
          }
        in
        match
          Storage.store_raw storage rel_hash (Storable.Relation.to_bytes stored)
        with
        | Error _ -> Error (Error.StorageError "Failed to store relation")
        | Ok () -> Ok ())

  (** Store a database state to storage *)
  let store_database (storage : storage) (db : Management.Database.t) :
      (unit, error) Result.t =
    if db.hash = "" then Ok () (* Don't store empty/initial database *)
    else
      (* Extract relation hashes from the actual relations *)
      let relation_hashes =
        Management.Database.RelationMap.fold
          (fun name rel acc ->
            match rel.Relation.hash with
            | Some h -> (name, h) :: acc
            | None -> acc)
          db.relations []
      in
      let stored : Storable.Database.t =
        {
          name = db.name;
          relations = relation_hashes;
          tree_keys = Merkle.keys db.tree;
          history = db.history;
          timestamp = db.timestamp;
        }
      in
      match
        Storage.store_raw storage db.hash (Storable.Database.to_bytes stored)
      with
      | Error _ -> Error (Error.StorageError "Failed to store database")
      | Ok () -> Ok ()

  (** Load a relation state from storage by hash *)
  let load_relation (storage : storage) (rel_hash : Conventions.Hash.t) :
      (Relation.t option, error) Result.t =
    match Storage.load_raw storage rel_hash with
    | Error _ -> Error (Error.StorageError "Failed to load relation")
    | Ok None -> Ok None
    | Ok (Some bytes) ->
        let stored = Storable.Relation.of_bytes bytes in
        let tree =
          List.fold_left
            (fun t h -> Merkle.insert h t)
            Merkle.empty stored.tree_keys
        in
        let relation =
          Relation.make ~hash:(Some rel_hash) ~name:stored.name
            ~schema:stored.schema ~tree:(Some tree) ~constraints:None
            ~cardinality:stored.cardinality ~generator:None
            ~membership_criteria:
              (build_membership_criteria ~name:stored.name stored.schema)
            ~provenance:(Relation.Provenance.Base stored.name)
            ~lineage:(Relation.Lineage.Base stored.name)
        in
        Ok (Some relation)

  (** Load a database state from storage by hash. Note: This loads the database
      structure with relation hashes, but the relations themselves need to be
      loaded separately. *)
  let load_database (storage : storage) (db_hash : Conventions.Hash.t) :
      (Management.Database.t option, error) Result.t =
    match Storage.load_raw storage db_hash with
    | Error _ -> Error (Error.StorageError "Failed to load database")
    | Ok None -> Ok None
    | Ok (Some bytes) ->
        let stored = Storable.Database.of_bytes bytes in
        let tree =
          List.fold_left
            (fun t h -> Merkle.insert h t)
            Merkle.empty stored.tree_keys
        in
        let rec load_relations acc = function
          | [] -> Ok (List.rev acc)
          | (name, rel_hash) :: rest -> (
              let* rel_opt = load_relation storage rel_hash in
              match rel_opt with
              | None ->
                  Error (Error.StorageError ("Relation not found: " ^ name))
              | Some rel -> load_relations ((name, rel) :: acc) rest)
        in
        let* relation_list = load_relations [] stored.relations in
        let relations =
          List.fold_left
            (fun m (name, rel) ->
              Management.Database.RelationMap.add name rel m)
            Management.Database.RelationMap.empty relation_list
        in
        Ok
          (Some
             {
               Management.Database.hash = db_hash;
               name = stored.name;
               tree;
               relations;
               domains = Management.Database.RelationMap.empty;
               history = stored.history;
               timestamp = stored.timestamp;
               deferred = [];
             })

  (*   Relation Operations - forward declarations needed by catalog helpers
      *)

  (** Create a new empty relation with the given schema *)
  let create_relation_raw (storage : storage) (db : Management.Database.t)
      ~(name : string) ~(schema : Schema.t) :
      (Management.Database.t * Relation.t, Error.t) Result.t =
    if Management.Database.has_relation db name then
      Error (Error.RelationAlreadyExists name)
    else
      let tree = Merkle.empty in
      let relation_hash = Hashing.hash_relation ~name ~schema ~tree in
      let relation =
        Relation.make ~hash:(Some relation_hash) ~name ~schema ~tree:(Some tree)
          ~constraints:None ~cardinality:(Conventions.Cardinality.Finite 0)
          ~generator:None
          ~membership_criteria:(build_membership_criteria ~name schema)
          ~provenance:(Relation.Provenance.Base name)
          ~lineage:(Relation.Lineage.Base name)
      in
      let new_db = Management.Database.add_relation db ~relation in
      let* () = store_relation storage relation in
      Ok (new_db, relation)

  (* Tuple Operations - Storage Integrated *)

  (** Store each attribute value and return map of attr_name -> attr_hash *)
  let store_attributes (storage : storage) (tuple : Tuple.materialized) :
      ((string * Conventions.Hash.t) list, error) Result.t =
    let attrs = Tuple.AttributeMap.bindings tuple.attributes in
    let rec store_all acc = function
      | [] -> Ok (List.rev acc)
      | (name, attr) :: rest -> (
          let value_bytes =
            Marshal.to_bytes attr.Attribute.value [ Marshal.Closures ]
          in
          let value_hash =
            Conventions.Hash.hash_text (Hashing.bytes_to_hex value_bytes)
          in
          match Storage.store_raw storage value_hash value_bytes with
          | Error _ ->
              Error (Error.StorageError ("Failed to store attribute: " ^ name))
          | Ok () -> store_all ((name, value_hash) :: acc) rest)
    in
    store_all [] attrs

  (** Load a tuple from storage by its hash *)
  let load_tuple (storage : storage) (tuple_hash : Conventions.Hash.t) :
      (Tuple.materialized option, error) Result.t =
    match Storage.load_raw storage tuple_hash with
    | Error _ -> Error (Error.StorageError "Failed to load tuple")
    | Ok None -> Ok None
    | Ok (Some tuple_bytes) ->
        let stored = Storable.Tuple.of_bytes tuple_bytes in
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
        let* attrs = load_attrs [] stored.attributes in
        let attributes =
          List.fold_left
            (fun m (k, v) -> Tuple.AttributeMap.add k v m)
            Tuple.AttributeMap.empty attrs
        in
        Ok (Some { Tuple.relation = stored.relation; attributes })

  (** Load multiple tuples from storage *)
  let load_tuples (storage : storage) (hashes : Conventions.Hash.t list) :
      (Tuple.materialized list, error) Result.t =
    let rec load_all acc = function
      | [] -> Ok (List.rev acc)
      | hash :: rest -> (
          let* tup_opt = load_tuple storage hash in
          match tup_opt with
          | None -> Error (Error.TupleNotFound hash)
          | Some tuple -> load_all (tuple :: acc) rest)
    in
    load_all [] hashes

  let with_loaded_tuple storage tuple_hash ~on_some =
    let* tup_opt = load_tuple storage tuple_hash in
    match tup_opt with
    | Some tup -> on_some tup
    | None ->
        Error
          (Error.StorageError
             ("Tuple missing for referenced hash: " ^ tuple_hash))

  let transition_attrs (tuple : Tuple.materialized) =
    Tuple.AttributeMap.bindings tuple.attributes
    |> List.map (fun (k, attr) -> (k, attr.Attribute.value))

  let ensure_relation_exists (db : Management.Database.t) name =
    if Management.Database.has_relation db name then Ok ()
    else Error (Error.RelationNotFound name)

  let bump_cardinality delta = function
    | Conventions.Cardinality.Finite n ->
        Conventions.Cardinality.Finite (max 0 (n + delta))
    | other -> other

  let rehashed_relation_with_tree ~name ~schema (relation : Relation.t) tree
      cardinality =
    let hash = Hashing.hash_relation ~name ~schema ~tree in
    {
      relation with
      hash = Some hash;
      tree = Some tree;
      cardinality;
      membership_criteria = build_membership_criteria ~name schema;
    }

  let validate_tuple_constraints (storage : storage)
      (db : Management.Database.t) (relation : Relation.t)
      (tuple : Tuple.materialized) =
    let eval_result =
      match relation.constraints with
      | None | Some [] -> Ok true
      | Some named_constraints ->
          let ctx = build_eval_context storage db in
          Constraint.evaluate_named ctx tuple named_constraints
    in
    match eval_result with
    | Ok true -> Ok ()
    | Ok false -> Error (Error.ConstraintViolation "Constraint not satisfied")
    | Error (Constraint.ConstraintFailures failures) ->
        let msg =
          String.concat "; "
            (List.map (fun (n, _) -> "constraint " ^ n ^ " violated") failures)
        in
        Error (Error.ConstraintViolation msg)
    | Error _ ->
        Error (Error.ConstraintViolation "Constraint evaluation failed")

  let store_tuple_payload storage tuple_hash (tuple : Tuple.materialized) =
    let* attr_hashes = store_attributes storage tuple in
    let stored_tuple : Storable.Tuple.t =
      { relation = tuple.relation; attributes = attr_hashes }
    in
    let tuple_bytes = Storable.Tuple.to_bytes stored_tuple in
    match Storage.store_raw storage tuple_hash tuple_bytes with
    | Error _ -> Error (Error.StorageError "Failed to store tuple")
    | Ok () -> Ok ()

  (* Cascade constraint helpers *)

  let polarity_triggered_by kind pol =
    match (kind, pol) with
    | Constraint.Insert, (Constraint.Negative | Constraint.Both) -> true
    | Constraint.Delete, (Constraint.Positive | Constraint.Both) -> true
    | _ -> false

  let cascade_violation_msg kind target_relation cname constrained_name =
    let verb =
      match kind with
      | Constraint.Insert -> "inserting into"
      | Constraint.Delete -> "deleting from"
    in
    Printf.sprintf "cascade: %s %s violates constraint %s on %s" verb
      target_relation cname constrained_name

  let is_deferred (db : Management.Database.t) rel_name cname =
    List.exists
      (fun (d : Management.Database.deferred_entry) ->
        d.constraint_name = cname && d.relation_name = rel_name)
      db.deferred

  let needs_recheck filter (tup : Tuple.materialized) =
    filter = []
    || List.for_all
         (fun (attr, fval) ->
           match Tuple.AttributeMap.find_opt attr tup.attributes with
           | Some a -> Stdlib.( = ) a.Attribute.value fval
           | None -> false)
         filter

  let recheck_one storage ctx (mctx : Constraint.mutation_context) cname cbody
      constrained_name filter (h : Conventions.Hash.t) : (unit, error) Result.t
      =
    with_loaded_tuple storage h ~on_some:(fun tup ->
        if not (needs_recheck filter tup) then Ok ()
        else
          let cbody' =
            Constraint.substitute_transition cbody mctx.target_relation
              mctx.transition
          in
          match
            Constraint.evaluate_first_failure ctx tup [ (cname, cbody') ]
          with
          | Ok true -> Ok ()
          | Ok false | Error _ ->
              Error
                (Error.ConstraintViolation
                   (cascade_violation_msg mctx.kind mctx.target_relation cname
                      constrained_name)))

  let check_one_constraint storage ctx db (mctx : Constraint.mutation_context)
      constrained_name constrained_rel (cname, cbody) : (unit, error) Result.t =
    if is_deferred db constrained_name cname then Ok ()
    else
      let pols = Constraint.polarity_of cbody in
      match Constraint.polarity_find mctx.target_relation pols with
      | None -> Ok ()
      | Some pol when not (polarity_triggered_by mctx.kind pol) -> Ok ()
      | Some _ ->
          let filter =
            Constraint.focused_filter cbody mctx.target_relation mctx.transition
          in
          let hashes =
            match constrained_rel.Relation.tree with
            | None -> []
            | Some t -> Merkle.keys t
          in
          fold_result
            (fun () h ->
              recheck_one storage ctx mctx cname cbody constrained_name filter h)
            () hashes

  (** Scan every relation in [db] for constraints that reference
      [mctx.target_relation] with a polarity matched by the mutation kind, then
      re-evaluate those constraints against the affected tuples in each
      constrained relation.

      Returns [Ok ()] when all cascade checks pass. *)
  let check_cascade_constraints (storage : storage) (db : Management.Database.t)
      (mctx : Constraint.mutation_context) =
    let ctx = build_eval_context storage db in
    Management.Database.RelationMap.fold
      (fun constrained_name constrained_rel acc ->
        let* () = acc in
        match constrained_rel.Relation.constraints with
        | None | Some [] -> Ok ()
        | Some named ->
            fold_result
              (fun () nc ->
                check_one_constraint storage ctx db mctx constrained_name
                  constrained_rel nc)
              () named)
      db.relations (Ok ())

  (** Insert a tuple into a relation, storing it in the backend *)
  let create_tuple (storage : storage) (db : Management.Database.t)
      (relation : Relation.t) (tuple : Tuple.materialized) :
      (Management.Database.t * Relation.t * Conventions.Hash.t, error) Result.t
      =
    let name = relation.name in
    let* () = ensure_relation_exists db name in
    if
      not
        (relation.membership_criteria (tree_of_db db) (Tuple.Materialized tuple))
    then
      Error
        (Error.ConstraintViolation "Tuple does not satisfy membership criteria")
    else
      let* () = validate_tuple_constraints storage db relation tuple in
      let tuple_hash = Hashing.hash_tuple tuple in
      let current_tree =
        match relation.tree with Some t -> t | None -> Merkle.empty
      in
      if Merkle.member tuple_hash current_tree then
        Error (Error.DuplicateTuple tuple_hash)
      else
        let* () = store_tuple_payload storage tuple_hash tuple in
        let new_tree = Merkle.insert tuple_hash current_tree in
        let new_relation =
          rehashed_relation_with_tree ~name ~schema:relation.schema relation
            new_tree
            (bump_cardinality 1 relation.cardinality)
        in
        let new_db =
          Management.Database.update_relation db ~relation:new_relation
        in
        let* () =
          check_cascade_constraints storage new_db
            {
              target_relation = name;
              transition = transition_attrs tuple;
              kind = Constraint.Insert;
            }
        in
        let* () = store_relation storage new_relation in
        let* () = store_database storage new_db in
        Ok (new_db, new_relation, tuple_hash)

  (** Insert multiple tuples into a relation *)
  let create_tuples (storage : storage) (db : Management.Database.t)
      (relation : Relation.t) (tuples : Tuple.materialized list) :
      ( Management.Database.t * Relation.t * Conventions.Hash.t list,
        error )
      Result.t =
    let rec insert_all db rel hashes = function
      | [] -> Ok (db, rel, List.rev hashes)
      | tuple :: rest ->
          let* new_db, new_rel, hash = create_tuple storage db rel tuple in
          insert_all new_db new_rel (hash :: hashes) rest
    in
    insert_all db relation [] tuples

  (** Remove a tuple from a relation by its hash *)
  let retract_tuple (storage : storage) (db : Management.Database.t)
      (relation : Relation.t) ~(tuple_hash : Conventions.Hash.t) :
      (Management.Database.t * Relation.t, error) Result.t =
    let name = relation.name in
    let* () = ensure_relation_exists db name in
    let* current_tree =
      match relation.tree with
      | None -> Error (Error.TupleNotFound tuple_hash)
      | Some tree -> Ok tree
    in
    if not (Merkle.member tuple_hash current_tree) then
      Error (Error.TupleNotFound tuple_hash)
    else
      (* Note: We don't delete from storage - content-addressed storage is append-only.
         The tuple data remains for historical queries / garbage collection later. *)
      let new_tree = Merkle.delete tuple_hash current_tree in
      let new_relation =
        rehashed_relation_with_tree ~name ~schema:relation.schema relation
          new_tree
          (bump_cardinality (-1) relation.cardinality)
      in
      let new_db =
        Management.Database.update_relation db ~relation:new_relation
      in
      let* () =
        with_loaded_tuple storage tuple_hash ~on_some:(fun deleted_tuple ->
            check_cascade_constraints storage new_db
              {
                target_relation = name;
                transition = transition_attrs deleted_tuple;
                kind = Constraint.Delete;
              })
      in
      let* () = store_relation storage new_relation in
      let* () = store_database storage new_db in
      Ok (new_db, new_relation)

  (*
     System Catalog Maintenance
      *)

  (** Update catalog when a new user relation is created. Skipped if the
      relation being created is itself a catalog relation. *)
  let update_catalog_on_create (storage : storage) (db : Management.Database.t)
      (relation : Relation.t) : (Management.Database.t, error) Result.t =
    if Prelude.Catalog.is_catalog_relation relation.name then Ok db
    else
      (* Insert into public:relation *)
      match
        Management.Database.get_relation db Prelude.Catalog.relation_rel_name
      with
      | None -> Ok db (* catalog not yet initialized *)
      | Some rel_cat -> (
          let rel_tuple = Prelude.Catalog.build_relation_tuple relation.name in
          let* db, _, _ = create_tuple storage db rel_cat rel_tuple in
          (* Insert into public:attribute for each schema entry *)
          match
            Management.Database.get_relation db
              Prelude.Catalog.attribute_rel_name
          with
          | None -> Ok db
          | Some attr_cat ->
              let attr_tuples =
                Prelude.Catalog.build_attribute_tuples
                  ~relation_name:relation.name relation.schema
              in
              let* db, _, _ = create_tuples storage db attr_cat attr_tuples in
              Ok db)

  (** Update catalog when a user relation is retracted. Skipped if the relation
      being retracted is itself a catalog relation. *)
  let update_catalog_on_retract (storage : storage) (db : Management.Database.t)
      (relation : Relation.t) : (Management.Database.t, error) Result.t =
    if Prelude.Catalog.is_catalog_relation relation.name then Ok db
    else
      let* db =
        match
          Management.Database.get_relation db Prelude.Catalog.relation_rel_name
        with
        | None -> Ok db
        | Some rel_cat ->
            let rel_tuple_hash =
              Hashing.hash_tuple
                (Prelude.Catalog.build_relation_tuple relation.name)
            in
            let tree =
              match rel_cat.tree with Some t -> t | None -> Merkle.empty
            in
            if Merkle.member rel_tuple_hash tree then
              let* new_db, _ =
                retract_tuple storage db rel_cat ~tuple_hash:rel_tuple_hash
              in
              Ok new_db
            else Ok db
      in
      (* Remove attribute tuples for this relation from public:attribute *)
      let attr_tuples =
        Prelude.Catalog.build_attribute_tuples ~relation_name:relation.name
          relation.schema
      in
      fold_result
        (fun db tup ->
          let tup_hash = Hashing.hash_tuple tup in
          match
            Management.Database.get_relation db
              Prelude.Catalog.attribute_rel_name
          with
          | None -> Ok db
          | Some attr_cat ->
              let tree =
                match attr_cat.tree with Some t -> t | None -> Merkle.empty
              in
              if Merkle.member tup_hash tree then
                let* new_db, _ =
                  retract_tuple storage db attr_cat ~tuple_hash:tup_hash
                in
                Ok new_db
              else Ok db)
        db attr_tuples

  (** Bootstrap the 6 system catalog relations into a fresh database. Called
      only from [create_database]; suppresses recursive catalog updates. *)
  let init_catalog_relations (storage : storage) (db : Management.Database.t) :
      (Management.Database.t, error) Result.t =
    let* db =
      fold_result
        (fun db (name, schema) ->
          let* new_db, _ = create_relation_raw storage db ~name ~schema in
          Ok new_db)
        db Prelude.Catalog.catalog_definitions
    in
    let get_required (db : Management.Database.t) rel_name =
      match Management.Database.get_relation db rel_name with
      | Some rel -> Ok rel
      | None ->
          Error (Error.StorageError ("Catalog relation missing: " ^ rel_name))
    in
    let* relation_rel = get_required db Prelude.Catalog.relation_rel_name in
    let* db, _, _ =
      create_tuples storage db relation_rel
        (List.map Prelude.Catalog.build_relation_tuple
           Prelude.Catalog.catalog_relation_names)
    in
    let* attribute_rel = get_required db Prelude.Catalog.attribute_rel_name in
    let* db, _, _ =
      create_tuples storage db attribute_rel
        (List.concat_map
           (fun (name, schema) ->
             Prelude.Catalog.build_attribute_tuples ~relation_name:name schema)
           Prelude.Catalog.catalog_definitions)
    in
    let* on_rel = get_required db Prelude.Catalog.on_rel_name in
    let* db, _, _ =
      create_tuples storage db on_rel
        (List.map Prelude.Catalog.build_on_tuple
           [ "insert"; "update"; "delete" ])
    in
    let* timing_rel = get_required db Prelude.Catalog.timing_rel_name in
    let* db, _, _ =
      create_tuples storage db timing_rel
        (List.map Prelude.Catalog.build_timing_tuple
           [ "immediate"; "deferred" ])
    in
    let* domain_rel = get_required db Prelude.Catalog.domain_rel_name in
    let* db, _, _ =
      create_tuples storage db domain_rel
        (List.map Prelude.Catalog.build_domain_tuple
           [ "integer"; "natural"; "rational"; "string" ])
    in
    let* schema_rel = get_required db Prelude.Catalog.schema_rel_name in
    let* db, _, _ =
      create_tuples storage db schema_rel
        [ Prelude.Catalog.build_schema_tuple Qualified_name.default_schema ]
    in
    Ok db

  (*
     Database Operations
      *)

  (** Register a domain in the database. Can be called after [create_database]
      to add user-defined domains (e.g. [money], [email], [uuid]). Also inserts
      a tuple into [public:domain] if the catalog is live. *)
  let register_domain (storage : storage) (db : Management.Database.t)
      (domain : Domain.t) : (Management.Database.t, error) Result.t =
    let db = Management.Database.add_domain db domain in
    match
      Management.Database.get_relation db Prelude.Catalog.domain_rel_name
    with
    | None -> Ok db (* catalog not yet initialized *)
    | Some dom_cat ->
        let dom_tuple = Prelude.Catalog.build_domain_tuple domain.name in
        let* new_db, _, _ = create_tuple storage db dom_cat dom_tuple in
        Ok new_db

  (** Create a new database pre-seeded with the standard prelude domains and the
      system catalog relations. *)
  let create_database (storage : storage) ~name :
      (Management.Database.t, error) Result.t =
    (* Register prelude domains first (catalog does not exist yet, so no catalog update) *)
    let db =
      List.fold_left Management.Database.add_domain
        (Management.Database.empty ~name)
        Prelude.Domains.[ integer; natural; rational; string ]
    in
    (* Bootstrap catalog relations and seed them *)
    init_catalog_relations storage db

  (** Get database history *)
  let database_history (db : Management.Database.t) : Conventions.Hash.t list =
    db.history

  (*
     Relation Operations
      *)

  (** Create a new empty relation with the given schema. Also updates
      [public:relation] and [public:attribute] in the system catalog. *)
  let create_relation (storage : storage) (db : Management.Database.t)
      ~(name : string) ~(schema : Schema.t) :
      (Management.Database.t * Relation.t, error) Result.t =
    let name = normalize_name name in
    if Management.Database.has_relation db name then
      Error (Error.RelationAlreadyExists name)
    else
      let tree = Merkle.empty in
      let relation_hash = Hashing.hash_relation ~name ~schema ~tree in
      let relation =
        Relation.make ~hash:(Some relation_hash) ~name ~schema ~tree:(Some tree)
          ~constraints:None ~cardinality:(Conventions.Cardinality.Finite 0)
          ~generator:None
          ~membership_criteria:(build_membership_criteria ~name schema)
          ~provenance:(Relation.Provenance.Base name)
          ~lineage:(Relation.Lineage.Base name)
      in
      let new_db = Management.Database.add_relation db ~relation in
      let* () = store_relation storage relation in
      let* final_db = update_catalog_on_create storage new_db relation in
      let* () = store_database storage final_db in
      Ok (final_db, relation)

  (** Create an immutable/generator-based relation *)
  let create_immutable_relation (storage : storage) (db : Management.Database.t)
      ~(name : string) ~(schema : Schema.t) ~(generator : Generator.t)
      ~(membership_criteria : (string -> Merkle.t option) -> Tuple.t -> bool)
      ~(cardinality : Conventions.Cardinality.t) :
      (Management.Database.t * Relation.t, error) Result.t =
    let name = normalize_name name in
    if Management.Database.has_relation db name then
      Error (Error.RelationAlreadyExists name)
    else
      let relation_hash =
        Hashing.hash_relation ~name ~schema ~tree:Merkle.empty
      in
      let relation =
        Relation.make ~hash:(Some relation_hash) ~name ~schema
          ~tree:None (* No tree for generator-based relations *)
          ~constraints:None ~cardinality ~generator:(Some generator)
          ~membership_criteria ~provenance:(Relation.Provenance.Base name)
          ~lineage:(Relation.Lineage.Base name)
      in
      let new_db = Management.Database.add_relation db ~relation in
      let* () = store_relation storage relation in
      let* final_db = update_catalog_on_create storage new_db relation in
      let* () = store_database storage final_db in
      Ok (final_db, relation)

  (** Remove a relation from the database. Also removes its entries from
      [public:relation] and [public:attribute]. *)
  let retract_relation (storage : storage) (db : Management.Database.t)
      ~(name : string) : (Management.Database.t, error) Result.t =
    let name = normalize_name name in
    match Management.Database.get_relation db name with
    | None -> Error (Error.RelationNotFound name)
    | Some relation ->
        let new_db = Management.Database.remove_relation db ~name in
        (* Update catalog on the db state that no longer contains the retracted relation *)
        let* final_db = update_catalog_on_retract storage new_db relation in
        let* () = store_database storage final_db in
        Ok final_db

  (** Clear all tuples from a relation (truncate) *)
  let clear_relation (storage : storage) (db : Management.Database.t)
      (relation : Relation.t) :
      (Management.Database.t * Relation.t, error) Result.t =
    let name = relation.name in
    match Management.Database.get_relation db name with
    | None -> Error (Error.RelationNotFound name)
    | Some _ ->
        let new_tree = Merkle.empty in
        let new_hash =
          Hashing.hash_relation ~name ~schema:relation.schema ~tree:new_tree
        in
        let new_relation =
          {
            relation with
            hash = Some new_hash;
            tree = Some new_tree;
            cardinality = Conventions.Cardinality.Finite 0;
            membership_criteria =
              build_membership_criteria ~name relation.schema;
          }
        in
        let new_db =
          Management.Database.update_relation db ~relation:new_relation
        in
        let* () = store_relation storage new_relation in
        let* () = store_database storage new_db in
        Ok (new_db, new_relation)

  (** Attach constraints to an existing relation. Rebuilds membership_criteria
      to include constraint checking, re-hashes, and persists. *)
  let update_relation_constraints (storage : storage)
      (db : Management.Database.t) ~(relation_name : string)
      ~(constraints : Relation.RelationConstraint.t) :
      (Management.Database.t * Relation.t, error) Result.t =
    let relation_name = normalize_name relation_name in
    match Management.Database.get_relation db relation_name with
    | None -> Error (Error.RelationNotFound relation_name)
    | Some relation ->
        let merged_constraints =
          match relation.constraints with
          | None -> constraints
          | Some existing -> Constraint.merge existing constraints
        in
        (* Do not bake constraint evaluation into membership_criteria.
         create_tuple evaluates constraints with the current db snapshot.
         membership_criteria stays for schema/domain validation only. *)
        let new_relation =
          { relation with constraints = Some merged_constraints }
        in
        (* Re-hash *)
        let new_relation =
          match new_relation.tree with
          | Some tree ->
              let h =
                Hashing.hash_relation ~name:relation_name
                  ~schema:relation.schema ~tree
              in
              { new_relation with hash = Some h }
          | None -> new_relation
        in
        let new_db =
          Management.Database.update_relation db ~relation:new_relation
        in
        let* () = store_relation storage new_relation in
        let* () = store_database storage new_db in
        Ok (new_db, new_relation)

  (** Register a named constraint on a relation. Attaches the constraint body to
      the relation AND records it in [public:constraint]. *)
  let register_constraint (storage : storage) (db : Management.Database.t)
      ~(constraint_name : string) ~(relation_name : string)
      ~(body : Constraint.t) : (Management.Database.t, error) Result.t =
    let relation_name = normalize_name relation_name in
    let body = normalize_constraint_body body in
    let* db, _ =
      update_relation_constraints storage db ~relation_name
        ~constraints:[ (constraint_name, body) ]
    in
    match
      Management.Database.get_relation db Prelude.Catalog.constraint_rel_name
    with
    | None -> Ok db
    | Some con_cat ->
        let con_tuple =
          Prelude.Catalog.build_constraint_tuple constraint_name relation_name
        in
        let* new_db, _, _ = create_tuple storage db con_cat con_tuple in
        Ok new_db

  (*Query Operations *)

  (** Get all tuple hashes from a relation *)
  let tuple_hashes (relation : Relation.t) : Conventions.Hash.t list =
    match relation.tree with None -> [] | Some tree -> Merkle.keys tree

  (** Get relation by name from database *)
  let get_relation (db : Management.Database.t) ~(name : string) :
      Relation.t option =
    Management.Database.get_relation db (normalize_name name)

  (** Count tuples in a relation *)
  let tuple_count (relation : Relation.t) : int =
    match relation.tree with None -> 0 | Some tree -> Merkle.size tree

  (** Check if a tuple exists in a relation *)
  let tuple_exists (relation : Relation.t) (tuple_hash : Conventions.Hash.t) :
      bool =
    match relation.tree with
    | None -> false
    | Some tree -> Merkle.member tuple_hash tree

  (** Attach a named constraint with timing metadata. Immediate constraints are
      added to the relation's constraint list. Deferred constraints are recorded
      in the database's deferred list and also attached to the relation (for
      schema visibility). *)
  let attach_constraint (storage : storage) (db : Management.Database.t)
      ~(constraint_name : string) ~(relation_name : string)
      ~(body : Constraint.t) ~(timing : Constraint.timing) :
      (Management.Database.t, error) Result.t =
    let relation_name = normalize_name relation_name in
    let body = normalize_constraint_body body in
    let* db, _ =
      update_relation_constraints storage db ~relation_name
        ~constraints:[ (constraint_name, body) ]
    in
    match timing with
    | Constraint.Immediate -> Ok db
    | Constraint.Deferred ->
        let entry : Management.Database.deferred_entry =
          { relation_name; constraint_name; body }
        in
        Ok { db with deferred = entry :: db.deferred }

  (** Check all deferred constraints against the current database state. Returns
      [Ok ()] if all pass, or an error on the first violation. *)
  let check_deferred_constraints (storage : storage)
      (db : Management.Database.t) : (unit, error) Result.t =
    let ctx = build_eval_context storage db in
    fold_result
      (fun () (entry : Management.Database.deferred_entry) ->
        match Management.Database.get_relation db entry.relation_name with
        | None -> Ok ()
        | Some rel ->
            let hashes =
              match rel.tree with None -> [] | Some t -> Merkle.keys t
            in
            fold_result
              (fun () h ->
                with_loaded_tuple storage h ~on_some:(fun tup ->
                    match
                      Constraint.evaluate_first_failure ctx tup
                        [ (entry.constraint_name, entry.body) ]
                    with
                    | Ok true -> Ok ()
                    | Ok false | Error _ ->
                        Error
                          (Error.ConstraintViolation
                             (Printf.sprintf
                                "deferred constraint %s on %s violated"
                                entry.constraint_name entry.relation_name))))
              () hashes)
      () db.deferred

  (** Complete a mutation sequence by evaluating all deferred constraints
      against the current database state.

      On success, returns the database with the deferred list cleared.
      Subsequent mutations start a fresh deferral window. On failure, returns
      the constraint violation error and leaves the database unchanged so the
      caller can inspect or roll back.

      Typical usage: let db = create_tuple storage db rel t1 in let db =
      retract_tuple storage db rel t2 in let db = commit storage db in (*
      deferred checks run here *) ... *)
  let commit (storage : storage) (db : Management.Database.t) :
      (Management.Database.t, error) Result.t =
    let* () = check_deferred_constraints storage db in
    let db' = { db with deferred = [] } in
    let* () = store_database storage db' in
    Ok db'
end
