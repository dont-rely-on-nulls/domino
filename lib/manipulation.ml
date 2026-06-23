(** Multigroup manipulation operations.

    This module implements the core operations for the relational engine:
    - Multigroup creation
    - Relation creation and deletion
    - Tuple insertion and deletion
    - Query operations

    All operations are immutable: they return new multigroup/relation states
    rather than modifying existing ones. The storage layer handles persistence.

    This is a functor that takes a storage backend, ensuring all tuple data is
    properly persisted to the content-addressed store. *)

(** Functor to create manipulation operations with a storage backend *)
module Make (Storage : Management.Physical.S) = struct
  type storage = Storage.t

  type error = Condition.t

  module Error = struct
    open Condition

    (* TODO: more specific errors *)
    let storage_error message = condition "storage-error" message empty
  end

  let ( let* ) = Result.bind

  (* State Persistence - Store relation and database states *)

  (** Store a relation state to storage *)
  let store_relation (storage : storage) (relation : Relation.relation) : (unit, error) Result.t =
    match
      Storage.store_raw storage relation#hash (Storable.Relation.to_bytes @@ relation#serialize ())
    with
    | Error _ ->
        Error (Error.storage_error "Failed to store relation")
    | Ok () ->
        Ok ()

  (** Store a database state to storage. Keyed by [db#name]; content-hashing of
      mgs is no longer maintained on the Sakura side — the kernel owns mg
      content addressing. *)
  let store_multigroup (storage : storage) (db : Management.Multigroup.multigroup) :
      (unit, error) Result.t =
    match Storage.store_raw storage db#name (Storable.Multigroup.to_bytes @@ db#serialize ()) with
    | Error _ ->
        Error (Error.storage_error "Failed to store multigroup")
    | Ok () ->
        Ok ()

  (** Load a relation state from storage by hash *)
  let load_relation (storage : storage) (rel_hash : Conventions.Hash.t) :
      (Relation.stored option, error) Result.t =
    match Storage.load_raw storage rel_hash with
    | Error _ ->
        Error (Error.storage_error "Failed to load relation")
    | Ok None ->
        Ok None
    | Ok (Some bytes) ->
        let _stored = Storable.Relation.of_bytes bytes in
        let name = failwith "NOT IMPLEMENTED" in
        let schema = failwith "NOT IMPLEMENTED" in
        let constraints = failwith "NOT IMPLEMENTED" in
        let cardinality = failwith "NOT IMPLEMENTED" in
        let membership_criteria = Constraint.build_membership_criteria ~_name:name ~schema in
        let relation =
          new Relation.stored
            ~name ~schema ~constraints ~cardinality ~membership_criteria ~provenance:None
            ~lineage:None
        in
        Ok (Some relation)

  let load_multigroup (storage : storage) (mg_hash : Conventions.Hash.t) :
      (Management.Multigroup.multigroup option, error) Result.t =
    match Storage.load_raw storage mg_hash with
    | Error _ ->
        Error (Error.storage_error "Failed to load multigroup")
    | Ok None ->
        Ok None
    | Ok (Some bytes) ->
        (* TODO: Properly deserialize multigroup state *)
        let _stored = Storable.Multigroup.of_bytes bytes in
        let name = failwith "NOT IMPLEMENTED" in
        let mg = new Management.Multigroup.multigroup ~name in
        Ok (Some mg)

  (* Tuple Operations - Storage Integrated *)

  (** Store each attribute value and return map of attr_name -> attr_hash *)
  let store_attributes (_storage : storage) (_tuple : Tuple.materialized) :
      ((string * Conventions.Hash.t) list, error) Result.t =
    failwith "NOT IMPLEMENTED"

  (** Load a tuple from storage by its hash *)
  let load_tuple (storage : storage) (tuple_hash : Conventions.Hash.t) :
      (Tuple.materialized option, error) Result.t =
    match Storage.load_raw storage tuple_hash with
    | Error _ ->
        Error (Error.storage_error "Failed to load tuple")
    | Ok None ->
        Ok None
    | Ok (Some tuple_bytes) ->
        let _stored = Storable.Tuple.of_bytes tuple_bytes in
        let attributes = failwith "NOT IMPLEMENTED" in
        let relation = failwith "NOT IMPLEMENTED" in
        let rec load_attrs acc = function
          | [] ->
              Ok (List.rev acc)
          | (name, attr_hash) :: rest -> (
            match Storage.load_raw storage attr_hash with
            | Error _ ->
                Error (Error.storage_error ("Failed to load attribute: " ^ name))
            | Ok None ->
                Error (Error.storage_error ("Attribute not found: " ^ name))
            | Ok (Some value_bytes) ->
                let value : Conventions.AbstractValue.t = Marshal.from_bytes value_bytes 0 in
                let attr : Attribute.materialized = {value} in
                load_attrs ((name, attr) :: acc) rest )
        in
        load_attrs [] attributes
        |> Result.map
             (List.fold_left
                (fun m (k, v) -> Tuple.AttributeMap.add k v m)
                Tuple.AttributeMap.empty )
        |> Result.map (fun attributes -> Some {Tuple.relation; attributes})
end
