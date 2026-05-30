(* Interface to the RNT kernel *)

open Ctypes

(* Shared types independent of backend, shared across all Make instances *)

type auth_method = Certificate | PlainText

(** Opaque claim token returned by [authenticate]. *)
type claims = string

(** Opaque handle to an open branch (multigroup head) in the NT registry. *)
type branch_handle = nativeint

(** Opaque handle to an open relation object in the NT registry. *)
type relation_handle = nativeint

(** Distinguishes a simple single-relation cursor from a full VM plan cursor. *)
type cursor_kind = Simple | Vm

(** A streaming cursor over tuples produced either by a simple scan or the VM. *)
type tuple_stream = {
  ts_cursor   : nativeint;
  ts_relation : string;
  ts_kind     : cursor_kind;
}

type path_arg = Var of string | Const of string

type plan_node =
  | Scan of { path : string; args : (string * path_arg) list }
  (* Given this is the query plan, this should probably specify a
     concrete join algorithm (e.g. HashJoin, NestedLoop, LeapFrog, &c) *)
  | Join of { left : plan_node; right : plan_node; on_attrs : string list }
  | Take of { limit : int; source : plan_node }
  | Project of { source : plan_node; attrs : string list }

module Error = struct
  open Condition
  (* TODO: better error structure *)
  let auth_failed msg = condition "auth-failed" msg empty
  let handle_error rc msg = condition "handle-error" msg ("return-code" |=| (of_int rc))
  let object_not_found kind id = condition "object-not-found" "Object not found"
                                   ("kind" |=| (of_string kind) &
                                    "identifier" |=| (of_string id))
  let cursor_error msg = condition "cursor-error" msg empty
  let not_supported msg = condition "not-supported" msg empty
  let initialization_error = condition "initialization-error" "Couldn't initialize" empty
end

(* --------------------------------------------------------------------------
   Backend selector
   -------------------------------------------------------------------------- *)

module type Backend = sig
  (** RNT storage driver: "sqlite" or "memory". *)
  val driver   : string
  (** Passed to the driver as its init argument.
      For "sqlite": file path or ":memory:".
      For "memory": ignored. *)
  val init_arg : string
end

(* --------------------------------------------------------------------------
   Functor — one instance per backend
   -------------------------------------------------------------------------- *)

module Make (B : Backend) = struct

  let ( let* ) = Result.bind

  (* --------------------------------------------------------------------------
     Authentication
     -------------------------------------------------------------------------- *)

  let authenticate (method_ : auth_method) : (claims, Condition.t) result =
    let method_str = match method_ with
      | Certificate -> "certificate"
      | PlainText   -> "plain_text"
    in
    let (rc, claims_opt) =
      Nt_ffi.with_out_string (fun pp -> Nt_ffi.rnt_firewall method_str pp)
    in
    match (rc, claims_opt) with
    | (0, Some s) -> Ok s
    | (_, Some s) -> Error (Error.auth_failed s)
    | _           -> Error (Error.auth_failed "firewall rejected connection")

  (* --------------------------------------------------------------------------
     Path helpers (shared by branch and relation lifecycle)
     -------------------------------------------------------------------------- *)

  let branch_path (branch_name : string) : string =
    "/system/branches/" ^ branch_name

  let multigroup_path ~(branch_name : string) ~(mg_name : string) : string =
    branch_path branch_name ^ "/multigroups/" ^ mg_name

  let relation_path ~(branch_name : string) ~(mg_name : string)
      ~(rel_name : string) : string =
    multigroup_path ~branch_name ~mg_name ^ "/relations/" ^ rel_name

  (* --------------------------------------------------------------------------
     Branch lifecycle
     --------------------------------------------------------------------------
     RNT now owns branch state: each branch is a named hash-pointer to an
     immutable snapshot. Sakura's [multigroup] is the OCaml-side mirror of
     that snapshot's content (relations, schemas, constraints, lineage).
     There is no payload to serialise — every relation/tuple mutation
     advances the branch to a new snapshot internally. *)

  let ensure_branch (path : string) : unit =
    (* Idempotent: rnt_register_branch with "" creates an unborn branch when
       missing, no-ops when present. *)
    ignore (Nt_ffi.rnt_register_branch path "")

  let branch_target_of_handle (bh : branch_handle) : (string, Condition.t) result =
    let (rc, target_opt) =
      Nt_ffi.with_out_string
        (fun pp -> Nt_ffi.rnt_branch_target (Nt_ffi.nint_to_ptr bh) pp)
    in
    match (rc, target_opt) with
    | (0, Some s) -> Ok s
    | (0, None)   -> Ok ""
    | _           -> Error (Error.handle_error rc "branch_target read failed")

  let list_relations ~(branch_name : string) ~(mg_name : string) :
      ((string * string) list, Condition.t) result =
    let path = multigroup_path ~branch_name ~mg_name in
    let (rc, out_opt) =
      Nt_ffi.with_out_string (fun pp -> Nt_ffi.rnt_list_relations path pp)
    in
    match (rc, out_opt) with
    | (0, Some s) ->
        let pairs =
          String.split_on_char '\n' s
          |> List.filter_map (fun line ->
              match String.split_on_char '\t' line with
              | [n; r] when n <> "" -> Some (n, r)
              | _ -> None)
        in
        Ok pairs
    | (0, None)   -> Ok []
    | _           -> Error (Error.handle_error rc
                              ("list_relations failed: " ^ branch_name
                               ^ "/" ^ mg_name))

  let list_branch_multigroups (branch_name : string) :
      ((string * string) list, Condition.t) result =
    let path = branch_path branch_name in
    let (rc, out_opt) =
      Nt_ffi.with_out_string
        (fun pp -> Nt_ffi.rnt_list_branch_multigroups path pp)
    in
    match (rc, out_opt) with
    | (0, Some s) ->
        let pairs =
          String.split_on_char '\n' s
          |> List.filter_map (fun line ->
              match String.split_on_char '\t' line with
              | [n; h] when n <> "" -> Some (n, h)
              | _ -> None)
        in
        Ok pairs
    | (0, None)   -> Ok []
    | _           -> Error (Error.handle_error rc
                              ("list_branch_multigroups failed: " ^ branch_name))

  let session_open () : (string, Condition.t) result =
    let (rc, id_opt) =
      Nt_ffi.with_out_string
        (fun pp -> Nt_ffi.rnt_session_open (from_voidp void null) pp)
    in
    match (rc, id_opt) with
    | (0, Some id) -> Ok id
    | _            -> Error (Error.handle_error rc "session_open failed")

  let session_close (sid : string) : (unit, Condition.t) result =
    let rc = Nt_ffi.rnt_session_close sid in
    if rc = 0 then Ok () else Error (Error.handle_error rc "session_close failed")

  let session_set_branch (sid : string) (branch_name : string)
      (target_hash : string) : (unit, Condition.t) result =
    let rc = Nt_ffi.rnt_session_set_branch sid branch_name target_hash in
    if rc = 0 then Ok () else Error (Error.handle_error rc "session_set_branch failed")

  let open_branch (_claims : claims) (name : string) :
      (branch_handle * Management.Multigroup.multigroup, Condition.t) result =
    let path = branch_path name in
    ensure_branch path;
    let raw = Nt_ffi.rnt_open_handle path (from_voidp void null) in
    match Nt_ffi.ptr_to_opt raw with
    | None -> Error (Error.object_not_found "branch" name)
    | Some bh ->
        let* _tip          = branch_target_of_handle bh in
        let* rel_entries   = list_relations ~branch_name:name ~mg_name:name in
        (* Populate mg with stored relations from RNT snapshot. Schema is not
           persisted in the snapshot codec so relations start schema-less; the
           session rebuilds schema state via DDL or catalog queries. *)
        let mg =
          List.fold_left
            (fun mg (rel_name, root) ->
              let rel =
                (new Relation.stored
                  ~name:rel_name
                  ~schema:[]
                  ~constraints:None
                  ~cardinality:Conventions.Cardinality.AlephZero
                  ~lineage:(Some (Relation.Lineage.Base rel_name))
                  ~provenance:(Some (Relation.Provenance.base
                                       ~relation:rel_name ~attributes:[]))
                  ~membership_criteria:(fun _ -> true))
                  #set_tree_pointer (Some root)
              in
              mg#add_relation (rel :> Relation.relation))
            (new Management.Multigroup.multigroup ~name)
            rel_entries
        in
        Ok (bh, mg)

  let close_branch (bh : branch_handle) : (unit, Condition.t) result =
    let rc = Nt_ffi.rnt_close_handle (Nt_ffi.nint_to_ptr bh) in
    if rc = 0 then Ok () else Error (Error.handle_error rc "close_branch failed")

  let list_snapshot_relations (snapshot_hash : string) :
      ((string * string) list, Condition.t) result =
    let (rc, out_opt) =
      Nt_ffi.with_out_string
        (fun pp -> Nt_ffi.rnt_list_snapshot_relations snapshot_hash pp)
    in
    match (rc, out_opt) with
    | (0, Some s) ->
        let pairs =
          String.split_on_char '\n' s
          |> List.filter_map (fun line ->
              match String.split_on_char '\t' line with
              | [n; r] when n <> "" -> Some (n, r)
              | _ -> None)
        in
        Ok pairs
    | (0, None)   -> Ok []
    | _           -> Error (Error.handle_error rc ("list_snapshot_relations failed: " ^ snapshot_hash))

  (* Opens a read-only view pinned to a specific snapshot hash.
     Returns (bh, mg) where bh is a handle on the snapshot object and mg
     reflects exactly the relation set at that hash. The caller is
     responsible for refusing mutations (see Branch.Detached). *)
  let open_snapshot (_claims : claims) (branch_name : string)
      (snapshot_hash : string) :
      (branch_handle * Management.Multigroup.multigroup, Condition.t) result =
    let snap_path = "/system/snapshots/" ^ snapshot_hash in
    let raw = Nt_ffi.rnt_open_handle snap_path (from_voidp void null) in
    match Nt_ffi.ptr_to_opt raw with
    | None -> Error (Error.object_not_found "snapshot" snapshot_hash)
    | Some bh ->
        let* rel_entries = list_snapshot_relations snapshot_hash in
        let mg =
          List.fold_left
            (fun mg (rel_name, root) ->
              let rel =
                (new Relation.stored
                  ~name:rel_name
                  ~schema:[]
                  ~constraints:None
                  ~cardinality:Conventions.Cardinality.AlephZero
                  ~lineage:(Some (Relation.Lineage.Base rel_name))
                  ~provenance:(Some (Relation.Provenance.base
                                       ~relation:rel_name ~attributes:[]))
                  ~membership_criteria:(fun _ -> true))
                  #set_tree_pointer (Some root)
              in
              mg#add_relation (rel :> Relation.relation))
            (new Management.Multigroup.multigroup ~name:branch_name)
            rel_entries
        in
        Ok (bh, mg)

  (* --------------------------------------------------------------------------
     Relation handles
     -------------------------------------------------------------------------- *)

  let open_relation ~(branch_name : string) ~(mg_name : string)
      ~(rel_name : string) : (relation_handle, Condition.t) result =
    let path = relation_path ~branch_name ~mg_name ~rel_name in
    ignore (Nt_ffi.rnt_register_relation path);
    let raw = Nt_ffi.rnt_open_handle path (from_voidp void null) in
    match Nt_ffi.ptr_to_opt raw with
    | None -> Error (Error.object_not_found "relation" rel_name)
    | Some rh -> Ok rh

  let close_relation (rh : relation_handle) : (unit, Condition.t) result =
    let rc = Nt_ffi.rnt_close_handle (Nt_ffi.nint_to_ptr rh) in
    if rc = 0 then Ok () else Error (Error.handle_error rc "close_relation failed")

  (* --------------------------------------------------------------------------
     Query execution via the Tarski VM plan builder
     -------------------------------------------------------------------------- *)

  (* Recursively builds a C-side PlanWrapper from an OCaml plan_node tree.
     On partial failure the partially-built plan is freed before returning Error. *)
  let rec build_plan (plan : plan_node) : (nativeint, Condition.t) result =
    match plan with
    | Scan { path; _ } ->
        let raw = Nt_ffi.rnt_plan_scan path in
        (match Nt_ffi.ptr_to_opt raw with
         | None   -> Error (Error.cursor_error ("plan_scan failed for path: " ^ path))
         | Some p -> Ok p)
    | Join { left; right; _ } ->
        let* lp = build_plan left in
        (match build_plan right with
         | Error e ->
             Nt_ffi.rnt_plan_free (Nt_ffi.nint_to_ptr lp);
             Error e
         | Ok rp ->
             let raw = Nt_ffi.rnt_plan_join
                         (Nt_ffi.nint_to_ptr lp) (Nt_ffi.nint_to_ptr rp) in
             (* join takes ownership of both children — no manual free needed *)
             (match Nt_ffi.ptr_to_opt raw with
              | None   -> Error (Error.cursor_error "plan_join failed")
              | Some p -> Ok p))
    | Take { limit; source } ->
        let* sp = build_plan source in
        let raw = Nt_ffi.rnt_plan_take
                    (Nt_ffi.nint_to_ptr sp)
                    (Unsigned.Size_t.of_int limit) in
        (* take takes ownership of source — no manual free needed *)
        (match Nt_ffi.ptr_to_opt raw with
         | None   -> Error (Error.cursor_error "plan_take failed")
         | Some p -> Ok p)
    | Project { source; attrs } ->
       Nt_ffi.with_strings_as_char_array attrs (fun attrs ->
           let* sp = build_plan source in
           let raw = Nt_ffi.rnt_plan_project (Nt_ffi.nint_to_ptr sp) attrs in
           Nt_ffi.ptr_to_opt raw
           |> Option.to_result ~none:(Error.cursor_error "plan_project failed"))

  let execute_query (plan : plan_node) ~(rel_name : string) :
      (tuple_stream, Condition.t) result =
    let* plan_ptr = build_plan plan in
    let raw = Nt_ffi.rnt_vm_execute_plan (Nt_ffi.nint_to_ptr plan_ptr) in
    match Nt_ffi.ptr_to_opt raw with
    | None           -> Error (Error.cursor_error "vm_execute_plan failed")
    | Some ts_cursor -> Ok { ts_cursor; ts_relation = rel_name; ts_kind = Vm }

  let parse_kv_tuple (relation : string) (kv : string) : Tuple.materialized =
    let attributes =
      String.split_on_char '\n' kv
      |> List.filter_map (fun line ->
          match String.split_on_char '=' line with
          | k :: rest when k <> "" ->
              let v = String.concat "=" rest in
              Some (k, Attribute.{ value = Obj.repr v })
          | _ -> None)
      |> List.fold_left
          (fun m (k, v) -> Tuple.AttributeMap.add k v m)
          Tuple.AttributeMap.empty
    in
    { Tuple.relation; attributes }

  let stream_next (stream : tuple_stream) :
      (Tuple.materialized option, Condition.t) result =
    let next_fn = match stream.ts_kind with
      | Simple -> Nt_ffi.rnt_cursor_next
      | Vm     -> Nt_ffi.rnt_vm_cursor_next
    in
    let (rc, kv_opt) =
      Nt_ffi.with_out_string
        (fun pp -> next_fn (Nt_ffi.nint_to_ptr stream.ts_cursor) pp)
    in
    match rc with
    | 0 -> Ok None
    | 1 -> (match kv_opt with
            | None    -> Ok None
            | Some kv -> Ok (Some (parse_kv_tuple stream.ts_relation kv)))
    | _ -> Error (Error.cursor_error "cursor_next error")

  let stream_close (stream : tuple_stream) : (unit, Condition.t) result =
    let close_fn = match stream.ts_kind with
      | Simple -> Nt_ffi.rnt_cursor_close
      | Vm     -> Nt_ffi.rnt_vm_cursor_close
    in
    let rc = close_fn (Nt_ffi.nint_to_ptr stream.ts_cursor) in
    if rc = 0 then Ok () else Error (Error.cursor_error "cursor_close failed")

  (* --------------------------------------------------------------------------
     Tuple storage helpers
     -------------------------------------------------------------------------- *)

  let materialized_to_kv (tuple : Tuple.materialized) : (string * string) list =
    Tuple.AttributeMap.fold
      (fun k v acc -> (k, Obj.obj v.Attribute.value) :: acc)
      tuple.Tuple.attributes []

  (* --------------------------------------------------------------------------
     Multigroup / branch mutations

     Each of these advances the underlying RNT branch to a new snapshot as a
     side effect of the [rnt_register_relation] / [rnt_link_tuple] / ... call.
     The OCaml multigroup is the pure-value mirror: the caller threads the
     returned [mg] through subsequent calls. There is no separate commit step.
     -------------------------------------------------------------------------- *)

  let create_multigroup (claims : claims) (name : string) :
      (branch_handle * Management.Multigroup.multigroup, Condition.t) result =
    open_branch claims name

  let create_relation (bh : branch_handle) (mg : Management.Multigroup.multigroup)
      ~(branch_name : string) ~(name : string) ~(schema : Schema.t) :
      (branch_handle * Management.Multigroup.multigroup, Condition.t) result =
    let mg_name = mg#name in
    let path = relation_path ~branch_name ~mg_name ~rel_name:name in
    let rc   = Nt_ffi.rnt_register_relation path in
    if rc <> 0 then Error (Error.handle_error rc ("register_relation failed: " ^ name))
    else
      let rel_root =
        let (rc, opt) =
          Nt_ffi.with_out_string (fun pp -> Nt_ffi.rnt_relation_root path pp)
        in
        if rc = 0 then Option.value opt ~default:"" else ""
      in
      let rel = (new Relation.stored
        ~name
        ~schema
        ~constraints:None
        ~cardinality:Conventions.Cardinality.AlephZero
        ~lineage:(Some (Relation.Lineage.Base name))
        ~provenance:(Some (Relation.Provenance.base ~relation:name
                             ~attributes:(List.map fst schema)))
        ~membership_criteria:(fun _ -> true))#set_tree_pointer (Some rel_root)
      in
      let* _tip = branch_target_of_handle bh in
      Ok (bh, mg#add_relation (rel :> Relation.relation))

  let retract_relation (bh : branch_handle) (mg : Management.Multigroup.multigroup)
      ~(name : string) :
      (branch_handle * Management.Multigroup.multigroup, Condition.t) result =
    (* TODO: there is no rnt_unregister_relation today; the OCaml mg drops the
       relation but RNT continues to carry it in the current snapshot. This
       drifts the two views — fine while the in-memory mg is the source of
       truth for the connection, but worth revisiting once a removal API
       exists on the RNT side. *)
    Ok (bh, mg#remove_relation name)

  let clear_relation (bh : branch_handle) (mg : Management.Multigroup.multigroup)
      ~(branch_name : string) ~(mg_name : string)
      (rel : Relation.relation) :
      (branch_handle * Management.Multigroup.multigroup, Condition.t) result =
    let path = relation_path ~branch_name ~mg_name ~rel_name:rel#name in
    let rc = Nt_ffi.rnt_clear_relation path in
    if rc <> 0 then Error (Error.handle_error rc ("clear_relation failed: " ^ rel#name))
    else
      let* _tip = branch_target_of_handle bh in
      Ok (bh, mg)

  let register_domain (_bh : branch_handle) (mg : Management.Multigroup.multigroup)
      (domain : Relation.domain) :
      (branch_handle * Management.Multigroup.multigroup, Condition.t) result =
    Ok (_bh, mg#add_relation (domain :> Relation.relation))

  let create_immutable_relation (bh : branch_handle)
      (mg : Management.Multigroup.multigroup) ~name ~schema ~generator
      ~membership_criteria ~cardinality ~(producer : Relation.producer option) :
      (branch_handle * Management.Multigroup.multigroup * Relation.relation, Condition.t) result =
    let rel : Relation.relation =
      match producer with
      | Some p ->
          (new Relation.pseudo ~name ~schema ~constraints:None ~cardinality
             ~membership_criteria ~lineage:None
             ~provenance:Relation.Provenance.empty ~producer:p
            :> Relation.relation)
      | None ->
          (new Relation.ephemeral ~name ~schema ~constraints:None ~cardinality
             ~membership_criteria ~lineage:None ~provenance:None ~generator
            :> Relation.relation)
    in
    Ok (bh, mg#add_relation rel, rel)

  let register_constraint (bh : branch_handle)
      (mg : Management.Multigroup.multigroup) ~constraint_name ~relation_name
      ~(body : Constraint.t) :
      (branch_handle * Management.Multigroup.multigroup, Condition.t) result =
    match mg#get_relation relation_name with
    | None -> Error (Error.object_not_found "relation" relation_name)
    | Some rel ->
        let existing =
          match rel#constraints with Some cs -> cs | None -> []
        in
        let new_constraints = (constraint_name, body) :: existing in
        let updated_rel =
          (Obj.magic rel
            : < set_constraints :
                  Relation.RelationConstraint.t ->
                  < constraints : Relation.RelationConstraint.t option
                  ; .. > >)
            #set_constraints new_constraints
        in
        Ok (bh, mg#update_relation (updated_rel :> Relation.relation))

  (* --------------------------------------------------------------------------
     Tuple mutations
     -------------------------------------------------------------------------- *)

  let insert_tuple ~(branch_name : string) ~(mg_name : string)
      ~(rel_name : string) (attrs : (string * string) list) :
      (string, Condition.t) result =
    let path   = relation_path ~branch_name ~mg_name ~rel_name in
    let kv_str = List.map (fun (k, v) -> k ^ "=" ^ v ^ "\n") attrs
                 |> String.concat "" in
    let (rc, hash_opt) =
      Nt_ffi.with_out_string (fun pp -> Nt_ffi.rnt_link_tuple path kv_str pp)
    in
    match (rc, hash_opt) with
    | (0, Some h) -> Ok h
    | _           -> Error (Error.handle_error rc "insert_tuple failed")

  let create_tuple ~branch_name ~mg_name ~rel_name (tuple : Tuple.materialized) :
      (string, Condition.t) result =
    insert_tuple ~branch_name ~mg_name ~rel_name (materialized_to_kv tuple)

  let create_tuples ~branch_name ~mg_name ~rel_name
      (tuples : Tuple.materialized list) : (string, Condition.t) result =
    List.fold_left
      (fun acc t ->
        let* _ = acc in
        let* h = create_tuple ~branch_name ~mg_name ~rel_name t in
        Ok h)
      (Ok "") tuples

  let retract_tuple ~branch_name ~mg_name ~rel_name (hash : string) :
      (unit, Condition.t) result =
    let path = relation_path ~branch_name ~mg_name ~rel_name in
    let rc = Nt_ffi.rnt_unlink_tuple path hash in
    if rc = 0 then Ok () else Error (Error.handle_error rc "retract_tuple failed")

  let relation_root ~(branch_name : string) ~(mg_name : string)
      ~(rel_name : string) : (string, Condition.t) result =
    let path = relation_path ~branch_name ~mg_name ~rel_name in
    let (rc, root_opt) =
      Nt_ffi.with_out_string (fun pp -> Nt_ffi.rnt_relation_root path pp)
    in
    match (rc, root_opt) with
    | (0, Some r) -> Ok r
    | (0, None)   -> Ok ""
    | _           -> Error (Error.handle_error rc "relation_root query failed")

  (* --------------------------------------------------------------------------
     Prelude / catalog bootstrap
     -------------------------------------------------------------------------- *)

  (* Seed the catalog prelude into a brand-new (empty) multigroup.
     Creates all catalog relations as stored relations and inserts the
     self-describing tuples into public:relation and public:attribute. Each
     mutation auto-advances the branch internally; there is no separate
     commit step. *)
  let seed_prelude ~(branch_name : string) (branch_handle : branch_handle)
      (multigroup : Management.Multigroup.multigroup) :
      (branch_handle * Management.Multigroup.multigroup, Condition.t) result =
    let mg_name = multigroup#name in
    let* (branch_handle, multigroup) =
      List.fold_left
        (fun acc (name, schema) ->
          let* (branch_handle, multigroup) = acc in
          create_relation branch_handle multigroup
            ~branch_name ~name ~schema)
        (Ok (branch_handle, multigroup))
        Prelude.Catalog.catalog_definitions
    in
    let rel_tuples =
      List.map
        (fun (name, _) -> Prelude.Catalog.build_relation_tuple name)
        Prelude.Catalog.catalog_definitions
    in
    let* _ =
      create_tuples ~branch_name ~mg_name
        ~rel_name:Prelude.Catalog.relation_rel_name rel_tuples
    in
    let attr_tuples =
      List.concat_map
        (fun (rel_name, schema) ->
          Prelude.Catalog.build_attribute_tuples ~relation_name:rel_name schema)
        Prelude.Catalog.catalog_definitions
    in
    let* _ =
      create_tuples ~branch_name ~mg_name
        ~rel_name:Prelude.Catalog.attribute_rel_name attr_tuples
    in
    Ok (branch_handle, multigroup)

  (* Runtime initialization
     Seeds the catalog of the master branch when it doesn't exist yet; otherwise
     trusts the registry's existing snapshot. The seeded multigroup is
     discarded — each connection's [open_branch] starts from an empty mg
     and the caller threads catalog state from there. Persistence of the
     OCaml-side metadata across process restarts is a separate concern. *)
  let initialize () : (unit, Condition.t) result =
    let runtime_count = Nt_ffi.rnt_init B.driver B.init_arg in
    let* () = if runtime_count <> 0 then Error Error.initialization_error else Ok () in
    let path = "/system/branches/master" in
    ensure_branch path;
    let raw = Nt_ffi.rnt_open_handle path (from_voidp void null) in
    match Nt_ffi.ptr_to_opt raw with
    | None -> Error (Error.object_not_found "branch" "master")
    | Some bh ->
        let* target = branch_target_of_handle bh in
        let result =
          if target = "" then
            let mg =
              new Management.Multigroup.multigroup
                ~name:Prelude.Catalog.prelude_mg
            in
            seed_prelude ~branch_name:"master" bh mg
            |> Result.map ignore
          else Ok ()
        in
        ignore (Nt_ffi.rnt_close_handle (Nt_ffi.nint_to_ptr bh));
        result

  (* --------------------------------------------------------------------------
     Multigroup queries
     -------------------------------------------------------------------------- *)

  let get_relation (mg : Management.Multigroup.multigroup) (name : string) :
      Relation.ephemeral option =
    match mg#get_relation name with
    | None -> None
    | Some rel ->
        let gen : Generator.t =
          match rel#kind with
          | `Ephemeral | `Domain ->
              ((Obj.magic rel) : < generator : Generator.t >)#generator
          | `Pseudo ->
              ((Obj.magic rel)
                : < as_generator :
                      (string * Conventions.AbstractValue.t) list -> Generator.t >)
                #as_generator []
          | `Stored ->
              fun _pos ->
                Generator.Error
                  ("stored relation must be accessed via Nt.execute_query: "
                  ^ rel#name)
        in
        Some
          (new Relation.ephemeral ~name:rel#name ~schema:rel#schema
             ~constraints:rel#constraints ~cardinality:rel#cardinality
             ~membership_criteria:rel#membership_criteria
             ~lineage:(Some rel#lineage) ~provenance:(Some rel#provenance)
             ~generator:gen)

end

module type S = sig
  val initialize   : unit -> (unit, Condition.t) result
  val authenticate : auth_method -> (claims, Condition.t) result

  val session_open      : unit -> (string, Condition.t) result
  val session_close     : string -> (unit, Condition.t) result
  val session_set_branch : string -> string -> string -> (unit, Condition.t) result

  val open_branch             : claims -> string -> (branch_handle * Management.Multigroup.multigroup, Condition.t) result
  val open_snapshot           : claims -> string -> string -> (branch_handle * Management.Multigroup.multigroup, Condition.t) result
  val close_branch            : branch_handle -> (unit, Condition.t) result
  val branch_target_of_handle : branch_handle -> (string, Condition.t) result

  val list_relations          : branch_name:string -> mg_name:string -> ((string * string) list, Condition.t) result
  val list_branch_multigroups : string -> ((string * string) list, Condition.t) result
  val list_snapshot_relations : string -> ((string * string) list, Condition.t) result

  val open_relation  : branch_name:string -> mg_name:string -> rel_name:string -> (relation_handle, Condition.t) result
  val close_relation : relation_handle -> (unit, Condition.t) result

  val execute_query : plan_node -> rel_name:string -> (tuple_stream, Condition.t) result
  val stream_next   : tuple_stream -> (Tuple.materialized option, Condition.t) result
  val stream_close  : tuple_stream -> (unit, Condition.t) result

  val get_relation : Management.Multigroup.multigroup -> string -> Relation.ephemeral option

  val create_multigroup : claims -> string -> (branch_handle * Management.Multigroup.multigroup, Condition.t) result
  val create_relation   : branch_handle -> Management.Multigroup.multigroup -> branch_name:string -> name:string -> schema:Schema.t -> (branch_handle * Management.Multigroup.multigroup, Condition.t) result
  val retract_relation  : branch_handle -> Management.Multigroup.multigroup -> name:string -> (branch_handle * Management.Multigroup.multigroup, Condition.t) result
  val clear_relation    : branch_handle -> Management.Multigroup.multigroup -> branch_name:string -> mg_name:string -> Relation.relation -> (branch_handle * Management.Multigroup.multigroup, Condition.t) result
  val register_domain   : branch_handle -> Management.Multigroup.multigroup -> Relation.domain -> (branch_handle * Management.Multigroup.multigroup, Condition.t) result
  val create_immutable_relation : branch_handle -> Management.Multigroup.multigroup -> name:string -> schema:Schema.t -> generator:Generator.t -> membership_criteria:(Tuple.t -> bool) -> cardinality:Conventions.Cardinality.t -> producer:Relation.producer option -> (branch_handle * Management.Multigroup.multigroup * Relation.relation, Condition.t) result
  val register_constraint : branch_handle -> Management.Multigroup.multigroup -> constraint_name:string -> relation_name:string -> body:Constraint.t -> (branch_handle * Management.Multigroup.multigroup, Condition.t) result

  val create_tuple  : branch_name:string -> mg_name:string -> rel_name:string -> Tuple.materialized -> (string, Condition.t) result
  val create_tuples : branch_name:string -> mg_name:string -> rel_name:string -> Tuple.materialized list -> (string, Condition.t) result
  val retract_tuple : branch_name:string -> mg_name:string -> rel_name:string -> string -> (unit, Condition.t) result

  val insert_tuple    : branch_name:string -> mg_name:string -> rel_name:string -> (string * string) list -> (string, Condition.t) result
  val relation_root   : branch_name:string -> mg_name:string -> rel_name:string -> (string, Condition.t) result
end

module Sqlite = Make (struct
  let driver   = "sqlite"
  let init_arg = ":memory:"
end)

module Memory = Make (struct
  let driver   = "memory"
  let init_arg = ""
end)
