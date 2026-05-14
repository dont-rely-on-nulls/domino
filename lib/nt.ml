(* Clean interface to the RNT kernel.
   This is the ONLY module in sakura that may call Nt_ffi. All ctypes types
   are contained here; callers receive plain OCaml values. *)

open Ctypes

(* --------------------------------------------------------------------------
   Shared types — independent of backend, shared across all Make instances
   -------------------------------------------------------------------------- *)

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
  | Join of { left : plan_node; right : plan_node; on_attrs : string list }
  | Take of { limit : int; source : plan_node }

type error =
  | AuthFailed    of string
  | HandleError   of string
  | CursorError   of string
  | NotSupported  of string

let string_of_error = function
  | AuthFailed  s -> "AuthFailed: "   ^ s
  | HandleError s -> "HandleError: "  ^ s
  | CursorError s -> "CursorError: "  ^ s
  | NotSupported s -> "NotSupported: " ^ s

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

  let authenticate (method_ : auth_method) : (claims, error) result =
    let method_str = match method_ with
      | Certificate -> "certificate"
      | PlainText   -> "plain_text"
    in
    let (rc, claims_opt) =
      Nt_ffi.with_out_string (fun pp -> Nt_ffi.rnt_firewall method_str pp)
    in
    match (rc, claims_opt) with
    | (0, Some s) -> Ok s
    | (_, Some s) -> Error (AuthFailed s)
    | _           -> Error (AuthFailed "firewall rejected connection")

  (* --------------------------------------------------------------------------
     Path helpers (shared by branch and relation lifecycle)
     -------------------------------------------------------------------------- *)

  let relation_path (branch_name : string) (rel_name : string) : string =
    "/system/branches/" ^ branch_name ^ "/relations/" ^ rel_name

  (* --------------------------------------------------------------------------
     Branch (multigroup head) lifecycle
     -------------------------------------------------------------------------- *)

  let multigroup_of_payload (name : string) (payload : bytes) :
      (Management.Multigroup.multigroup, error) result =
    if Bytes.length payload = 0 then
      Ok (new Management.Multigroup.multigroup ~name)
    else
      match Management.Multigroup.deserialize payload with
      | Ok mg   -> Ok mg
      | Error m -> Error (HandleError ("multigroup deserialize failed: " ^ m))

  (* After deserializing a branch payload, register each stored relation in
     RNT's ObjectManager and restore its persisted Merkle root so that
     subsequent cursors see the correct tuple set. *)
  let restore_merkle_roots (mg : Management.Multigroup.multigroup) : unit =
    BatMap.String.iter (fun _rel_name (rel : Relation.relation) ->
      match rel#kind with
      | `Stored ->
          let path = relation_path mg#name rel#name in
          ignore (Nt_ffi.rnt_register_relation path);
          let root =
            match
              ((Obj.magic rel)
                : < tree_pointer : Conventions.Hash.t option ; .. >)
              #tree_pointer
            with
            | None   -> ""
            | Some r -> r
          in
          ignore (Nt_ffi.rnt_set_relation_root path root)
      | _ -> ()
    ) mg#relations

  let ensure_branch (path : string) : unit =
    ignore (Nt_ffi.rnt_register_branch path (from_voidp uint8_t null) Unsigned.Size_t.zero)

  let open_branch (claims : claims) (name : string) :
      (branch_handle * Management.Multigroup.multigroup, error) result =
    let path = "/system/branches/" ^ name in
    ensure_branch path;
    let raw = Nt_ffi.rnt_open_handle path (from_voidp void null) in
    match Nt_ffi.ptr_to_opt raw with
    | None -> Error (HandleError ("branch not found: " ^ name))
    | Some bh ->
        ignore claims;
        let pp  = allocate (ptr uint8_t) (from_voidp uint8_t null) in
        let lp  = allocate size_t Unsigned.Size_t.zero in
        let rc  = Nt_ffi.rnt_branch_payload (Nt_ffi.nint_to_ptr bh) pp lp in
        if rc <> 0 then Error (HandleError "could not read branch payload")
        else
          let payload = Nt_ffi.consume_uint8_array !@pp !@lp in
          (match multigroup_of_payload name payload with
           | Error e -> Error e
           | Ok mg   ->
               restore_merkle_roots mg;
               Ok (bh, mg))

  let close_branch (bh : branch_handle) : (unit, error) result =
    let rc = Nt_ffi.rnt_close_handle (Nt_ffi.nint_to_ptr bh) in
    if rc = 0 then Ok () else Error (HandleError "close_branch failed")

  let commit_branch (bh : branch_handle) (mg : Management.Multigroup.multigroup) :
      (unit, error) result =
    let storable = mg#serialize () in
    let payload  = Storable.Multigroup.to_bytes storable in
    let (p, len) = Nt_ffi.bytes_to_uint8_array payload in
    let rc = Nt_ffi.rnt_branch_set_payload (Nt_ffi.nint_to_ptr bh) p len in
    if rc = 0 then Ok () else Error (HandleError "commit_branch failed")

  (* --------------------------------------------------------------------------
     Relation handles
     -------------------------------------------------------------------------- *)

  let open_relation (branch_name : string) (rel_name : string) :
      (relation_handle, error) result =
    let path = relation_path branch_name rel_name in
    ignore (Nt_ffi.rnt_register_relation path);
    let raw = Nt_ffi.rnt_open_handle path (from_voidp void null) in
    match Nt_ffi.ptr_to_opt raw with
    | None -> Error (HandleError ("relation not found: " ^ rel_name))
    | Some rh -> Ok rh

  let close_relation (rh : relation_handle) : (unit, error) result =
    let rc = Nt_ffi.rnt_close_handle (Nt_ffi.nint_to_ptr rh) in
    if rc = 0 then Ok () else Error (HandleError "close_relation failed")

  (* --------------------------------------------------------------------------
     Query execution via the Tarski VM plan builder
     -------------------------------------------------------------------------- *)

  (* Recursively builds a C-side PlanWrapper from an OCaml plan_node tree.
     On partial failure the partially-built plan is freed before returning Error. *)
  let rec build_plan (plan : plan_node) : (nativeint, error) result =
    match plan with
    | Scan { path; _ } ->
        let raw = Nt_ffi.rnt_plan_scan path in
        (match Nt_ffi.ptr_to_opt raw with
         | None   -> Error (CursorError ("plan_scan failed for path: " ^ path))
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
              | None   -> Error (CursorError "plan_join failed")
              | Some p -> Ok p))
    | Take { limit; source } ->
        let* sp = build_plan source in
        let raw = Nt_ffi.rnt_plan_take
                    (Nt_ffi.nint_to_ptr sp)
                    (Unsigned.Size_t.of_int limit) in
        (* take takes ownership of source — no manual free needed *)
        (match Nt_ffi.ptr_to_opt raw with
         | None   -> Error (CursorError "plan_take failed")
         | Some p -> Ok p)

  let execute_query (_bh : branch_handle) (plan : plan_node) ~(rel_name : string) :
      (tuple_stream, error) result =
    let* plan_ptr = build_plan plan in
    let raw = Nt_ffi.rnt_vm_execute_plan (Nt_ffi.nint_to_ptr plan_ptr) in
    match Nt_ffi.ptr_to_opt raw with
    | None           -> Error (CursorError "vm_execute_plan failed")
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
      (Tuple.materialized option, error) result =
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
    | _ -> Error (CursorError "cursor_next error")

  let stream_close (stream : tuple_stream) : (unit, error) result =
    let close_fn = match stream.ts_kind with
      | Simple -> Nt_ffi.rnt_cursor_close
      | Vm     -> Nt_ffi.rnt_vm_cursor_close
    in
    let rc = close_fn (Nt_ffi.nint_to_ptr stream.ts_cursor) in
    if rc = 0 then Ok () else Error (CursorError "cursor_close failed")

  (* --------------------------------------------------------------------------
     Tuple storage helpers
     -------------------------------------------------------------------------- *)

  let materialized_to_kv (tuple : Tuple.materialized) : (string * string) list =
    Tuple.AttributeMap.fold
      (fun k v acc -> (k, Obj.obj v.Attribute.value) :: acc)
      tuple.Tuple.attributes []

  (* --------------------------------------------------------------------------
     Multigroup / branch mutations
     -------------------------------------------------------------------------- *)

  let create_multigroup (claims : claims) (name : string) :
      (branch_handle * Management.Multigroup.multigroup, error) result =
    open_branch claims name

  let create_relation (bh : branch_handle) (mg : Management.Multigroup.multigroup)
      ~(branch_name : string) ~(name : string) ~(schema : Schema.t) :
      (branch_handle * Management.Multigroup.multigroup, error) result =
    let path = relation_path branch_name name in
    let rc   = Nt_ffi.rnt_register_relation path in
    if rc <> 0 then Error (HandleError ("register_relation failed: " ^ name))
    else
      let rel = new Relation.stored
        ~name
        ~schema
        ~constraints:None
        ~cardinality:Conventions.Cardinality.AlephZero
        ~lineage:(Some (Relation.Lineage.Base name))
        ~provenance:(Some (Relation.Provenance.base ~relation:name
                             ~attributes:(List.map fst schema)))
        ~membership_criteria:(fun _ -> true)
      in
      let new_mg = mg#add_relation (rel :> Relation.relation) in
      let* () = commit_branch bh new_mg in
      Ok (bh, new_mg)

  let retract_relation (bh : branch_handle) (mg : Management.Multigroup.multigroup)
      ~(name : string) :
      (branch_handle * Management.Multigroup.multigroup, error) result =
    let new_mg = mg#remove_relation name in
    let* () = commit_branch bh new_mg in
    Ok (bh, new_mg)

  let clear_relation (bh : branch_handle) (mg : Management.Multigroup.multigroup)
      (rel : Relation.relation) :
      (branch_handle * Management.Multigroup.multigroup, error) result =
    let path = relation_path mg#name rel#name in
    let rc = Nt_ffi.rnt_clear_relation path in
    if rc <> 0 then Error (HandleError ("clear_relation failed: " ^ rel#name))
    else Ok (bh, mg)

  let register_domain (bh : branch_handle) (mg : Management.Multigroup.multigroup)
      (domain : Relation.domain) :
      (branch_handle * Management.Multigroup.multigroup, error) result =
    let new_mg = mg#add_relation (domain :> Relation.relation) in
    let* () = commit_branch bh new_mg in
    Ok (bh, new_mg)

  let create_immutable_relation (bh : branch_handle)
      (mg : Management.Multigroup.multigroup) ~name ~schema ~generator
      ~membership_criteria ~cardinality ~(producer : Relation.producer option) :
      (branch_handle * Management.Multigroup.multigroup * Relation.relation, error) result =
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
    let new_mg = mg#add_relation rel in
    let* () = commit_branch bh new_mg in
    Ok (bh, new_mg, rel)

  let register_constraint (bh : branch_handle)
      (mg : Management.Multigroup.multigroup) ~constraint_name ~relation_name
      ~(body : Constraint.t) :
      (branch_handle * Management.Multigroup.multigroup, error) result =
    match mg#get_relation relation_name with
    | None -> Error (HandleError ("relation not found: " ^ relation_name))
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
        let new_mg = mg#update_relation (updated_rel :> Relation.relation) in
        let* () = commit_branch bh new_mg in
        Ok (bh, new_mg)

  (* --------------------------------------------------------------------------
     Tuple mutations
     -------------------------------------------------------------------------- *)

  let insert_tuple (branch_name : string) (rel_name : string)
      (attrs : (string * string) list) : (string, error) result =
    let path   = relation_path branch_name rel_name in
    let kv_str = List.map (fun (k, v) -> k ^ "=" ^ v ^ "\n") attrs
                 |> String.concat "" in
    let (rc, hash_opt) =
      Nt_ffi.with_out_string (fun pp -> Nt_ffi.rnt_link_tuple path kv_str pp)
    in
    match (rc, hash_opt) with
    | (0, Some h) -> Ok h
    | _           -> Error (HandleError "insert_tuple failed")

  let create_tuple ~branch_name ~rel_name (tuple : Tuple.materialized) :
      (string, error) result =
    insert_tuple branch_name rel_name (materialized_to_kv tuple)

  let create_tuples ~branch_name ~rel_name (tuples : Tuple.materialized list) :
      (string list, error) result =
    List.fold_left
      (fun acc t ->
        let* hashes = acc in
        let* h = create_tuple ~branch_name ~rel_name t in
        Ok (hashes @ [h]))
      (Ok []) tuples

  let retract_tuple ~branch_name ~rel_name (hash : string) :
      (unit, error) result =
    let path = relation_path branch_name rel_name in
    let rc = Nt_ffi.rnt_unlink_tuple path hash in
    if rc = 0 then Ok () else Error (HandleError "retract_tuple failed")

  let relation_root (branch_name : string) (rel_name : string) :
      (string, error) result =
    let path = relation_path branch_name rel_name in
    let (rc, root_opt) =
      Nt_ffi.with_out_string (fun pp -> Nt_ffi.rnt_relation_root path pp)
    in
    match (rc, root_opt) with
    | (0, Some r) -> Ok r
    | _           -> Error (HandleError "relation_root query failed")

  let set_relation_root (branch_name : string) (rel_name : string) (root : string) :
      (unit, error) result =
    let path = relation_path branch_name rel_name in
    let rc = Nt_ffi.rnt_set_relation_root path root in
    if rc = 0 then Ok () else Error (HandleError "set_relation_root failed")

  (* --------------------------------------------------------------------------
     Prelude / catalog bootstrap
     -------------------------------------------------------------------------- *)

  (* After bulk tuple insertion, reads each stored relation's current Merkle
     root from RNT and stamps it into the multigroup, then commits. This
     ensures the persisted payload carries real tree_pointers so that
     restore_merkle_roots on the next boot does not wipe the Merkle tree. *)
  let sync_merkle_roots_and_commit (bh : branch_handle)
      (mg : Management.Multigroup.multigroup) :
      (branch_handle * Management.Multigroup.multigroup, error) result =
    let rels = mg#relations in
    let* mg' =
      BatMap.String.fold
        (fun _name (rel : Relation.relation) acc ->
          let* cur_mg = acc in
          match rel#kind with
          | `Stored ->
              let* root = relation_root cur_mg#name rel#name in
              if root = "" then Ok cur_mg
              else
                let updated =
                  ((Obj.magic rel)
                    : < set_tree_pointer :
                          Conventions.Hash.t option -> Relation.stored ; .. >)
                  #set_tree_pointer (Some root)
                in
                Ok (cur_mg#update_relation (updated :> Relation.relation))
          | _ -> Ok cur_mg)
        rels (Ok mg)
    in
    let* () = commit_branch bh mg' in
    Ok (bh, mg')

  (* Seed the catalog prelude into a brand-new (empty) multigroup.
     Creates all catalog relations as stored relations, inserts the
     self-describing tuples into public:relation and public:attribute,
     then syncs Merkle roots so the payload is durable across restarts. *)
  let seed_prelude (bh : branch_handle) (mg : Management.Multigroup.multigroup) :
      (branch_handle * Management.Multigroup.multigroup, error) result =
    let* (bh, mg) =
      List.fold_left
        (fun acc (name, schema) ->
          let* (bh, mg) = acc in
          create_relation bh mg ~branch_name:mg#name ~name ~schema)
        (Ok (bh, mg))
        Prelude.Catalog.catalog_definitions
    in
    let rel_tuples =
      List.map
        (fun (name, _) -> Prelude.Catalog.build_relation_tuple name)
        Prelude.Catalog.catalog_definitions
    in
    let* _ =
      create_tuples ~branch_name:mg#name
        ~rel_name:Prelude.Catalog.relation_rel_name rel_tuples
    in
    let attr_tuples =
      List.concat_map
        (fun (rel_name, schema) ->
          Prelude.Catalog.build_attribute_tuples ~relation_name:rel_name schema)
        Prelude.Catalog.catalog_definitions
    in
    let* _ =
      create_tuples ~branch_name:mg#name
        ~rel_name:Prelude.Catalog.attribute_rel_name attr_tuples
    in
    sync_merkle_roots_and_commit bh mg

  (* Seed or restore the master branch once, before any connections are
     accepted.  Called by init() via do_init_ref so that seeding is
     single-threaded and races between concurrent connection-open calls
     are impossible. *)
  let do_init () : (unit, error) result =
    let path = "/system/branches/master" in
    let raw = Nt_ffi.rnt_open_handle path (from_voidp void null) in
    match Nt_ffi.ptr_to_opt raw with
    | None -> Error (HandleError "init: could not open master branch")
    | Some bh ->
        let pp = allocate (ptr uint8_t) (from_voidp uint8_t null) in
        let lp = allocate size_t Unsigned.Size_t.zero in
        (* TODO: consider using a struct type, rather than a bag of bytes *)
        let rc = Nt_ffi.rnt_branch_payload (Nt_ffi.nint_to_ptr bh) pp lp in
        if rc <> 0 then (
          ignore (Nt_ffi.rnt_close_handle (Nt_ffi.nint_to_ptr bh));
          Error (HandleError "init: could not read master branch payload")
        ) else
          let payload = Nt_ffi.consume_uint8_array !@pp !@lp in
          let result =
            if Bytes.length payload = 0 then
              let mg = new Management.Multigroup.multigroup ~name:"master" in
              seed_prelude bh mg
            else
              (match Management.Multigroup.deserialize payload with
               | Error m -> Error (HandleError ("init: deserialize failed: " ^ m))
               | Ok mg ->
                   restore_merkle_roots mg;
                   Ok (bh, mg))
          in
          ignore (Nt_ffi.rnt_close_handle (Nt_ffi.nint_to_ptr bh));
          (match result with
           | Ok _ -> Ok ()
           | Error e -> Error e)

  (* --------------------------------------------------------------------------
     Runtime initialisation
     -------------------------------------------------------------------------- *)

  let init () =
    let rc = Nt_ffi.rnt_init B.driver B.init_arg in
    if rc <> 0 then Error (HandleError "rnt_init failed")
    else do_init ()

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

(* --------------------------------------------------------------------------
   Module type — output signature of Make; used by executors and listener
   -------------------------------------------------------------------------- *)

module type S = sig
  val init         : unit -> (unit, error) result
  val authenticate : auth_method -> (claims, error) result

  val open_branch  : claims -> string -> (branch_handle * Management.Multigroup.multigroup, error) result
  val close_branch : branch_handle -> (unit, error) result
  val commit_branch: branch_handle -> Management.Multigroup.multigroup -> (unit, error) result

  val open_relation  : string -> string -> (relation_handle, error) result
  val close_relation : relation_handle -> (unit, error) result

  val execute_query : branch_handle -> plan_node -> rel_name:string -> (tuple_stream, error) result
  val stream_next   : tuple_stream -> (Tuple.materialized option, error) result
  val stream_close  : tuple_stream -> (unit, error) result

  val get_relation : Management.Multigroup.multigroup -> string -> Relation.ephemeral option

  val create_multigroup : claims -> string -> (branch_handle * Management.Multigroup.multigroup, error) result
  val create_relation   : branch_handle -> Management.Multigroup.multigroup -> branch_name:string -> name:string -> schema:Schema.t -> (branch_handle * Management.Multigroup.multigroup, error) result
  val retract_relation  : branch_handle -> Management.Multigroup.multigroup -> name:string -> (branch_handle * Management.Multigroup.multigroup, error) result
  val clear_relation    : branch_handle -> Management.Multigroup.multigroup -> Relation.relation -> (branch_handle * Management.Multigroup.multigroup, error) result
  val register_domain   : branch_handle -> Management.Multigroup.multigroup -> Relation.domain -> (branch_handle * Management.Multigroup.multigroup, error) result
  val create_immutable_relation : branch_handle -> Management.Multigroup.multigroup -> name:string -> schema:Schema.t -> generator:Generator.t -> membership_criteria:(Tuple.t -> bool) -> cardinality:Conventions.Cardinality.t -> producer:Relation.producer option -> (branch_handle * Management.Multigroup.multigroup * Relation.relation, error) result
  val register_constraint : branch_handle -> Management.Multigroup.multigroup -> constraint_name:string -> relation_name:string -> body:Constraint.t -> (branch_handle * Management.Multigroup.multigroup, error) result

  val create_tuple  : branch_name:string -> rel_name:string -> Tuple.materialized -> (string, error) result
  val create_tuples : branch_name:string -> rel_name:string -> Tuple.materialized list -> (string list, error) result
  val retract_tuple : branch_name:string -> rel_name:string -> string -> (unit, error) result

  val insert_tuple    : string -> string -> (string * string) list -> (string, error) result
  val relation_root   : string -> string -> (string, error) result
  val set_relation_root : string -> string -> string -> (unit, error) result
end

(* --------------------------------------------------------------------------
   Concrete instances
   -------------------------------------------------------------------------- *)

module Sqlite = Make (struct
  let driver   = "sqlite"
  let init_arg = ":memory:"
end)

module Memory = Make (struct
  let driver   = "memory"
  let init_arg = ""
end)
