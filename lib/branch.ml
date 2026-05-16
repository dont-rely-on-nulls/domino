(* Branch: the effectful session layer.
   Owns the RNT branch handle and is the sole entry point for mutations.
   Multigroup and Relation objects are pure mirrors updated here after
   each FFI call. *)

module Make (NT : Nt.S) = struct

  let ( let* ) = Result.bind

  type mode =
    | Live                (* tracking branch tip; mutations allowed *)
    | Detached of string  (* pinned to snapshot hash; read-only    *)

  class branch
    ~(init_bh   : Nt.branch_handle)
    ~(init_mg   : Management.Multigroup.multigroup)
    ~(init_mode : mode) =
  object (self)
    val mutable bh   : Nt.branch_handle                  = init_bh
    val mutable mg   : Management.Multigroup.multigroup  = init_mg
    val mutable mode : mode                              = init_mode

    method name           = mg#name
    method snapshot       = mg#hash
    method mg             = mg
    method mode           = mode
    (* Exposed for legacy sublanguage callers that need the raw NT handle.
       Prefer routing mutations through the branch methods instead. *)
    method branch_handle  = bh

    (* Path for a relation on this branch (Live) or its pinned snapshot
       (Detached).  Pass ~branch_name to reference a relation on a different
       live branch (cross-branch joins). *)
    method relation_path ?(branch_name : string option) (rel_name : string) =
      match branch_name with
      | Some b -> "/system/branches/" ^ b ^ "/relations/" ^ rel_name
      | None ->
          match mode with
          | Live ->
              "/system/branches/" ^ mg#name ^ "/relations/" ^ rel_name
          | Detached hash ->
              "/system/snapshots/" ^ hash ^ "/relations/" ^ rel_name

    method private assert_live op =
      match mode with
      | Live         -> Ok ()
      | Detached _   ->
          Error (Nt.NotSupported (op ^ ": detached snapshot is read-only"))

    method private sync_hash () =
      match mode with
      | Detached _ -> ()
      | Live ->
          match NT.branch_target_of_handle bh with
          | Error _  -> ()
          | Ok h     -> mg <- mg#with_hash h

    method create_relation ~name ~schema =
      let* () = self#assert_live "create_relation" in
      let* (bh', mg') =
        NT.create_relation bh mg ~branch_name:mg#name ~name ~schema
      in
      bh <- bh'; mg <- mg'; Ok ()

    method retract_relation ~name =
      let* () = self#assert_live "retract_relation" in
      let* (bh', mg') = NT.retract_relation bh mg ~name in
      bh <- bh'; mg <- mg'; Ok ()

    method clear_relation rel_name =
      let* () = self#assert_live "clear_relation" in
      match mg#get_relation rel_name with
      | None     -> Error (Nt.HandleError ("relation not found: " ^ rel_name))
      | Some rel ->
          let* (bh', mg') = NT.clear_relation bh mg rel in
          bh <- bh'; mg <- mg'; Ok ()

    method register_domain domain =
      let* () = self#assert_live "register_domain" in
      let* (bh', mg') = NT.register_domain bh mg domain in
      bh <- bh'; mg <- mg'; Ok ()

    method create_immutable_relation ~name ~schema ~generator
        ~membership_criteria ~cardinality ~producer =
      let* () = self#assert_live "create_immutable_relation" in
      let* (bh', mg', rel) =
        NT.create_immutable_relation bh mg ~name ~schema ~generator
          ~membership_criteria ~cardinality ~producer
      in
      bh <- bh'; mg <- mg'; Ok rel

    method register_constraint ~constraint_name ~relation_name ~body =
      let* () = self#assert_live "register_constraint" in
      let* (bh', mg') =
        NT.register_constraint bh mg ~constraint_name ~relation_name ~body
      in
      bh <- bh'; mg <- mg'; Ok ()

    method insert_tuple ~rel_name attrs =
      let* () = self#assert_live "insert_tuple" in
      let* hash = NT.insert_tuple mg#name rel_name attrs in
      self#sync_hash (); Ok hash

    method create_tuple ~rel_name tuple =
      let* () = self#assert_live "create_tuple" in
      let* hash = NT.create_tuple ~branch_name:mg#name ~rel_name tuple in
      self#sync_hash (); Ok hash

    method create_tuples ~rel_name tuples =
      let* () = self#assert_live "create_tuples" in
      let* hash = NT.create_tuples ~branch_name:mg#name ~rel_name tuples in
      self#sync_hash (); Ok hash

    method retract_tuple ~rel_name ~tuple_hash =
      let* () = self#assert_live "retract_tuple" in
      let* () = NT.retract_tuple ~branch_name:mg#name ~rel_name tuple_hash in
      self#sync_hash (); Ok ()

    (* Used by the listener to sync back an mg returned by a sublanguage
       Transition result.  Remove once all sublanguages route mutations
       through branch methods instead of calling NT directly. *)
    method refresh_mg (new_mg : Management.Multigroup.multigroup) =
      mg <- new_mg;
      self#sync_hash ()

    method close () = NT.close_branch bh
  end

  let open_branch claims name =
    let* (bh, mg) = NT.open_branch claims name in
    Ok (new branch ~init_bh:bh ~init_mg:mg ~init_mode:Live)

  let open_snapshot claims ~branch_name ~snapshot_hash =
    let* (bh, mg) = NT.open_snapshot claims branch_name snapshot_hash in
    Ok (new branch ~init_bh:bh ~init_mg:mg ~init_mode:(Detached snapshot_hash))

end
