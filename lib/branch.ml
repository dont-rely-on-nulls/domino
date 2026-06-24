(* Branch: the effectful session layer.
   Owns the RNT branch handle and the in-memory mirror of every multigroup
   bound to that branch.  Each mutation names its target mg explicitly. *)

module Make (NT : Nt.S) = struct
  module Error = struct
    open Condition

    (* TODO: more structure *)
    let not_supported msg = condition "not-supported" msg empty
    let handle_error msg = condition "handle-error" msg empty
  end

  let ( let* ) = Result.bind

  type mode =
    | Live (* tracking branch tip; mutations allowed *)
    | Detached of string (* pinned to snapshot hash; read-only    *)

  class branch ~(init_bh : Nt.branch_handle) ~(init_name : string) ~(init_tip : string)
    ~(init_mgs : (string * Management.Multigroup.multigroup) list) ~(init_mode : mode) =
    object (self)
      val mutable bh : Nt.branch_handle = init_bh
      val name : string = init_name
      val mutable tip : string = init_tip
      val mutable mode : mode = init_mode

      val mgs : (string, Management.Multigroup.multigroup) Hashtbl.t =
        let t = Hashtbl.create 8 in
        List.iter (fun (n, mg) -> Hashtbl.replace t n mg) init_mgs;
        t

      method name = name
      method tip = tip
      method snapshot = tip
      method mode = mode
      method branch_handle = bh
      method mg_of (n : string) : Management.Multigroup.multigroup option = Hashtbl.find_opt mgs n

      method multigroups : (string * Management.Multigroup.multigroup) list =
        Hashtbl.to_seq mgs |> List.of_seq

      (* Register a fresh mg under this branch.  Idempotent. *)
      method add_multigroup ~(name : string) : unit =
        if not (Hashtbl.mem mgs name) then
          Hashtbl.add mgs name (new Management.Multigroup.multigroup ~name)

      (* Replace the cached mg value (e.g. after an NT mutation returned a
       new mg).  Idempotent insert if the mg was not present. *)
      method set_mg ~(name : string) (mg : Management.Multigroup.multigroup) : unit =
        Hashtbl.replace mgs name mg

      (* Path for a relation in this branch (Live) or its pinned snapshot
       (Detached).  Pass [~branch] to reference a relation on a different
       live branch (cross-multigroup reads). *)
      method path ?(branch : string option) (fqn : Qualified_name.t) : string =
        match branch with
        | Some b -> "/system/branches/" ^ b ^ "/multigroups/" ^ fqn.mg ^ "/relations/" ^ fqn.name
        | None -> (
          match mode with
          | Live -> "/system/branches/" ^ name ^ "/multigroups/" ^ fqn.mg ^ "/relations/" ^ fqn.name
          | Detached hash -> "/system/snapshots/" ^ hash ^ "/relations/" ^ fqn.name )

      method private assert_live op =
        match mode with
        | Live -> Ok ()
        | Detached _ -> Error (Error.not_supported (op ^ ": detached snapshot is read-only"))

      method private sync_tip () =
        match mode with
        | Detached _ -> ()
        | Live -> ( match NT.branch_target_of_handle bh with Error _ -> () | Ok h -> tip <- h )

      method private get_mg op (mg_name : string) :
          (Management.Multigroup.multigroup, Condition.t) result =
        match Hashtbl.find_opt mgs mg_name with
        | Some mg -> Ok mg
        | None -> Error (Error.handle_error (op ^ ": multigroup not found: " ^ mg_name))

      method create_relation ~(mg : string) ~(rel_name : string) ~schema =
        let* () = self#assert_live "create_relation" in
        let* mg_val = self#get_mg "create_relation" mg in
        let* bh', mg' = NT.create_relation bh mg_val ~branch_name:name ~name:rel_name ~schema in
        bh <- bh';
        Hashtbl.replace mgs mg mg';
        self#sync_tip ();
        Ok ()

      method retract_relation ~(mg : string) ~(rel_name : string) =
        let* () = self#assert_live "retract_relation" in
        let* mg_val = self#get_mg "retract_relation" mg in
        let* bh', mg' = NT.retract_relation bh mg_val ~name:rel_name in
        bh <- bh';
        Hashtbl.replace mgs mg mg';
        self#sync_tip ();
        Ok ()

      method clear_relation ~(mg : string) ~(rel_name : string) =
        let* () = self#assert_live "clear_relation" in
        let* mg_val = self#get_mg "clear_relation" mg in
        match mg_val#get_relation rel_name with
        | None -> Error (Error.handle_error ("relation not found: " ^ rel_name))
        | Some rel ->
            let* bh', mg' = NT.clear_relation bh mg_val ~branch_name:name ~mg_name:mg rel in
            bh <- bh';
            Hashtbl.replace mgs mg mg';
            self#sync_tip ();
            Ok ()

      method register_domain ~(mg : string) domain =
        let* () = self#assert_live "register_domain" in
        let* mg_val = self#get_mg "register_domain" mg in
        let* bh', mg' = NT.register_domain bh mg_val domain in
        bh <- bh';
        Hashtbl.replace mgs mg mg';
        Ok ()

      method create_immutable_relation ~(mg : string) ~name:rel_name ~schema ~generator
          ~membership_criteria ~cardinality ~producer =
        let* () = self#assert_live "create_immutable_relation" in
        let* mg_val = self#get_mg "create_immutable_relation" mg in
        let* bh', mg', rel =
          NT.create_immutable_relation bh mg_val ~name:rel_name ~schema ~generator
            ~membership_criteria ~cardinality ~producer
        in
        bh <- bh';
        Hashtbl.replace mgs mg mg';
        Ok rel

      method register_constraint ~(mg : string) ~constraint_name ~relation_name ~body =
        let* () = self#assert_live "register_constraint" in
        let* mg_val = self#get_mg "register_constraint" mg in
        let* bh', mg' = NT.register_constraint bh mg_val ~constraint_name ~relation_name ~body in
        bh <- bh';
        Hashtbl.replace mgs mg mg';
        Ok ()

      method insert_tuple ~(mg : string) ~(rel_name : string) attrs =
        let* () = self#assert_live "insert_tuple" in
        let* hash = NT.insert_tuple ~branch_name:name ~mg_name:mg ~rel_name attrs in
        self#sync_tip (); Ok hash

      method create_tuple ~(mg : string) ~(rel_name : string) tuple =
        let* () = self#assert_live "create_tuple" in
        let* hash = NT.create_tuple ~branch_name:name ~mg_name:mg ~rel_name tuple in
        self#sync_tip (); Ok hash

      method create_tuples ~(mg : string) ~(rel_name : string) tuples =
        let* () = self#assert_live "create_tuples" in
        let* hash = NT.create_tuples ~branch_name:name ~mg_name:mg ~rel_name tuples in
        self#sync_tip (); Ok hash

      method retract_tuple ~(mg : string) ~(rel_name : string) ~tuple_hash =
        let* () = self#assert_live "retract_tuple" in
        let* () = NT.retract_tuple ~branch_name:name ~mg_name:mg ~rel_name tuple_hash in
        self#sync_tip (); Ok ()

      (* Applies a Transition delta from a sublanguage executor.  Each entry
       replaces the named mg in the cache, then the tip is re-read. *)
      method apply_delta (delta : (string * Management.Multigroup.multigroup) list) =
        List.iter (fun (mg_name, mg_val) -> Hashtbl.replace mgs mg_name mg_val) delta;
        self#sync_tip ()

      method close () = NT.close_branch bh
    end

  (* Default mg name seeded into an unborn branch so the session can write
     into something.  Real catalog seeding (under "sakura") is the prelude's
     responsibility. *)
  let default_mg_name = "public"

  let open_branch claims branch_name =
    let* bh, _legacy_mg = NT.open_branch claims branch_name in
    let tip = match NT.branch_target_of_handle bh with Ok h -> h | Error _ -> "" in
    let* mg_entries = NT.list_branch_multigroups branch_name in
    let mgs =
      List.map
        (fun (mg_name, _mg_hash) ->
          (* For each bound mg, enumerate its relations from the kernel and
             rebuild the OCaml mirror.  Schemas are not persisted in the
             snapshot codec — they are repopulated by DDL replay / catalog
             reads later. *)
          let rels =
            match NT.list_relations ~branch_name ~mg_name with Ok pairs -> pairs | Error _ -> []
          in
          let mg =
            List.fold_left
              (fun mg (rel_name, root) ->
                let rel =
                  (new Relation.stored
                     ~name:rel_name ~schema:[] ~constraints:None
                     ~cardinality:Conventions.Cardinality.AlephZero
                     ~lineage:(Some (Relation.Lineage.Base rel_name))
                     ~provenance:(Some (Relation.Provenance.base ~relation:rel_name ~attributes:[]))
                     ~membership_criteria:(fun _ -> true) )
                    #set_tree_pointer
                    (Some root)
                in
                mg#add_relation (rel :> Relation.relation) )
              (new Management.Multigroup.multigroup ~name:mg_name)
              rels
          in
          mg_name, mg )
        mg_entries
    in
    let mgs =
      if mgs = [] then [default_mg_name, new Management.Multigroup.multigroup ~name:default_mg_name]
      else mgs
    in
    Ok (new branch ~init_bh:bh ~init_name:branch_name ~init_tip:tip ~init_mgs:mgs ~init_mode:Live)

  let open_snapshot claims ~branch_name ~snapshot_hash =
    let* bh, mg = NT.open_snapshot claims branch_name snapshot_hash in
    Ok
      (new branch
         ~init_bh:bh ~init_name:branch_name ~init_tip:snapshot_hash
         ~init_mgs:[mg#name, mg]
         ~init_mode:(Detached snapshot_hash) )
end
