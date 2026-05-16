(** Execution context passed to every sublanguage.
    Replaces the old (bh, mg) pair with explicit, named roles:

    - [write_handle]  NT branch handle; sublanguages use it for mutations
                      (create_relation, link_tuple, &c).  Read-only
                      sublanguages (DRL) ignore it.

    - [resolve]       Maps a relation name to its full RNT path.
                      [resolve None rel]        → write target (or pinned
                                                  snapshot if detached)
                      [resolve (Some branch) r] → explicit branch, enabling
                                                  cross-multigroup reads
                                                  without switching context.

    - [schema_cache]  In-memory mirror of the write target's current snapshot
                      (formerly called [db] or [mg]).  Used for schema
                      lookups only — NOT the scope restriction.  Sublanguages
                      must not assume the world is limited to relations
                      present in this cache. *)

type t = {
  write_handle  : Nt.branch_handle;
  resolve       : string option -> string -> string;
  schema_cache  : Management.Multigroup.multigroup;
  (* Switches the session's active branch (write target) to the named branch.
     Closes the current branch, opens the new one, updates the session, and
     returns the new branch's schema_cache.  Provided as a closure by the
     listener so sublanguages (VCL) can perform branch switches without
     needing direct access to session state or auth claims. *)
  switch_branch : string -> (Management.Multigroup.multigroup, Nt.error) result;
}
