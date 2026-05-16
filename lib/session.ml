(* Session: outermost connection object.
   Each user connection mints a session registered in RNT at
   /system/sessions/<id>.  The session owns exactly one branch at a time;
   DDL / DML / DRL sublanguages operate through that branch. *)

module Make (NT : Nt.S) = struct

  module B = Branch.Make (NT)

  let ( let* ) = Result.bind

  class session
    ~(sid    : string)
    ~(branch : B.branch) =
  object
    val mutable br : B.branch = branch

    method id       = sid
    method branch   = br
    method snapshot = br#snapshot

    (* Switch to a different branch within the same session. *)
    method set_branch (b : B.branch) = br <- b

    method close () =
      let* () = br#close () in
      NT.session_close sid
  end

  (* Open a session on a live (mutable) branch.
     Creates the branch if it does not exist yet. *)
  let open_session claims ~branch_name =
    let* sid    = NT.session_open () in
    let* branch = B.open_branch claims branch_name in
    Ok (new session ~sid ~branch)

  (* Open a session pinned to a specific snapshot hash (detached / read-only).
     Mutations through the returned branch are refused. *)
  let open_snapshot_session claims ~branch_name ~snapshot_hash =
    let* sid    = NT.session_open () in
    let* branch = B.open_snapshot claims ~branch_name ~snapshot_hash in
    (* Inform RNT of the session's snapshot override so future session-path
       resolution (once NamespaceReferenceManager handles /system/sessions/...)
       can route queries through the pinned snapshot. *)
    let* ()     = NT.session_set_branch sid branch_name snapshot_hash in
    Ok (new session ~sid ~branch)

end
