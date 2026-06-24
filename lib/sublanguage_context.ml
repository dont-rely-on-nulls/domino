(** Execution context passed to every sublanguage.

    - [write_handle] NT branch handle; sublanguages use it for mutations
      (create_relation, link_tuple, etc). Read-only sublanguages (DRL) ignore
      it.

    - [branch] Structural view over the active branch — gives every executor
      random-access to every multigroup bound to the branch via [mg_of]. Schema
      lookups for an FQN [{mg; name}] use
      [branch#mg_of mg |> Option.bind (fun m -> m#get_relation name)].

    - [resolve] Builds an absolute RNT path for an FQN. Pass [~branch:b] to
      address a relation on a different live branch (cross-multigroup reads).

    - [switch_branch] Closes the current branch and opens the named one,
      returning the delta describing the new branch's mg set. Provided by the
      listener so VCL does not need direct access to session state. *)

type branch_view =
  < name: string
  ; tip: string
  ; mg_of: string -> Management.Multigroup.multigroup option
  ; multigroups: (string * Management.Multigroup.multigroup) list
  ; add_multigroup: name:string -> unit
  ; set_mg: name:string -> Management.Multigroup.multigroup -> unit
  ; path: ?branch:string -> Qualified_name.t -> string >

type t =
  { write_handle: Nt.branch_handle;
    branch: branch_view;
    resolve: ?branch:string -> Qualified_name.t -> string;
    switch_branch: string -> (Sublanguage_types.transition_delta, Condition.t) result;
    session_hash: string
        (** RNT session hash owning this connection. Session-scoped ephemeral
          relations (with DML [Define]) register under
          /system/sessions/<session_hash>/ephemeral/<name>. *)
  }

(** Absolute RNT path of a session-scoped ephemeral relation. An unqualified
    name in a DRL [Base] resolves here: named bindings shadow nothing since
    stored relations always carry an [mg:] qualifier. *)
let ephemeral_path (ctx : t) (name : string) : string =
  "/system/sessions/" ^ ctx.session_hash ^ "/ephemeral/" ^ name

module Make (NT : Nt.S) = struct
  module Session = Session.Make (NT)
  module Branch = Branch.Make (NT)

  let make_ctx (session : Session.session) (claims : string) : t =
    let br = session#branch in
    { write_handle= br#branch_handle;
      branch= (br :> branch_view);
      resolve= br#path;
      session_hash= session#id;
      switch_branch=
        (fun name ->
          match br#close () with
          | Error e -> Error e
          | Ok () ->
            begin match Branch.open_branch claims name with
            | Error e ->
                begin match Branch.open_branch claims "master" with
                | Ok mbr -> session#set_branch mbr
                | Error _ -> ()
                end;
                Error e
            | Ok new_br -> session#set_branch new_br; Ok new_br#multigroups
            end ) }
end
