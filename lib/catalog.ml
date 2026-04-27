(** Multi-multigroup catalog.

    Each multigroup gets its own [Atomic.t], so mutations to independent
    multigroups never contend.  The [sakura] multigroup is auto-bootstrapped,
    undeletable, and self-referential: it carries a [public:multigroup]
    relation listing every registered multigroup. *)

type t = {
  multigroups : Management.Database.t Atomic.t Utilities.StringMap.t Atomic.t;
  default_multigroup : string;
  mutex : Mutex.t;
}

let sakura_name = "sakura"

module Make (S : Management.Physical.S with type error = string) = struct
  open Utilities.Result
  module Manip = Manipulation.Make (S)

  type nonrec t = t

  let map_error r = Result.map_error
    (fun e -> Sexplib.Sexp.to_string (Error.sexp_of_error e)) r

  let get catalog name =
    Utilities.StringMap.find_opt name (Atomic.get catalog.multigroups)

  let list catalog =
    Utilities.StringMap.bindings (Atomic.get catalog.multigroups)
    |> List.map fst

  let publish_multigroups catalog ~expected ~updated =
    if Atomic.compare_and_set catalog.multigroups expected updated then Ok ()
    else Error "Concurrent multigroup map update detected"

  (** Swallow errors: a missing prelude relation is non-fatal. *)
  let register_prelude_relation storage db (rel : Relation.t) =
    match
      Manip.create_immutable_relation storage db ~name:rel.name
        ~schema:rel.schema ~generator:(Option.get rel.generator)
        ~membership_criteria:rel.membership_criteria
        ~cardinality:rel.cardinality
    with
    | Ok (new_db, _) -> new_db
    | Error e ->
        Printf.eprintf "Warning: prelude relation %s: %s\n%!"
          rel.name (Sexplib.Sexp.to_string (Error.sexp_of_error e));
        db

  (** Create DB + seed catalog + fold in [prelude_relations]. *)
  let bootstrap_multigroup storage ~prelude_relations ~name =
    let* db = Manip.create_database storage ~name |> map_error in
    Ok (List.fold_left (register_prelude_relation storage) db prelude_relations)

  (** Create additional catalog relations beyond the base six.
      Used to seed [public:multigroup] into the sakura multigroup only. *)
  let seed_extra_catalog storage ~definitions db =
    List.fold_left
      (fun acc (name, schema) ->
        let* db = acc in
        let* new_db, _ =
          Manip.create_relation storage db ~name ~schema |> map_error
        in
        Ok new_db)
      (Ok db) definitions

  (** Write a tuple into sakura's [public:multigroup] and update the ref.
      No-op if the relation doesn't exist yet (bootstrap ordering). *)
  let register_in_sakura storage sakura_ref mg_name =
    let rec go () =
      let sakura = Atomic.get sakura_ref in
      match
        Management.Database.get_relation sakura Prelude.Catalog.multigroup_rel_name
      with
      | None -> Ok ()
      | Some mg_rel ->
          let tuple = Prelude.Catalog.build_multigroup_tuple mg_name in
          let* new_sakura, _, _ =
            Manip.create_tuple storage sakura mg_rel tuple |> map_error
          in
          if Atomic.compare_and_set sakura_ref sakura new_sakura then Ok ()
          else go ()
    in
    go ()

  let create storage ~prelude_relations =
    let* sakura_db =
      bootstrap_multigroup storage ~prelude_relations ~name:sakura_name
    in
    let* sakura_db =
      seed_extra_catalog storage
        ~definitions:Prelude.Catalog.sakura_only_definitions sakura_db
    in
    let sakura_ref = Atomic.make sakura_db in
    let* () = register_in_sakura storage sakura_ref sakura_name in
    let map = Utilities.StringMap.singleton sakura_name sakura_ref in
    Ok { multigroups = Atomic.make map; default_multigroup = sakura_name; mutex = Mutex.create () }

  let add catalog storage ~prelude_relations name =
    Mutex.protect catalog.mutex (fun () ->
      let map = Atomic.get catalog.multigroups in
      if Utilities.StringMap.mem name map then
        Error (Printf.sprintf "Multigroup %s already exists" name)
      else
        let* db = bootstrap_multigroup storage ~prelude_relations ~name in
        let db_ref = Atomic.make db in
        let* () =
          match Utilities.StringMap.find_opt sakura_name map with
          | Some sakura_ref -> register_in_sakura storage sakura_ref name
          | None -> Ok ()
        in
        let new_map = Utilities.StringMap.add name db_ref map in
        let* () = publish_multigroups catalog ~expected:map ~updated:new_map in
        Ok db_ref)

  let unregister_from_sakura storage sakura_ref mg_name =
    let rec go () =
      let sakura = Atomic.get sakura_ref in
      match Management.Database.get_relation sakura Prelude.Catalog.multigroup_rel_name with
      | None -> Ok ()
      | Some mg_rel ->
          let tuple_hash =
            Hashing.hash_tuple (Prelude.Catalog.build_multigroup_tuple mg_name)
          in
          let* new_sakura, _ =
            Manip.retract_tuple storage sakura mg_rel ~tuple_hash |> map_error
          in
          if Atomic.compare_and_set sakura_ref sakura new_sakura then Ok ()
          else go ()
    in
    go ()

  let remove catalog storage name =
    if name = sakura_name then
      Error "Cannot remove the sakura system catalog"
    else
      Mutex.protect catalog.mutex (fun () ->
        let map = Atomic.get catalog.multigroups in
        if not (Utilities.StringMap.mem name map) then
          Error (Printf.sprintf "Multigroup %s does not exist" name)
        else
          let* () =
            match Utilities.StringMap.find_opt sakura_name map with
            | None -> Ok ()
            | Some sakura_ref -> unregister_from_sakura storage sakura_ref name
          in
          let new_map = Utilities.StringMap.remove name map in
          let* () =
            publish_multigroups catalog ~expected:map ~updated:new_map
          in
          Ok ())
end
