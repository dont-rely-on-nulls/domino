(** Database state: a named collection of relations with a Merkle tree over
    their hashes, a domain registry, and a history chain of prior hashes. All
    mutations produce a new value; no in-place update. *)

(* TODO: history is an unbounded list of hashes; there is no way to
   reconstruct a past state from a hash alone without a separate snapshot
   store. Either cap the history or make it actually useful. *)

(* TODO: refactor RelationMap keying from composite "schema:relation" strings
   to nested maps (schema -> relation -> Relation.t) for cleaner semantics.
   Currently, callers are responsible for passing composite keys. *)
module RelationMap = Map.Make (String)

type deferred_entry = {
  relation_name : string;
  constraint_name : string;
  body : Constraint.t;
}

type t = {
  hash : Conventions.Hash.t;
  name : Conventions.Name.t;
  tree : Merkle.t;
  relations : Relation.t RelationMap.t;
  domains : Domain.t RelationMap.t;
  history : Conventions.Hash.t list;
  timestamp : float;
  deferred : deferred_entry list;
}

let empty ~name =
  {
    hash = "";
    name;
    tree = Merkle.empty;
    relations = RelationMap.empty;
    domains = RelationMap.empty;
    history = [];
    timestamp = Unix.gettimeofday ();
    deferred = [];
  }

let compute_hash db =
  match Merkle.root_hash db.tree with
  | Some root -> root
  | None -> Conventions.Hash.hash_text db.name

let max_history = 128

let rec take_at_most n = function
  | [] -> []
  | _ when n = 0 -> []
  | x :: xs -> x :: take_at_most (n - 1) xs

let update_state db ~relations ~tree =
  let new_hash =
    match Merkle.root_hash tree with
    | Some root -> root
    | None -> Conventions.Hash.hash_text db.name
  in
  let history =
    if db.hash = "" then db.history
    else take_at_most max_history (db.hash :: db.history)
  in
  {
    hash = new_hash;
    name = db.name;
    tree;
    relations;
    domains = db.domains;
    history;
    timestamp = Unix.gettimeofday ();
    deferred = db.deferred;
  }

let get_relation db name = RelationMap.find_opt name db.relations

let get_relation_hash db name =
  match RelationMap.find_opt name db.relations with
  | None -> None
  | Some rel -> rel.Relation.hash

let get_relation_names db =
  RelationMap.fold (fun name _ acc -> name :: acc) db.relations []

let has_relation db name = RelationMap.mem name db.relations

let relation_hash_or_compute (relation : Relation.t) =
  match relation.hash with
  | Some h -> h
  | None ->
      let tree = Option.value relation.tree ~default:Merkle.empty in
      Hashing.hash_relation ~name:relation.name ~schema:relation.schema ~tree

(* TODO: computing the hash here for hash:None relations (ephemeral/prelude)
   is a layering violation — callers should assign the hash before calling
   add_relation, not rely on this fallback. *)
let add_relation db ~(relation : Relation.t) =
  let relation_hash = relation_hash_or_compute relation in
  let relation = { relation with hash = Some relation_hash } in
  let tree = Merkle.insert relation_hash db.tree in
  let relations = RelationMap.add relation.name relation db.relations in
  update_state db ~relations ~tree

let remove_relation db ~name =
  match RelationMap.find_opt name db.relations with
  | None -> db
  | Some relation ->
      let relation_hash = relation_hash_or_compute relation in
      let tree = Merkle.delete relation_hash db.tree in
      let relations = RelationMap.remove name db.relations in
      update_state db ~relations ~tree

let update_relation db ~(relation : Relation.t) =
  let name = relation.name in
  match RelationMap.find_opt name db.relations with
  | None -> db
  | Some old_relation ->
      let old_hash = relation_hash_or_compute old_relation in
      let new_hash = relation_hash_or_compute relation in
      let tree = db.tree |> Merkle.delete old_hash |> Merkle.insert new_hash in
      let relations = RelationMap.add name relation db.relations in
      update_state db ~relations ~tree

let add_domain db (domain : Domain.t) =
  { db with domains = RelationMap.add domain.name domain db.domains }

let get_domain db name = RelationMap.find_opt name db.domains

let get_domain_names db =
  RelationMap.fold (fun name _ acc -> name :: acc) db.domains []

let has_domain db name = RelationMap.mem name db.domains

let remove_domain db name =
  { db with domains = RelationMap.remove name db.domains }
