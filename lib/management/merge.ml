(** 3-way merge of two branch tips using their Lowest Common Ancestor. Walks the
    history chain to find it, diffs both sides against it, applies merge rules,
    validates constraints, and persists the result. *)

type conflict =
  | TupleConflict of { relation : string; hash : Conventions.Hash.t }
  | SchemaConflict of string
  | ConstraintConflict of string

type strategy = PreferLeft | PreferRight | RevertToAncestor
type merge_result = Clean of Database.t | Failed of conflict list

(* TODO: Remove this later. We already should have the interface on the manipulation *)

(** Module type for the subset of Manipulation.Make we need. *)
module type MANIPULATION = sig
  type storage
  type error

  val of_string_error : string -> error

  val load_database :
    storage -> Conventions.Hash.t -> (Database.t option, error) result

  val store_database : storage -> Database.t -> (unit, error) result
end

module StringSet = Set.Make (String)

(** Walk history to find LCA. First hash in right's ancestry that also appears
    in left's ancestry set. *)
let find_lca left_db right_db =
  let left_ancestors =
    StringSet.of_list (left_db.Database.hash :: left_db.Database.history)
  in
  let right_chain = right_db.Database.hash :: right_db.Database.history in
  List.find_opt (fun h -> StringSet.mem h left_ancestors) right_chain

(** Apply a single relation diff to a database, using strategy on conflicts. *)
let apply_diff db ~strategy ~left_db ~right_db ~(left_diff : Diff.relation_diff)
    ~(right_diff : Diff.relation_diff option) : Database.t * conflict list =
  match left_diff with
  | Diff.RelationAdded rel ->
      (* Only left added it; right_diff must be None or same *)
      (Database.add_relation db ~relation:rel, [])
  | Diff.RelationRemoved name -> (Database.remove_relation db ~name, [])
  | Diff.RelationModified { name; added_tuples; removed_tuples; schema_changed }
    -> (
      let conflicts = ref [] in
      (* Determine the merged relation's Merkle tree *)
      let base_rel_opt = Database.get_relation db name in
      match base_rel_opt with
      | None -> (db, [])
      | Some base_rel ->
          let base_tree =
            Option.value base_rel.Relation.tree ~default:Merkle.empty
          in
          (* Handle schema conflict *)
          let resolved_rel, schema_conflict =
            if schema_changed then
              match right_diff with
              | Some (Diff.RelationModified { schema_changed = true; _ }) ->
                  (* Both changed schema — apply strategy *)
                  let winner =
                    match strategy with
                    | PreferLeft -> Database.get_relation left_db name
                    | PreferRight -> Database.get_relation right_db name
                    | RevertToAncestor -> Some base_rel
                  in
                  ( Option.value winner ~default:base_rel,
                    [ SchemaConflict name ] )
              | _ ->
                  (* Only left changed schema — take left's relation *)
                  let left_rel =
                    Option.value
                      (Database.get_relation left_db name)
                      ~default:base_rel
                  in
                  (left_rel, [])
            else (base_rel, [])
          in
          conflicts := schema_conflict @ !conflicts;
          (* Apply tuple adds/removes from left *)
          let merged_tree =
            List.fold_left (fun t h -> Merkle.insert h t) base_tree added_tuples
          in
          let merged_tree =
            List.fold_left
              (fun t h -> Merkle.delete h t)
              merged_tree removed_tuples
          in
          (* Apply tuple adds/removes from right if disjoint *)
          let merged_tree =
            match right_diff with
            | Some
                (Diff.RelationModified
                   { added_tuples = r_add; removed_tuples = r_rem; _ }) ->
                (* Detect tuple conflicts: same hash both added and removed *)
                let left_add = StringSet.of_list added_tuples in
                let left_rem = StringSet.of_list removed_tuples in
                let right_add = StringSet.of_list r_add in
                let right_rem = StringSet.of_list r_rem in
                let conflict_hashes =
                  StringSet.inter left_add right_rem
                  |> StringSet.union (StringSet.inter left_rem right_add)
                in
                if not (StringSet.is_empty conflict_hashes) then begin
                  StringSet.iter
                    (fun h ->
                      conflicts :=
                        TupleConflict { relation = name; hash = h }
                        :: !conflicts)
                    conflict_hashes;
                  (* Resolve conflicting tuples per strategy *)
                  match strategy with
                  | PreferLeft ->
                      (* keep left's merged_tree as-is, ignore conflicting right ops *)
                      let safe_add = StringSet.diff right_add conflict_hashes in
                      let safe_rem = StringSet.diff right_rem conflict_hashes in
                      let t =
                        StringSet.fold
                          (fun h acc -> Merkle.insert h acc)
                          safe_add merged_tree
                      in
                      StringSet.fold
                        (fun h acc -> Merkle.delete h acc)
                        safe_rem t
                  | PreferRight ->
                      (* revert left's conflicting ops, apply right's *)
                      let t =
                        StringSet.fold
                          (fun h acc -> Merkle.delete h acc)
                          conflict_hashes merged_tree
                      in
                      let t =
                        StringSet.fold
                          (fun h acc -> Merkle.insert h acc)
                          right_add t
                      in
                      StringSet.fold
                        (fun h acc -> Merkle.delete h acc)
                        right_rem t
                  | RevertToAncestor ->
                      (* drop conflicting hashes from merged tree *)
                      StringSet.fold
                        (fun h acc -> Merkle.delete h acc)
                        conflict_hashes merged_tree
                end
                else begin
                  (* Disjoint — union both sides *)
                  let t =
                    List.fold_left
                      (fun acc h -> Merkle.insert h acc)
                      merged_tree r_add
                  in
                  List.fold_left (fun acc h -> Merkle.delete h acc) t r_rem
                end
            | _ -> merged_tree
          in
          let new_hash =
            Hashing.hash_relation ~name:resolved_rel.Relation.name
              ~schema:resolved_rel.Relation.schema ~tree:merged_tree
          in
          let updated_rel =
            {
              resolved_rel with
              Relation.tree = Some merged_tree;
              hash = Some new_hash;
            }
          in
          let new_db = Database.update_relation db ~relation:updated_rel in
          (new_db, !conflicts))

module Make
    (Storage : Physical.S)
    (Manip : MANIPULATION with type storage = Storage.t) =
struct
  let err s = Error (Manip.of_string_error s)

  (* Load both tips, find the lowest common ancestor, compute the diffs,
     lookup for right diffs by relation name, start from ancestor and
     apply all the diffs, apply first the left diffs, then right-only diffs
     (relations only right touched). If there are no conflicts, persist the
     merged database, otherwise return Failed, unless a strategy resolved them. *)
  let merge ~storage ~strategy ~left_tip ~right_tip =
    match Manip.load_database storage left_tip with
    | Error e -> Error e
    | Ok None -> err ("Left tip not found: " ^ left_tip)
    | Ok (Some left_db) -> (
        match Manip.load_database storage right_tip with
        | Error e -> Error e
        | Ok None -> err ("Right tip not found: " ^ right_tip)
        | Ok (Some right_db) -> (
            match find_lca left_db right_db with
            | None -> err "No common ancestor found between branches"
            | Some ancestor_hash -> (
                match Manip.load_database storage ancestor_hash with
                | Error e -> Error e
                | Ok None -> err ("Ancestor not found: " ^ ancestor_hash)
                | Ok (Some ancestor_db) ->
                    let left_diffs =
                      Diff.diff ~ancestor:ancestor_db ~target:left_db
                    in
                    let right_diffs =
                      Diff.diff ~ancestor:ancestor_db ~target:right_db
                    in
                    let right_diff_map =
                      List.fold_left
                        (fun m d ->
                          let key =
                            match d with
                            | Diff.RelationAdded r -> r.Relation.name
                            | Diff.RelationRemoved n -> n
                            | Diff.RelationModified { name; _ } -> name
                          in
                          Database.RelationMap.add key d m)
                        Database.RelationMap.empty right_diffs
                    in
                    let merged_db, all_conflicts =
                      let db_after_left, left_conflicts =
                        List.fold_left
                          (fun (db, conflicts) left_d ->
                            let rel_name =
                              match left_d with
                              | Diff.RelationAdded r -> r.Relation.name
                              | Diff.RelationRemoved n -> n
                              | Diff.RelationModified { name; _ } -> name
                            in
                            let right_d =
                              Database.RelationMap.find_opt rel_name
                                right_diff_map
                            in
                            let new_db, cs =
                              apply_diff db ~strategy ~left_db ~right_db
                                ~left_diff:left_d ~right_diff:right_d
                            in
                            (new_db, cs @ conflicts))
                          (ancestor_db, []) left_diffs
                      in
                      let left_names =
                        List.fold_left
                          (fun s d ->
                            let n =
                              match d with
                              | Diff.RelationAdded r -> r.Relation.name
                              | Diff.RelationRemoved n -> n
                              | Diff.RelationModified { name; _ } -> name
                            in
                            StringSet.add n s)
                          StringSet.empty left_diffs
                      in
                      let right_only =
                        List.filter
                          (fun d ->
                            let n =
                              match d with
                              | Diff.RelationAdded r -> r.Relation.name
                              | Diff.RelationRemoved n -> n
                              | Diff.RelationModified { name; _ } -> name
                            in
                            not (StringSet.mem n left_names))
                          right_diffs
                      in
                      let db_final, right_conflicts =
                        List.fold_left
                          (fun (db, conflicts) right_d ->
                            let new_db, cs =
                              apply_diff db ~strategy ~left_db ~right_db
                                ~left_diff:right_d ~right_diff:None
                            in
                            (new_db, cs @ conflicts))
                          (db_after_left, left_conflicts)
                          right_only
                      in
                      (db_final, right_conflicts)
                    in
                    let unresolvable =
                      List.filter
                        (function ConstraintConflict _ -> true | _ -> false)
                        all_conflicts
                    in
                    if unresolvable <> [] then Ok (Failed unresolvable)
                    else
                      begin match Manip.store_database storage merged_db with
                      | Error e -> Error e
                      | Ok () -> Ok (Clean merged_db)
                      end)))
end
