(** Multigroup root: a lightweight versioned descriptor that maps relation names
    to content-addressed relation values. The multigroup object owns the B+ tree
    pointer and advances it when relations change. *)


class multigroup ~name:(init_name : Conventions.Name.t) =
  object (_self)
    val name : Conventions.Name.t = init_name
    val hash : Conventions.Hash.t = ""
    val relations : Relation.relation BatMap.String.t = BatMap.String.empty
    val timestamp : float = Unix.gettimeofday ()

    method name = name
    method hash = hash
    method relations = relations
    method timestamp = timestamp

    method get_relation rel_name = BatMap.String.find_opt rel_name relations
    method get_relation_names = BatMap.String.fold (fun n _ acc -> n :: acc) relations []
    method has_relation rel_name = BatMap.String.mem rel_name relations

    method with_hash h = {< hash = h >}

    method add_relation (rel : Relation.relation) =
      {< relations = BatMap.String.add rel#name rel relations >}

    method remove_relation rel_name =
      {< relations = BatMap.String.remove rel_name relations >}

    method update_relation (rel : Relation.relation) =
      if not (BatMap.String.mem rel#name relations) then {< >}
      else {< relations = BatMap.String.add rel#name rel relations >}

    method serialize () : Storable.Multigroup.t =
      {
        Storable.Multigroup.name = name;
        relations = BatMap.String.bindings relations |> List.map snd |> List.map (fun r -> r#serialize ());
      }
end

let deserialize (bytes : bytes) : (multigroup, string) result =
  (* Reconstruct a multigroup from its persisted payload.
     Only name, schema, and tree_pointer (Merkle root) are restored per
     relation.  Constraints are not yet round-tripped — see the TODO in
     storable.ml for the constraint serialisation plan. *)
  try
    let stored = Storable.Multigroup.of_bytes bytes in
    let db = new multigroup ~name:stored.name in
    let db =
      List.fold_left
        (fun mg rel ->
          let relation =
            new Relation.stored
              ~name:rel.Storable.Relation.name
              ~schema:rel.schema
              ~constraints:None
              ~cardinality:Conventions.Cardinality.AlephZero
              ~lineage:None
              ~provenance:None
              ~membership_criteria:(fun _ -> true)
          in
          let relation =
            if rel.tree_pointer = "" then relation
            else relation#set_tree_pointer (Some rel.tree_pointer)
          in
          mg#add_relation (relation :> Relation.relation))
        db stored.relations
    in
    Ok db
  with Invalid_argument message -> Error message
