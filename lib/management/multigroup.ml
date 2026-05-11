(** Multigroup root: a lightweight versioned descriptor that maps relation names
    to content-addressed relation values. The multigroup object owns the B+ tree
    pointer and advances it when relations change. *)


class multigroup ~name:(init_name : Conventions.Name.t) =
  let init_hash = Conventions.Hash.hash_text init_name in
  object (self)
    val name : Conventions.Name.t = init_name
    val hash : Conventions.Hash.t = init_hash
    val previous_version : Conventions.Hash.t option = None
    val tree_pointer : Conventions.Hash.t option = None
    val relations : Relation.relation BatMap.String.t = BatMap.String.empty
    val timestamp : float = Unix.gettimeofday ()

    method name = name
    method hash = hash
    method previous_version = previous_version
    method tree_pointer = tree_pointer
    method relations = relations
    method timestamp = timestamp

    method get_relation rel_name = BatMap.String.find_opt rel_name relations
    method get_relation_names = BatMap.String.fold (fun n _ acc -> n :: acc) relations []
    method has_relation rel_name = BatMap.String.mem rel_name relations

    (* Derive the B+ tree root hash from sorted relation hashes. *)
    method private compute_tree_pointer rels =
      BatMap.String.fold
        (fun _n (r : Relation.relation) acc -> acc ^ r#hash)
        rels ""
      |> Conventions.Hash.hash_text

    method private compute_hash tp =
      Conventions.Hash.hash_text (name ^ tp)

    method private advance rels =
      let tp = self#compute_tree_pointer rels in
      {< hash = self#compute_hash tp;
         previous_version = (if hash = "" then None else Some hash);
         tree_pointer = Some tp;
         relations = rels;
         timestamp = Unix.gettimeofday () >}

    method add_relation (rel : Relation.relation) =
      let rels = BatMap.String.add rel#name rel relations in
      self#advance rels

    method remove_relation rel_name =
      if not (BatMap.String.mem rel_name relations) then self
      else self#advance (BatMap.String.remove rel_name relations)

    method update_relation (rel : Relation.relation) =
      if not (BatMap.String.mem rel#name relations) then self
      else self#advance (BatMap.String.add rel#name rel relations)

    method serialize () : Storable.Multigroup.t =
      {
        Storable.Multigroup.name = name;
        relations = BatMap.String.bindings relations |> List.map snd |> List.map (fun r -> r#serialize ());
      }
end

let deserialize (_bytes : bytes) : (multigroup, string) result =
  try
    let stored = Storable.Multigroup.of_bytes _bytes in
    let db = new multigroup ~name:stored.name in
    let db =
      List.fold_left
        (fun db rel ->
          let membership_criteria =
            Constraint.build_membership_criteria ~_name:rel.Storable.Relation.name
              ~schema:rel.schema
          in
          let relation =
            new Relation.stored ~name:rel.name ~schema:rel.schema ~constraints:None
              ~cardinality:Conventions.Cardinality.ConstrainedFinite
              ~membership_criteria ~lineage:None ~provenance:None
          in
          db#add_relation relation)
        db stored.relations
    in
    Ok db
  with Invalid_argument message -> Error message
