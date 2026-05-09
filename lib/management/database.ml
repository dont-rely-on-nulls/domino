(** Database root: a lightweight versioned descriptor that maps relation names
    to content-addressed relation values. The database object owns the B+ tree
    pointer and advances it when relations change. *)


class database ~name:(init_name : Conventions.Name.t) =
  let init_hash = Conventions.Hash.hash_text init_name in
  object (self)
    val name : Conventions.Name.t = init_name
    val hash : Conventions.Hash.t = init_hash
    val previous_version : Conventions.Hash.t option = None
    val tree_pointer : Conventions.Hash.t option = None
    val relations : Conventions.Hash.t BatMap.String.t = BatMap.String.empty
    val timestamp : float = Unix.gettimeofday ()

    method name = name
    method hash = hash
    method previous_version = previous_version
    method tree_pointer = tree_pointer
    method relations = relations
    method timestamp = timestamp

    method get_relation_hash rel_name = BatMap.String.find_opt rel_name relations
    method get_relation_names = BatMap.String.fold (fun n _ acc -> n :: acc) relations []
    method has_relation rel_name = BatMap.String.mem rel_name relations

    method private compute_hash rels =
      BatMap.String.fold
        (fun _n h acc -> Conventions.Hash.hash_text (acc ^ h))
        rels ""

    method private advance rels tp =
      {< hash = self#compute_hash rels;
         previous_version = (if hash = "" then None else Some hash);
         tree_pointer = tp;
         relations = rels;
         timestamp = Unix.gettimeofday () >}

    method add_relation ~name:rel_name ~relation_hash ~tree_pointer:tp =
      let rels = BatMap.String.add rel_name relation_hash relations in
      self#advance rels (Some tp)

    method remove_relation ~name:rel_name ~tree_pointer:tp =
      if not (BatMap.String.mem rel_name relations) then self
      else
        let rels = BatMap.String.remove rel_name relations in
        self#advance rels (Some tp)

    method update_relation ~name:rel_name ~relation_hash ~tree_pointer:tp =
      if not (BatMap.String.mem rel_name relations) then self
      else
        let rels = BatMap.String.add rel_name relation_hash relations in
        self#advance rels (Some tp)
    
    method serialize (): Storable.Database.t = failwith "NOT IMPLEMENTED"
end
