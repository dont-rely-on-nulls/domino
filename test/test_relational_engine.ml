(** Tests for the relational engine *)

open Relational_engine

module Memory = Manipulation.Make (Management.Physical.Memory)

let%test_unit "merkle: empty tree" =
  let tree = Merkle.empty in
  assert (Merkle.is_empty tree);
  assert (Merkle.size tree = 0);
  assert (Merkle.root_hash tree = None)

let%test_unit "merkle: insert single element" =
  let tree = Merkle.empty in
  let hash = "abc123" in
  let tree = Merkle.insert hash tree in
  assert (not (Merkle.is_empty tree));
  assert (Merkle.size tree = 1);
  assert (Merkle.member hash tree);
  assert (Merkle.root_hash tree <> None)

let%test_unit "merkle: insert multiple elements" =
  let tree = Merkle.empty in
  let tree = Merkle.insert "hash1" tree in
  let tree = Merkle.insert "hash2" tree in
  let tree = Merkle.insert "hash3" tree in
  assert (Merkle.size tree = 3);
  assert (Merkle.member "hash1" tree);
  assert (Merkle.member "hash2" tree);
  assert (Merkle.member "hash3" tree);
  assert (not (Merkle.member "hash4" tree))

let%test_unit "merkle: delete element" =
  let tree = Merkle.empty in
  let tree = Merkle.insert "hash1" tree in
  let tree = Merkle.insert "hash2" tree in
  assert (Merkle.size tree = 2);
  let tree = Merkle.delete "hash1" tree in
  assert (Merkle.size tree = 1);
  assert (not (Merkle.member "hash1" tree));
  assert (Merkle.member "hash2" tree)

let%test_unit "merkle: keys returns all elements" =
  let tree = Merkle.empty in
  let tree = Merkle.insert "c" tree in
  let tree = Merkle.insert "a" tree in
  let tree = Merkle.insert "b" tree in
  let keys = Merkle.keys tree in
  assert (List.length keys = 3);
  assert (List.mem "a" keys);
  assert (List.mem "b" keys);
  assert (List.mem "c" keys)

let%test_unit "merkle: root hash changes on insert" =
  let tree = Merkle.empty in
  let tree1 = Merkle.insert "hash1" tree in
  let root1 = Merkle.root_hash tree1 in
  let tree2 = Merkle.insert "hash2" tree1 in
  let root2 = Merkle.root_hash tree2 in
  assert (root1 <> root2)

let%test_unit "merkle: same elements produce same root hash" =
  let tree1 = Merkle.empty |> Merkle.insert "a" |> Merkle.insert "b" in
  let tree2 = Merkle.empty |> Merkle.insert "b" |> Merkle.insert "a" in
  assert (Merkle.root_hash tree1 = Merkle.root_hash tree2)

let%test_unit "storage: create and close" =
  match Management.Physical.Memory.create () with
  | Error _ -> assert false
  | Ok storage -> Management.Physical.Memory.close storage

let%test_unit "storage: store and load attribute" =
  match Management.Physical.Memory.create () with
  | Error _ -> assert false
  | Ok storage ->
      let value = Obj.magic 42 in
      (match Management.Physical.Memory.store_attribute storage value with
      | Error _ -> assert false
      | Ok hash -> (
          match Management.Physical.Memory.load_attribute storage hash with
          | Error _ -> assert false
          | Ok None -> assert false
          | Ok (Some loaded) -> assert ((Obj.magic loaded : int) = 42)));
      Management.Physical.Memory.close storage

let%test_unit "storage: store and load raw bytes" =
  match Management.Physical.Memory.create () with
  | Error _ -> assert false
  | Ok storage ->
      let hash = "test_hash" in
      let data = Bytes.of_string "hello world" in
      (match Management.Physical.Memory.store_raw storage hash data with
      | Error _ -> assert false
      | Ok () -> (
          match Management.Physical.Memory.load_raw storage hash with
          | Error _ -> assert false
          | Ok None -> assert false
          | Ok (Some loaded) -> assert (Bytes.equal loaded data)));
      Management.Physical.Memory.close storage

let%test_unit "storage: exists check" =
  match Management.Physical.Memory.create () with
  | Error _ -> assert false
  | Ok storage ->
      let hash = "exists_test" in
      let data = Bytes.of_string "data" in
      (match Management.Physical.Memory.exists storage hash with
      | Error _ -> assert false
      | Ok exists -> assert (not exists));
      (match Management.Physical.Memory.store_raw storage hash data with
      | Error _ -> assert false
      | Ok () -> (
          match Management.Physical.Memory.exists storage hash with
          | Error _ -> assert false
          | Ok exists -> assert exists));
      Management.Physical.Memory.close storage

let%test_unit "storage: transaction commit" =
  match Management.Physical.Memory.create () with
  | Error _ -> assert false
  | Ok storage ->
      let result =
        Management.Physical.Memory.with_transaction storage (fun () ->
            let hash = "tx_test" in
            let data = Bytes.of_string "tx_data" in
            Management.Physical.Memory.store_raw storage hash data)
      in
      (match result with
      | Error _ -> assert false
      | Ok () -> (
          match Management.Physical.Memory.exists storage "tx_test" with
          | Error _ -> assert false
          | Ok exists -> assert exists));
      Management.Physical.Memory.close storage

let%test_unit "database: create empty" =
  let db = Management.Database.empty ~name:"test_db" in
  assert (db.name = "test_db");
  assert (Merkle.is_empty db.tree);
  assert (Management.Database.RelationMap.is_empty db.relations);
  assert (db.history = [])

let%test_unit "database: add relation" =
  let db = Management.Database.empty ~name:"test_db" in
  (* Create a minimal relation for testing *)
  let relation =
    Relation.make ~hash:(Some "rel_hash_1") ~name:"users" ~schema:Schema.empty
      ~tree:(Some Merkle.empty) ~constraints:None
      ~cardinality:(Conventions.Cardinality.Finite 0) ~generator:None
      ~membership_criteria:(fun _ _ -> true)
      ~provenance:Relation.Provenance.Undefined
      ~lineage:(Relation.Lineage.Base "users")
  in
  let db = Management.Database.add_relation db ~relation in
  assert (Management.Database.has_relation db "users");
  assert (Management.Database.get_relation_hash db "users" = Some "rel_hash_1");
  assert (not (Management.Database.has_relation db "orders"))

let%test_unit "database: remove relation" =
  let db = Management.Database.empty ~name:"test_db" in
  let relation =
    Relation.make ~hash:(Some "hash1") ~name:"users" ~schema:Schema.empty
      ~tree:(Some Merkle.empty) ~constraints:None
      ~cardinality:(Conventions.Cardinality.Finite 0) ~generator:None
      ~membership_criteria:(fun _ _ -> true)
      ~provenance:Relation.Provenance.Undefined
      ~lineage:(Relation.Lineage.Base "users")
  in
  let db = Management.Database.add_relation db ~relation in
  assert (Management.Database.has_relation db "users");
  let db = Management.Database.remove_relation db ~name:"users" in
  assert (not (Management.Database.has_relation db "users"))

let%test_unit "database: update relation" =
  let db = Management.Database.empty ~name:"test_db" in
  let relation =
    Relation.make ~hash:(Some "hash1") ~name:"users" ~schema:Schema.empty
      ~tree:(Some Merkle.empty) ~constraints:None
      ~cardinality:(Conventions.Cardinality.Finite 0) ~generator:None
      ~membership_criteria:(fun _ _ -> true)
      ~provenance:Relation.Provenance.Undefined
      ~lineage:(Relation.Lineage.Base "users")
  in
  let db = Management.Database.add_relation db ~relation in
  let old_db_hash = db.hash in
  let updated_relation = { relation with hash = Some "hash2" } in
  let db = Management.Database.update_relation db ~relation:updated_relation in
  assert (Management.Database.get_relation_hash db "users" = Some "hash2");
  assert (db.hash <> old_db_hash);
  assert (List.mem old_db_hash db.history)

let%test_unit "database: get relation names" =
  let db = Management.Database.empty ~name:"test_db" in
  let rel1 =
    Relation.make ~hash:(Some "h1") ~name:"users" ~schema:Schema.empty
      ~tree:(Some Merkle.empty) ~constraints:None
      ~cardinality:(Conventions.Cardinality.Finite 0) ~generator:None
      ~membership_criteria:(fun _ _ -> true)
      ~provenance:Relation.Provenance.Undefined
      ~lineage:(Relation.Lineage.Base "users")
  in
  let rel2 =
    Relation.make ~hash:(Some "h2") ~name:"orders" ~schema:Schema.empty
      ~tree:(Some Merkle.empty) ~constraints:None
      ~cardinality:(Conventions.Cardinality.Finite 0) ~generator:None
      ~membership_criteria:(fun _ _ -> true)
      ~provenance:Relation.Provenance.Undefined
      ~lineage:(Relation.Lineage.Base "orders")
  in
  let db = Management.Database.add_relation db ~relation:rel1 in
  let db = Management.Database.add_relation db ~relation:rel2 in
  let names = Management.Database.get_relation_names db in
  assert (List.length names = 2);
  assert (List.mem "users" names);
  assert (List.mem "orders" names)

(* Helper to create storage for tests *)
let with_storage f =
  match Management.Physical.Memory.create () with
  | Error _ -> assert false
  | Ok storage ->
      let result = f storage in
      Management.Physical.Memory.close storage;
      result

let%test_unit "manipulation: create database" =
  with_storage (fun storage ->
      match Memory.create_database storage ~name:"my_db" with
      | Error _ -> assert false
      | Ok db -> assert (db.name = "my_db"))

let%test_unit "manipulation: create relation" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema =
        Schema.empty |> Schema.add "name" "string" |> Schema.add "age" "integer"
      in
      match
        Memory.create_relation storage db ~name:"users" ~schema
      with
      | Error _ -> assert false
      | Ok (db, relation) ->
          assert (relation.name = "users");
          assert (relation.schema = schema);
          assert (Management.Database.has_relation db "users"))

let%test_unit "manipulation: create relation already exists" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty in
      match
        Memory.create_relation storage db ~name:"users" ~schema
      with
      | Error _ -> assert false
      | Ok (db, _) -> (
          match
            Memory.create_relation storage db ~name:"users" ~schema
          with
          | Error (Error.RelationAlreadyExists _) -> ()
          | _ -> assert false))

let%test_unit "manipulation: retract relation" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty in
      match
        Memory.create_relation storage db ~name:"users" ~schema
      with
      | Error _ -> assert false
      | Ok (db, _) -> (
          assert (Management.Database.has_relation db "users");
          match
            Memory.retract_relation storage db ~name:"users"
          with
          | Error _ -> assert false
          | Ok db -> assert (not (Management.Database.has_relation db "users"))))

let%test_unit "manipulation: create tuple with storage" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty |> Schema.add "name" "string" in
      match
        Memory.create_relation storage db ~name:"users" ~schema
      with
      | Error _ -> assert false
      | Ok (db, relation) -> (
          let attrs =
            Tuple.AttributeMap.empty
            |> Tuple.AttributeMap.add "name"
                 { Attribute.value = Obj.magic "Alice" }
          in
          let tuple : Tuple.materialized =
            { relation = "users"; attributes = attrs }
          in
          match Memory.create_tuple storage db relation tuple with
          | Error _ -> assert false
          | Ok (_db, new_relation, _hash) ->
              assert (Memory.tuple_count new_relation = 1)))

let%test_unit "manipulation: create and load tuple" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty |> Schema.add "value" "integer" in
      match
        Memory.create_relation storage db ~name:"numbers" ~schema
      with
      | Error _ -> assert false
      | Ok (db, relation) -> (
          let attrs =
            Tuple.AttributeMap.empty
            |> Tuple.AttributeMap.add "value" { Attribute.value = Obj.magic 42 }
          in
          let tuple : Tuple.materialized =
            { relation = "numbers"; attributes = attrs }
          in
          match Memory.create_tuple storage db relation tuple with
          | Error _ -> assert false
          | Ok (_db, _relation, tuple_hash) -> (
              (* Now load the tuple back from storage *)
              match Memory.load_tuple storage tuple_hash with
              | Error _ -> assert false
              | Ok None -> assert false
              | Ok (Some loaded_tuple) ->
                  assert (loaded_tuple.relation = "numbers");
                  let loaded_value =
                    Tuple.AttributeMap.find "value" loaded_tuple.attributes
                  in
                  assert ((Obj.magic loaded_value.Attribute.value : int) = 42))))

let%test_unit "manipulation: create multiple tuples with storage" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty |> Schema.add "id" "integer" in
      match
        Memory.create_relation storage db ~name:"items" ~schema
      with
      | Error _ -> assert false
      | Ok (db, relation) -> (
          let make_tuple id =
            let attrs =
              Tuple.AttributeMap.empty
              |> Tuple.AttributeMap.add "id" { Attribute.value = Obj.magic id }
            in
            ({ Tuple.relation = "items"; attributes = attrs }
              : Tuple.materialized)
          in
          let tuples = [ make_tuple 1; make_tuple 2; make_tuple 3 ] in
          match
            Memory.create_tuples storage db relation tuples
          with
          | Error _ -> assert false
          | Ok (_db, new_relation, hashes) ->
              assert (Memory.tuple_count new_relation = 3);
              assert (List.length hashes = 3)))

let%test_unit "manipulation: load multiple tuples" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty |> Schema.add "n" "integer" in
      match
        Memory.create_relation storage db ~name:"test" ~schema
      with
      | Error _ -> assert false
      | Ok (db, relation) -> (
          let make_tuple n =
            let attrs =
              Tuple.AttributeMap.empty
              |> Tuple.AttributeMap.add "n" { Attribute.value = Obj.magic n }
            in
            ({ Tuple.relation = "test"; attributes = attrs }
              : Tuple.materialized)
          in
          match
            Memory.create_tuples storage db relation
              [ make_tuple 10; make_tuple 20 ]
          with
          | Error _ -> assert false
          | Ok (_db, _relation, hashes) -> (
              match Memory.load_tuples storage hashes with
              | Error _ -> assert false
              | Ok loaded ->
                  assert (List.length loaded = 2);
                  let values =
                    List.map
                      (fun t ->
                        let attr =
                          Tuple.AttributeMap.find "n" t.Tuple.attributes
                        in
                        (Obj.magic attr.Attribute.value : int))
                      loaded
                  in
                  assert (List.mem 10 values);
                  assert (List.mem 20 values))))

let%test_unit "manipulation: retract tuple" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty |> Schema.add "value" "integer" in
      match
        Memory.create_relation storage db ~name:"numbers" ~schema
      with
      | Error _ -> assert false
      | Ok (db, relation) -> (
          let attrs =
            Tuple.AttributeMap.empty
            |> Tuple.AttributeMap.add "value" { Attribute.value = Obj.magic 42 }
          in
          let tuple : Tuple.materialized =
            { relation = "numbers"; attributes = attrs }
          in
          match Memory.create_tuple storage db relation tuple with
          | Error _ -> assert false
          | Ok (db, relation, tuple_hash) -> (
              assert (Memory.tuple_count relation = 1);
              match
                Memory.retract_tuple storage db relation
                  ~tuple_hash
              with
              | Error _ -> assert false
              | Ok (_db, relation) -> (
                  assert (Memory.tuple_count relation = 0);
                  (* Tuple data still exists in storage (append-only) *)
                  match Memory.load_tuple storage tuple_hash with
                  | Error _ -> assert false
                  | Ok None -> assert false
                  | Ok (Some _) -> () (* Data is still there *)))))

let%test_unit "manipulation: tuple hashes" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty in
      match
        Memory.create_relation storage db ~name:"test" ~schema
      with
      | Error _ -> assert false
      | Ok (db, relation) -> (
          let make_tuple n =
            let attrs =
              Tuple.AttributeMap.empty
              |> Tuple.AttributeMap.add "n" { Attribute.value = Obj.magic n }
            in
            ({ Tuple.relation = "test"; attributes = attrs }
              : Tuple.materialized)
          in
          match
            Memory.create_tuples storage db relation
              [ make_tuple 1; make_tuple 2 ]
          with
          | Error _ -> assert false
          | Ok (_db, relation, _) ->
              let hashes = Memory.tuple_hashes relation in
              assert (List.length hashes = 2)))

let%test_unit "manipulation: clear relation" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty in
      match
        Memory.create_relation storage db ~name:"test" ~schema
      with
      | Error _ -> assert false
      | Ok (db, relation) -> (
          let make_tuple n =
            let attrs =
              Tuple.AttributeMap.empty
              |> Tuple.AttributeMap.add "n" { Attribute.value = Obj.magic n }
            in
            ({ Tuple.relation = "test"; attributes = attrs }
              : Tuple.materialized)
          in
          match
            Memory.create_tuples storage db relation
              [ make_tuple 1; make_tuple 2; make_tuple 3 ]
          with
          | Error _ -> assert false
          | Ok (db, relation, _) -> (
              assert (Memory.tuple_count relation = 3);
              match Memory.clear_relation storage db relation with
              | Error _ -> assert false
              | Ok (_db, relation) ->
                  assert (Memory.tuple_count relation = 0))))

let%test_unit "manipulation: duplicate tuple rejected" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty |> Schema.add "x" "integer" in
      match
        Memory.create_relation storage db ~name:"test" ~schema
      with
      | Error _ -> assert false
      | Ok (db, relation) -> (
          let attrs =
            Tuple.AttributeMap.empty
            |> Tuple.AttributeMap.add "x" { Attribute.value = Obj.magic 1 }
          in
          let tuple : Tuple.materialized =
            { relation = "test"; attributes = attrs }
          in
          match Memory.create_tuple storage db relation tuple with
          | Error _ -> assert false
          | Ok (db, relation, _) -> (
              (* Try to insert the same tuple again *)
              match
                Memory.create_tuple storage db relation tuple
              with
              | Error (Error.DuplicateTuple _) -> ()
              | _ -> assert false)))

let%test_unit "manipulation: tuple_exists check" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty in
      match
        Memory.create_relation storage db ~name:"test" ~schema
      with
      | Error _ -> assert false
      | Ok (db, relation) -> (
          let attrs =
            Tuple.AttributeMap.empty
            |> Tuple.AttributeMap.add "v" { Attribute.value = Obj.magic 99 }
          in
          let tuple : Tuple.materialized =
            { relation = "test"; attributes = attrs }
          in
          let tuple_hash = Hashing.hash_tuple tuple in
          assert (not (Memory.tuple_exists relation tuple_hash));
          match Memory.create_tuple storage db relation tuple with
          | Error _ -> assert false
          | Ok (_db, relation, hash) ->
              assert (hash = tuple_hash);
              assert (Memory.tuple_exists relation tuple_hash)))

let%test_unit "manipulation: hash_tuple deterministic" =
  let attrs =
    Tuple.AttributeMap.empty
    |> Tuple.AttributeMap.add "a" { Attribute.value = Obj.magic 1 }
    |> Tuple.AttributeMap.add "b" { Attribute.value = Obj.magic 2 }
  in
  let tuple : Tuple.materialized = { relation = "test"; attributes = attrs } in
  let hash1 = Hashing.hash_tuple tuple in
  let hash2 = Hashing.hash_tuple tuple in
  assert (hash1 = hash2)

let%test_unit "manipulation: different tuples different hashes" =
  let tuple1 : Tuple.materialized =
    {
      relation = "test";
      attributes =
        Tuple.AttributeMap.singleton "x" { Attribute.value = Obj.magic 1 };
    }
  in
  let tuple2 : Tuple.materialized =
    {
      relation = "test";
      attributes =
        Tuple.AttributeMap.singleton "x" { Attribute.value = Obj.magic 2 };
    }
  in
  let hash1 = Hashing.hash_tuple tuple1 in
  let hash2 = Hashing.hash_tuple tuple2 in
  assert (hash1 <> hash2)

let%test_unit "manipulation: get_relation from database" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty |> Schema.add "id" "integer" in
      match
        Memory.create_relation storage db ~name:"items" ~schema
      with
      | Error _ -> assert false
      | Ok (db, _relation) -> (
          (* Now get the relation from the database *)
          match Memory.get_relation db ~name:"items" with
          | None -> assert false
          | Some rel ->
              assert (rel.name = "items");
              assert (rel.schema = schema)))

let%test_unit "schema: persisted and loaded correctly" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema =
        Schema.empty |> Schema.add "id" "integer" |> Schema.add "name" "string"
        |> Schema.add "email" "string"
      in
      match
        Memory.create_relation storage db ~name:"users" ~schema
      with
      | Error _ -> assert false
      | Ok (db, _relation) -> (
          (* Get the relation hash *)
          let rel_hash =
            Option.get (Management.Database.get_relation_hash db "users")
          in
          (* Load the relation from storage *)
          match Memory.load_relation storage rel_hash with
          | Error _ -> assert false
          | Ok None -> assert false
          | Ok (Some loaded_rel) ->
              (* Verify schema was persisted and loaded correctly *)
              assert (loaded_rel.schema = schema);
              assert (List.mem ("id", "integer") loaded_rel.schema);
              assert (List.mem ("name", "string") loaded_rel.schema);
              assert (List.mem ("email", "string") loaded_rel.schema)))

let%test_unit "catalog: create_database seeds 6 catalog relations" =
  with_storage (fun storage ->
      match Memory.create_database storage ~name:"test" with
      | Error _ -> assert false
      | Ok db ->
          let catalog_names = Prelude.Catalog.catalog_relation_names in
          List.iter
            (fun name -> assert (Management.Database.has_relation db name))
            catalog_names)

let%test_unit "catalog: sakura:relation contains all 6 catalog names" =
  with_storage (fun storage ->
      match Memory.create_database storage ~name:"test" with
      | Error _ -> assert false
      | Ok db ->
          let rel =
            match
              Memory.get_relation db ~name:"sakura:relation"
            with
            | None -> assert false
            | Some r -> r
          in
          assert (Memory.tuple_count rel = 6))

let%test_unit "catalog: sakura:on contains insert, update, delete" =
  with_storage (fun storage ->
      match Memory.create_database storage ~name:"test" with
      | Error _ -> assert false
      | Ok db ->
          let on_rel =
            match Memory.get_relation db ~name:"sakura:on" with
            | None -> assert false
            | Some r -> r
          in
          assert (Memory.tuple_count on_rel = 3))

let%test_unit "catalog: sakura:timing contains immediate, deferred" =
  with_storage (fun storage ->
      match Memory.create_database storage ~name:"test" with
      | Error _ -> assert false
      | Ok db ->
          let timing_rel =
            match Memory.get_relation db ~name:"sakura:timing" with
            | None -> assert false
            | Some r -> r
          in
          assert (Memory.tuple_count timing_rel = 2))

let%test_unit "catalog: sakura:domain seeded with 4 prelude domains" =
  with_storage (fun storage ->
      match Memory.create_database storage ~name:"test" with
      | Error _ -> assert false
      | Ok db ->
          let dom_rel =
            match Memory.get_relation db ~name:"sakura:domain" with
            | None -> assert false
            | Some r -> r
          in
          assert (Memory.tuple_count dom_rel = 4))

let%test_unit "catalog: create_relation updates sakura:relation" =
  with_storage (fun storage ->
      match Memory.create_database storage ~name:"test" with
      | Error _ -> assert false
      | Ok db ->
          let schema = Schema.empty |> Schema.add "id" "natural" in
          let db =
            match
              Memory.create_relation storage db ~name:"employees"
                ~schema
            with
            | Error _ -> assert false
            | Ok (db, _) -> db
          in
          let rel =
            match
              Memory.get_relation db ~name:"sakura:relation"
            with
            | None -> assert false
            | Some r -> r
          in
          (* 6 catalog relations + 1 user relation *)
          assert (Memory.tuple_count rel = 7))

let%test_unit "catalog: create_relation updates sakura:attribute" =
  with_storage (fun storage ->
      match Memory.create_database storage ~name:"test" with
      | Error _ -> assert false
      | Ok db ->
          let schema = Schema.empty |> Schema.add "id" "natural" in
          let db =
            match
              Memory.create_relation storage db ~name:"employees"
                ~schema
            with
            | Error _ -> assert false
            | Ok (db, _) -> db
          in
          let attr_rel =
            match
              Memory.get_relation db ~name:"sakura:attribute"
            with
            | None -> assert false
            | Some r -> r
          in
          (* catalog attribute tuples (sum of schema sizes of all 6 catalog relations) + 1 for employees *)
          (* sakura:relation: 1, sakura:domain: 1, sakura:attribute: 3, sakura:constraint: 2,
         sakura:on: 1, sakura:timing: 1  => 9 catalog attrs + 1 for employees = 10 *)
          let count = Memory.tuple_count attr_rel in
          assert (count = 10))

let%test_unit "catalog: retract_relation removes from sakura:relation" =
  with_storage (fun storage ->
      match Memory.create_database storage ~name:"test" with
      | Error _ -> assert false
      | Ok db ->
          let schema = Schema.empty |> Schema.add "id" "natural" in
          let db =
            match
              Memory.create_relation storage db ~name:"employees"
                ~schema
            with
            | Error _ -> assert false
            | Ok (db, _) -> db
          in
          let db =
            match
              Memory.retract_relation storage db ~name:"employees"
            with
            | Error _ -> assert false
            | Ok db -> db
          in
          let rel =
            match
              Memory.get_relation db ~name:"sakura:relation"
            with
            | None -> assert false
            | Some r -> r
          in
          (* Back to 6 catalog relations *)
          assert (Memory.tuple_count rel = 6))

let%test_unit "catalog: register_constraint inserts into sakura:constraint" =
  with_storage (fun storage ->
      match Memory.create_database storage ~name:"test" with
      | Error _ -> assert false
      | Ok db ->
          let schema = Schema.empty |> Schema.add "id" "natural" in
          let db =
            match
              Memory.create_relation storage db ~name:"orders"
                ~schema
            with
            | Error _ -> assert false
            | Ok (db, _) -> db
          in
          let db =
            match
              Memory.register_constraint storage db
                ~constraint_name:"orders_id_positive" ~relation_name:"orders"
                ~body:(Constraint.And [])
            with
            | Error _ -> assert false
            | Ok db -> db
          in
          let con_rel =
            match
              Memory.get_relation db ~name:"sakura:constraint"
            with
            | None -> assert false
            | Some r -> r
          in
          assert (Memory.tuple_count con_rel = 1))

let%test_unit "integration: full workflow with storage" =
  with_storage (fun storage ->
      (* Create database *)
      let db =
        match Memory.create_database storage ~name:"shop" with
        | Error _ -> assert false
        | Ok db -> db
      in

      (* Create products relation *)
      let schema =
        Schema.empty |> Schema.add "id" "integer" |> Schema.add "name" "string"
        |> Schema.add "price" "integer"
      in

      let db, products =
        match
          Memory.create_relation storage db ~name:"products"
            ~schema
        with
        | Error _ -> assert false
        | Ok x -> x
      in

      (* Insert some products *)
      let make_product id name price =
        let attrs =
          Tuple.AttributeMap.empty
          |> Tuple.AttributeMap.add "id" { Attribute.value = Obj.magic id }
          |> Tuple.AttributeMap.add "name" { Attribute.value = Obj.magic name }
          |> Tuple.AttributeMap.add "price"
               { Attribute.value = Obj.magic price }
        in
        ({ Tuple.relation = "products"; attributes = attrs }
          : Tuple.materialized)
      in

      let tuples =
        [
          make_product 1 "Apple" 100;
          make_product 2 "Banana" 50;
          make_product 3 "Cherry" 200;
        ]
      in

      let db, products, hashes =
        match Memory.create_tuples storage db products tuples with
        | Error _ -> assert false
        | Ok x -> x
      in

      assert (Memory.tuple_count products = 3);
      assert (List.length hashes = 3);

      (* Verify we can load all tuples back *)
      (match Memory.load_tuples storage hashes with
      | Error _ -> assert false
      | Ok loaded -> assert (List.length loaded = 3));

      (* Delete one product *)
      let banana_hash = Hashing.hash_tuple (make_product 2 "Banana" 50) in
      let _db, products =
        match
          Memory.retract_tuple storage db products
            ~tuple_hash:banana_hash
        with
        | Error _ -> assert false
        | Ok x -> x
      in

      assert (Memory.tuple_count products = 2);

      (* Verify remaining hashes *)
      let remaining_hashes = Memory.tuple_hashes products in
      assert (List.length remaining_hashes = 2);
      assert (not (List.mem banana_hash remaining_hashes));

      (* But banana data is still in storage (append-only) *)
      match Memory.load_tuple storage banana_hash with
      | Error _ -> assert false
      | Ok None -> assert false
      | Ok (Some loaded) ->
          let name_attr = Tuple.AttributeMap.find "name" loaded.attributes in
          assert ((Obj.magic name_attr.Attribute.value : string) = "Banana"))

let%test_unit "integration: database history tracking" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"versioned" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let initial_hash = db.hash in

      let schema = Schema.empty in
      let db, _ =
        match
          Memory.create_relation storage db ~name:"rel1" ~schema
        with
        | Error _ -> assert false
        | Ok x -> x
      in

      (* After first change, initial hash should be in history *)
      assert (db.hash <> initial_hash);

      let hash_after_rel1 = db.hash in

      let db, _ =
        match
          Memory.create_relation storage db ~name:"rel2" ~schema
        with
        | Error _ -> assert false
        | Ok x -> x
      in

      (* After second change, previous hash should be in history *)
      assert (db.hash <> hash_after_rel1);
      assert (List.mem hash_after_rel1 db.history))

let%test_unit "integration: hash bubbles up correctly" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty in

      let db, relation =
        match
          Memory.create_relation storage db ~name:"items" ~schema
        with
        | Error _ -> assert false
        | Ok x -> x
      in

      let db_hash_before = db.hash in
      let rel_hash_before = relation.hash in

      (* Insert a tuple *)
      let attrs =
        Tuple.AttributeMap.empty
        |> Tuple.AttributeMap.add "x" { Attribute.value = Obj.magic 1 }
      in
      let tuple : Tuple.materialized =
        { relation = "items"; attributes = attrs }
      in

      let db, relation, _ =
        match Memory.create_tuple storage db relation tuple with
        | Error _ -> assert false
        | Ok x -> x
      in

      (* Relation hash should change *)
      assert (relation.hash <> rel_hash_before);

      (* Database hash should change *)
      assert (db.hash <> db_hash_before);

      (* Old database hash should be in history *)
      assert (List.mem db_hash_before db.history);

      (* Database's merkle tree should contain the new relation hash *)
      match relation.hash with
      | None -> assert false
      | Some rel_hash -> assert (Merkle.member rel_hash db.tree))

let%test_unit "branching: load database from historical hash" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty in

      (* Create relation and add a tuple *)
      let db, relation =
        match
          Memory.create_relation storage db ~name:"items" ~schema
        with
        | Error _ -> assert false
        | Ok x -> x
      in

      let attrs =
        Tuple.AttributeMap.empty
        |> Tuple.AttributeMap.add "x" { Attribute.value = Obj.magic 1 }
      in
      let tuple : Tuple.materialized =
        { relation = "items"; attributes = attrs }
      in

      let db, _relation, _ =
        match Memory.create_tuple storage db relation tuple with
        | Error _ -> assert false
        | Ok x -> x
      in

      (* Capture hash H1 - database with 1 tuple *)
      let h1 = db.hash in

      (* Add another tuple *)
      let attrs2 =
        Tuple.AttributeMap.empty
        |> Tuple.AttributeMap.add "x" { Attribute.value = Obj.magic 2 }
      in
      let tuple2 : Tuple.materialized =
        { relation = "items"; attributes = attrs2 }
      in

      (* Get relation from database - it's now integrated! *)
      let relation =
        match Memory.get_relation db ~name:"items" with
        | None -> assert false
        | Some r -> r
      in

      let db, _relation, _ =
        match Memory.create_tuple storage db relation tuple2 with
        | Error _ -> assert false
        | Ok x -> x
      in

      let h2 = db.hash in
      assert (h1 <> h2);

      (* Now load H1 from storage - should get database state with 1 tuple *)
      match Memory.load_database storage h1 with
      | Error _ -> assert false
      | Ok None -> assert false
      | Ok (Some loaded_db) -> (
          assert (loaded_db.hash = h1);
          assert (loaded_db.name = "test");
          (* The relation is now loaded directly in the database *)
          match Memory.get_relation loaded_db ~name:"items" with
          | None -> assert false
          | Some loaded_rel ->
              (* Should have 1 tuple *)
              assert (Memory.tuple_count loaded_rel = 1)))

let%test_unit "branching: branch from historical state" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty in

      (* Create relation *)
      let db, relation =
        match
          Memory.create_relation storage db ~name:"data" ~schema
        with
        | Error _ -> assert false
        | Ok x -> x
      in

      (* Add tuple A *)
      let tuple_a : Tuple.materialized =
        {
          relation = "data";
          attributes =
            Tuple.AttributeMap.singleton "v" { Attribute.value = Obj.magic "A" };
        }
      in
      let db, relation, _ =
        match Memory.create_tuple storage db relation tuple_a with
        | Error _ -> assert false
        | Ok x -> x
      in

      (* Capture H1 - state with tuple A *)
      let h1 = db.hash in

      (* Continue on main branch: add tuple B *)
      let tuple_b : Tuple.materialized =
        {
          relation = "data";
          attributes =
            Tuple.AttributeMap.singleton "v" { Attribute.value = Obj.magic "B" };
        }
      in
      let db_main, relation_main, _ =
        match Memory.create_tuple storage db relation tuple_b with
        | Error _ -> assert false
        | Ok x -> x
      in
      let h2 = db_main.hash in

      (* Now branch from H1 and add tuple C instead *)
      let db_branch =
        match Memory.load_database storage h1 with
        | Error _ -> assert false
        | Ok None -> assert false
        | Ok (Some d) -> d
      in
      (* Relation is now loaded directly in the database *)
      let relation_branch =
        match Memory.get_relation db_branch ~name:"data" with
        | None -> assert false
        | Some r -> r
      in

      let tuple_c : Tuple.materialized =
        {
          relation = "data";
          attributes =
            Tuple.AttributeMap.singleton "v" { Attribute.value = Obj.magic "C" };
        }
      in
      let db_branch, _relation_branch, _ =
        match
          Memory.create_tuple storage db_branch relation_branch
            tuple_c
        with
        | Error _ -> assert false
        | Ok x -> x
      in
      let h3 = db_branch.hash in

      (* Verify: H1, H2, H3 are all different *)
      assert (h1 <> h2);
      assert (h1 <> h3);
      assert (h2 <> h3);

      (* H2 (main branch) has tuples A and B *)
      assert (Memory.tuple_count relation_main = 2);

      (* H1 is still loadable and unchanged *)
      match Memory.load_database storage h1 with
      | Error _ -> assert false
      | Ok None -> assert false
      | Ok (Some loaded_h1) -> (
          match Memory.get_relation loaded_h1 ~name:"data" with
          | None -> assert false
          | Some r -> assert (Memory.tuple_count r = 1)))

let%test_unit "branching: full reconstruction from hash" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"shop" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty in

      (* Create relation *)
      let db, relation =
        match
          Memory.create_relation storage db ~name:"products"
            ~schema
        with
        | Error _ -> assert false
        | Ok x -> x
      in

      (* Add products *)
      let make_product name =
        {
          Tuple.relation = "products";
          attributes =
            Tuple.AttributeMap.singleton "name"
              { Attribute.value = Obj.magic name };
        }
      in
      let db, _relation, _ =
        match
          Memory.create_tuples storage db relation
            [
              make_product "Apple"; make_product "Banana"; make_product "Cherry";
            ]
        with
        | Error _ -> assert false
        | Ok x -> x
      in

      let saved_hash = db.hash in

      (* Now reconstruct everything from just the hash *)
      match Memory.load_database storage saved_hash with
      | Error _ -> assert false
      | Ok None -> assert false
      | Ok (Some loaded_db) -> (
          (* Relation is now loaded directly in the database *)
          match Memory.get_relation loaded_db ~name:"products" with
          | None -> assert false
          | Some loaded_rel -> (
              (* Get tuple hashes from relation *)
              let tuple_hashes = Memory.tuple_hashes loaded_rel in
              assert (List.length tuple_hashes = 3);

              (* Load all tuples *)
              match Memory.load_tuples storage tuple_hashes with
              | Error _ -> assert false
              | Ok tuples ->
                  assert (List.length tuples = 3);
                  let names =
                    List.map
                      (fun t ->
                        let attr =
                          Tuple.AttributeMap.find "name" t.Tuple.attributes
                        in
                        (Obj.magic attr.Attribute.value : string))
                      tuples
                  in
                  assert (List.mem "Apple" names);
                  assert (List.mem "Banana" names);
                  assert (List.mem "Cherry" names))))

(* Helper: build a materialized tuple with int attributes *)
let make_int_tuple relation pairs =
  let attributes =
    List.fold_left
      (fun acc (k, v) ->
        Tuple.AttributeMap.add k { Attribute.value = Obj.repr (v : int) } acc)
      Tuple.AttributeMap.empty pairs
  in
  (Tuple.Materialized { Tuple.relation; attributes } : Tuple.t)

(* Helper: build a stored relation with one integer column and given values *)
let stored_relation_with_ints storage db ~name ~attr values =
  let schema = Schema.empty |> Schema.add attr "integer" in
  let db, rel =
    match Memory.create_relation storage db ~name ~schema with
    | Error _ -> assert false
    | Ok x -> x
  in
  let tuples =
    List.map
      (fun v ->
        let attributes =
          Tuple.AttributeMap.singleton attr
            { Attribute.value = Obj.repr (v : int) }
        in
        ({ Tuple.relation = name; attributes } : Tuple.materialized))
      values
  in
  let db, rel, _ =
    match Memory.create_tuples storage db rel tuples with
    | Error _ -> assert false
    | Ok x -> x
  in
  (db, rel)

let%test_unit "algebra: const_relation single tuple" =
  with_storage (fun storage ->
      let rel =
        Algebra.Memory.const_relation
          [ ("x", Obj.repr (42 : int)); ("y", Obj.repr (99 : int)) ]
      in
      match Algebra.Memory.materialize storage rel with
      | Error _ -> assert false
      | Ok [ tup ] ->
          let x : int =
            Obj.obj
              (Tuple.AttributeMap.find "x" tup.Tuple.attributes).Attribute.value
          in
          assert (x = 42)
      | Ok _ -> assert false)

let%test_unit "algebra: select_fn with predicate" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let _, rel =
        stored_relation_with_ints storage db ~name:"nums" ~attr:"n"
          [ 1; 2; 3; 4; 5 ]
      in
      let predicate = function
        | Tuple.Materialized t ->
            let v : int =
              Obj.obj
                (Tuple.AttributeMap.find "n" t.Tuple.attributes).Attribute.value
            in
            v > 3
        | _ -> false
      in
      match Algebra.Memory.select_fn storage predicate rel with
      | Error _ -> assert false
      | Ok result -> (
          match Algebra.Memory.materialize storage result with
          | Error _ -> assert false
          | Ok rows ->
              assert (List.length rows = 2);
              List.iter
                (fun t ->
                  let v : int =
                    Obj.obj
                      (Tuple.AttributeMap.find "n" t.Tuple.attributes)
                        .Attribute.value
                  in
                  assert (v > 3))
                rows))

let%test_unit "algebra: project restricts schema" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema =
        Schema.empty |> Schema.add "a" "integer" |> Schema.add "b" "integer"
      in
      let db, rel =
        match
          Memory.create_relation storage db ~name:"ab" ~schema
        with
        | Error _ -> assert false
        | Ok x -> x
      in
      let attrs =
        Tuple.AttributeMap.empty
        |> Tuple.AttributeMap.add "a" { Attribute.value = Obj.repr (1 : int) }
        |> Tuple.AttributeMap.add "b" { Attribute.value = Obj.repr (2 : int) }
      in
      let _, rel, _ =
        match
          Memory.create_tuple storage db rel
            { Tuple.relation = "ab"; attributes = attrs }
        with
        | Error _ -> assert false
        | Ok x -> x
      in
      match Algebra.Memory.project storage [ "a" ] rel with
      | Error _ -> assert false
      | Ok projected -> (
          match Algebra.Memory.materialize storage projected with
          | Error _ -> assert false
          | Ok [ t ] ->
              assert (Tuple.AttributeMap.mem "a" t.Tuple.attributes);
              assert (not (Tuple.AttributeMap.mem "b" t.Tuple.attributes))
          | Ok _ -> assert false))

let%test_unit "algebra: rename changes attr names" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let _, rel =
        stored_relation_with_ints storage db ~name:"r" ~attr:"x" [ 7 ]
      in
      match Algebra.Memory.rename storage [ ("x", "y") ] rel with
      | Error _ -> assert false
      | Ok renamed -> (
          match Algebra.Memory.materialize storage renamed with
          | Error _ -> assert false
          | Ok [ t ] ->
              assert (not (Tuple.AttributeMap.mem "x" t.Tuple.attributes));
              assert (Tuple.AttributeMap.mem "y" t.Tuple.attributes);
              let v : int =
                Obj.obj
                  (Tuple.AttributeMap.find "y" t.Tuple.attributes)
                    .Attribute.value
              in
              assert (v = 7)
          | Ok _ -> assert false))

let%test_unit "algebra: equijoin merges matching tuples" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      (* left: {id=1, name="Alice"}, {id=2, name="Bob"} *)
      let schema_l =
        Schema.empty |> Schema.add "id" "integer" |> Schema.add "name" "string"
      in
      let db, left =
        match
          Memory.create_relation storage db ~name:"L"
            ~schema:schema_l
        with
        | Error _ -> assert false
        | Ok x -> x
      in
      let make_left id name =
        let attrs =
          Tuple.AttributeMap.empty
          |> Tuple.AttributeMap.add "id"
               { Attribute.value = Obj.repr (id : int) }
          |> Tuple.AttributeMap.add "name"
               { Attribute.value = Obj.repr (name : string) }
        in
        ({ Tuple.relation = "L"; attributes = attrs } : Tuple.materialized)
      in
      let db, left, _ =
        match
          Memory.create_tuples storage db left
            [ make_left 1 "Alice"; make_left 2 "Bob" ]
        with
        | Error _ -> assert false
        | Ok x -> x
      in
      (* right: {id=1, score=100}, {id=3, score=999} *)
      let schema_r =
        Schema.empty |> Schema.add "id" "integer"
        |> Schema.add "score" "integer"
      in
      let db, right =
        match
          Memory.create_relation storage db ~name:"R"
            ~schema:schema_r
        with
        | Error _ -> assert false
        | Ok x -> x
      in
      let make_right id score =
        let attrs =
          Tuple.AttributeMap.empty
          |> Tuple.AttributeMap.add "id"
               { Attribute.value = Obj.repr (id : int) }
          |> Tuple.AttributeMap.add "score"
               { Attribute.value = Obj.repr (score : int) }
        in
        ({ Tuple.relation = "R"; attributes = attrs } : Tuple.materialized)
      in
      let _, right, _ =
        match
          Memory.create_tuples storage db right
            [ make_right 1 100; make_right 3 999 ]
        with
        | Error _ -> assert false
        | Ok x -> x
      in
      match Algebra.Memory.equijoin storage [ "id" ] left right with
      | Error _ -> assert false
      | Ok joined -> (
          match Algebra.Memory.materialize storage joined with
          | Error _ -> assert false
          | Ok rows ->
              (* Only id=1 matches *)
              assert (List.length rows = 1);
              let t = List.hd rows in
              let score : int =
                Obj.obj
                  (Tuple.AttributeMap.find "score" t.Tuple.attributes)
                    .Attribute.value
              in
              assert (score = 100)))

let%test_unit "algebra: equijoin empty match" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let db, left =
        stored_relation_with_ints storage db ~name:"L" ~attr:"id" [ 1; 2 ]
      in
      let _, right =
        stored_relation_with_ints storage db ~name:"R" ~attr:"id" [ 9; 8 ]
      in
      match Algebra.Memory.equijoin storage [ "id" ] left right with
      | Error _ -> assert false
      | Ok joined -> (
          match Algebra.Memory.materialize storage joined with
          | Error _ -> assert false
          | Ok rows -> assert (List.length rows = 0)))

let%test_unit "algebra: union concatenates streams" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let db, r1 =
        stored_relation_with_ints storage db ~name:"A" ~attr:"n" [ 1; 2 ]
      in
      let _, r2 =
        stored_relation_with_ints storage db ~name:"B" ~attr:"n" [ 3; 4 ]
      in
      match Algebra.Memory.union storage r1 r2 with
      | Error _ -> assert false
      | Ok unioned -> (
          match Algebra.Memory.materialize storage unioned with
          | Error _ -> assert false
          | Ok rows ->
              assert (List.length rows = 4);
              let vals =
                List.map
                  (fun t ->
                    Obj.obj
                      (Tuple.AttributeMap.find "n" t.Tuple.attributes)
                        .Attribute.value)
                  rows
              in
              assert (List.mem 1 vals);
              assert (List.mem 2 vals);
              assert (List.mem 3 vals);
              assert (List.mem 4 vals)))

let%test_unit "algebra: diff removes right from left" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let db, r1 =
        stored_relation_with_ints storage db ~name:"A" ~attr:"n" [ 1; 2; 3 ]
      in
      let _, r2 =
        stored_relation_with_ints storage db ~name:"B" ~attr:"n" [ 2; 3 ]
      in
      match Algebra.Memory.diff storage r1 r2 with
      | Error _ -> assert false
      | Ok result -> (
          match Algebra.Memory.materialize storage result with
          | Error _ -> assert false
          | Ok rows ->
              assert (List.length rows = 1);
              let v : int =
                Obj.obj
                  (Tuple.AttributeMap.find "n" (List.hd rows).Tuple.attributes)
                    .Attribute.value
              in
              assert (v = 1)))

let%test_unit "algebra: take limits output" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let _, rel =
        stored_relation_with_ints storage db ~name:"big" ~attr:"n"
          [ 10; 20; 30; 40; 50 ]
      in
      match Algebra.Memory.take storage 3 rel with
      | Error _ -> assert false
      | Ok result -> (
          match Algebra.Memory.materialize storage result with
          | Error _ -> assert false
          | Ok rows -> assert (List.length rows = 3)))

let%test_unit "drl: parse Base" =
  match Drl.Parser.of_string {|(Base "users")|} with
  | Error _ -> assert false
  | Ok (Drl.Ast.Base s) -> assert (s = "users")
  | Ok _ -> assert false

let%test_unit "drl: parse Const" =
  match Drl.Parser.of_string {|(Const (("age" (Int 18))))|} with
  | Error _ -> assert false
  | Ok (Drl.Ast.Const [ ("age", Drl.Ast.Int 18) ]) -> ()
  | Ok _ -> assert false

let%test_unit "drl: parse Join" =
  match Drl.Parser.of_string {|(Join (id) (Base "L") (Base "R"))|} with
  | Error _ -> assert false
  | Ok (Drl.Ast.Join ([ "id" ], Drl.Ast.Base "L", Drl.Ast.Base "R")) -> ()
  | Ok _ -> assert false

let%test_unit "drl: parse Select" =
  let s = {|(Select (Const (("age" (Int 18)))) (Base "users"))|} in
  match Drl.Parser.of_string s with
  | Error _ -> assert false
  | Ok (Drl.Ast.Select (Drl.Ast.Const _, Drl.Ast.Base "users")) -> ()
  | Ok _ -> assert false

let%test_unit "drl: execute Base" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let db, _ =
        stored_relation_with_ints storage db ~name:"items" ~attr:"v" [ 10; 20 ]
      in
      match Drl.Parser.of_string {|(Base "items")|} with
      | Error _ -> assert false
      | Ok query -> (
          match Drl.Executor.Memory.execute storage db query with
          | Error _ -> assert false
          | Ok rel -> (
              match Algebra.Memory.materialize storage rel with
              | Error _ -> assert false
              | Ok rows -> assert (List.length rows = 2))))

let%test_unit "drl: execute Select+Const" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      (* users: {age=18, name="Alice"}, {age=25, name="Bob"} *)
      let schema =
        Schema.empty |> Schema.add "age" "integer" |> Schema.add "name" "string"
      in
      let db, users =
        match
          Memory.create_relation storage db ~name:"users" ~schema
        with
        | Error _ -> assert false
        | Ok x -> x
      in
      let make_user age name =
        let attrs =
          Tuple.AttributeMap.empty
          |> Tuple.AttributeMap.add "age"
               { Attribute.value = Obj.repr (age : int) }
          |> Tuple.AttributeMap.add "name"
               { Attribute.value = Obj.repr (name : string) }
        in
        ({ Tuple.relation = "users"; attributes = attrs } : Tuple.materialized)
      in
      let db, _, _ =
        match
          Memory.create_tuples storage db users
            [ make_user 18 "Alice"; make_user 25 "Bob" ]
        with
        | Error _ -> assert false
        | Ok x -> x
      in
      (* Query: select users where age = 18 *)
      let s = {|(Select (Const (("age" (Int 18)))) (Base "users"))|} in
      match Drl.Parser.of_string s with
      | Error _ -> assert false
      | Ok query -> (
          match Drl.Executor.Memory.execute storage db query with
          | Error _ -> assert false
          | Ok rel -> (
              match Algebra.Memory.materialize storage rel with
              | Error _ -> assert false
              | Ok rows ->
                  assert (List.length rows = 1);
                  let name_v : string =
                    Obj.obj
                      (Tuple.AttributeMap.find "name"
                         (List.hd rows).Tuple.attributes)
                        .Attribute.value
                  in
                  assert (name_v = "Alice"))))

(* ---------- Constraint construction ---------- *)

let%test_unit "constraint: vars_in MemberOf" =
  let b =
    Constraint.BindingMap.empty
    |> Constraint.BindingMap.add "left" (Constraint.Var "x")
    |> Constraint.BindingMap.add "right" (Constraint.Const (Obj.repr 10))
  in
  let c = Constraint.MemberOf { target = "less_than"; binding = b } in
  let vars = Constraint.vars_in c in
  assert (List.mem "x" vars);
  assert (not (List.mem "right" vars))

let%test_unit "constraint: vars_in And" =
  let b1 = Constraint.BindingMap.singleton "left" (Constraint.Var "a") in
  let b2 = Constraint.BindingMap.singleton "left" (Constraint.Var "b") in
  let c =
    Constraint.And
      [
        Constraint.MemberOf { target = "t1"; binding = b1 };
        Constraint.MemberOf { target = "t2"; binding = b2 };
      ]
  in
  let vars = Constraint.vars_in c in
  assert (List.mem "a" vars);
  assert (List.mem "b" vars)

let%test_unit "constraint: rename_vars" =
  let b = Constraint.BindingMap.singleton "left" (Constraint.Var "old_name") in
  let c = Constraint.MemberOf { target = "t"; binding = b } in
  let c' = Constraint.rename_vars [ ("old_name", "new_name") ] c in
  let vars = Constraint.vars_in c' in
  assert (List.mem "new_name" vars);
  assert (not (List.mem "old_name" vars))

let%test_unit "constraint: filter_by_attrs keeps relevant" =
  let b = Constraint.BindingMap.singleton "left" (Constraint.Var "x") in
  let c = Constraint.MemberOf { target = "t"; binding = b } in
  assert (Constraint.filter_by_attrs [ "x" ] c <> None);
  assert (Constraint.filter_by_attrs [ "y" ] c = None)

let%test_unit "constraint: merge named constraints" =
  let b = Constraint.BindingMap.empty in
  let cs1 = [ ("c1", Constraint.MemberOf { target = "t1"; binding = b }) ] in
  let cs2 = [ ("c2", Constraint.MemberOf { target = "t2"; binding = b }) ] in
  let merged = Constraint.merge cs1 cs2 in
  assert (List.length merged = 2)

let%test_unit "constraint: merge duplicate names produces And" =
  let b = Constraint.BindingMap.empty in
  let c1 = Constraint.MemberOf { target = "t1"; binding = b } in
  let c2 = Constraint.MemberOf { target = "t2"; binding = b } in
  let merged = Constraint.merge [ ("shared", c1) ] [ ("shared", c2) ] in
  assert (List.length merged = 1);
  match List.assoc "shared" merged with
  | Constraint.And _ -> ()
  | _ -> assert false

(* ---------- Smart constructors ---------- *)

let%test_unit "constraint: and_ singleton" =
  let b = Constraint.BindingMap.empty in
  let c = Constraint.MemberOf { target = "t"; binding = b } in
  match Constraint.and_ [ c ] with
  | Constraint.MemberOf _ -> ()
  | _ -> assert false

let%test_unit "constraint: or_ singleton" =
  let b = Constraint.BindingMap.empty in
  let c = Constraint.MemberOf { target = "t"; binding = b } in
  match Constraint.or_ [ c ] with
  | Constraint.MemberOf _ -> ()
  | _ -> assert false

(* ---------- Comparison shorthands ---------- *)

let%test_unit "constraint: lt shorthand" =
  match Constraint.lt ~left:(Var "x") ~right:(Const (Obj.repr 5)) with
  | Constraint.MemberOf { target = "less_than"; _ } -> ()
  | _ -> assert false

let%test_unit "constraint: between shorthand" =
  match
    Constraint.between ~value:(Var "x")
      ~low:(Const (Obj.repr 0))
      ~high:(Const (Obj.repr 100))
  with
  | Constraint.And [ _; _ ] -> ()
  | _ -> assert false

(* ---------- Binding resolution ---------- *)

let%test_unit "constraint: bind resolves Var and Const" =
  let b =
    Constraint.BindingMap.empty
    |> Constraint.BindingMap.add "left" (Constraint.Var "x")
    |> Constraint.BindingMap.add "right" (Constraint.Const (Obj.repr 42))
  in
  let tuple : Tuple.materialized =
    {
      relation = "test";
      attributes =
        Tuple.AttributeMap.singleton "x" { Attribute.value = Obj.repr 10 };
    }
  in
  let bound = Constraint.bind b tuple in
  assert (List.length bound = 2);
  let left_v : int = Obj.obj (List.assoc "left" bound) in
  let right_v : int = Obj.obj (List.assoc "right" bound) in
  assert (left_v = 10);
  assert (right_v = 42)

(* ---------- Evaluation engine ---------- *)

let make_eval_ctx ?(relations = []) () : Constraint.eval_context =
  {
    check_membership =
      (fun rel_name bound_pairs ->
        match List.assoc_opt rel_name relations with
        | None -> false
        | Some checker -> checker bound_pairs);
    iterate_finite =
      (fun rel_name ->
        match List.assoc_opt rel_name relations with
        | None -> None
        | Some _ -> Some []);
  }

let%test_unit "constraint: evaluate MemberOf success" =
  let ctx = make_eval_ctx ~relations:[ ("my_rel", fun _pairs -> true) ] () in
  let b = Constraint.BindingMap.empty in
  let c = Constraint.MemberOf { target = "my_rel"; binding = b } in
  let tuple : Tuple.materialized =
    { relation = "test"; attributes = Tuple.AttributeMap.empty }
  in
  match Constraint.evaluate ctx tuple c with Ok true -> () | _ -> assert false

let%test_unit "constraint: evaluate MemberOf failure" =
  let ctx = make_eval_ctx ~relations:[ ("my_rel", fun _pairs -> false) ] () in
  let b = Constraint.BindingMap.empty in
  let c = Constraint.MemberOf { target = "my_rel"; binding = b } in
  let tuple : Tuple.materialized =
    { relation = "test"; attributes = Tuple.AttributeMap.empty }
  in
  match Constraint.evaluate ctx tuple c with
  | Error (Constraint.MembershipFailed _) -> ()
  | _ -> assert false

let%test_unit "constraint: evaluate And short-circuits" =
  let call_count = ref 0 in
  let ctx =
    make_eval_ctx
      ~relations:
        [
          ( "fail_rel",
            fun _ ->
              incr call_count;
              false );
          ( "ok_rel",
            fun _ ->
              incr call_count;
              true );
        ]
      ()
  in
  let b = Constraint.BindingMap.empty in
  let c =
    Constraint.And
      [
        Constraint.MemberOf { target = "fail_rel"; binding = b };
        Constraint.MemberOf { target = "ok_rel"; binding = b };
      ]
  in
  let tuple : Tuple.materialized =
    { relation = "test"; attributes = Tuple.AttributeMap.empty }
  in
  (match Constraint.evaluate ctx tuple c with
  | Error _ -> ()
  | _ -> assert false);
  (* Second branch should not have been called *)
  assert (!call_count = 1)

let%test_unit "constraint: evaluate Or succeeds on first match" =
  let ctx =
    make_eval_ctx ~relations:[ ("a", fun _ -> false); ("b", fun _ -> true) ] ()
  in
  let b = Constraint.BindingMap.empty in
  let c =
    Constraint.Or
      [
        Constraint.MemberOf { target = "a"; binding = b };
        Constraint.MemberOf { target = "b"; binding = b };
      ]
  in
  let tuple : Tuple.materialized =
    { relation = "test"; attributes = Tuple.AttributeMap.empty }
  in
  match Constraint.evaluate ctx tuple c with Ok true -> () | _ -> assert false

let%test_unit "constraint: evaluate Not negates" =
  let ctx : Constraint.eval_context =
    {
      check_membership =
        (fun rel_name _pairs ->
          match rel_name with
          | "universe" -> true
          | "body_rel" -> false
          | _ -> false);
      iterate_finite = (fun _ -> None);
    }
  in
  let b = Constraint.BindingMap.empty in
  let c =
    Constraint.Not
      {
        body = Constraint.MemberOf { target = "body_rel"; binding = b };
        universe = "universe";
      }
  in
  let tuple : Tuple.materialized =
    { relation = "test"; attributes = Tuple.AttributeMap.empty }
  in
  match Constraint.evaluate ctx tuple c with Ok true -> () | _ -> assert false

let%test_unit "constraint: evaluate Exists over finite relation" =
  let ctx : Constraint.eval_context =
    {
      check_membership =
        (fun _rel_name pairs ->
          match (List.assoc_opt "left" pairs, List.assoc_opt "right" pairs) with
          | Some l, Some r -> (Obj.obj l : int) < (Obj.obj r : int)
          | _ -> false);
      iterate_finite =
        (fun rel_name ->
          if rel_name = "small_set" then
            Some
              [
                [ ("value", Obj.repr 1) ];
                [ ("value", Obj.repr 5) ];
                [ ("value", Obj.repr 10) ];
              ]
          else None);
    }
  in
  let b =
    Constraint.BindingMap.empty
    |> Constraint.BindingMap.add "left" (Constraint.Var "x")
    |> Constraint.BindingMap.add "right" (Constraint.Var "value.value")
  in
  let c =
    Constraint.Exists
      {
        variable = "value";
        quantifier = "small_set";
        body = Constraint.MemberOf { target = "less_than"; binding = b };
      }
  in
  (* x=3, exists value in {1,5,10} where 3 < value => true (5 or 10 work) *)
  let tuple : Tuple.materialized =
    {
      relation = "test";
      attributes =
        Tuple.AttributeMap.singleton "x" { Attribute.value = Obj.repr 3 };
    }
  in
  match Constraint.evaluate ctx tuple c with Ok true -> () | _ -> assert false

let%test_unit "constraint: evaluate Forall fails when not all match" =
  let ctx : Constraint.eval_context =
    {
      check_membership =
        (fun _rel_name pairs ->
          match (List.assoc_opt "left" pairs, List.assoc_opt "right" pairs) with
          | Some l, Some r -> (Obj.obj l : int) < (Obj.obj r : int)
          | _ -> false);
      iterate_finite =
        (fun rel_name ->
          if rel_name = "small_set" then
            Some [ [ ("value", Obj.repr 1) ]; [ ("value", Obj.repr 5) ] ]
          else None);
    }
  in
  let b =
    Constraint.BindingMap.empty
    |> Constraint.BindingMap.add "left" (Constraint.Var "x")
    |> Constraint.BindingMap.add "right" (Constraint.Var "value.value")
  in
  let c =
    Constraint.Forall
      {
        variable = "value";
        quantifier = "small_set";
        body = Constraint.MemberOf { target = "less_than"; binding = b };
      }
  in
  (* x=3, forall value in {1,5} where 3 < value => false (3 is not < 1) *)
  let tuple : Tuple.materialized =
    {
      relation = "test";
      attributes =
        Tuple.AttributeMap.singleton "x" { Attribute.value = Obj.repr 3 };
    }
  in
  match Constraint.evaluate ctx tuple c with
  | Ok false -> ()
  | _ -> assert false

let%test_unit "constraint: Forall unbounded quantifier errors" =
  let ctx : Constraint.eval_context =
    { check_membership = (fun _ _ -> true); iterate_finite = (fun _ -> None) }
  in
  let b = Constraint.BindingMap.empty in
  let c =
    Constraint.Forall
      {
        variable = "v";
        quantifier = "infinite_rel";
        body = Constraint.MemberOf { target = "t"; binding = b };
      }
  in
  let tuple : Tuple.materialized =
    { relation = "test"; attributes = Tuple.AttributeMap.empty }
  in
  match Constraint.evaluate ctx tuple c with
  | Error (Constraint.UnboundedQuantifier _) -> ()
  | _ -> assert false

(* ---------- Integrated constraint enforcement in manipulation ---------- *)

let%test_unit "constraint: create_tuple with passing constraint" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty |> Schema.add "value" "natural" in
      let db, _rel =
        match
          Memory.create_relation storage db ~name:"positives"
            ~schema
        with
        | Error _ -> assert false
        | Ok x -> x
      in
      (* Register a constraint: value must be member of natural (always true for naturals) *)
      let db =
        match
          Memory.register_constraint storage db
            ~constraint_name:"is_natural" ~relation_name:"positives"
            ~body:(Constraint.And [])
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      let rel =
        match Memory.get_relation db ~name:"positives" with
        | None -> assert false
        | Some r -> r
      in
      let tuple : Tuple.materialized =
        {
          relation = "positives";
          attributes =
            Tuple.AttributeMap.singleton "value"
              { Attribute.value = Obj.repr 5 };
        }
      in
      match Memory.create_tuple storage db rel tuple with
      | Ok _ -> ()
      | Error _ -> assert false)

let%test_unit "constraint: create_tuple with failing constraint" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty |> Schema.add "value" "natural" in
      let db, _rel =
        match
          Memory.create_relation storage db ~name:"checked" ~schema
        with
        | Error _ -> assert false
        | Ok x -> x
      in
      (* Register a constraint that always fails: MemberOf a nonexistent relation *)
      let db =
        match
          Memory.register_constraint storage db
            ~constraint_name:"impossible" ~relation_name:"checked"
            ~body:
              (Constraint.MemberOf
                 {
                   target = "nonexistent_relation";
                   binding =
                     Constraint.BindingMap.singleton "x"
                       (Constraint.Var "value");
                 })
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      let rel =
        match Memory.get_relation db ~name:"checked" with
        | None -> assert false
        | Some r -> r
      in
      let tuple : Tuple.materialized =
        {
          relation = "checked";
          attributes =
            Tuple.AttributeMap.singleton "value"
              { Attribute.value = Obj.repr 5 };
        }
      in
      match Memory.create_tuple storage db rel tuple with
      | Error (Error.ConstraintViolation _) -> ()
      | _ -> assert false)

(* ---------- Scenario tests ported from Erlang ---------- *)

(* Scenario 1: Mutual exclusion between subtypes *)
let%test_unit "constraint scenario: mutual exclusion subtypes" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty |> Schema.add "id" "natural" in
      let db, _employee =
        match
          Memory.create_relation storage db ~name:"employee"
            ~schema
        with
        | Error _ -> assert false
        | Ok x -> x
      in
      let db, _manager =
        match
          Memory.create_relation storage db ~name:"manager" ~schema
        with
        | Error _ -> assert false
        | Ok x -> x
      in
      (* manager must NOT be in employee (mutual exclusion) *)
      let body =
        Constraint.Not
          {
            body =
              Constraint.MemberOf
                {
                  target = "employee";
                  binding =
                    Constraint.BindingMap.singleton "id" (Constraint.Var "id");
                };
            universe = "manager";
          }
      in
      let db =
        match
          Memory.register_constraint storage db
            ~constraint_name:"not_employee" ~relation_name:"manager" ~body
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      (* Insert into employee first *)
      let employee =
        match Memory.get_relation db ~name:"employee" with
        | None -> assert false
        | Some r -> r
      in
      let emp_tuple : Tuple.materialized =
        {
          relation = "employee";
          attributes =
            Tuple.AttributeMap.singleton "id" { Attribute.value = Obj.repr 1 };
        }
      in
      let db, _employee, _ =
        match
          Memory.create_tuple storage db employee emp_tuple
        with
        | Error _ -> assert false
        | Ok x -> x
      in
      (* Now try to insert same id into manager, should fail *)
      let manager =
        match Memory.get_relation db ~name:"manager" with
        | None -> assert false
        | Some r -> r
      in
      let mgr_tuple : Tuple.materialized =
        {
          relation = "manager";
          attributes =
            Tuple.AttributeMap.singleton "id" { Attribute.value = Obj.repr 1 };
        }
      in
      match Memory.create_tuple storage db manager mgr_tuple with
      | Error (Error.ConstraintViolation _) -> ()
      | _ -> assert false)

(* Scenario 2: Foreign key constraint *)
let%test_unit "constraint scenario: foreign key" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let order_schema = Schema.empty |> Schema.add "order_id" "natural" in
      let db, _orders =
        match
          Memory.create_relation storage db ~name:"orders"
            ~schema:order_schema
        with
        | Error _ -> assert false
        | Ok x -> x
      in
      let item_schema =
        Schema.empty
        |> Schema.add "item_id" "natural"
        |> Schema.add "order_id" "natural"
      in
      let db, _items =
        match
          Memory.create_relation storage db ~name:"order_items"
            ~schema:item_schema
        with
        | Error _ -> assert false
        | Ok x -> x
      in
      (* FK: order_items.order_id must exist in orders *)
      let fk_body =
        Constraint.MemberOf
          {
            target = "orders";
            binding =
              Constraint.BindingMap.singleton "order_id"
                (Constraint.Var "order_id");
          }
      in
      let db =
        match
          Memory.register_constraint storage db
            ~constraint_name:"fk_order" ~relation_name:"order_items"
            ~body:fk_body
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      (* Insert an order *)
      let orders =
        match Memory.get_relation db ~name:"orders" with
        | None -> assert false
        | Some r -> r
      in
      let order_tuple : Tuple.materialized =
        {
          relation = "orders";
          attributes =
            Tuple.AttributeMap.singleton "order_id"
              { Attribute.value = Obj.repr 100 };
        }
      in
      let db, _orders, _ =
        match
          Memory.create_tuple storage db orders order_tuple
        with
        | Error _ -> assert false
        | Ok x -> x
      in
      (* Insert item referencing valid order, should succeed *)
      let items =
        match Memory.get_relation db ~name:"order_items" with
        | None -> assert false
        | Some r -> r
      in
      let valid_item : Tuple.materialized =
        {
          relation = "order_items";
          attributes =
            Tuple.AttributeMap.of_list
              [
                ("item_id", { Attribute.value = Obj.repr 1 });
                ("order_id", { Attribute.value = Obj.repr 100 });
              ];
        }
      in
      let db, items, _ =
        match Memory.create_tuple storage db items valid_item with
        | Error _ -> assert false
        | Ok x -> x
      in
      (* Insert item referencing nonexistent order, should fail *)
      let invalid_item : Tuple.materialized =
        {
          relation = "order_items";
          attributes =
            Tuple.AttributeMap.of_list
              [
                ("item_id", { Attribute.value = Obj.repr 2 });
                ("order_id", { Attribute.value = Obj.repr 999 });
              ];
        }
      in
      match Memory.create_tuple storage db items invalid_item with
      | Error (Error.ConstraintViolation _) -> ()
      | _ -> assert false)

(* Scenario 3: Self-reference (emp_id != mgr_id via not_equal comparison) *)
let%test_unit "constraint scenario: self-reference neq" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema =
        Schema.empty
        |> Schema.add "emp_id" "natural"
        |> Schema.add "mgr_id" "natural"
      in
      let db, _rel =
        match
          Memory.create_relation storage db ~name:"reports_to"
            ~schema
        with
        | Error _ -> assert false
        | Ok x -> x
      in
      (* Register the not_equal comparison relation *)
      let db, _ =
        match
          Memory.create_immutable_relation storage db
            ~name:"not_equal"
            ~schema:
              (Schema.empty
              |> Schema.add "left" "natural"
              |> Schema.add "right" "natural")
            ~generator:(fun _ -> Generator.Error "not enumerable")
            ~membership_criteria:(fun _tree_of t ->
              match t with
              | Tuple.Materialized m -> (
                  match
                    ( Tuple.AttributeMap.find_opt "left" m.attributes,
                      Tuple.AttributeMap.find_opt "right" m.attributes )
                  with
                  | Some l, Some r ->
                      (Obj.obj l.Attribute.value : int)
                      <> (Obj.obj r.Attribute.value : int)
                  | _ -> false)
              | _ -> false)
            ~cardinality:Conventions.Cardinality.AlephZero
        with
        | Error _ -> assert false
        | Ok x -> x
      in
      (* Constraint: emp_id != mgr_id via not_equal relation *)
      let neq_body =
        Constraint.neq ~left:(Var "emp_id") ~right:(Var "mgr_id")
      in
      let db =
        match
          Memory.register_constraint storage db
            ~constraint_name:"no_self_manage" ~relation_name:"reports_to"
            ~body:neq_body
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      let rel =
        match Memory.get_relation db ~name:"reports_to" with
        | None -> assert false
        | Some r -> r
      in
      (* Insert (emp_id=1, mgr_id=2), should succeed *)
      let valid : Tuple.materialized =
        {
          relation = "reports_to";
          attributes =
            Tuple.AttributeMap.of_list
              [
                ("emp_id", { Attribute.value = Obj.repr 1 });
                ("mgr_id", { Attribute.value = Obj.repr 2 });
              ];
        }
      in
      let db, rel, _ =
        match Memory.create_tuple storage db rel valid with
        | Error _ -> assert false
        | Ok x -> x
      in
      (* Insert (emp_id=3, mgr_id=3): same person, should fail *)
      let invalid : Tuple.materialized =
        {
          relation = "reports_to";
          attributes =
            Tuple.AttributeMap.of_list
              [
                ("emp_id", { Attribute.value = Obj.repr 3 });
                ("mgr_id", { Attribute.value = Obj.repr 3 });
              ];
        }
      in
      match Memory.create_tuple storage db rel invalid with
      | Error (Error.ConstraintViolation _) -> ()
      | _ -> assert false)

(* Scenario 4: Mutual exclusion between open_ticket and closed_ticket *)
let%test_unit "constraint scenario: open vs closed ticket" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty |> Schema.add "ticket_id" "natural" in
      let db, _ =
        match
          Memory.create_relation storage db ~name:"open_ticket"
            ~schema
        with
        | Error _ -> assert false
        | Ok x -> x
      in
      let db, _ =
        match
          Memory.create_relation storage db ~name:"closed_ticket"
            ~schema
        with
        | Error _ -> assert false
        | Ok x -> x
      in
      (* open_ticket must not be in closed_ticket *)
      let db =
        match
          Memory.register_constraint storage db
            ~constraint_name:"not_closed" ~relation_name:"open_ticket"
            ~body:
              (Constraint.Not
                 {
                   body =
                     Constraint.MemberOf
                       {
                         target = "closed_ticket";
                         binding =
                           Constraint.BindingMap.singleton "ticket_id"
                             (Constraint.Var "ticket_id");
                       };
                   universe = "open_ticket";
                 })
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      (* Insert ticket 1 as closed *)
      let closed =
        match Memory.get_relation db ~name:"closed_ticket" with
        | None -> assert false
        | Some r -> r
      in
      let t1 : Tuple.materialized =
        {
          relation = "closed_ticket";
          attributes =
            Tuple.AttributeMap.singleton "ticket_id"
              { Attribute.value = Obj.repr 1 };
        }
      in
      let db, _, _ =
        match Memory.create_tuple storage db closed t1 with
        | Error _ -> assert false
        | Ok x -> x
      in
      (* Try to open same ticket, should fail *)
      let open_ =
        match Memory.get_relation db ~name:"open_ticket" with
        | None -> assert false
        | Some r -> r
      in
      let t1_open : Tuple.materialized =
        {
          relation = "open_ticket";
          attributes =
            Tuple.AttributeMap.singleton "ticket_id"
              { Attribute.value = Obj.repr 1 };
        }
      in
      match Memory.create_tuple storage db open_ t1_open with
      | Error (Error.ConstraintViolation _) -> ()
      | _ -> assert false)

(* Scenario 5: Weak entity dependency *)
let%test_unit "constraint scenario: weak entity dependency" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let parent_schema = Schema.empty |> Schema.add "parent_id" "natural" in
      let db, _ =
        match
          Memory.create_relation storage db ~name:"parent"
            ~schema:parent_schema
        with
        | Error _ -> assert false
        | Ok x -> x
      in
      let dep_schema =
        Schema.empty
        |> Schema.add "dep_id" "natural"
        |> Schema.add "parent_id" "natural"
      in
      let db, _ =
        match
          Memory.create_relation storage db ~name:"dependent"
            ~schema:dep_schema
        with
        | Error _ -> assert false
        | Ok x -> x
      in
      (* dependent.parent_id must exist in parent *)
      let db =
        match
          Memory.register_constraint storage db
            ~constraint_name:"parent_exists" ~relation_name:"dependent"
            ~body:
              (Constraint.MemberOf
                 {
                   target = "parent";
                   binding =
                     Constraint.BindingMap.singleton "parent_id"
                       (Constraint.Var "parent_id");
                 })
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      (* Insert parent *)
      let parent =
        match Memory.get_relation db ~name:"parent" with
        | None -> assert false
        | Some r -> r
      in
      let pt : Tuple.materialized =
        {
          relation = "parent";
          attributes =
            Tuple.AttributeMap.singleton "parent_id"
              { Attribute.value = Obj.repr 10 };
        }
      in
      let db, _, _ =
        match Memory.create_tuple storage db parent pt with
        | Error _ -> assert false
        | Ok x -> x
      in
      (* Insert dependent with valid parent, should succeed *)
      let dep =
        match Memory.get_relation db ~name:"dependent" with
        | None -> assert false
        | Some r -> r
      in
      let valid_dep : Tuple.materialized =
        {
          relation = "dependent";
          attributes =
            Tuple.AttributeMap.of_list
              [
                ("dep_id", { Attribute.value = Obj.repr 1 });
                ("parent_id", { Attribute.value = Obj.repr 10 });
              ];
        }
      in
      let db, dep, _ =
        match Memory.create_tuple storage db dep valid_dep with
        | Error _ -> assert false
        | Ok x -> x
      in
      (* Insert dependent with nonexistent parent, should fail *)
      let invalid_dep : Tuple.materialized =
        {
          relation = "dependent";
          attributes =
            Tuple.AttributeMap.of_list
              [
                ("dep_id", { Attribute.value = Obj.repr 2 });
                ("parent_id", { Attribute.value = Obj.repr 999 });
              ];
        }
      in
      match Memory.create_tuple storage db dep invalid_dep with
      | Error (Error.ConstraintViolation _) -> ()
      | _ -> assert false)

(* Scenario 6: Algebra propagation, select preserves constraints *)
let%test_unit "constraint propagation: select preserves constraints" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty |> Schema.add "x" "natural" in
      let db, _rel =
        match
          Memory.create_relation storage db ~name:"constrained"
            ~schema
        with
        | Error _ -> assert false
        | Ok x -> x
      in
      let db =
        match
          Memory.register_constraint storage db
            ~constraint_name:"c1" ~relation_name:"constrained"
            ~body:(Constraint.And [])
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      let rel =
        match Memory.get_relation db ~name:"constrained" with
        | None -> assert false
        | Some r -> r
      in
      match Algebra.Memory.select_fn storage (fun _ -> true) rel with
      | Error _ -> assert false
      | Ok result -> assert (result.Relation.constraints <> None))

(* Scenario 7: Algebra propagation, project filters constraints *)
let%test_unit "constraint propagation: project filters constraints" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"test" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema =
        Schema.empty |> Schema.add "x" "natural" |> Schema.add "y" "natural"
      in
      let db, _rel =
        match
          Memory.create_relation storage db ~name:"xy" ~schema
        with
        | Error _ -> assert false
        | Ok x -> x
      in
      (* Constraint on x only *)
      let c_on_x =
        Constraint.MemberOf
          {
            target = "some_rel";
            binding =
              Constraint.BindingMap.singleton "left" (Constraint.Var "x");
          }
      in
      let db =
        match
          Memory.register_constraint storage db
            ~constraint_name:"x_only" ~relation_name:"xy" ~body:c_on_x
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      let rel =
        match Memory.get_relation db ~name:"xy" with
        | None -> assert false
        | Some r -> r
      in
      (* Project to only "x", constraint should survive *)
      (match Algebra.Memory.project storage [ "x" ] rel with
      | Error _ -> assert false
      | Ok result -> assert (result.Relation.constraints <> None));
      (* Project to only "y", constraint referencing "x" should be dropped *)
      match Algebra.Memory.project storage [ "y" ] rel with
      | Error _ -> assert false
      | Ok result -> assert (result.Relation.constraints = None))

(* Parse tests use canonical flat sexp format: (Constructor (field val) ...) *)

let%test_unit "ddl: parse CreateDatabase" =
  match Ddl.Parser.of_string {|(CreateDatabase "shop")|} with
  | Error _ -> assert false
  | Ok stmt -> assert (stmt = Ddl.Ast.CreateDatabase "shop")

let%test_unit "ddl: parse RetractRelation" =
  match Ddl.Parser.of_string {|(RetractRelation "users")|} with
  | Error _ -> assert false
  | Ok stmt -> assert (stmt = Ddl.Ast.RetractRelation "users")

let%test_unit "ddl: parse ClearRelation" =
  match Ddl.Parser.of_string {|(ClearRelation "users")|} with
  | Error _ -> assert false
  | Ok stmt -> assert (stmt = Ddl.Ast.ClearRelation "users")

let%test_unit "dml: round-trip InsertTuple" =
  let src =
    Dml.Ast.InsertTuple
      {
        relation = "users";
        attributes = [ ("name", Drl.Ast.Str "Alice"); ("age", Drl.Ast.Int 30) ];
      }
  in
  match Dml.Parser.of_string (Dml.Parser.to_string src) with
  | Error _ -> assert false
  | Ok parsed -> assert (parsed = src)

let%test_unit "ddl: round-trip CreateRelation" =
  let src =
    Ddl.Ast.CreateRelation
      { name = "users"; schema = [ ("name", "string"); ("age", "natural") ] }
  in
  match Ddl.Parser.of_string (Ddl.Parser.to_string src) with
  | Error _ -> assert false
  | Ok parsed -> assert (parsed = src)

let%test_unit "dml: round-trip InsertTuples" =
  let src =
    Dml.Ast.InsertTuples
      {
        relation = "users";
        tuples =
          [
            [ ("name", Drl.Ast.Str "Alice"); ("age", Drl.Ast.Int 30) ];
            [ ("name", Drl.Ast.Str "Bob"); ("age", Drl.Ast.Int 25) ];
          ];
      }
  in
  match Dml.Parser.of_string (Dml.Parser.to_string src) with
  | Error _ -> assert false
  | Ok parsed -> assert (parsed = src)

let%test_unit "ddl: round-trip RegisterDomain" =
  let src =
    Ddl.Ast.RegisterDomain { name = "money"; cardinality = Ddl.Ast.AlephZero }
  in
  match Ddl.Parser.of_string (Ddl.Parser.to_string src) with
  | Error _ -> assert false
  | Ok parsed -> assert (parsed = src)

let%test_unit "prl: round-trip DefineFunctionPredicate" =
  let src =
    Prl.Ast.DefineFunctionPredicate
      {
        name = "numbers:ones";
        schema = [ ("x", "natural") ];
        symbol = "test.ones";
        purity = Conventions.Purity.Pure;
        cardinality = Conventions.Cardinality.ConstrainedFinite;
      }
  in
  match Prl.Parser.of_string (Prl.Parser.to_string src) with
  | Error _ -> assert false
  | Ok parsed -> assert (parsed = src)

let%test_unit "prl: define predicate and query via DRL" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"prl_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      Prl.Plugin_api.register "test.ones"
        (Prl.Plugin_api.implementation_of_rows
           ~membership_criteria:(fun tuple ->
             match List.assoc_opt "x" tuple with
             | Some v -> Ok ((Obj.obj v : int) = 1)
             | None -> Ok false)
           [ [ ("x", Obj.repr 1) ] ]);
      let stmt =
        Prl.Ast.DefineFunctionPredicate
          {
            name = "ones";
            schema = [ ("x", "natural") ];
            symbol = "test.ones";
            purity = Conventions.Purity.Pure;
            cardinality = Conventions.Cardinality.ConstrainedFinite;
          }
      in
      let db =
        match Prl.Executor.Memory.execute storage db stmt with
        | Error _ -> assert false
        | Ok (db, _) -> db
      in
      let rel =
        match Drl.Executor.Memory.execute storage db (Drl.Ast.Base "ones") with
        | Error _ -> assert false
        | Ok rel -> rel
      in
      let tuples =
        match Algebra.Memory.materialize storage rel with
        | Error _ -> assert false
        | Ok tuples -> tuples
      in
      assert (List.length tuples = 1);
      match tuples with
      | [ tuple ] -> (
          match Tuple.AttributeMap.find_opt "x" tuple.Tuple.attributes with
          | None -> assert false
          | Some attr -> assert ((Obj.obj attr.Attribute.value : int) = 1))
      | _ -> assert false)

let%test_unit "prl: io predicates are rejected in DRL" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"prl_io_db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      Prl.Plugin_api.register "test.io"
        (Prl.Plugin_api.implementation_of_rows
           ~membership_criteria:(fun _ -> Ok true) [ [ ("x", Obj.repr 0) ] ]);
      let stmt =
        Prl.Ast.DefineFunctionPredicate
          {
            name = "io_rel";
            schema = [ ("x", "natural") ];
            symbol = "test.io";
            purity = Conventions.Purity.IO;
            cardinality = Conventions.Cardinality.ConstrainedFinite;
          }
      in
      let db =
        match Prl.Executor.Memory.execute storage db stmt with
        | Error _ -> assert false
        | Ok (db, _) -> db
      in
      match Drl.Executor.Memory.execute storage db (Drl.Ast.Base "io_rel") with
      | Error (Drl.Executor.Memory.IoPredicateNotAllowed "io_rel") -> ()
      | _ -> assert false)

(* Executor tests construct AST directly, no dependency on sexp format *)

let%test_unit "ddl: execute CreateDatabase" =
  with_storage (fun storage ->
      let db = Management.Database.empty ~name:"" in
      let stmt = Ddl.Ast.CreateDatabase "shop" in
      match Ddl.Executor.Memory.execute storage db stmt with
      | Error _ -> assert false
      | Ok (db, _msg) -> assert (db.name = "shop"))

let%test_unit "ddl: execute CreateRelation" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"shop" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let stmt =
        Ddl.Ast.CreateRelation
          {
            name = "users";
            schema = [ ("name", "string"); ("age", "natural") ];
          }
      in
      match Ddl.Executor.Memory.execute storage db stmt with
      | Error _ -> assert false
      | Ok (db, _msg) -> assert (Management.Database.has_relation db "users"))

let%test_unit "dml: execute InsertTuple" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"shop" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty |> Schema.add "name" "string" in
      let db =
        match
          Memory.create_relation storage db ~name:"users" ~schema
        with
        | Error _ -> assert false
        | Ok (db, _) -> db
      in
      let stmt =
        Dml.Ast.InsertTuple
          { relation = "users"; attributes = [ ("name", Drl.Ast.Str "Alice") ] }
      in
      match Dml.Executor.Memory.execute storage db stmt with
      | Error _ -> assert false
      | Ok db ->
          let rel =
            match Management.Database.get_relation db "users" with
            | None -> assert false
            | Some r -> r
          in
          assert (Memory.tuple_count rel = 1))

let%test_unit "dml: execute InsertTuples" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"shop" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty |> Schema.add "name" "string" in
      let db =
        match
          Memory.create_relation storage db ~name:"users" ~schema
        with
        | Error _ -> assert false
        | Ok (db, _) -> db
      in
      let stmt =
        Dml.Ast.InsertTuples
          {
            relation = "users";
            tuples =
              [
                [ ("name", Drl.Ast.Str "Alice") ];
                [ ("name", Drl.Ast.Str "Bob") ];
              ];
          }
      in
      match Dml.Executor.Memory.execute storage db stmt with
      | Error _ -> assert false
      | Ok db ->
          let rel =
            match Management.Database.get_relation db "users" with
            | None -> assert false
            | Some r -> r
          in
          assert (Memory.tuple_count rel = 2))

let%test_unit "dml: execute DeleteTuple" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"shop" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty |> Schema.add "name" "string" in
      let db =
        match
          Memory.create_relation storage db ~name:"users" ~schema
        with
        | Error _ -> assert false
        | Ok (db, _) -> db
      in
      let db =
        match
          Dml.Executor.Memory.execute storage db
            (Dml.Ast.InsertTuple
               {
                 relation = "users";
                 attributes = [ ("name", Drl.Ast.Str "Alice") ];
               })
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      let stmt =
        Dml.Ast.DeleteTuple
          { relation = "users"; attributes = [ ("name", Drl.Ast.Str "Alice") ] }
      in
      match Dml.Executor.Memory.execute storage db stmt with
      | Error _ -> assert false
      | Ok db ->
          let rel =
            match Management.Database.get_relation db "users" with
            | None -> assert false
            | Some r -> r
          in
          assert (Memory.tuple_count rel = 0))

let%test_unit "ddl: execute RetractRelation" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"shop" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty in
      let db =
        match
          Memory.create_relation storage db ~name:"users" ~schema
        with
        | Error _ -> assert false
        | Ok (db, _) -> db
      in
      match
        Ddl.Executor.Memory.execute storage db (Ddl.Ast.RetractRelation "users")
      with
      | Error _ -> assert false
      | Ok (db, _msg) ->
          assert (not (Management.Database.has_relation db "users")))

let%test_unit "ddl: execute ClearRelation" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"shop" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty |> Schema.add "name" "string" in
      let db =
        match
          Memory.create_relation storage db ~name:"users" ~schema
        with
        | Error _ -> assert false
        | Ok (db, _) -> db
      in
      let db =
        match
          Dml.Executor.Memory.execute storage db
            (Dml.Ast.InsertTuples
               {
                 relation = "users";
                 tuples =
                   [
                     [ ("name", Drl.Ast.Str "Alice") ];
                     [ ("name", Drl.Ast.Str "Bob") ];
                   ];
               })
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      match
        Ddl.Executor.Memory.execute storage db (Ddl.Ast.ClearRelation "users")
      with
      | Error _ -> assert false
      | Ok (db, _msg) ->
          let rel =
            match Management.Database.get_relation db "users" with
            | None -> assert false
            | Some r -> r
          in
          assert (Memory.tuple_count rel = 0))

let%test_unit "ddl: execute RegisterDomain" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"shop" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let stmt =
        Ddl.Ast.RegisterDomain
          { name = "money"; cardinality = Ddl.Ast.AlephZero }
      in
      match Ddl.Executor.Memory.execute storage db stmt with
      | Error _ -> assert false
      | Ok (db, _msg) -> assert (Management.Database.has_domain db "money"))

let%test_unit "dml: insert into nonexistent relation returns error" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"shop" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let stmt =
        Dml.Ast.InsertTuple
          { relation = "ghost"; attributes = [ ("x", Drl.Ast.Int 1) ] }
      in
      match Dml.Executor.Memory.execute storage db stmt with
      | Error (Dml.Executor.Memory.RelationNotFound "ghost") -> ()
      | _ -> assert false)

let%test_unit "dcl: round-trip RegisterConstraint MemberOf" =
  let src =
    Icl.Ast.RegisterConstraint
      {
        constraint_name = "fk_order";
        relation_name = "order_items";
        body =
          Icl.Ast.MemberOf
            {
              target = "orders";
              binding = [ ("order_id", Icl.Ast.Var "order_id") ];
            };
      }
  in
  match Icl.Parser.of_string (Icl.Parser.to_string src) with
  | Error _ -> assert false
  | Ok parsed -> assert (parsed = src)

let%test_unit "dcl: round-trip And constraint" =
  let src =
    Icl.Ast.RegisterConstraint
      {
        constraint_name = "valid_range";
        relation_name = "scores";
        body =
          Icl.Ast.And
            [
              Icl.Ast.MemberOf
                {
                  target = "ge_zero";
                  binding =
                    [
                      ("left", Icl.Ast.Var "score");
                      ("right", Icl.Ast.Const (Drl.Ast.Int 0));
                    ];
                };
              Icl.Ast.MemberOf
                {
                  target = "le_hundred";
                  binding =
                    [
                      ("left", Icl.Ast.Var "score");
                      ("right", Icl.Ast.Const (Drl.Ast.Int 100));
                    ];
                };
            ];
      }
  in
  match Icl.Parser.of_string (Icl.Parser.to_string src) with
  | Error _ -> assert false
  | Ok parsed -> assert (parsed = src)

let%test_unit "dcl: round-trip Not constraint" =
  let src =
    Icl.Ast.RegisterConstraint
      {
        constraint_name = "not_closed";
        relation_name = "open_ticket";
        body =
          Icl.Ast.Not
            {
              body =
                Icl.Ast.MemberOf
                  {
                    target = "closed_ticket";
                    binding = [ ("ticket_id", Icl.Ast.Var "ticket_id") ];
                  };
              universe = "open_ticket";
            };
      }
  in
  match Icl.Parser.of_string (Icl.Parser.to_string src) with
  | Error _ -> assert false
  | Ok parsed -> assert (parsed = src)

let%test_unit "dcl: execute RegisterConstraint attaches constraint" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"shop" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty |> Schema.add "order_id" "natural" in
      let db =
        match
          Memory.create_relation storage db ~name:"order_items"
            ~schema
        with
        | Error _ -> assert false
        | Ok (db, _) -> db
      in
      let stmt =
        Icl.Ast.RegisterConstraint
          {
            constraint_name = "fk_order";
            relation_name = "order_items";
            body =
              Icl.Ast.MemberOf
                {
                  target = "orders";
                  binding = [ ("order_id", Icl.Ast.Var "order_id") ];
                };
          }
      in
      match Icl.Executor.Memory.execute storage db stmt with
      | Error _ -> assert false
      | Ok (db, _) ->
          let rel =
            match Management.Database.get_relation db "order_items" with
            | None -> assert false
            | Some r -> r
          in
          assert (rel.Relation.constraints <> None))

let%test_unit "dcl: FK constraint enforced on insert" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"shop" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let db =
        match
          Memory.create_relation storage db ~name:"orders"
            ~schema:(Schema.empty |> Schema.add "id" "natural")
        with
        | Error _ -> assert false
        | Ok (db, _) -> db
      in
      (* Insert a valid order *)
      let db =
        match
          Dml.Executor.Memory.execute storage db
            (Dml.Ast.InsertTuple
               { relation = "orders"; attributes = [ ("id", Drl.Ast.Int 1) ] })
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      let db =
        match
          Memory.create_relation storage db ~name:"order_items"
            ~schema:(Schema.empty |> Schema.add "order_id" "natural")
        with
        | Error _ -> assert false
        | Ok (db, _) -> db
      in
      (* Register FK: order_id in order_items must match id in orders *)
      let db =
        match
          Icl.Executor.Memory.execute storage db
            (Icl.Ast.RegisterConstraint
               {
                 constraint_name = "fk_order";
                 relation_name = "order_items";
                 body =
                   Icl.Ast.MemberOf
                     {
                       target = "orders";
                       binding = [ ("id", Icl.Ast.Var "order_id") ];
                     };
               })
        with
        | Error _ -> assert false
        | Ok (db, _) -> db
      in
      (* Valid insert: order_id=1 exists in orders *)
      let db =
        match
          Dml.Executor.Memory.execute storage db
            (Dml.Ast.InsertTuple
               {
                 relation = "order_items";
                 attributes = [ ("order_id", Drl.Ast.Int 1) ];
               })
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      let rel =
        match Management.Database.get_relation db "order_items" with
        | None -> assert false
        | Some r -> r
      in
      assert (Memory.tuple_count rel = 1);
      (* Invalid insert: order_id=99 does not exist in orders *)
      match
        Dml.Executor.Memory.execute storage db
          (Dml.Ast.InsertTuple
             {
               relation = "order_items";
               attributes = [ ("order_id", Drl.Ast.Int 99) ];
             })
      with
      | Error
          (Dml.Executor.Memory.ManipulationError
             (Error.ConstraintViolation _)) ->
          ()
      | _ -> assert false)

module BranchMemory = Management.Branch.Make (Management.Physical.Memory)

let%test_unit "branch: create and get_tip" =
  with_storage (fun storage ->
      let tip = "deadbeef" in
      (match BranchMemory.create storage ~name:"main" ~tip with
      | Error _ -> assert false
      | Ok () -> ());
      match BranchMemory.get_tip storage "main" with
      | Error _ -> assert false
      | Ok None -> assert false
      | Ok (Some t) -> assert (t = tip))

let%test_unit "branch: get_tip returns None for unknown branch" =
  with_storage (fun storage ->
      match BranchMemory.get_tip storage "nonexistent" with
      | Error _ -> assert false
      | Ok None -> ()
      | Ok (Some _) -> assert false)

let%test_unit "branch: checkout and get_head" =
  with_storage (fun storage ->
      (match BranchMemory.create storage ~name:"main" ~tip:"abc" with
      | Error _ -> assert false
      | Ok () -> ());
      (match BranchMemory.checkout storage "main" with
      | Error _ -> assert false
      | Ok () -> ());
      match BranchMemory.get_head storage with
      | Error _ -> assert false
      | Ok None -> assert false
      | Ok (Some h) -> assert (h = "main"))

let%test_unit "branch: get_head returns None when not set" =
  with_storage (fun storage ->
      match BranchMemory.get_head storage with
      | Error _ -> assert false
      | Ok None -> ()
      | Ok (Some _) -> assert false)

let%test_unit "branch: update_tip advances the branch" =
  with_storage (fun storage ->
      (match BranchMemory.create storage ~name:"main" ~tip:"v1" with
      | Error _ -> assert false
      | Ok () -> ());
      (match BranchMemory.update_tip storage ~name:"main" ~tip:"v2" with
      | Error _ -> assert false
      | Ok () -> ());
      match BranchMemory.get_tip storage "main" with
      | Error _ -> assert false
      | Ok None -> assert false
      | Ok (Some t) -> assert (t = "v2"))

let%test_unit "branch: update_tip fails on unknown branch" =
  with_storage (fun storage ->
      match BranchMemory.update_tip storage ~name:"ghost" ~tip:"v1" with
      | Error _ -> ()
      | Ok () -> assert false)

let%test_unit "branch: multiple branches are independent" =
  with_storage (fun storage ->
      (match BranchMemory.create storage ~name:"main" ~tip:"hash-main" with
      | Error _ -> assert false
      | Ok () -> ());
      (match
         BranchMemory.create storage ~name:"feature" ~tip:"hash-feature"
       with
      | Error _ -> assert false
      | Ok () -> ());
      let main_tip =
        match BranchMemory.get_tip storage "main" with
        | Ok (Some t) -> t
        | _ -> assert false
      in
      let feat_tip =
        match BranchMemory.get_tip storage "feature" with
        | Ok (Some t) -> t
        | _ -> assert false
      in
      assert (main_tip = "hash-main");
      assert (feat_tip = "hash-feature");
      assert (main_tip <> feat_tip))

let%test_unit "diff: identical databases produce empty diff" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let diffs = Management.Diff.diff ~ancestor:db ~target:db in
      assert (diffs = []))

let%test_unit "diff: added relation detected" =
  with_storage (fun storage ->
      let ancestor =
        match Memory.create_database storage ~name:"db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let schema = Schema.empty |> Schema.add "x" "natural" in
      let target, _ =
        match
          Memory.create_relation storage ancestor ~name:"new_rel"
            ~schema
        with
        | Error _ -> assert false
        | Ok p -> p
      in
      let diffs = Management.Diff.diff ~ancestor ~target in
      let added =
        List.filter_map
          (function Management.Diff.RelationAdded r -> Some r | _ -> None)
          diffs
      in
      assert (List.exists (fun r -> r.Relation.name = "new_rel") added))

let%test_unit "diff: removed relation detected" =
  with_storage (fun storage ->
      let schema = Schema.empty |> Schema.add "x" "natural" in
      let ancestor =
        match Memory.create_database storage ~name:"db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let ancestor, _ =
        match
          Memory.create_relation storage ancestor ~name:"gone"
            ~schema
        with
        | Error _ -> assert false
        | Ok p -> p
      in
      let target =
        match
          Memory.retract_relation storage ancestor ~name:"gone"
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      let diffs = Management.Diff.diff ~ancestor ~target in
      let removed =
        List.filter_map
          (function Management.Diff.RelationRemoved n -> Some n | _ -> None)
          diffs
      in
      assert (List.mem "gone" removed))

let%test_unit "diff: modified relation detected with added tuple" =
  with_storage (fun storage ->
      let schema = Schema.empty |> Schema.add "val" "natural" in
      let ancestor =
        match Memory.create_database storage ~name:"db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let ancestor, _ =
        match
          Memory.create_relation storage ancestor ~name:"r" ~schema
        with
        | Error _ -> assert false
        | Ok p -> p
      in
      let rel = Option.get (Management.Database.get_relation ancestor "r") in
      let t : Tuple.materialized =
        {
          Tuple.relation = "r";
          attributes =
            Tuple.AttributeMap.singleton "val" { Attribute.value = Obj.repr 42 };
        }
      in
      let target, _, _ =
        match Memory.create_tuple storage ancestor rel t with
        | Error _ -> assert false
        | Ok p -> p
      in
      let diffs = Management.Diff.diff ~ancestor ~target in
      assert (List.length diffs = 1);
      match diffs with
      | [
       Management.Diff.RelationModified
         { name; added_tuples; removed_tuples; _ };
      ] ->
          assert (name = "r");
          assert (List.length added_tuples = 1);
          assert (removed_tuples = [])
      | _ -> assert false)

module MergeMemory =
  Management.Merge.Make (Management.Physical.Memory) (Memory)

let%test_unit "merge: fast-forward when only one side changed" =
  with_storage (fun storage ->
      let schema = Schema.empty |> Schema.add "val" "natural" in
      let base_db =
        match Memory.create_database storage ~name:"db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let base_db, _ =
        match
          Memory.create_relation storage base_db ~name:"r" ~schema
        with
        | Error _ -> assert false
        | Ok p -> p
      in
      (match Memory.store_database storage base_db with
      | Error _ -> assert false
      | Ok () -> ());
      let rel = Option.get (Management.Database.get_relation base_db "r") in
      let t1 : Tuple.materialized =
        {
          Tuple.relation = "r";
          attributes =
            Tuple.AttributeMap.singleton "val" { Attribute.value = Obj.repr 1 };
        }
      in
      let left_db, _, _ =
        match Memory.create_tuple storage base_db rel t1 with
        | Error _ -> assert false
        | Ok p -> p
      in
      (match Memory.store_database storage left_db with
      | Error _ -> assert false
      | Ok () -> ());
      match
        MergeMemory.merge ~storage ~strategy:Management.Merge.PreferLeft
          ~left_tip:left_db.Management.Database.hash
          ~right_tip:base_db.Management.Database.hash
      with
      | Error _ -> assert false
      | Ok (Management.Merge.Failed _) -> assert false
      | Ok (Management.Merge.Clean merged) ->
          let merged_rel =
            Option.get (Management.Database.get_relation merged "r")
          in
          assert (Memory.tuple_count merged_rel = 1))

let%test_unit "merge: independent tuple additions produce union" =
  with_storage (fun storage ->
      let schema = Schema.empty |> Schema.add "val" "natural" in
      let base_db =
        match Memory.create_database storage ~name:"db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let base_db, _ =
        match
          Memory.create_relation storage base_db ~name:"r" ~schema
        with
        | Error _ -> assert false
        | Ok p -> p
      in
      (match Memory.store_database storage base_db with
      | Error _ -> assert false
      | Ok () -> ());
      let rel = Option.get (Management.Database.get_relation base_db "r") in
      let t_left : Tuple.materialized =
        {
          Tuple.relation = "r";
          attributes =
            Tuple.AttributeMap.singleton "val" { Attribute.value = Obj.repr 10 };
        }
      in
      let left_db, _, _ =
        match Memory.create_tuple storage base_db rel t_left with
        | Error _ -> assert false
        | Ok p -> p
      in
      (match Memory.store_database storage left_db with
      | Error _ -> assert false
      | Ok () -> ());
      let t_right : Tuple.materialized =
        {
          Tuple.relation = "r";
          attributes =
            Tuple.AttributeMap.singleton "val" { Attribute.value = Obj.repr 20 };
        }
      in
      let right_db, _, _ =
        match Memory.create_tuple storage base_db rel t_right with
        | Error _ -> assert false
        | Ok p -> p
      in
      (match Memory.store_database storage right_db with
      | Error _ -> assert false
      | Ok () -> ());
      match
        MergeMemory.merge ~storage ~strategy:Management.Merge.PreferLeft
          ~left_tip:left_db.Management.Database.hash
          ~right_tip:right_db.Management.Database.hash
      with
      | Error _ -> assert false
      | Ok (Management.Merge.Failed _) -> assert false
      | Ok (Management.Merge.Clean merged) ->
          let merged_rel =
            Option.get (Management.Database.get_relation merged "r")
          in
          assert (Memory.tuple_count merged_rel = 2))

let%test_unit "merge: no-op when both sides are identical" =
  with_storage (fun storage ->
      let base_db =
        match Memory.create_database storage ~name:"db" with
        | Error _ -> assert false
        | Ok db -> db
      in
      (match Memory.store_database storage base_db with
      | Error _ -> assert false
      | Ok () -> ());
      match
        MergeMemory.merge ~storage ~strategy:Management.Merge.PreferLeft
          ~left_tip:base_db.Management.Database.hash
          ~right_tip:base_db.Management.Database.hash
      with
      | Error _ -> assert false
      | Ok (Management.Merge.Failed _) -> assert false
      | Ok (Management.Merge.Clean merged) ->
          assert (
            Management.Database.get_relation_names merged
            = Management.Database.get_relation_names base_db))

(* Helper: look up polarity for a relation name in the result map *)
let find_polarity name pols = Constraint.polarity_find name pols

let%test_unit "polarity: MemberOf target is Positive" =
  let c =
    Constraint.MemberOf { target = "R"; binding = Constraint.BindingMap.empty }
  in
  assert (
    find_polarity "R" (Constraint.polarity_of c) = Some Constraint.Positive)

let%test_unit "polarity: Not MemberOf flips to Negative" =
  let c =
    Constraint.Not
      {
        body =
          Constraint.MemberOf
            { target = "R"; binding = Constraint.BindingMap.empty };
        universe = "U";
      }
  in
  assert (
    find_polarity "R" (Constraint.polarity_of c) = Some Constraint.Negative)

let%test_unit "polarity: Exists quantifier is Positive" =
  let c =
    Constraint.Exists
      {
        variable = "x";
        quantifier = "Q";
        body =
          Constraint.MemberOf
            { target = "Q"; binding = Constraint.BindingMap.empty };
      }
  in
  assert (
    find_polarity "Q" (Constraint.polarity_of c) = Some Constraint.Positive)

let%test_unit "polarity: Forall quantifier is Negative" =
  let c =
    Constraint.Forall
      {
        variable = "x";
        quantifier = "Q";
        body =
          Constraint.MemberOf
            { target = "T"; binding = Constraint.BindingMap.empty };
      }
  in
  assert (
    find_polarity "Q" (Constraint.polarity_of c) = Some Constraint.Negative);
  assert (
    find_polarity "T" (Constraint.polarity_of c) = Some Constraint.Positive)

let%test_unit "polarity: same relation with both polarities merges to Both" =
  (* Not(MemberOf R) AND MemberOf R => R appears Negative and Positive => Both *)
  let c =
    Constraint.And
      [
        Constraint.Not
          {
            body =
              Constraint.MemberOf
                { target = "R"; binding = Constraint.BindingMap.empty };
            universe = "U";
          };
        Constraint.MemberOf
          { target = "R"; binding = Constraint.BindingMap.empty };
      ]
  in
  assert (find_polarity "R" (Constraint.polarity_of c) = Some Constraint.Both)

let%test_unit "polarity: Forall body MemberOf keeps Positive" =
  (* Forall x in Q, MemberOf(T): T still has positive polarity *)
  let c =
    Constraint.Forall
      {
        variable = "x";
        quantifier = "Q";
        body =
          Constraint.MemberOf
            { target = "T"; binding = Constraint.BindingMap.empty };
      }
  in
  assert (
    find_polarity "T" (Constraint.polarity_of c) = Some Constraint.Positive)

let%test_unit "polarity: nested Not double-negation restores Positive" =
  let c =
    Constraint.Not
      {
        body =
          Constraint.Not
            {
              body =
                Constraint.MemberOf
                  { target = "R"; binding = Constraint.BindingMap.empty };
              universe = "U";
            };
        universe = "V";
      }
  in
  assert (
    find_polarity "R" (Constraint.polarity_of c) = Some Constraint.Positive)

let%test_unit "polarity: unrelated relation absent from result" =
  let c =
    Constraint.MemberOf { target = "R"; binding = Constraint.BindingMap.empty }
  in
  assert (find_polarity "S" (Constraint.polarity_of c) = None)

let abs i = Obj.magic (i : int) (* lift int to AbstractValue for tests *)

let%test_unit
    "focused_filter: Var binding maps deleted value to constrained attr" =
  (* Constraint: MemberOf Dept (dept_id = Var "dept_id")
     Deleted:    Dept { dept_id = 99 }
     Expected filter: [("dept_id", 99)]

     This is the core FK pattern. The Var "dept_id" in the binding says:
     "the constrained tuple's dept_id must equal whatever the Dept tuple's
     dept_id is." When we delete Dept{dept_id=99}, focused_filter resolves
     the Var to the deleted value and returns [("dept_id", 99)].

     The cascade checker uses this to scan only Employee tuples where
     dept_id=99 rather than re-checking every Employee row. *)
  let binding =
    Constraint.BindingMap.singleton "dept_id" (Constraint.Var "dept_id")
  in
  let c = Constraint.MemberOf { target = "Dept"; binding } in
  let deleted = [ ("dept_id", abs 99) ] in
  let filter = Constraint.focused_filter c "Dept" deleted in
  assert (List.length filter = 1);
  assert (fst (List.hd filter) = "dept_id");
  assert (Stdlib.( = ) (snd (List.hd filter)) (abs 99))

let%test_unit "focused_filter: Const binding is ignored (no var link)" =
  (* Constraint: MemberOf Dept (code = Const "eng")
     Deleted:    Dept { code = "eng" }
     Expected filter: []

     A Const binding is a fixed literal that restricts which rows the constraint
     *applies to*: it is not a join condition. There is no Var, so no link
     exists between the deleted tuple's attributes and the attributes of any
     constrained relation. focused_filter correctly returns [] here.

     [] does NOT mean "nothing is affected." It means "no tighter filter could
     be derived": the caller must fall back to re-checking every tuple in the
     constrained relation. The tempting but wrong reading would be to see
     that the deleted value "eng" matches Const "eng" and emit [("code","eng")].
     Doing so would conflate a constraint literal with a join variable. *)
  let binding =
    Constraint.BindingMap.singleton "code"
      (Constraint.Const (Obj.magic ("eng" : string)))
  in
  let c = Constraint.MemberOf { target = "Dept"; binding } in
  let filter =
    Constraint.focused_filter c "Dept" [ ("code", Obj.magic ("eng" : string)) ]
  in
  assert (filter = [])

let%test_unit "focused_filter: Exists body MemberOf same relation is followed" =
  (* Constraint on Employee: Exists d in Dept, MemberOf Dept (dept_id = Var "dept_id")
     Deleted: Dept { dept_id = 7 }
     Expected filter on Employee: [("dept_id", 7)]

     Real FK constraints are wrapped in Exists: "there exists a dept d such
     that this employee's dept_id matches d's dept_id." The dep_rel ("Dept")
     appears as the Exists quantifier, not directly in a top-level MemberOf.

     focused_filter must recognise this pattern and recurse into the Exists
     body to find the Var binding. If it stopped at the Exists node and
     returned [], the cascade checker would lose the narrowing and fall back
     to scanning every Employee, defeating the optimisation entirely. *)
  let binding =
    Constraint.BindingMap.singleton "dept_id" (Constraint.Var "dept_id")
  in
  let c =
    Constraint.Exists
      {
        variable = "d";
        quantifier = "Dept";
        body = Constraint.MemberOf { target = "Dept"; binding };
      }
  in
  let filter = Constraint.focused_filter c "Dept" [ ("dept_id", abs 7) ] in
  assert (List.length filter = 1);
  assert (fst (List.hd filter) = "dept_id")

let%test_unit "focused_filter: unrelated dep_rel yields empty filter" =
  (* Constraint references relation R. We are deleting from relation S.
     Expected filter: []

     If the deleted relation does not appear anywhere in the constraint,
     the deletion cannot possibly affect any constrained tuple since no join
     variable links them. The cascade checker uses [] as a signal to skip
     the re-check for this constraint entirely, not to re-check everything. *)
  let binding = Constraint.BindingMap.singleton "x" (Constraint.Var "x") in
  let c = Constraint.MemberOf { target = "R"; binding } in
  let filter = Constraint.focused_filter c "S" [ ("x", abs 1) ] in
  assert (filter = [])

let%test_unit "trigger_constants: Const value in binding is extracted" =
  (* Constraint: MemberOf R (status = Const "active")
     Expected constants: [("status", "active")]

     trigger_constants answers: "for a DELETE from dep_rel to even risk
     violating this constraint, what attribute values must the deleted tuple
     have?" A Const binding says the constraint only applies when the dep_rel
     tuple has that exact value. If the deleted tuple's status ≠ "active",
     this constraint can never be violated by that deletion, so it is skipped.

     The cascade checker calls trigger_constants before doing any tuple scan
     and bails out early if the deleted tuple doesn't match. *)
  let binding =
    Constraint.BindingMap.singleton "status"
      (Constraint.Const (Obj.magic ("active" : string)))
  in
  let c = Constraint.MemberOf { target = "R"; binding } in
  let consts = Constraint.trigger_constants c "R" in
  assert (List.length consts = 1);
  assert (fst (List.hd consts) = "status")

let%test_unit "trigger_constants: Var binding produces no constant" =
  (* Constraint: MemberOf R (id = Var "id")
     Expected constants: []

     A Var binding imposes no fixed-value precondition on the deleted tuple.
     It is a join variable, not a filter. Any deleted tuple from R could
     match a constrained tuple via the Var link, so no early-exit is possible.
     [] means: always proceed to the cascade scan. *)
  let binding = Constraint.BindingMap.singleton "id" (Constraint.Var "id") in
  let c = Constraint.MemberOf { target = "R"; binding } in
  let consts = Constraint.trigger_constants c "R" in
  assert (consts = [])

let%test_unit "trigger_constants: unrelated dep_rel yields empty" =
  (* Constraint references relation R. We are deleting from relation S.
     Expected constants: []

     When dep_rel does not appear in the constraint at all, trigger_constants
     returns [], meaning no precondition was found so the cascade scan proceeds. However,
     focused_filter will also return [] for an unrelated dep_rel, causing the
     cascade checker to skip the re-check for a different reason (no join
     link). Both [] cases ultimately mean no affected tuples are found. *)
  let binding =
    Constraint.BindingMap.singleton "x" (Constraint.Const (abs 1))
  in
  let c = Constraint.MemberOf { target = "R"; binding } in
  let consts = Constraint.trigger_constants c "S" in
  assert (consts = [])

let%test_unit
    "substitute_transition: Var \"variable.attr\" is replaced by Const from \
     transition tuple" =
  (* Constraint: Exists d in Department, MemberOf Target (key = Var "d.dept_id")
     Transition: Department row { dept_id = 99 }
     After substitution: Exists d in Department, MemberOf Target (key = Const 99)
     The namespaced Var "d.dept_id" (quantifier-scoped) is replaced;
     a base-tuple Var "dept_id" would not be. *)
  let c =
    Constraint.Exists
      {
        variable = "d";
        quantifier = "Department";
        body =
          Constraint.MemberOf
            {
              target = "Target";
              binding =
                Constraint.BindingMap.singleton "key"
                  (Constraint.Var "d.dept_id");
            };
      }
  in
  let result =
    Constraint.substitute_transition c "Department" [ ("dept_id", abs 99) ]
  in
  match result with
  | Constraint.Exists { body = Constraint.MemberOf { binding; _ }; _ } -> (
      match Constraint.BindingMap.find_opt "key" binding with
      | Some (Constraint.Const v) -> assert (Stdlib.( = ) v (abs 99))
      | _ -> assert false)
  | _ -> assert false

let%test_unit "substitute_transition: base-tuple Var is not substituted" =
  (* Constraint: Exists d in Department, MemberOf Target (key = Var "dept_id")
     Var "dept_id" is a base-tuple reference (no "d." prefix), so it must
     survive substitution unchanged. Only "d.dept_id" would be replaced. *)
  let c =
    Constraint.Exists
      {
        variable = "d";
        quantifier = "Department";
        body =
          Constraint.MemberOf
            {
              target = "Target";
              binding =
                Constraint.BindingMap.singleton "key" (Constraint.Var "dept_id");
            };
      }
  in
  let result =
    Constraint.substitute_transition c "Department" [ ("dept_id", abs 99) ]
  in
  match result with
  | Constraint.Exists { body = Constraint.MemberOf { binding; _ }; _ } -> (
      match Constraint.BindingMap.find_opt "key" binding with
      | Some (Constraint.Var "dept_id") -> ()
      | _ -> assert false)
  | _ -> assert false

let%test_unit
    "substitute_transition: non-matching quantifier relation is unchanged" =
  (* Constraint: Exists d in Department, MemberOf Target (key = Var "d.dept_id")
     Transition is for relation "Other", not "Department".
     The body must survive unmodified. *)
  let c =
    Constraint.Exists
      {
        variable = "d";
        quantifier = "Department";
        body =
          Constraint.MemberOf
            {
              target = "Target";
              binding =
                Constraint.BindingMap.singleton "key"
                  (Constraint.Var "d.dept_id");
            };
      }
  in
  let result =
    Constraint.substitute_transition c "Other" [ ("dept_id", abs 99) ]
  in
  match result with
  | Constraint.Exists { body = Constraint.MemberOf { binding; _ }; _ } -> (
      match Constraint.BindingMap.find_opt "key" binding with
      | Some (Constraint.Var "d.dept_id") -> ()
      | _ -> assert false)
  | _ -> assert false

let%test_unit "substitute_transition: substitution applies through And and Not"
    =
  (* Constraint:
       Exists d in Department,
         And [
           MemberOf A (x = Var "d.x");
           Not { MemberOf B (y = Var "d.y"); universe = "B" }
         ]
     Transition: { x = 1, y = 2 }
     After substitution: Var "d.x" -> Const 1, Var "d.y" -> Const 2 *)
  let c =
    Constraint.Exists
      {
        variable = "d";
        quantifier = "Department";
        body =
          Constraint.And
            [
              Constraint.MemberOf
                {
                  target = "A";
                  binding =
                    Constraint.BindingMap.singleton "x" (Constraint.Var "d.x");
                };
              Constraint.Not
                {
                  universe = "B";
                  body =
                    Constraint.MemberOf
                      {
                        target = "B";
                        binding =
                          Constraint.BindingMap.singleton "y"
                            (Constraint.Var "d.y");
                      };
                };
            ];
      }
  in
  let result =
    Constraint.substitute_transition c "Department"
      [ ("x", abs 1); ("y", abs 2) ]
  in
  match result with
  | Constraint.Exists
      {
        body =
          Constraint.And
            [
              Constraint.MemberOf { binding = b1; _ };
              Constraint.Not
                { body = Constraint.MemberOf { binding = b2; _ }; _ };
            ];
        _;
      } -> (
      match
        ( Constraint.BindingMap.find_opt "x" b1,
          Constraint.BindingMap.find_opt "y" b2 )
      with
      | Some (Constraint.Const v1), Some (Constraint.Const v2) ->
          assert (Stdlib.( = ) v1 (abs 1));
          assert (Stdlib.( = ) v2 (abs 2))
      | _ -> assert false)
  | _ -> assert false

(* Build a minimal db with two relations and an FK constraint:
   Department { dept_id }  <--  Employee { emp_id, dept_id }
   Constraint on Employee: Exists d in Department, MemberOf Department (dept_id = Var "dept_id") *)
let setup_fk_db storage =
  let db =
    match Memory.create_database storage ~name:"hr" with
    | Error _ -> assert false
    | Ok db -> db
  in
  let db =
    match
      Memory.create_relation storage db ~name:"Department"
        ~schema:(Schema.empty |> Schema.add "dept_id" "natural")
    with
    | Error _ -> assert false
    | Ok (db, _) -> db
  in
  let db =
    match
      Memory.create_relation storage db ~name:"Employee"
        ~schema:
          (Schema.empty
          |> Schema.add "emp_id" "natural"
          |> Schema.add "dept_id" "natural")
    with
    | Error _ -> assert false
    | Ok (db, _) -> db
  in
  let fk_body =
    let binding =
      Constraint.BindingMap.singleton "dept_id" (Constraint.Var "dept_id")
    in
    Constraint.Exists
      {
        variable = "d";
        quantifier = "Department";
        body = Constraint.MemberOf { target = "Department"; binding };
      }
  in
  let db =
    match
      Memory.register_constraint storage db
        ~constraint_name:"fk_dept" ~relation_name:"Employee" ~body:fk_body
    with
    | Error _ -> assert false
    | Ok db -> db
  in
  db

let%test_unit
    "extend_tuple namespacing: insert Employee with non-existent dept_id is \
     rejected" =
  (* Regression test for the extend_tuple namespacing bug.
     Without namespacing, extend_tuple would overwrite the Employee's dept_id=99
     with the Department row's dept_id=1 during Exists iteration.  Var "dept_id"
     would then resolve to 1, check_membership would succeed, and the insertion
     would be accepted despite dept_id=99 not existing in Department.
     With namespacing, Department attributes are stored as d.dept_id=1 and the
     base Employee attribute dept_id=99 is preserved, so the check correctly fails. *)
  with_storage (fun storage ->
      let db = setup_fk_db storage in
      let db =
        match
          Dml.Executor.Memory.execute storage db
            (Dml.Ast.InsertTuple
               {
                 relation = "Department";
                 attributes = [ ("dept_id", Drl.Ast.Int 1) ];
               })
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      match
        Dml.Executor.Memory.execute storage db
          (Dml.Ast.InsertTuple
             {
               relation = "Employee";
               attributes =
                 [ ("emp_id", Drl.Ast.Int 1); ("dept_id", Drl.Ast.Int 99) ];
             })
      with
      | Ok _ -> assert false (* dept_id=99 does not exist in Department *)
      | Error _ -> ())

let%test_unit "cascade: delete referenced row violates FK and is rejected" =
  with_storage (fun storage ->
      let db = setup_fk_db storage in
      (* Insert Dept 1 and Employee referencing it *)
      let db =
        match
          Dml.Executor.Memory.execute storage db
            (Dml.Ast.InsertTuple
               {
                 relation = "Department";
                 attributes = [ ("dept_id", Drl.Ast.Int 1) ];
               })
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      let db =
        match
          Dml.Executor.Memory.execute storage db
            (Dml.Ast.InsertTuple
               {
                 relation = "Employee";
                 attributes =
                   [ ("emp_id", Drl.Ast.Int 10); ("dept_id", Drl.Ast.Int 1) ];
               })
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      let dept_rel =
        match Memory.get_relation db ~name:"Department" with
        | None -> assert false
        | Some r -> r
      in
      let dept_hash =
        Hashing.hash_tuple
          {
            Tuple.relation = "Department";
            attributes =
              Tuple.AttributeMap.singleton "dept_id"
                { Attribute.value = Obj.magic (1 : int) };
          }
      in
      (* Deleting Dept 1 while Employee references it must fail *)
      match
        Memory.retract_tuple storage db dept_rel
          ~tuple_hash:dept_hash
      with
      | Ok _ -> assert false (* should have been rejected *)
      | Error (Error.ConstraintViolation msg) ->
          assert (String.length msg > 0)
      | Error _ -> assert false)

let%test_unit "cascade: delete unreferenced row succeeds" =
  with_storage (fun storage ->
      let db = setup_fk_db storage in
      (* Insert two departments; employee only references dept 1 *)
      let db =
        match
          Dml.Executor.Memory.execute storage db
            (Dml.Ast.InsertTuple
               {
                 relation = "Department";
                 attributes = [ ("dept_id", Drl.Ast.Int 1) ];
               })
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      let db =
        match
          Dml.Executor.Memory.execute storage db
            (Dml.Ast.InsertTuple
               {
                 relation = "Department";
                 attributes = [ ("dept_id", Drl.Ast.Int 2) ];
               })
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      let db =
        match
          Dml.Executor.Memory.execute storage db
            (Dml.Ast.InsertTuple
               {
                 relation = "Employee";
                 attributes =
                   [ ("emp_id", Drl.Ast.Int 10); ("dept_id", Drl.Ast.Int 1) ];
               })
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      let dept_rel =
        match Memory.get_relation db ~name:"Department" with
        | None -> assert false
        | Some r -> r
      in
      let dept2_hash =
        Hashing.hash_tuple
          {
            Tuple.relation = "Department";
            attributes =
              Tuple.AttributeMap.singleton "dept_id"
                { Attribute.value = Obj.magic (2 : int) };
          }
      in
      (* Deleting unreferenced Dept 2 must succeed *)
      match
        Memory.retract_tuple storage db dept_rel
          ~tuple_hash:dept2_hash
      with
      | Error _ -> assert false
      | Ok (new_db, _) ->
          let dept =
            match
              Memory.get_relation new_db ~name:"Department"
            with
            | None -> assert false
            | Some r -> r
          in
          assert (Memory.tuple_count dept = 1))

let%test_unit "cascade: Negative-polarity relation deletion is not checked" =
  (* Constraint on Employee: NOT (MemberOf Blacklist (emp_id = Var "emp_id"))
     Blacklist has Negative polarity (INSERT into Blacklist could violate).
     DELETE from Blacklist should never trigger a cascade check. *)
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"hr" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let db =
        match
          Memory.create_relation storage db ~name:"Blacklist"
            ~schema:(Schema.empty |> Schema.add "emp_id" "natural")
        with
        | Error _ -> assert false
        | Ok (db, _) -> db
      in
      let db =
        match
          Memory.create_relation storage db ~name:"Employee"
            ~schema:(Schema.empty |> Schema.add "emp_id" "natural")
        with
        | Error _ -> assert false
        | Ok (db, _) -> db
      in
      let not_body =
        let binding =
          Constraint.BindingMap.singleton "emp_id" (Constraint.Var "emp_id")
        in
        Constraint.Not
          {
            body = Constraint.MemberOf { target = "Blacklist"; binding };
            universe = "Employee";
          }
      in
      let db =
        match
          Memory.register_constraint storage db
            ~constraint_name:"not_blacklisted" ~relation_name:"Employee"
            ~body:not_body
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      (* Insert emp_id=5 into Blacklist, then insert emp_id=99 into Employee (unrelated) *)
      let db =
        match
          Dml.Executor.Memory.execute storage db
            (Dml.Ast.InsertTuple
               {
                 relation = "Blacklist";
                 attributes = [ ("emp_id", Drl.Ast.Int 5) ];
               })
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      let db =
        match
          Dml.Executor.Memory.execute storage db
            (Dml.Ast.InsertTuple
               {
                 relation = "Employee";
                 attributes = [ ("emp_id", Drl.Ast.Int 99) ];
               })
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      let bl_rel =
        match Memory.get_relation db ~name:"Blacklist" with
        | None -> assert false
        | Some r -> r
      in
      let bl_hash =
        Hashing.hash_tuple
          {
            Tuple.relation = "Blacklist";
            attributes =
              Tuple.AttributeMap.singleton "emp_id"
                { Attribute.value = Obj.magic (5 : int) };
          }
      in
      (* Deleting from Blacklist (negative polarity) must not trigger cascade check *)
      match
        Memory.retract_tuple storage db bl_rel ~tuple_hash:bl_hash
      with
      | Error _ -> assert false
      | Ok _ -> ())

let%test_unit "cascade: deferred constraint not checked during retract_tuple" =
  with_storage (fun storage ->
      let db =
        match Memory.create_database storage ~name:"hr" with
        | Error _ -> assert false
        | Ok db -> db
      in
      let db =
        match
          Memory.create_relation storage db ~name:"Department"
            ~schema:(Schema.empty |> Schema.add "dept_id" "natural")
        with
        | Error _ -> assert false
        | Ok (db, _) -> db
      in
      let db =
        match
          Memory.create_relation storage db ~name:"Employee"
            ~schema:
              (Schema.empty
              |> Schema.add "emp_id" "natural"
              |> Schema.add "dept_id" "natural")
        with
        | Error _ -> assert false
        | Ok (db, _) -> db
      in
      let fk_body =
        let binding =
          Constraint.BindingMap.singleton "dept_id" (Constraint.Var "dept_id")
        in
        Constraint.Exists
          {
            variable = "d";
            quantifier = "Department";
            body = Constraint.MemberOf { target = "Department"; binding };
          }
      in
      (* Register as DEFERRED *)
      let db =
        match
          Memory.attach_constraint storage db
            ~constraint_name:"fk_dept_deferred" ~relation_name:"Employee"
            ~body:fk_body ~timing:Constraint.Deferred
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      let db =
        match
          Dml.Executor.Memory.execute storage db
            (Dml.Ast.InsertTuple
               {
                 relation = "Department";
                 attributes = [ ("dept_id", Drl.Ast.Int 1) ];
               })
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      let db =
        match
          Dml.Executor.Memory.execute storage db
            (Dml.Ast.InsertTuple
               {
                 relation = "Employee";
                 attributes =
                   [ ("emp_id", Drl.Ast.Int 10); ("dept_id", Drl.Ast.Int 1) ];
               })
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      let dept_rel =
        match Memory.get_relation db ~name:"Department" with
        | None -> assert false
        | Some r -> r
      in
      let dept_hash =
        Hashing.hash_tuple
          {
            Tuple.relation = "Department";
            attributes =
              Tuple.AttributeMap.singleton "dept_id"
                { Attribute.value = Obj.magic (1 : int) };
          }
      in
      (* Deferred: retract_tuple itself should NOT reject the deletion *)
      match
        Memory.retract_tuple storage db dept_rel
          ~tuple_hash:dept_hash
      with
      | Error _ -> assert false (* deferred: must pass here *)
      | Ok (new_db, _) -> (
          (* But check_deferred_constraints must catch the violation *)
          match
            Memory.check_deferred_constraints storage new_db
          with
          | Ok () -> assert false (* should have caught the violation *)
          | Error (Error.ConstraintViolation _) -> ()
          | Error _ -> assert false))

(* INSERT cascade tests.

   Schema:
     Blacklist { emp_id }
     Employee  { emp_id }

   Constraint on Employee (negative polarity on Blacklist):
     Not { body = MemberOf Blacklist (emp_id = Var "emp_id"); universe = "Employee" }

   Blacklist has Negative polarity in this constraint, so inserting into Blacklist
   must trigger a re-check of Employee tuples. *)
let setup_blacklist_db storage =
  let db =
    match Memory.create_database storage ~name:"hr" with
    | Error _ -> assert false
    | Ok db -> db
  in
  let db =
    match
      Memory.create_relation storage db ~name:"Blacklist"
        ~schema:(Schema.empty |> Schema.add "emp_id" "natural")
    with
    | Error _ -> assert false
    | Ok (db, _) -> db
  in
  let db =
    match
      Memory.create_relation storage db ~name:"Employee"
        ~schema:(Schema.empty |> Schema.add "emp_id" "natural")
    with
    | Error _ -> assert false
    | Ok (db, _) -> db
  in
  let not_body =
    Constraint.Not
      {
        body =
          Constraint.MemberOf
            {
              target = "Blacklist";
              binding =
                Constraint.BindingMap.singleton "emp_id"
                  (Constraint.Var "emp_id");
            };
        universe = "Employee";
      }
  in
  match
    Memory.register_constraint storage db
      ~constraint_name:"not_blacklisted" ~relation_name:"Employee"
      ~body:not_body
  with
  | Error _ -> assert false
  | Ok db -> db

let%test_unit
    "cascade INSERT: inserting into Blacklist violates constraint on existing \
     Employee" =
  (* Employee emp_id=5 already exists. Inserting Blacklist{emp_id=5} must be
     rejected because the Employee constraint (NOT member of Blacklist) would
     be violated for the existing emp_id=5 row. *)
  with_storage (fun storage ->
      let db = setup_blacklist_db storage in
      let db =
        match
          Dml.Executor.Memory.execute storage db
            (Dml.Ast.InsertTuple
               {
                 relation = "Employee";
                 attributes = [ ("emp_id", Drl.Ast.Int 5) ];
               })
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      match
        Dml.Executor.Memory.execute storage db
          (Dml.Ast.InsertTuple
             {
               relation = "Blacklist";
               attributes = [ ("emp_id", Drl.Ast.Int 5) ];
             })
      with
      | Ok _ -> assert false (* emp_id=5 exists in Employee, must be rejected *)
      | Error _ -> ())

let%test_unit
    "cascade INSERT: inserting into Blacklist with no matching Employee \
     succeeds" =
  (* Employee emp_id=99 does not exist. Inserting Blacklist{emp_id=5} must
     succeed because no Employee tuple is affected. *)
  with_storage (fun storage ->
      let db = setup_blacklist_db storage in
      let db =
        match
          Dml.Executor.Memory.execute storage db
            (Dml.Ast.InsertTuple
               {
                 relation = "Employee";
                 attributes = [ ("emp_id", Drl.Ast.Int 99) ];
               })
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      match
        Dml.Executor.Memory.execute storage db
          (Dml.Ast.InsertTuple
             {
               relation = "Blacklist";
               attributes = [ ("emp_id", Drl.Ast.Int 5) ];
             })
      with
      | Error _ -> assert false (* emp_id=5 not in Employee, must succeed *)
      | Ok _ -> ())

(* commit tests.

   Uses the same Department/Employee FK schema with a DEFERRED constraint.
   Individual mutations succeed even when they temporarily violate the constraint;
   commit is the boundary where deferred violations are caught. *)
let setup_deferred_fk_db storage =
  let db =
    match Memory.create_database storage ~name:"hr" with
    | Error _ -> assert false
    | Ok db -> db
  in
  let db =
    match
      Memory.create_relation storage db ~name:"Department"
        ~schema:(Schema.empty |> Schema.add "dept_id" "natural")
    with
    | Error _ -> assert false
    | Ok (db, _) -> db
  in
  let db =
    match
      Memory.create_relation storage db ~name:"Employee"
        ~schema:
          (Schema.empty
          |> Schema.add "emp_id" "natural"
          |> Schema.add "dept_id" "natural")
    with
    | Error _ -> assert false
    | Ok (db, _) -> db
  in
  let fk_body =
    Constraint.Exists
      {
        variable = "d";
        quantifier = "Department";
        body =
          Constraint.MemberOf
            {
              target = "Department";
              binding =
                Constraint.BindingMap.singleton "dept_id"
                  (Constraint.Var "dept_id");
            };
      }
  in
  match
    Memory.attach_constraint storage db ~constraint_name:"fk_dept"
      ~relation_name:"Employee" ~body:fk_body ~timing:Constraint.Deferred
  with
  | Error _ -> assert false
  | Ok db -> db

let%test_unit "commit: no deferred constraints, returns Ok with db unchanged" =
  with_storage (fun storage ->
      let db = setup_deferred_fk_db storage in
      let db =
        match
          Dml.Executor.Memory.execute storage db
            (Dml.Ast.InsertTuple
               {
                 relation = "Department";
                 attributes = [ ("dept_id", Drl.Ast.Int 1) ];
               })
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      let db =
        match
          Dml.Executor.Memory.execute storage db
            (Dml.Ast.InsertTuple
               {
                 relation = "Employee";
                 attributes =
                   [ ("emp_id", Drl.Ast.Int 10); ("dept_id", Drl.Ast.Int 1) ];
               })
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      (* All constraints satisfied, commit must pass *)
      match Memory.commit storage db with
      | Error _ -> assert false
      | Ok _ -> ())

let%test_unit "commit: deferred violation is caught at commit boundary" =
  with_storage (fun storage ->
      let db = setup_deferred_fk_db storage in
      let db =
        match
          Dml.Executor.Memory.execute storage db
            (Dml.Ast.InsertTuple
               {
                 relation = "Department";
                 attributes = [ ("dept_id", Drl.Ast.Int 1) ];
               })
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      let db =
        match
          Dml.Executor.Memory.execute storage db
            (Dml.Ast.InsertTuple
               {
                 relation = "Employee";
                 attributes =
                   [ ("emp_id", Drl.Ast.Int 10); ("dept_id", Drl.Ast.Int 1) ];
               })
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      let dept_rel =
        match Memory.get_relation db ~name:"Department" with
        | None -> assert false
        | Some r -> r
      in
      let dept_hash =
        Hashing.hash_tuple
          {
            Tuple.relation = "Department";
            attributes =
              Tuple.AttributeMap.singleton "dept_id"
                { Attribute.value = Obj.magic (1 : int) };
          }
      in
      (* Delete the referenced department. Deferred, so retract succeeds *)
      let db =
        match
          Memory.retract_tuple storage db dept_rel
            ~tuple_hash:dept_hash
        with
        | Error _ -> assert false
        | Ok (db, _) -> db
      in
      (* commit must now catch the orphaned Employee *)
      match Memory.commit storage db with
      | Ok _ -> assert false
      | Error (Error.ConstraintViolation _) -> ()
      | Error _ -> assert false)

let%test_unit "commit: clears deferred list, second commit passes" =
  (* After a successful commit the deferred list is empty.
     A follow-up mutation + commit on the returned db must also work correctly
     rather than re-running stale entries from the previous window. *)
  with_storage (fun storage ->
      let db = setup_deferred_fk_db storage in
      let db =
        match
          Dml.Executor.Memory.execute storage db
            (Dml.Ast.InsertTuple
               {
                 relation = "Department";
                 attributes = [ ("dept_id", Drl.Ast.Int 1) ];
               })
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      let db =
        match
          Dml.Executor.Memory.execute storage db
            (Dml.Ast.InsertTuple
               {
                 relation = "Employee";
                 attributes =
                   [ ("emp_id", Drl.Ast.Int 10); ("dept_id", Drl.Ast.Int 1) ];
               })
        with
        | Error _ -> assert false
        | Ok db -> db
      in
      let db =
        match Memory.commit storage db with
        | Error _ -> assert false
        | Ok db -> db
      in
      (* Second commit with no new mutations must also pass *)
      match Memory.commit storage db with
      | Error _ -> assert false
      | Ok _ -> ())
