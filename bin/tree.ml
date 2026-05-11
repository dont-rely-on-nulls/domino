open Relational_engine

(* Content-addressed store: hash -> bytes. In production this is NT storage. *)
let store : (string, bytes) Hashtbl.t = Hashtbl.create 64

(* --- Object construction helpers --- *)

let make_tuple ~relation attributes =
  let attr_map =
    List.fold_left
      (fun acc (name, value) ->
        Tuple.AttributeMap.add name
          { Attribute.value = Obj.repr (value : string) }
          acc)
      Tuple.AttributeMap.empty attributes
  in
  { Tuple.relation; attributes = attr_map }

(* Simulate B+ tree root: hash of sorted tuple hashes. *)
let tuple_tree_pointer tuples =
  List.map Hashing.hash_tuple tuples
  |> List.sort String.compare
  |> List.fold_left ( ^ ) ""
  |> Conventions.Hash.hash_text

let make_relation ~name ~schema ~cardinality tuples =
  (new Relation.stored
    ~name ~schema ~constraints:None
    ~cardinality:(Conventions.Cardinality.Finite cardinality)
    ~membership_criteria:(fun _ -> true)
    ~lineage:None ~provenance:None)
  #set_tree_pointer (Some (tuple_tree_pointer tuples))

(* --- Persistence layer (writing into the store) --- *)

let persist_tuple tuple =
  let hash = Hashing.hash_tuple tuple in
  Hashtbl.add store hash (Marshal.to_bytes tuple [ Marshal.Closures ]);
  hash

(* Store an ordered list of child hashes at a tree node address. *)
let persist_tree_node node_hash (child_hashes : string list) =
  Hashtbl.add store node_hash (Marshal.to_bytes child_hashes [])

(* Store a relation's metadata (name, schema, tree_pointer) at its hash. *)
let persist_relation (rel : Relation.stored) =
  Hashtbl.add store rel#hash
    (Marshal.to_bytes (rel#name, rel#schema, rel#tree_pointer) [])

(* --- Reconstruction layer (reading from the store) --- *)

let load : 'a. string -> 'a = fun hash ->
  Marshal.from_bytes (Hashtbl.find store hash) 0

let reconstruct_from_tree_pointer mg_tp =
  let rel_hashes : string list = load mg_tp in
  List.map (fun rel_hash ->
    let (name, schema, tp_opt) : string * Schema.t * string option =
      load rel_hash
    in
    let tuples : Tuple.materialized list =
      match tp_opt with
      | None -> []
      | Some tp ->
          let tuple_hashes : string list = load tp in
          List.map load tuple_hashes
    in
    (name, schema, tuples)
  ) rel_hashes

(* --- Printing --- *)

let print_tuple (t : Tuple.materialized) =
  let fields =
    Tuple.AttributeMap.bindings t.attributes
    |> List.map (fun (k, attr) ->
        k ^ " = " ^ (Obj.magic attr.Attribute.value : string))
    |> String.concat ", "
  in
  Printf.printf "      { %s }\n" fields

let print_reconstructed relations =
  List.iter (fun (name, schema, tuples) ->
    Printf.printf "  relation: %s  [%s]\n"
      name
      (String.concat ", " (List.map (fun (a, d) -> a ^ ":" ^ d) schema));
    List.iter print_tuple tuples
  ) relations

(* --- Main --- *)

let boot () =
  (* Tuples *)
  let dept_engineering =
    make_tuple ~relation:"departments" [ ("id", "1"); ("name", "Engineering") ]
  in
  let dept_sales =
    make_tuple ~relation:"departments" [ ("id", "2"); ("name", "Sales") ]
  in
  let emp_alice =
    make_tuple ~relation:"employees"
      [ ("dept_id", "1"); ("id", "1"); ("name", "Alice") ]
  in
  let emp_bob =
    make_tuple ~relation:"employees"
      [ ("dept_id", "2"); ("id", "2"); ("name", "Bob") ]
  in

  (* Persist tuples and capture their hashes *)
  let dept_eng_hash   = persist_tuple dept_engineering in
  let dept_sales_hash = persist_tuple dept_sales in
  let emp_alice_hash  = persist_tuple emp_alice in
  let emp_bob_hash    = persist_tuple emp_bob in

  (* Build relations with tree_pointers derived from tuple hashes *)
  let departments =
    make_relation
      ~name:"departments"
      ~schema:[ ("id", "int"); ("name", "string") ]
      ~cardinality:2
      [ dept_engineering; dept_sales ]
  in
  let employees =
    make_relation
      ~name:"employees"
      ~schema:[ ("id", "int"); ("name", "string"); ("dept_id", "int") ]
      ~cardinality:2
      [ emp_alice; emp_bob ]
  in

  (* Persist each relation's B+ tree node (tuple hashes) at its tree_pointer *)
  let dept_tp = Option.get departments#tree_pointer in
  let emp_tp  = Option.get employees#tree_pointer in
  persist_tree_node dept_tp
    (List.sort String.compare [ dept_eng_hash; dept_sales_hash ]);
  persist_tree_node emp_tp
    (List.sort String.compare [ emp_alice_hash; emp_bob_hash ]);

  (* Persist relation metadata at each relation hash *)
  persist_relation departments;
  persist_relation employees;

  (* Build multigroup *)
  let company =
    List.fold_left
      (fun mg rel -> mg#add_relation rel)
      (new Management.Multigroup.multigroup ~name:"company")
      [ (departments :> Relation.relation); (employees :> Relation.relation) ]
  in

  (* Persist multigroup's B+ tree node (relation hashes) at its tree_pointer *)
  let mg_tp = Option.get company#tree_pointer in
  persist_tree_node mg_tp
    (List.sort String.compare [ departments#hash; employees#hash ]);

  Printf.printf "=== Built state ===\n";
  Printf.printf "  multigroup hash:   %s\n" company#hash;
  Printf.printf "  tree_pointer:      %s\n\n" mg_tp;

  (* Drop everything except the tree_pointer and reconstruct *)
  Printf.printf "=== Reconstructing from tree_pointer only ===\n";
  let relations = reconstruct_from_tree_pointer mg_tp in
  print_reconstructed relations

let () = boot ()
