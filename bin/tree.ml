open Relational_engine

let boot () =
  let attribute = Attribute.make_materialized ~value:(Obj.repr "sakura") in
  let attribute_hash =
    match attribute with
    | Attribute.Materialized { value } -> Conventions.AbstractValue.hash value
    | Attribute.Referenced { value_hash } -> value_hash
  in
  let relation1 =
    (new Relation.stored ~name:"something" ~schema:[ ("name", "string") ]
      ~constraints:None ~cardinality:(Conventions.Cardinality.Finite 1)
      ~membership_criteria:(fun _ -> true) ~lineage:None ~provenance:None)#set_tree_pointer (Some attribute_hash)
  in
  let relation2 =
    new Relation.stored ~name:"other" ~schema:[ ("name", "string") ]
      ~constraints:None ~cardinality:(Conventions.Cardinality.Finite 1)
      ~membership_criteria:(fun _ -> true) ~lineage:None ~provenance:None
  in
  let database =
    (new Management.Multigroup.multigroup ~name:"main")#add_relation
      ~name:relation1#name ~relation_hash:relation1#hash ~tree_pointer:relation1#hash
  in
  let database2 =
    (new Management.Multigroup.multigroup ~name:"main")#add_relation
      ~name:relation2#name ~relation_hash:relation2#hash ~tree_pointer:relation2#hash
  in
  Printf.printf "attribute hash: %s\n" attribute_hash;
  Printf.printf "relation1 hash:  %s\n" relation1#hash;
  Printf.printf "relation2 hash:  %s\n" relation2#hash;
  Printf.printf "database1 hash:  %s\n" database#hash;
  Printf.printf "database2 hash:  %s\n" database2#hash

let () = boot ()
