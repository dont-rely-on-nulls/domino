let get_relation _db _name = failwith "NOT IMPLEMENTED"
let create_multigroup _storage _name = failwith "NOT IMPLEMENTED"
let create_relation _storage _db _name _schema = failwith "NOT IMPLEMENTED"
let create_tuple _storage _db _ll_rel _tuple = failwith "NOT IMPLEMENTED"
let [@warning "-27"] register_constraint storage db ~constraint_name ~relation_name ~body = failwith "NOT IMPLEMENTED"
let [@warning "-27"] create_tuples storage db rel tuples = failwith "NOT IMPLEMENTED"
let [@warning "-27"] create_immutable_relation storage db ~name ~schema ~generator
      ~membership_criteria 
      ~cardinality
      ~producer = failwith "NOT IMPLEMENTED"
let retract_relation _storage _db _name = failwith "NOT IMPLEMENTED"
let retract_tuple _storage _db _mg_rel _tuple_hash = failwith "NOT IMPLEMENTED"
let clear_relation _storage _db _rel = failwith "NOT IMPLEMENTED"
let register_domain _storage _db _domain = failwith "NOT IMPLEMENTED"
(* let tuple_hashes _rel = failwith "NOT IMPLEMENTED" *)
(* let load_tuples _storage _hashes = failwith "NOT IMPLEMENTED" *)
let tuple_hashes = failwith "NOT IMPLEMENTED"
let load_tuples _storage _rel_hash = failwith "NOT IMPLEMENTED"


type catalog_object =                                                                
  | Multigroup of Management.Multigroup.multigroup
  | Relation of Relation.relation

type object_type =
  | Multigroup
  | Relation

type handle =
  { path : string list
  ; obj  : bytes }
                                                               
(* Each class should have their own *)
let deserialize _ _ : object_type -> bytes -> (catalog_object, string) result = failwith "NOT IMPLEMENTED"

let open_handle _ _ : path:string list -> connection_context:string -> (handle, string) result = failwith "NOT IMPLEMENTED"                                                   
let close _ : handle -> (unit, string) result = failwith "NOT IMPLEMENTED"
