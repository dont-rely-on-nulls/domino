(* algebra.ml — plan compilation support.
   Generator-based evaluation is removed; all query execution goes through the
   Tarski VM via Nt.execute_query.  This module retains only what is needed to
   construct plan nodes and handle compile-time errors. *)

type error = UnsupportedOperator of string

(* const_relation is kept for the Ast.Const case which produces an in-memory
   ephemeral relation from literal pairs.  It is used only in DRL tests and
   REPL exploration; it does not go through the VM.
   TODO: materialise const tuples into InMemoryBackend and SCAN them so they
   can participate in VM JOINs. *)
let const_relation (pairs : (string * Conventions.AbstractValue.t) list) : Relation.ephemeral =
  let make_gen () =
    let attrs =
      List.fold_left
        (fun acc (k, v) -> Tuple.AttributeMap.add k {Attribute.value= v} acc)
        Tuple.AttributeMap.empty pairs
    in
    let tuple = Tuple.Materialized {Tuple.relation= "const"; attributes= attrs} in
    let gen = function
      | None | Some 0 -> Generator.Value (tuple, fun _ -> Generator.Done)
      | _ -> Generator.Done
    in
    gen
  in
  let schema = List.map (fun (k, _) -> k, "abstract") pairs in
  new Relation.ephemeral
    ~name:"const" ~schema ~constraints:None ~cardinality:(Conventions.Cardinality.Finite 1)
    ~membership_criteria:(fun _ -> true)
    ~lineage:None ~provenance:None ~generator:(make_gen ())
