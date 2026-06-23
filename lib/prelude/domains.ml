(** Predefined fundamental domains. Each domain carries a generator (for
    enumeration) and membership criteria (for validation). Values are stored as
    {!Conventions.AbstractValue.t} via [Obj.magic], following the same
    convention as {!Standard}. *)

(** Bijection from natural number to integer: 0->0, 1->1, 2->-1, 3->2, 4->-2,
    ... *)
let nat_to_int n = if n = 0 then 0 else if n mod 2 = 1 then (n + 1) / 2 else -(n / 2)

(** Cantor pairing inverse: given position [n], returns a pair [(a, b)] of
    natural numbers such that [a + b] lies on the [n]-th diagonal. *)
let cantor_unpair n =
  let w = int_of_float (floor ((sqrt (float_of_int ((8 * n) + 1)) -. 1.) /. 2.)) in
  let t = w * (w + 1) / 2 in
  let b = n - t in
  let a = w - b in
  (a, b)

(** Integer domain: all integers, enumerated via a bijection from the naturals.
    Schema: [value: integer]. *)
let integer : Relation.domain =
  let rec generator (position : int option) : Generator.result =
    match position with
    | Some n ->
        let v = nat_to_int n in
        let attributes = Tuple.AttributeMap.of_list [("value", Obj.magic v)] in
        Generator.Value (Tuple.make_materialized ~relation:"integer" ~attributes, generator)
    | None ->
        Generator.Error "Cannot produce a randomly enumerated value for the integer domain."
  in
  let membership_criteria : Tuple.t -> bool = function
    | Tuple.Materialized _ ->
        true
    | Tuple.NonMaterialized _ ->
        false
  in
  let schema = Schema.empty |> Schema.add "value" "integer" in
  new Relation.domain
    ~name:"integer" ~generator ~membership_criteria ~schema ~constraints:None
    ~cardinality:Conventions.Cardinality.AlephZero ~lineage:None ~provenance:None

(** Natural number domain: non-negative integers (0, 1, 2, …). Schema:
    [value: natural]. *)
let natural : Relation.domain =
  let rec generator (position : int option) : Generator.result =
    match position with
    | Some n ->
        let attributes = Tuple.AttributeMap.of_list [("value", Obj.magic n)] in
        Generator.Value (Tuple.make_materialized ~relation:"natural" ~attributes, generator)
    | None ->
        Generator.Error "Cannot produce a randomly enumerated value for the natural domain."
  in
  let membership_criteria : Tuple.t -> bool = function
    | Tuple.Materialized m -> (
      match Tuple.AttributeMap.find_opt "value" m.attributes with
      | Some attr ->
          (Obj.magic attr : int) >= 0
      | None ->
          false )
    | Tuple.NonMaterialized _ ->
        false
  in
  let schema = Schema.empty |> Schema.add "value" "natural" in
  new Relation.domain
    ~name:"natural" ~generator ~membership_criteria ~schema ~constraints:None
    ~cardinality:Conventions.Cardinality.AlephZero ~lineage:None ~provenance:None

(** Rational number domain: pairs [(numerator, denominator)] where the
    denominator is never zero. The generator enumerates all such pairs via
    Cantor pairing on the integers. Schema:
    [numerator: integer, denominator: integer]. *)
let rational : Relation.domain =
  let rec generator (position : int option) : Generator.result =
    match position with
    | Some n ->
        let a, b = cantor_unpair n in
        let num = nat_to_int a in
        (* Map b -> non-zero integer: 0->1, 1->-1, 2->2, 3->-2, … *)
        let den = if b mod 2 = 0 then (b / 2) + 1 else -((b + 1) / 2) in
        let attributes =
          Tuple.AttributeMap.of_list [("numerator", Obj.magic num); ("denominator", Obj.magic den)]
        in
        Generator.Value (Tuple.make_materialized ~relation:"rational" ~attributes, generator)
    | None ->
        Generator.Error "Cannot produce a randomly enumerated value for the rational domain."
  in
  let membership_criteria : Tuple.t -> bool = function
    | Tuple.Materialized m -> (
      match Tuple.AttributeMap.find_opt "denominator" m.attributes with
      | Some attr ->
          (Obj.magic attr : int) <> 0
      | None ->
          false )
    | Tuple.NonMaterialized _ ->
        false
  in
  (* Note: Additionally to the membership criteria, we add the constraint here as this relation
     can be further dismembered, rendering the denominator into a lone integer, that should
     propagate the constraint `integer <> 0`. If we declared a domain `integer_without_zero`,
     then this constraint would not be needed as the domain would not be dissociated from the value. *)
  let constraints : Relation.RelationConstraint.t option =
    let denominator_not_zero =
      let open Constraint in
      Not {body= Eq {left= Var "denominator"; right= Const (Obj.magic 0)}; universe= "integer"}
    in
    Some (("DENOMINATOR_DIFFERENT_FROM_ZERO", denominator_not_zero) |> List.singleton)
  in
  let schema =
    Schema.empty |> Schema.add "numerator" "integer" |> Schema.add "denominator" "integer"
  in
  new Relation.domain
    ~name:"rational" ~generator ~membership_criteria ~schema ~constraints
    ~cardinality:Conventions.Cardinality.AlephZero ~lineage:None ~provenance:None

(** String domain: any string value. Not enumerable. Schema: [value: string]. *)
let string : Relation.domain =
  let generator (_ : int option) : Generator.result =
    Generator.Error "Strings are not enumerable."
  in
  let membership_criteria : Tuple.t -> bool = function
    | Tuple.Materialized _ ->
        true
    | Tuple.NonMaterialized _ ->
        false
  in
  let schema = Schema.empty |> Schema.add "value" "string" in
  new Relation.domain
    ~name:"string" ~generator ~membership_criteria ~schema
    ~constraints:None
      (* TODO: Implement some form of enumeration for strings with a bijection on naturals. *)
    ~cardinality:Conventions.Cardinality.Continuum ~lineage:None ~provenance:None
