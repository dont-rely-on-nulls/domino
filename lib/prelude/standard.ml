(** Comparison and arithmetic relations over the natural domain. *)

(* TODO: all relations here are hardcoded to naturals. The make_comparison
   factory accepts a domain_name string but membership_criteria always uses
   Stdlib.compare on Obj.t, which only works correctly for unboxed integers.
   Extending to other domains requires a proper typed comparison dispatch. *)

let mk v = { Attribute.value = Obj.repr v }

(** Build a comparison relation over a single domain. [pred] receives the result
    of [Stdlib.compare left right]. *)
let make_comparison ~name ~domain_name ~pred ~cardinality ~generator =
  let schema =
    Schema.empty
    |> Schema.add "left" domain_name
    |> Schema.add "right" domain_name
  in
  let membership_criteria : (string -> Merkle.t option) -> Tuple.t -> bool =
   fun _tree_of -> function
    | Tuple.Materialized m -> (
        match
          ( Tuple.AttributeMap.find_opt "left" m.attributes,
            Tuple.AttributeMap.find_opt "right" m.attributes )
        with
        | Some l, Some r ->
            pred (Stdlib.compare l.Attribute.value r.Attribute.value)
        | _ -> false)
    | Tuple.NonMaterialized _ -> false
  in
  Relation.make ~hash:None ~name ~schema ~tree:None ~constraints:None
    ~cardinality ~generator:(Some generator) ~membership_criteria
    ~provenance:(Relation.Provenance.Base name)
    ~lineage:(Relation.Lineage.Base name)

(** Enumerate pairs (a, b) where a < b using triangular indexing. *)
let pair_of_nat_lt n =
  let nf = float_of_int n in
  let r = int_of_float (floor ((1. +. sqrt (1. +. (8. *. nf))) /. 2.)) in
  let t_prev = r * (r - 1) / 2 in
  let left = n - t_prev in
  (left, r)

(** Enumerate all pairs (a, b) via Cantor pairing. *)
let cantor_pair_of_nat n =
  let w =
    int_of_float (floor ((sqrt (float_of_int ((8 * n) + 1)) -. 1.) /. 2.))
  in
  let t = w * (w + 1) / 2 in
  let b = n - t in
  let a = w - b in
  (a, b)

let less_than_natural : Relation.t =
  let rec generator (position : int option) : Generator.result =
    match position with
    | Some position ->
        let left, right = pair_of_nat_lt position in
        let attributes : Attribute.materialized Tuple.AttributeMap.t =
          Tuple.AttributeMap.of_list [ ("left", mk left); ("right", mk right) ]
        in
        Generator.Value
          (Tuple.make_materialized ~relation:"less_than" ~attributes, generator)
    | None ->
        Error
          "Cannot produce a randomly enumerated value for less_than over \
           naturals."
  in
  make_comparison ~name:"natural_natural_less_than" ~domain_name:"natural"
    ~pred:(fun c -> c < 0)
    ~cardinality:Conventions.Cardinality.AlephZero ~generator

let less_than_or_equal_natural : Relation.t =
  let rec generator (position : int option) : Generator.result =
    match position with
    | Some n ->
        let a, b = cantor_pair_of_nat n in
        (* a <= b is always true when a <= b; we enumerate all (a, b) with a <= b *)
        let left = min a b in
        let right = max a b in
        let attributes =
          Tuple.AttributeMap.of_list [ ("left", mk left); ("right", mk right) ]
        in
        Generator.Value
          ( Tuple.make_materialized ~relation:"less_than_or_equal" ~attributes,
            generator )
    | None -> Error "Cannot enumerate less_than_or_equal randomly."
  in
  make_comparison ~name:"natural_natural_less_than_or_equal"
    ~domain_name:"natural"
    ~pred:(fun c -> c <= 0)
    ~cardinality:Conventions.Cardinality.AlephZero ~generator

let greater_than_natural : Relation.t =
  let rec generator (position : int option) : Generator.result =
    match position with
    | Some n ->
        let left, right = pair_of_nat_lt n in
        (* Swap: right > left *)
        let attributes =
          Tuple.AttributeMap.of_list [ ("left", mk right); ("right", mk left) ]
        in
        Generator.Value
          ( Tuple.make_materialized ~relation:"greater_than" ~attributes,
            generator )
    | None -> Error "Cannot enumerate greater_than randomly."
  in
  make_comparison ~name:"natural_natural_greater_than" ~domain_name:"natural"
    ~pred:(fun c -> c > 0)
    ~cardinality:Conventions.Cardinality.AlephZero ~generator

let greater_than_or_equal_natural : Relation.t =
  let rec generator (position : int option) : Generator.result =
    match position with
    | Some n ->
        let a, b = cantor_pair_of_nat n in
        let left = max a b in
        let right = min a b in
        let attributes =
          Tuple.AttributeMap.of_list [ ("left", mk left); ("right", mk right) ]
        in
        Generator.Value
          ( Tuple.make_materialized ~relation:"greater_than_or_equal" ~attributes,
            generator )
    | None -> Error "Cannot enumerate greater_than_or_equal randomly."
  in
  make_comparison ~name:"natural_natural_greater_than_or_equal"
    ~domain_name:"natural"
    ~pred:(fun c -> c >= 0)
    ~cardinality:Conventions.Cardinality.AlephZero ~generator

let equal_natural : Relation.t =
  let rec generator (position : int option) : Generator.result =
    match position with
    | Some n ->
        let attributes =
          Tuple.AttributeMap.of_list [ ("left", mk n); ("right", mk n) ]
        in
        Generator.Value
          (Tuple.make_materialized ~relation:"equal" ~attributes, generator)
    | None -> Error "Cannot enumerate equal randomly."
  in
  make_comparison ~name:"natural_natural_equal" ~domain_name:"natural"
    ~pred:(fun c -> c = 0)
    ~cardinality:Conventions.Cardinality.AlephZero ~generator

let not_equal_natural : Relation.t =
  let rec generator (position : int option) : Generator.result =
    match position with
    | Some n ->
        let a, b = cantor_pair_of_nat n in
        let left, right = if a = b then (a, b + 1) else (a, b) in
        let attributes =
          Tuple.AttributeMap.of_list [ ("left", mk left); ("right", mk right) ]
        in
        Generator.Value
          (Tuple.make_materialized ~relation:"not_equal" ~attributes, generator)
    | None -> Error "Cannot enumerate not_equal randomly."
  in
  make_comparison ~name:"natural_natural_not_equal" ~domain_name:"natural"
    ~pred:(fun c -> c <> 0)
    ~cardinality:Conventions.Cardinality.AlephZero ~generator

let plus_natural : Relation.t =
  let schema =
    Schema.empty |> Schema.add "a" "natural" |> Schema.add "b" "natural"
    |> Schema.add "sum" "natural"
  in
  let rec generator (position : int option) : Generator.result =
    match position with
    | Some n ->
        let a, b = cantor_pair_of_nat n in
        let s = a + b in
        let attributes =
          Tuple.AttributeMap.of_list [ ("a", mk a); ("b", mk b); ("sum", mk s) ]
        in
        Generator.Value
          (Tuple.make_materialized ~relation:"plus" ~attributes, generator)
    | None -> Error "Cannot enumerate plus randomly."
  in
  let membership_criteria : (string -> Merkle.t option) -> Tuple.t -> bool =
   fun _tree_of -> function
    | Tuple.Materialized m -> (
        match
          ( Tuple.AttributeMap.find_opt "a" m.attributes,
            Tuple.AttributeMap.find_opt "b" m.attributes,
            Tuple.AttributeMap.find_opt "sum" m.attributes )
        with
        | Some a, Some b, Some s ->
            (Obj.magic a.Attribute.value : int)
            + (Obj.magic b.Attribute.value : int)
            = (Obj.magic s.Attribute.value : int)
        | _ -> false)
    | Tuple.NonMaterialized _ -> false
  in
  Relation.make ~hash:None ~name:"natural_plus" ~schema ~tree:None
    ~constraints:None ~cardinality:Conventions.Cardinality.AlephZero
    ~generator:(Some generator) ~membership_criteria
    ~provenance:(Relation.Provenance.Base "plus")
    ~lineage:(Relation.Lineage.Base "plus")

let times_natural : Relation.t =
  let schema =
    Schema.empty |> Schema.add "a" "natural" |> Schema.add "b" "natural"
    |> Schema.add "product" "natural"
  in
  let rec generator (position : int option) : Generator.result =
    match position with
    | Some n ->
        let a, b = cantor_pair_of_nat n in
        let p = a * b in
        let attributes =
          Tuple.AttributeMap.of_list
            [ ("a", mk a); ("b", mk b); ("product", mk p) ]
        in
        Generator.Value
          (Tuple.make_materialized ~relation:"times" ~attributes, generator)
    | None -> Error "Cannot enumerate times randomly."
  in
  let membership_criteria : (string -> Merkle.t option) -> Tuple.t -> bool =
   fun _tree_of -> function
    | Tuple.Materialized m -> (
        match
          ( Tuple.AttributeMap.find_opt "a" m.attributes,
            Tuple.AttributeMap.find_opt "b" m.attributes,
            Tuple.AttributeMap.find_opt "product" m.attributes )
        with
        | Some a, Some b, Some p ->
            (Obj.magic a.Attribute.value : int)
            * (Obj.magic b.Attribute.value : int)
            = (Obj.magic p.Attribute.value : int)
        | _ -> false)
    | Tuple.NonMaterialized _ -> false
  in
  Relation.make ~hash:None ~name:"natural_times" ~schema ~tree:None
    ~constraints:None ~cardinality:Conventions.Cardinality.AlephZero
    ~generator:(Some generator) ~membership_criteria
    ~provenance:(Relation.Provenance.Base "times")
    ~lineage:(Relation.Lineage.Base "times")

let minus_natural : Relation.t =
  let schema =
    Schema.empty |> Schema.add "a" "natural" |> Schema.add "b" "natural"
    |> Schema.add "difference" "natural"
  in
  let rec generator (position : int option) : Generator.result =
    match position with
    | Some n ->
        (* Enumerate pairs where a >= b, so difference >= 0 *)
        let b, diff = cantor_pair_of_nat n in
        let a = b + diff in
        let attributes =
          Tuple.AttributeMap.of_list
            [ ("a", mk a); ("b", mk b); ("difference", mk diff) ]
        in
        Generator.Value
          (Tuple.make_materialized ~relation:"minus" ~attributes, generator)
    | None -> Error "Cannot enumerate minus randomly."
  in
  let membership_criteria : (string -> Merkle.t option) -> Tuple.t -> bool =
   fun _tree_of -> function
    | Tuple.Materialized m -> (
        match
          ( Tuple.AttributeMap.find_opt "a" m.attributes,
            Tuple.AttributeMap.find_opt "b" m.attributes,
            Tuple.AttributeMap.find_opt "difference" m.attributes )
        with
        | Some a, Some b, Some d ->
            let av = (Obj.magic a.Attribute.value : int) in
            let bv = (Obj.magic b.Attribute.value : int) in
            let dv = (Obj.magic d.Attribute.value : int) in
            av - bv = dv && dv >= 0
        | _ -> false)
    | Tuple.NonMaterialized _ -> false
  in
  Relation.make ~hash:None ~name:"natural_minus" ~schema ~tree:None
    ~constraints:None ~cardinality:Conventions.Cardinality.AlephZero
    ~generator:(Some generator) ~membership_criteria
    ~provenance:(Relation.Provenance.Base "minus")
    ~lineage:(Relation.Lineage.Base "minus")

let divide_natural : Relation.t =
  let schema =
    Schema.empty |> Schema.add "a" "natural" |> Schema.add "b" "natural"
    |> Schema.add "quotient" "natural"
    |> Schema.add "remainder" "natural"
  in
  let rec generator (position : int option) : Generator.result =
    match position with
    | Some n ->
        (* Enumerate (b, q, r) triples where b > 0, then a = b*q + r, 0 <= r < b *)
        (* We use a nested Cantor pairing: n -> (b_idx, qr_idx) -> (q, r) *)
        let b_idx, qr_idx = cantor_pair_of_nat n in
        let b = b_idx + 1 in
        (* b > 0 *)
        let q, r_raw = cantor_pair_of_nat qr_idx in
        let r = r_raw mod b in
        let a = (b * q) + r in
        let attributes =
          Tuple.AttributeMap.of_list
            [
              ("a", mk a); ("b", mk b); ("quotient", mk q); ("remainder", mk r);
            ]
        in
        Generator.Value
          (Tuple.make_materialized ~relation:"divide" ~attributes, generator)
    | None -> Error "Cannot enumerate divide randomly."
  in
  let membership_criteria : (string -> Merkle.t option) -> Tuple.t -> bool =
   fun _tree_of -> function
    | Tuple.Materialized m -> (
        match
          ( Tuple.AttributeMap.find_opt "a" m.attributes,
            Tuple.AttributeMap.find_opt "b" m.attributes,
            Tuple.AttributeMap.find_opt "quotient" m.attributes,
            Tuple.AttributeMap.find_opt "remainder" m.attributes )
        with
        | Some a, Some b, Some q, Some r ->
            let av = (Obj.magic a.Attribute.value : int) in
            let bv = (Obj.magic b.Attribute.value : int) in
            let qv = (Obj.magic q.Attribute.value : int) in
            let rv = (Obj.magic r.Attribute.value : int) in
            bv > 0 && av = (bv * qv) + rv && rv >= 0 && rv < bv
        | _ -> false)
    | Tuple.NonMaterialized _ -> false
  in
  Relation.make ~hash:None ~name:"natural_divide" ~schema ~tree:None
    ~constraints:None ~cardinality:Conventions.Cardinality.AlephZero
    ~generator:(Some generator) ~membership_criteria
    ~provenance:(Relation.Provenance.Base "divide")
    ~lineage:(Relation.Lineage.Base "divide")

let prelude_relations =
  [ less_than_natural
  ; less_than_or_equal_natural
  ; greater_than_natural
  ; greater_than_or_equal_natural
  ; equal_natural
  ; not_equal_natural
  ; plus_natural
  ; times_natural
  ; minus_natural
  ; divide_natural ]
