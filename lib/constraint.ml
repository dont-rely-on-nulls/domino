(** Constraint system for relational membership criteria.

    A relation's intension includes constraints that refine the space of
    admissible tuples. Since constraints are themselves relations, checking a
    constraint = checking membership in that constraint-relation.

    Negation requires an explicit universe relation name, forcing closed-world
    reasoning in first-order logic. *)

(* TODO: lt/lte/gt/gte/eq/neq/between at the bottom reference target names
   like "less_than" that don't match the prelude relation names
   (e.g. "natural_natural_less_than"). These constructors are currently dead
   code and will fail at runtime if used. *)

type relation_name = string

type attr_name = string

type binding_expr = Var of attr_name | Const of Conventions.AbstractValue.t

module BindingMap = Map.Make (String)
module PolarityMap = Map.Make (String)
module StringSet = Set.Make (String)
module FingerTree = BatFingerTree

type binding = binding_expr BindingMap.t

type t =
  | MemberOf of {target: relation_name; binding: binding}
  | Not of {body: t; universe: relation_name}
  | And of t list
  | Or of t list
  | Exists of {variable: attr_name; quantifier: relation_name; body: t}
  | Forall of {variable: attr_name; quantifier: relation_name; body: t}
  | Eq of {left: binding_expr; right: binding_expr}

type diagnostic =
  | MembershipFailed of
      {target: string; bound_values: (attr_name * Conventions.AbstractValue.t) list}
  | RelationNotFound of string
  | UnboundedQuantifier of {variable: attr_name; quantifier: relation_name}
  | ConstraintFailures of (string * diagnostic) list

let vars_in (c : t) : attr_name list =
  let add_from_binding set binding =
    BindingMap.fold
      (fun _ expr acc -> match expr with Var v -> StringSet.add v acc | Const _ -> acc)
      binding set
  in
  let rec loop set worklist =
    match FingerTree.front worklist with
    | None ->
        set
    | Some (rest, node) -> (
      match node with
      | MemberOf {binding; _} ->
          loop (add_from_binding set binding) rest
      | Not {body; _} ->
          loop set (FingerTree.cons rest body)
      | And cs | Or cs ->
          let next = FingerTree.append (FingerTree.of_list cs) rest in
          loop set next
      | Exists {variable; body; _} | Forall {variable; body; _} ->
          loop (StringSet.add variable set) (FingerTree.cons rest body)
      | Eq {left; right} ->
          let add_expr acc = function Var v -> StringSet.add v acc | Const _ -> acc in
          loop (add_expr (add_expr set left) right) rest )
  in
  loop StringSet.empty (FingerTree.singleton c) |> StringSet.elements

let rename_vars (renames : (attr_name * attr_name) list) : t -> t =
  let rename_map = BindingMap.of_seq (List.to_seq renames) in
  let rename_name name =
    match BindingMap.find_opt name rename_map with Some n -> n | None -> name
  in
  let rec go = function
    | MemberOf {target; binding} ->
        let binding' =
          BindingMap.fold
            (fun k expr acc ->
              let k' = rename_name k in
              let expr' = match expr with Var v -> Var (rename_name v) | Const _ -> expr in
              BindingMap.add k' expr' acc )
            binding BindingMap.empty
        in
        MemberOf {target; binding= binding'}
    | Not {body; universe} ->
        Not {body= go body; universe}
    | And cs ->
        And (List.map go cs)
    | Or cs ->
        Or (List.map go cs)
    | Exists {variable; quantifier; body} ->
        let variable' = rename_name variable in
        Exists {variable= variable'; quantifier; body= go body}
    | Forall {variable; quantifier; body} ->
        let variable' = rename_name variable in
        Forall {variable= variable'; quantifier; body= go body}
    | Eq {left; right} ->
        let rename_expr = function Var v -> Var (rename_name v) | Const _ as e -> e in
        Eq {left= rename_expr left; right= rename_expr right}
  in
  go

(** Drop constraints referencing attributes not in the given set. Returns [None]
    if the constraint cannot be kept. *)
let rec filter_by_attrs (attrs : attr_name list) (c : t) : t option =
  let all_present c = List.for_all (fun v -> List.mem v attrs) (vars_in c) in
  match c with
  | MemberOf _ ->
      if all_present c then Some c else None
  | Not {body; universe} -> (
    match filter_by_attrs attrs body with
    | Some body' ->
        Some (Not {body= body'; universe})
    | None ->
        None )
  | And cs ->
      let kept = List.filter_map (filter_by_attrs attrs) cs in
      if kept = [] then None else Some (And kept)
  | Or cs ->
      (* All branches must survive for Or to remain sound *)
      let kept = List.filter_map (filter_by_attrs attrs) cs in
      if List.length kept = List.length cs then Some (Or kept) else None
  | Exists {variable; quantifier; body} -> (
    match filter_by_attrs (variable :: attrs) body with
    | Some body' ->
        Some (Exists {variable; quantifier; body= body'})
    | None ->
        None )
  | Forall {variable; quantifier; body} -> (
    match filter_by_attrs (variable :: attrs) body with
    | Some body' ->
        Some (Forall {variable; quantifier; body= body'})
    | None ->
        None )
  | Eq _ ->
      if all_present c then Some c else None

let merge (cs1 : (string * t) list) (cs2 : (string * t) list) : (string * t) list =
  let combined = cs1 @ cs2 in
  let tbl = Hashtbl.create 16 in
  List.iter
    (fun (name, c) ->
      match Hashtbl.find_opt tbl name with
      | None ->
          Hashtbl.replace tbl name [c]
      | Some cs ->
          Hashtbl.replace tbl name (c :: cs) )
    combined ;
  Hashtbl.fold
    (fun name cs acc ->
      let merged = match cs with [c] -> c | _ -> And cs in
      (name, merged) :: acc )
    tbl []

let member_of ~target ~binding = MemberOf {target; binding}

let not_ ~universe body = Not {body; universe}

let and_ cs = match cs with [c] -> c | _ -> And cs

let or_ cs = match cs with [c] -> c | _ -> Or cs

let exists ~variable ~quantifier body = Exists {variable; quantifier; body}

let forall ~variable ~quantifier body = Forall {variable; quantifier; body}

type eval_context =
  { check_membership: relation_name -> (attr_name * Conventions.AbstractValue.t) list -> bool;
    iterate_finite: relation_name -> (attr_name * Conventions.AbstractValue.t) list list option }

let bind (b : binding) (tuple : Tuple.materialized) : (attr_name * Conventions.AbstractValue.t) list
    =
  BindingMap.fold
    (fun target_attr expr acc ->
      let value =
        match expr with
        | Const v ->
            Some v
        | Var src_attr -> (
          match Tuple.AttributeMap.find_opt src_attr tuple.attributes with
          | Some attr ->
              Some attr.Attribute.value
          | None ->
              None )
      in
      match value with Some v -> (target_attr, v) :: acc | None -> acc )
    b []

let rec evaluate (ctx : eval_context) (tuple : Tuple.materialized) (c : t) :
    (bool, diagnostic) result =
  match c with
  | MemberOf {target; binding} ->
      let bound_values = bind binding tuple in
      if ctx.check_membership target bound_values then Ok true
      else Error (MembershipFailed {target; bound_values})
  | Not {body; universe= _} -> (
    (* The universe is a declarative annotation for closed-world reasoning.
       At runtime we negate the body; the universe enforces logical soundness
       at the schema level, not as a runtime tuple-existence check. *)
    match evaluate ctx tuple body with
    | Ok true ->
        Ok false
    | Ok false ->
        Ok true
    | Error (MembershipFailed _) ->
        Ok true
    | Error d ->
        Error d )
  | And cs ->
      evaluate_and ctx tuple cs
  | Or cs ->
      evaluate_or ctx tuple cs
  | Eq {left; right} -> (
      let resolve = function
        | Const v ->
            Some v
        | Var a -> (
          match Tuple.AttributeMap.find_opt a tuple.attributes with
          | Some attr ->
              Some attr.Attribute.value
          | None ->
              None )
      in
      match (resolve left, resolve right) with Some l, Some r -> Ok (l = r) | _ -> Ok false )
  | Exists {variable; quantifier; body} -> (
    match ctx.iterate_finite quantifier with
    | None ->
        Error (UnboundedQuantifier {variable; quantifier})
    | Some rows ->
        let rec check = function
          | [] ->
              Ok false
          | row :: rest -> (
              let extended_tuple = extend_tuple tuple variable row in
              match evaluate ctx extended_tuple body with
              | Ok true ->
                  Ok true
              | Ok false ->
                  check rest
              | Error _ ->
                  check rest )
        in
        check rows )
  | Forall {variable; quantifier; body} -> (
    match ctx.iterate_finite quantifier with
    | None ->
        Error (UnboundedQuantifier {variable; quantifier})
    | Some rows ->
        let rec check = function
          | [] ->
              Ok true
          | row :: rest -> (
              let extended_tuple = extend_tuple tuple variable row in
              match evaluate ctx extended_tuple body with
              | Ok true ->
                  check rest
              | Ok false ->
                  Ok false
              | Error (MembershipFailed _) ->
                  Ok false
              | Error d ->
                  Error d )
        in
        check rows )

and evaluate_and ctx tuple = function
  | [] ->
      Ok true
  | c :: rest -> (
    match evaluate ctx tuple c with
    | Ok true ->
        evaluate_and ctx tuple rest
    | Ok false ->
        Ok false
    | Error d ->
        Error d )

and evaluate_or ctx tuple = function
  | [] ->
      Ok false
  | c :: rest -> (
    match evaluate ctx tuple c with
    | Ok true ->
        Ok true
    | Ok false ->
        evaluate_or ctx tuple rest
    | Error _ ->
        evaluate_or ctx tuple rest )

and extend_tuple (tuple : Tuple.materialized) (variable : attr_name)
    (row : (attr_name * Conventions.AbstractValue.t) list) : Tuple.materialized =
  (* Quantifier attributes are namespaced by the variable name (for example "d.dept_id")
     so they cannot silently overwrite same-named attributes from the base tuple
     or from other quantifiers. Without such namespacing, a constraint like
     Exists d in Department, MemberOf Department (dept_id = Var "dept_id")
     would potentially overwrite the base tuple's dept_id with the Department row's dept_id,
     causing Var "dept_id" to resolve to the wrong value and accept invalid inserts.
     Constraints reference quantifier attributes as Var "variable.attr"
     (for example Var "d.dept_id") and base tuple attributes as Var "attr" (like Var "dept_id").
     This convention is the prerequisite for universal variable substitution
     (see docs/incremental_constraint_checking.org, specifically Technique 2).
     A better long-term solution would attach provenance metadata directly
     to each attribute rather than relying on name conventions, but we don't have that yet. *)
  let extra_attrs =
    List.fold_left
      (fun acc (k, v) -> Tuple.AttributeMap.add (variable ^ "." ^ k) {Attribute.value= v} acc)
      tuple.attributes row
  in
  {tuple with attributes= extra_attrs}

(* TODO: Sometimes it's more efficient and sufficient to simply halt at the first constraint violated. However, it's also important to be able to adjudicate which constraints failed on the tuple materialization. For that reason, we should likely return a lazy stream that computes the constraints on demand. *)
let evaluate_named (ctx : eval_context) (tuple : Tuple.materialized) (named : (string * t) list) :
    (bool, diagnostic) result =
  let failures =
    List.filter_map
      (fun (name, c) ->
        match evaluate ctx tuple c with
        | Ok true ->
            None
        | Ok false ->
            Some (name, MembershipFailed {target= name; bound_values= []})
        | Error d ->
            Some (name, d) )
      named
  in
  match failures with [] -> Ok true | fs -> Error (ConstraintFailures fs)

(** Fast-path evaluator: halts at the first non-passing constraint. Use in
    cascade and deferred checking where a single violation suffices. *)
let evaluate_first_failure (ctx : eval_context) (tuple : Tuple.materialized)
    (named : (string * t) list) : (bool, diagnostic) result =
  let rec loop = function
    | [] ->
        Ok true
    | (_name, c) :: rest -> (
      match evaluate ctx tuple c with Ok true -> loop rest | r -> r )
  in
  loop named

(** Polarity analysis: determines whether each relation name appears in a
    positive (must-exist) or negative (must-not-exist) position within a
    constraint tree. Used for cascade checking: when a tuple is deleted, only
    relations with Positive or Both polarity need re-validation. *)

type polarity = Positive | Negative | Both

type polarity_index = polarity PolarityMap.t

let merge_polarity a b =
  match (a, b) with Positive, Positive -> Positive | Negative, Negative -> Negative | _ -> Both

let flip_polarity = function Positive -> Negative | Negative -> Positive | Both -> Both

let polarity_of ?(neg = false) (c : t) : polarity_index =
  let with_neg is_neg pol = if is_neg then flip_polarity pol else pol in
  let add_pol acc name pol =
    match PolarityMap.find_opt name acc with
    | None ->
        PolarityMap.add name pol acc
    | Some prev ->
        PolarityMap.add name (merge_polarity prev pol) acc
  in
  let rec loop acc worklist =
    match FingerTree.front worklist with
    | None ->
        acc
    | Some (rest, (is_neg, node)) -> (
      match node with
      | MemberOf {target; _} ->
          loop (add_pol acc target (with_neg is_neg Positive)) rest
      | Not {body; _} ->
          loop acc (FingerTree.cons rest (not is_neg, body))
      | And cs | Or cs ->
          let children = List.map (fun child -> (is_neg, child)) cs |> FingerTree.of_list in
          let next = FingerTree.append children rest in
          loop acc next
      | Exists {quantifier; body; _} ->
          let acc' = add_pol acc quantifier (with_neg is_neg Positive) in
          loop acc' (FingerTree.cons rest (is_neg, body))
      | Forall {quantifier; body; _} ->
          let acc' = add_pol acc quantifier (with_neg is_neg Negative) in
          loop acc' (FingerTree.cons rest (is_neg, body))
      | Eq _ ->
          loop acc rest )
  in
  loop PolarityMap.empty (FingerTree.singleton (neg, c))

let polarity_find name (pols : polarity_index) = PolarityMap.find_opt name pols

(** Constraint timing: immediate (checked on every mutation) or deferred
    (checked at transaction commit). *)
type timing = Immediate | Deferred

type substitute_mode = SubstituteGo | SubstituteApply of Conventions.AbstractValue.t BindingMap.t

type substitute_instr =
  | Visit of substitute_mode * t
  | BuildNot of relation_name
  | BuildAnd of int
  | BuildOr of int
  | BuildExists of attr_name * relation_name
  | BuildForall of attr_name * relation_name

(** Given a constraint, a deleted relation name [dep_rel], and the deleted
    tuple's attributes, return a filter: [(attr_name, value)] pairs that narrow
    which constrained-relation tuples could be affected.

    For a Var binding like [dept_id = Var "dept_id"] targeting [dep_rel], the
    deleted tuple's [dept_id] value becomes the filter: only constrained tuples
    with that exact [dept_id] need re-checking.

    Returns [] if no narrowing is possible (Const bindings, unrelated rel). *)
let focused_filter (c : t) (dep_rel : relation_name)
    (deleted : (attr_name * Conventions.AbstractValue.t) list) :
    (attr_name * Conventions.AbstractValue.t) list =
  let add_from_binding acc binding =
    BindingMap.fold
      (fun _target_attr expr acc ->
        match expr with
        | Var src_attr -> (
          match List.assoc_opt src_attr deleted with Some v -> (src_attr, v) :: acc | None -> acc )
        | Const _ ->
            acc )
      binding acc
  in
  let rec loop acc worklist =
    match FingerTree.front worklist with
    | None ->
        acc
    | Some (rest, node) -> (
      match node with
      | MemberOf {target; binding} when target = dep_rel ->
          loop (add_from_binding acc binding) rest
      | MemberOf _ ->
          loop acc rest
      | Not {body; _} | Exists {body; _} | Forall {body; _} ->
          loop acc (FingerTree.cons rest body)
      | And cs | Or cs ->
          let next = FingerTree.append (FingerTree.of_list cs) rest in
          loop acc next
      | Eq _ ->
          loop acc rest )
  in
  loop [] (FingerTree.singleton c)

(** Extract Const binding values from a constraint for a given [dep_rel].

    These are fixed-value preconditions: if the deleted tuple doesn't have these
    exact values, the constraint cannot be violated by the deletion. The cascade
    checker uses this to bail out early. *)
let trigger_constants (c : t) (dep_rel : relation_name) :
    (attr_name * Conventions.AbstractValue.t) list =
  let add_from_binding acc binding =
    BindingMap.fold
      (fun target_attr expr acc ->
        match expr with Const v -> (target_attr, v) :: acc | Var _ -> acc )
      binding acc
  in
  let rec loop acc worklist =
    match FingerTree.front worklist with
    | None ->
        acc
    | Some (rest, node) -> (
      match node with
      | MemberOf {target; binding} when target = dep_rel ->
          loop (add_from_binding acc binding) rest
      | MemberOf _ ->
          loop acc rest
      | Not {body; _} | Exists {body; _} | Forall {body; _} ->
          loop acc (FingerTree.cons rest body)
      | And cs | Or cs ->
          let next = FingerTree.append (FingerTree.of_list cs) rest in
          loop acc next
      | Eq _ ->
          loop acc rest )
  in
  loop [] (FingerTree.singleton c)

(** Substitute transition tuple values into a constraint for universal variable
    substitution. When [dep_rel] is being mutated (deleted from or inserted
    into), any [Exists] or [Forall] that quantifies over [dep_rel] has its body
    rewritten: occurrences of [Var "variable.attr"] are replaced by
    [Const value] using the transition tuple's attribute values.

    The convention that quantifier attributes are namespaced as [variable.attr]
    (established by [extend_tuple]) means base tuple [Var] references like
    [Var "dept_id"] are never substituted, only [Var "d.dept_id"] would match a
    quantifier variable [d] over a deleted [Department] row.

    This transforms the recheck from a full relation evaluation into a targeted
    expression that tests only the rows affected by the transition tuple. See
    Technique 2 in docs/incremental_constraint_checking.org. *)
let substitute_transition (c : t) (dep_rel : relation_name)
    (transition : (attr_name * Conventions.AbstractValue.t) list) : t =
  let namespaced_subs variable =
    List.fold_left
      (fun acc (a, v) -> BindingMap.add (variable ^ "." ^ a) v acc)
      BindingMap.empty transition
  in
  let enqueue_visit_then_build visit build rest =
    FingerTree.cons (FingerTree.cons rest build) visit
  in
  let enqueue_children mode cs build_of_count rest =
    let prefix, count =
      List.fold_left
        (fun (acc, n) child -> (FingerTree.snoc acc (Visit (mode, child)), n + 1))
        (FingerTree.empty, 0) cs
    in
    let prefix = FingerTree.snoc prefix (build_of_count count) in
    FingerTree.append prefix rest
  in
  let apply_subs subs binding =
    BindingMap.map
      (function
        | Var v -> (
          match BindingMap.find_opt v subs with Some value -> Const value | None -> Var v )
        | Const _ as expr ->
            expr )
      binding
  in
  let rec pop_n n vals acc =
    if n = 0 then (acc, vals)
    else match vals with [] -> (acc, []) | v :: rest -> pop_n (n - 1) rest (v :: acc)
  in
  let rec loop instrs vals =
    match FingerTree.front instrs with
    | None -> (
      match vals with [result] -> result | _ -> c )
    | Some (rest, instr) -> (
      match instr with
      | Visit (mode, node) -> (
        match node with
        | MemberOf {target; binding} ->
            let binding' =
              match mode with
              | SubstituteGo ->
                  binding
              | SubstituteApply subs ->
                  apply_subs subs binding
            in
            loop rest (MemberOf {target; binding= binding'} :: vals)
        | Not {body; universe} ->
            loop (enqueue_visit_then_build (Visit (mode, body)) (BuildNot universe) rest) vals
        | And cs ->
            let instrs' = enqueue_children mode cs (fun n -> BuildAnd n) rest in
            loop instrs' vals
        | Or cs ->
            let instrs' = enqueue_children mode cs (fun n -> BuildOr n) rest in
            loop instrs' vals
        | Exists {variable; quantifier; body} ->
            let body_mode =
              match mode with
              | SubstituteApply subs ->
                  SubstituteApply subs
              | SubstituteGo ->
                  if quantifier = dep_rel then SubstituteApply (namespaced_subs variable)
                  else SubstituteGo
            in
            loop
              (enqueue_visit_then_build
                 (Visit (body_mode, body))
                 (BuildExists (variable, quantifier))
                 rest )
              vals
        | Forall {variable; quantifier; body} ->
            let body_mode =
              match mode with
              | SubstituteApply subs ->
                  SubstituteApply subs
              | SubstituteGo ->
                  if quantifier = dep_rel then SubstituteApply (namespaced_subs variable)
                  else SubstituteGo
            in
            loop
              (enqueue_visit_then_build
                 (Visit (body_mode, body))
                 (BuildForall (variable, quantifier))
                 rest )
              vals
        | Eq {left; right} ->
            let subst_expr subs = function
              | Var v -> (
                match BindingMap.find_opt v subs with Some v' -> Const v' | None -> Var v )
              | Const _ as e ->
                  e
            in
            let left', right' =
              match mode with
              | SubstituteGo ->
                  (left, right)
              | SubstituteApply subs ->
                  (subst_expr subs left, subst_expr subs right)
            in
            loop rest (Eq {left= left'; right= right'} :: vals) )
      | BuildNot universe -> (
        match vals with
        | body :: vals' ->
            loop rest (Not {body; universe} :: vals')
        | [] ->
            loop rest vals )
      | BuildAnd n ->
          let children, vals' = pop_n n vals [] in
          loop rest (And children :: vals')
      | BuildOr n ->
          let children, vals' = pop_n n vals [] in
          loop rest (Or children :: vals')
      | BuildExists (variable, quantifier) -> (
        match vals with
        | body :: vals' ->
            loop rest (Exists {variable; quantifier; body} :: vals')
        | [] ->
            loop rest vals )
      | BuildForall (variable, quantifier) -> (
        match vals with
        | body :: vals' ->
            loop rest (Forall {variable; quantifier; body} :: vals')
        | [] ->
            loop rest vals ) )
  in
  loop (FingerTree.singleton (Visit (SubstituteGo, c))) []

let make_comparison_binding ~left ~right =
  BindingMap.empty |> BindingMap.add "left" left |> BindingMap.add "right" right

let lt ~left ~right = MemberOf {target= "less_than"; binding= make_comparison_binding ~left ~right}

let lte ~left ~right =
  MemberOf {target= "less_than_or_equal"; binding= make_comparison_binding ~left ~right}

let gt ~left ~right =
  MemberOf {target= "greater_than"; binding= make_comparison_binding ~left ~right}

let gte ~left ~right =
  MemberOf {target= "greater_than_or_equal"; binding= make_comparison_binding ~left ~right}

let eq ~left ~right = Eq {left; right}

let neq ~left ~right = Not {body= Eq {left; right}; universe= ""}

let between ~value ~low ~high =
  And
    [ MemberOf
        {target= "greater_than_or_equal"; binding= make_comparison_binding ~left:value ~right:low};
      MemberOf
        {target= "less_than_or_equal"; binding= make_comparison_binding ~left:value ~right:high} ]

(** Build membership criteria from schema (validates type membership). Returns a
    function that takes a tree-lookup function and a tuple, checking schema
    conformance and tree membership at call time. *)
let build_membership_criteria ~_name ~schema : (*(string -> Merkle.t option) ->*) Tuple.t -> bool =
  let value_conforms_to_domain domain_name (value : Conventions.AbstractValue.t) =
    match domain_name with
    | "integer" ->
        Obj.is_int value
    | "natural" ->
        Obj.is_int value && (Obj.obj value : int) >= 0
    | "string" ->
        Obj.tag value = Obj.string_tag
    | _ ->
        (* Unknown domain: could be a relation-typed attribute (nested tuple)
          or a user-defined domain. Accept for now; full validation would
          require looking up the target relation's schema. *)
        true
  in
  let n_expected = List.length schema in
  (* fun tree_of tuple -> *)
  fun tuple ->
    match tuple with
    | Tuple.NonMaterialized _nm ->
        (* begin match tree_of name with
        | None -> false
        | Some t ->
        Merkle.member nm.hash t
        && Tuple.AttributeMap.cardinal nm.attributes = n_expected
        && List.for_all
        (fun (attr_name, _) ->
        Tuple.AttributeMap.mem attr_name nm.attributes)
        schema
        end *)
        false
    | Tuple.Materialized m ->
        Tuple.AttributeMap.cardinal m.attributes = n_expected
        && List.for_all
             (fun (attr_name, domain_name) ->
               match Tuple.AttributeMap.find_opt attr_name m.attributes with
               | None ->
                   false
               | Some attr ->
                   value_conforms_to_domain domain_name attr.Attribute.value )
             schema

(** Mutation direction for cascade constraint checking *)
type mutation = Insert | Delete

(** Context for a single tuple mutation, used by cascade checking *)
type mutation_context =
  {target_relation: string; transition: (string * Conventions.AbstractValue.t) list; kind: mutation}
