type relation_name = string
type attr_name = string
type binding_expr = Var of attr_name | Const of Conventions.AbstractValue.t

module BindingMap : Map.S with type key = String.t

type binding = binding_expr BindingMap.t

type t =
  | MemberOf of { target : relation_name; binding : binding }
  | Not of { body : t; universe : relation_name }
  | And of t list
  | Or of t list
  | Exists of { variable : attr_name; quantifier : relation_name; body : t }
  | Forall of { variable : attr_name; quantifier : relation_name; body : t }
  | Eq of { left : binding_expr; right : binding_expr }

type diagnostic =
  | MembershipFailed of {
      target : string;
      bound_values : (attr_name * Conventions.AbstractValue.t) list;
    }
  | RelationNotFound of string
  | UnboundedQuantifier of { variable : attr_name; quantifier : relation_name }
  | ConstraintFailures of (string * diagnostic) list

val vars_in : t -> attr_name list
val rename_vars : (attr_name * attr_name) list -> t -> t
val filter_by_attrs : attr_name list -> t -> t option
val merge : (string * t) list -> (string * t) list -> (string * t) list
val member_of : target:relation_name -> binding:binding -> t
val not_ : universe:relation_name -> t -> t
val and_ : t list -> t
val or_ : t list -> t
val exists : variable:attr_name -> quantifier:relation_name -> t -> t
val forall : variable:attr_name -> quantifier:relation_name -> t -> t

type eval_context = {
  check_membership :
    relation_name -> (attr_name * Conventions.AbstractValue.t) list -> bool;
  iterate_finite :
    relation_name -> (attr_name * Conventions.AbstractValue.t) list list option;
}

val bind :
  binding ->
  Tuple.materialized ->
  (attr_name * Conventions.AbstractValue.t) list

val evaluate :
  eval_context -> Tuple.materialized -> t -> (bool, diagnostic) result

val evaluate_named :
  eval_context ->
  Tuple.materialized ->
  (string * t) list ->
  (bool, diagnostic) result

val evaluate_first_failure :
  eval_context ->
  Tuple.materialized ->
  (string * t) list ->
  (bool, diagnostic) result

type polarity = Positive | Negative | Both
type polarity_index

val polarity_of : ?neg:bool -> t -> polarity_index
val polarity_find : relation_name -> polarity_index -> polarity option

type timing = Immediate | Deferred

val focused_filter :
  t ->
  relation_name ->
  (attr_name * Conventions.AbstractValue.t) list ->
  (attr_name * Conventions.AbstractValue.t) list

val trigger_constants :
  t -> relation_name -> (attr_name * Conventions.AbstractValue.t) list

val substitute_transition :
  t -> relation_name -> (attr_name * Conventions.AbstractValue.t) list -> t

val make_comparison_binding : left:binding_expr -> right:binding_expr -> binding
val lt : left:binding_expr -> right:binding_expr -> t
val lte : left:binding_expr -> right:binding_expr -> t
val gt : left:binding_expr -> right:binding_expr -> t
val gte : left:binding_expr -> right:binding_expr -> t
val eq : left:binding_expr -> right:binding_expr -> t
val neq : left:binding_expr -> right:binding_expr -> t
val between : value:binding_expr -> low:binding_expr -> high:binding_expr -> t
