open Sexplib.Std

type binding_expr = Var of string | Const of Drl.Ast.value  (** reuse DRL value for literals *)
[@@deriving sexp]

type constraint_body =
  | MemberOf of {target: string; binding: (string * binding_expr) list}
  | Not of {body: constraint_body; universe: string}
  | And of constraint_body list
  | Or of constraint_body list
  | Exists of {variable: string; quantifier: string; body: constraint_body}
  | Forall of {variable: string; quantifier: string; body: constraint_body}
[@@deriving sexp]

type statement =
  | RegisterConstraint of {constraint_name: string; relation_name: string; body: constraint_body}
[@@deriving sexp]
