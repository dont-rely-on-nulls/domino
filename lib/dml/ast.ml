open Sexplib.Std

type attr_value = string * Drl.Ast.value [@@deriving sexp]

type statement =
  | InsertTuple of { relation : string; attributes : attr_value list }
  | InsertTuples of { relation : string; tuples : attr_value list list }
  | DeleteTuple of { relation : string; attributes : attr_value list }
  | Assign of { target : string; body : Drl.Ast.query }
  | InsertFrom of { target : string; source : Drl.Ast.query }
  | DeleteWhere of { target : string; predicate : Drl.Ast.query }
  (** Binds an unqualified name to a session-scoped ephemeral relation.
      The body is recomputed on each scan and never materialized;
      the binding lives until [DropDefine] or a session close.
      In contrast, [Assign] drains the body into an existing
      stored relation. *)
  | Define of { target : string; body : Drl.Ast.query }
  (** Drops a [Define] binding, releasing its session-ownership pin. *)
  | DropDefine of { target : string }
[@@deriving sexp]
