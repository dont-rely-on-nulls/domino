(** Fully qualified relation name: [multigroup:schema:relation].

    Unqualified names fill in defaults: empty multigroup, ["public"] schema. *)

type t = {
  multigroup : string;
  schema : string;
  relation : string;
}

val default_schema : string

(** Split on [:] with default filling.
    - ["rel"]           -> multigroup="", schema="public", relation="rel"
    - ["s:rel"]         -> multigroup="", schema="s",      relation="rel"
    - ["mg:s:rel"]      -> multigroup="mg", schema="s",    relation="rel" *)
val parse : string -> t

val make : ?multigroup:string -> ?schema:string -> string -> t
val to_string : t -> string

(** Composite key for [RelationMap] lookup within a single database:
    ["schema:relation"]. *)
val to_key : t -> string
