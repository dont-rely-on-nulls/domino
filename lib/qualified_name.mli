(** Fully qualified relation reference: [mg:rel_name].

    The first colon separates the multigroup name from the relation name; any
    further colons are part of the relation name verbatim. *)

type t = { mg : string; name : string }

exception Unqualified of string

val parse : string -> t
(** Splits on the first [:].  Raises [Unqualified] when no mg segment is
    present (i.e. no colon, or empty mg / empty name). *)

val try_parse : string -> (t, Condition.t) result

val make : mg:string -> name:string -> t

val to_string : t -> string
