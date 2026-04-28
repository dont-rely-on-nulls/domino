(** Envelope parser for tagged S-expressions.

    Parses commands of the form (tag payload...) where the tag selects which
    sublanguage handles the payload. The tag set is open — future sublanguages
    (tcl, ppl, acl) just add new tags. *)

type tagged = { tag : string; payload : Sexplib.Sexp.t }

let parse (input : Sexplib.Sexp.t) : (tagged, Error.t) Result.t =
  match input with
  | Sexplib.Sexp.List (Sexplib.Sexp.Atom tag :: rest) ->
      let payload =
        match rest with [ single ] -> single | many -> Sexplib.Sexp.List many
      in
      Ok { tag; payload }
  | s -> Error (Error.MalformedExpression s)
