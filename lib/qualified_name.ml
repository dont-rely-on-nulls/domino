(** Fully qualified relation reference: [mg:rel_name].

    The first colon separates the multigroup name from the relation name; any
    further colons are part of the relation name verbatim.

    Examples:
    - ["warehouse:orders"]        -> mg=["warehouse"], name=["orders"]
    - ["warehouse:public:orders"] -> mg=["warehouse"], name=["public:orders"]
    - ["sakura:public:relation"]  -> mg=["sakura"],    name=["public:relation"]

    A reference with no leading [<mg>:] segment is a parse error — callers must
    supply the mg explicitly. *)

type t = { mg : string; name : string }

module Error = struct
  open Condition
  let unqualified_name name = condition "unqualified-name" "A qualified name was expected, but an unqualified one was given"
                                ("name" |=| (of_string name))
end

exception Unqualified of string

let parse (s : string) : t =
  match String.index_opt s ':' with
  | None -> raise (Unqualified s)
  | Some i ->
      let mg = String.sub s 0 i in
      let name = String.sub s (i + 1) (String.length s - i - 1) in
      if mg = "" || name = "" then raise (Unqualified s)
      else { mg; name }

let try_parse (s : string) : (t, Condition.t) result =
  try Ok (parse s) with Unqualified s -> Error (Error.unqualified_name s)

let make ~mg ~name = { mg; name }

let to_string { mg; name } = mg ^ ":" ^ name
