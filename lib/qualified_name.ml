(** Fully qualified relation name: [multigroup:schema:relation].

    Splits on [:], fills defaults for missing parts.
    - ["relation"]             -> { multigroup=""; schema="public"; relation }
    - ["schema:relation"]      -> { multigroup=""; schema; relation }
    - ["mg:schema:relation"]   -> { multigroup=mg; schema; relation }
*)

type t = {
  multigroup : string;
  schema : string;
  relation : string;
}

let default_schema = "public"

let parse s =
  match String.split_on_char ':' s with
  | [ rel ] -> { multigroup = ""; schema = default_schema; relation = rel }
  | [ schema; rel ] -> { multigroup = ""; schema; relation = rel }
  | [ mg; schema; rel ] -> { multigroup = mg; schema; relation = rel }
  | _ -> { multigroup = ""; schema = default_schema; relation = s }

let make ?(multigroup = "") ?(schema = default_schema) relation =
  { multigroup; schema; relation }

let to_string { multigroup; schema; relation } =
  if multigroup = "" then schema ^ ":" ^ relation
  else multigroup ^ ":" ^ schema ^ ":" ^ relation

let to_key { schema; relation; _ } = schema ^ ":" ^ relation
