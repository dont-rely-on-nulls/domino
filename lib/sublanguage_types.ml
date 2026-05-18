type cursor_result = {
  cursor_id : string;
  rows : Tuple.materialized list;
  has_more : bool;
}

(** A Transition delta names every multigroup that the statement touched and
    the updated value for each.  Most statements yield a single-element list;
    VCL/SCL/ICL statements may touch more than one. *)
type transition_delta = (string * Management.Multigroup.multigroup) list

type result =
  | Query of Relation.relation
  | Transition of transition_delta
  | Cursor of cursor_result
  | SessionSwitch of string
  | CreateMultigroup of string
