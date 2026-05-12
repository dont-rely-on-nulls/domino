type cursor_result = {
  cursor_id : string;
  rows : Tuple.materialized list;
  has_more : bool;
}

type result =
  | Query of Relation.relation
  | Transition of Management.Multigroup.multigroup
  | Cursor of cursor_result
  | SessionSwitch of string
  | CreateMultigroup of string
