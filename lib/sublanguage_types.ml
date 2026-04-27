type cursor_result = {
  cursor_id : string;
  rows : Tuple.materialized list;
  has_more : bool;
}

type result =
  | Query of Relation.t
  | Transition of Management.Database.t * string
  | Cursor of cursor_result
  | SessionSwitch of string
  | CreateMultigroup of string