open Sexplib.Std

type merge_strategy = PreferLeft | PreferRight | RevertToAncestor
[@@deriving sexp]

type statement =
  | CreateBranch of { name : string; hash : string option [@sexp.option] }
  | Checkout of string
  | GetHead
  | GetBranchTip of string
  | UpdateBranchTip of { name : string; hash : string }
  | Merge of { left : string; right : string; strategy : merge_strategy }
  | Use of string
  | CreateMultigroup of string
[@@deriving sexp]
