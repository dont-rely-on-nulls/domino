open Protocol_conv_xml
open Sexplib0.Sexp_conv
(* open Sexplib *)

type tuple = string list [@@deriving sexp, protocol ~driver:(module Xml_light)]

type command = Disk.Command.t [@@deriving sexp, protocol ~driver:(module Xml_light)]

type relation = {
  attribute_name : string;
  attribute_type : string;
  tuples : tuple;
}
[@@deriving sexp, protocol ~driver:(module Xml_light)]
(* (int64 * (string*Executor.relational_literal) list) list *)
(* type facts = relation list *)
type facts = (int64 * (string*Disk.Executor.relational_literal) list) list
[@@deriving sexp, protocol ~driver:(module Xml_light)]
