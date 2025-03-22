open Protocol_conv_xml
open Sexplib0.Sexp_conv

type tuple = string list [@@deriving sexp, protocol ~driver:(module Xml_light)]

type relation = {
  attribute_name : string;
  attribute_type : string;
  tuples : tuple;
}
[@@deriving sexp, protocol ~driver:(module Xml_light)]

type facts = relation list
[@@deriving sexp, protocol ~driver:(module Xml_light)]
