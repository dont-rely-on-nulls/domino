module Wire = struct
  let split_once ch value =
    match String.index_opt value ch with
    | None ->
        None
    | Some index ->
        Some
          (String.sub value 0 index, String.sub value (index + 1) (String.length value - index - 1))

  let lines bytes =
    bytes |> Bytes.to_string |> String.split_on_char '\n'
    |> List.filter (fun line -> String.trim line <> "")

  let fields bytes =
    lines bytes
    |> List.filter_map (fun line -> split_once '=' line)
    |> List.map (fun (key, value) -> (String.trim key, String.trim value))

  let require_type expected fields =
    match List.assoc_opt "type" fields with
    | Some actual when actual = expected ->
        ()
    | Some actual ->
        invalid_arg (Printf.sprintf "expected storable type %S, got %S" expected actual)
    | None ->
        invalid_arg "missing storable type"

  let values key fields = List.filter_map (fun (k, v) -> if k = key then Some v else None) fields

  let required key fields =
    match List.assoc_opt key fields with
    | Some value ->
        value
    | None ->
        invalid_arg ("missing storable field: " ^ key)

  let attribute value =
    match split_once ':' value with
    | Some pair ->
        pair
    | None ->
        invalid_arg ("invalid attribute field: " ^ value)

  let encode fields =
    fields
    |> List.map (fun (key, value) -> key ^ "=" ^ value)
    |> String.concat "\n"
    |> fun value -> Bytes.of_string (value ^ "\n")
end

module Tuple = struct
  type t = {relation: string; attributes: (string * string) list}

  let to_bytes tuple =
    Wire.encode
      ( [("type", "tuple"); ("relation", tuple.relation)]
      @ List.map (fun (name, value) -> ("attribute", name ^ ":" ^ value)) tuple.attributes )

  let of_bytes bytes =
    let fields = Wire.fields bytes in
    Wire.require_type "tuple" fields ;
    { relation= Wire.required "relation" fields;
      attributes= List.map Wire.attribute (Wire.values "attribute" fields) }
end

module Relation = struct
  type t =
    { name: string;
      schema: (string * string) list;
          (** Hex Merkle root of the relation's tuple set. Empty string = empty
            relation. *)
      tree_pointer: string }

  let to_bytes relation =
    Wire.encode
      ( [ ("type", "relation"); ("name", relation.name); ("kind", "stored");
          ("tree_pointer", relation.tree_pointer) ]
      @ List.map (fun (name, domain) -> ("attribute", name ^ ":" ^ domain)) relation.schema )

  let of_bytes bytes =
    let fields = Wire.fields bytes in
    Wire.require_type "relation" fields ;
    { name= Wire.required "name" fields;
      schema= List.map Wire.attribute (Wire.values "attribute" fields);
      tree_pointer= Option.value (List.assoc_opt "tree_pointer" fields) ~default:"" }
end

module Multigroup = struct
  type t = {name: string; relations: Relation.t list}

  (* Wire format: "name|attr1:dom1,attr2:dom2|<merkle_root>"
     The third segment is the Merkle root hex; empty string = empty relation.
     A fourth segment for constraints is reserved but not yet implemented.
     TODO: add constraints serialisation once a canonical sexp representation
     is agreed upon (two syntactically distinct bodies may be semantically
     equivalent — the round-trip must account for that). *)
  let relation_value relation =
    let attrs =
      relation.Relation.schema
      |> List.map (fun (name, domain) -> name ^ ":" ^ domain)
      |> String.concat ","
    in
    relation.name ^ "|" ^ attrs ^ "|" ^ relation.tree_pointer

  let parse_relation value =
    match Wire.split_once '|' value with
    | None ->
        {Relation.name= value; schema= []; tree_pointer= ""}
    | Some (name, rest) ->
        let attrs_str, tree_pointer =
          match Wire.split_once '|' rest with None -> (rest, "") | Some (a, tp) -> (a, tp)
        in
        let schema =
          if attrs_str = "" then []
          else attrs_str |> String.split_on_char ',' |> List.map Wire.attribute
        in
        {Relation.name; schema; tree_pointer}

  let to_bytes multigroup =
    Wire.encode
      ( [("type", "multigroup"); ("name", multigroup.name)]
      @ List.map (fun relation -> ("relation", relation_value relation)) multigroup.relations )

  let of_bytes bytes =
    let fields = Wire.fields bytes in
    Wire.require_type "multigroup" fields ;
    { name= Wire.required "name" fields;
      relations= List.map parse_relation (Wire.values "relation" fields) }
end
