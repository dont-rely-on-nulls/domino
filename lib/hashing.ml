(** Hash computation for tuples and relations.

    This module provides content-addressable hashing for the relational engine.
    All hashes are deterministic - the same content always produces the same
    hash. This enables:
    - Deduplication of tuples
    - Merkle tree construction for relations and databases
    - Time-travel queries (load any historical state by hash) *)

(** Convert bytes to hex string (to avoid null bytes in hash input) *)
let bytes_to_hex bytes =
  let len = Bytes.length bytes in
  let hex = Bytes.create (len * 2) in
  for i = 0 to len - 1 do
    let c = Bytes.get_uint8 bytes i in
    let hi = c lsr 4 in
    let lo = c land 0xf in
    Bytes.set hex (i * 2) (Char.chr (if hi < 10 then hi + 48 else hi + 87));
    Bytes.set hex ((i * 2) + 1) (Char.chr (if lo < 10 then lo + 48 else lo + 87))
  done;
  Bytes.to_string hex

(** Compute hash for a tuple. The hash is computed from:
    - The relation name
    - All attribute names and their marshalled values (sorted by name for
      determinism) *)
let hash_tuple (tuple : Tuple.materialized) : Conventions.Hash.t =
  let sorted_attrs =
    Tuple.AttributeMap.bindings tuple.attributes
    |> List.sort (fun (a, _) (b, _) -> String.compare a b)
  in
  let content =
    List.fold_left
      (fun acc (name, attr) ->
        (* Marshal the value and hex-encode to avoid null bytes *)
        let value_bytes = Marshal.to_bytes attr.Attribute.value [Marshal.Closures] in
        let value_hex = bytes_to_hex value_bytes in
        acc ^ name ^ ":" ^ value_hex ^ ";" )
      (tuple.relation ^ "|") sorted_attrs
  in
  Conventions.Hash.hash_text content

(** Compute hash for a relation. The hash is computed from:
    - The relation name
    - The schema (attribute names and types)
    - The merkle tree root hash (representing all tuples) *)
(* let hash_relation ~name ~(schema : Schema.t) ~tree : Conventions.Hash.t =
  let schema_str = Schema.to_string schema in
  let tree_hash =
    match Merkle.root_hash tree with Some h -> h | None -> "empty"
  in
  Conventions.Hash.hash_text (name ^ "|" ^ schema_str ^ "|" ^ tree_hash) *)
