(** Schema: maps attribute names to domain/type names.

    A schema defines the structure of a relation by listing each attribute and
    its associated domain (type). This is used for:
    - Hashing relations (schema is part of the relation identity)
    - Validation (checking tuple membership)
    - Persistence (storing and loading relations with their structure) *)

type t = (string * string) list

let empty : t = []

let add name domain_name (schema : t) : t = (name, domain_name) :: schema

let attributes (schema : t) = List.map fst schema

let to_string schema = List.map (fun (n, t) -> n ^ ":" ^ t) schema |> String.concat ","
