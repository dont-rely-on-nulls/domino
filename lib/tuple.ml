module AttributeMap = struct
  include BatMap.String

  let sexp_of_t sexp_of_v m =
    Sexplib.Sexp.List
      (List.map
         (fun (k, v) -> Sexplib.Sexp.List [ Sexplib.Sexp.Atom k; sexp_of_v v ])
         (bindings m))
end

type non_materialized = {
  hash : Conventions.Hash.t;
  relation : Conventions.Name.t;
  attributes : Attribute.referenced AttributeMap.t;
  (* lineage : Relation.Lineage.t option; *)
}
[@@deriving sexp_of]

type materialized = {
  relation : Conventions.Name.t;
  attributes : Attribute.materialized AttributeMap.t;
  (* lineage : Relation.Lineage.t option; *)
}
[@@deriving sexp_of]

type t = Materialized of materialized | NonMaterialized of non_materialized
[@@deriving sexp_of]

let make_materialized ~relation ~attributes =
  Materialized {
      relation;
      attributes;
      (* lineage = None *)
    }
