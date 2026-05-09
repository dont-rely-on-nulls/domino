module Make (Storage : Management.Physical.S) = struct
  module Ops = Manipulation.Make (Storage)

  type error =
    | ParseError of string
    | ManipulationError of Error.t
    | RelationNotFound of string

  let sexp_of_error e =
    let open Sexplib.Sexp in
    match e with
    | ParseError s -> List [ Atom "parse-error"; Atom s ]
    | ManipulationError e -> Error.sexp_of_error e
    | RelationNotFound s -> List [ Atom "relation-not-found"; Atom s ]

  let ( let* ) = Result.bind
  let wrap_manip r = Result.map_error (fun e -> ManipulationError e) r

  let get_rel db name =
    Ops.get_relation db ~name |> Option.to_result ~none:(RelationNotFound name)

  let convert_cardinality : Ast.cardinality_spec -> Conventions.Cardinality.t =
    function
    | Ast.Finite n -> Conventions.Cardinality.Finite n
    | Ast.AlephZero -> Conventions.Cardinality.AlephZero
    | Ast.Continuum -> Conventions.Cardinality.Continuum
    | Ast.ConstrainedFinite -> Conventions.Cardinality.ConstrainedFinite

  let execute (storage : Storage.t) (db : Management.Database.t)
      (stmt : Ast.statement) : (Management.Database.t * string, error) result =
    match stmt with
    | Ast.CreateDatabase name ->
        let* db = Ops.create_database storage ~name |> wrap_manip in
        Ok (db, "Database created: " ^ name)
    | Ast.CreateRelation { name; schema = schema_pairs } ->
        let schema =
          List.fold_left
            (fun s (attr, dom) -> Schema.add attr dom s)
            Schema.empty schema_pairs
        in
        let* db, _ =
          Ops.create_relation storage db ~name ~schema |> wrap_manip
        in
        Ok (db, "Relation created: " ^ name)
    | Ast.RetractRelation name ->
        let* db = Ops.retract_relation storage db ~name |> wrap_manip in
        Ok (db, "Relation retracted: " ^ name)
    | Ast.ClearRelation name ->
        let* rel = get_rel db name in
        let* db, _ = Ops.clear_relation storage db rel |> wrap_manip in
        Ok (db, "Relation cleared: " ^ name)
    | Ast.RegisterDomain { name; cardinality } ->
        let domain =
          Domain.make ~name
            ~generator:(fun _ -> Generator.Error "not enumerable via DDL")
            ~membership_criteria:(fun _ -> true)
            ~cardinality:(convert_cardinality cardinality)
            ~compare:Stdlib.compare
        in
        let* db = Ops.register_domain storage db domain |> wrap_manip in
        Ok (db, "Domain registered: " ^ name)
end
