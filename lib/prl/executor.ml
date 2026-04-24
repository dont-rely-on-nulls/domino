module Make (Storage : Management.Physical.S) = struct
  module Ops = Manipulation.Make (Storage)

  type error =
    | ParseError of string
    | RuntimeError of string
    | RelationError of Error.t
    | UnknownPluginSymbol of string
    | RelationNotFound of string

  let sexp_of_error e =
    let open Sexplib.Sexp in
    match e with
    | ParseError s -> List [ Atom "parse-error"; Atom s ]
    | RuntimeError s -> List [ Atom "runtime-error"; Atom s ]
    | RelationError err -> Error.sexp_of_error err
    | UnknownPluginSymbol s -> List [ Atom "unknown-plugin-symbol"; Atom s ]
    | RelationNotFound s -> List [ Atom "relation-not-found"; Atom s ]

  let convert_cardinality : Ast.cardinality_spec -> Conventions.Cardinality.t =
    function
    | Ast.Finite n -> Conventions.Cardinality.Finite n
    | Ast.AlephZero -> Conventions.Cardinality.AlephZero
    | Ast.Continuum -> Conventions.Cardinality.Continuum
    | Ast.ConstrainedFinite -> Conventions.Cardinality.ConstrainedFinite

  let convert_purity : Ast.purity -> Runtime.purity = function
    | Ast.Pure -> Runtime.Pure
    | Ast.Io -> Runtime.Io

  let list_names_msg () =
    let names =
      Runtime.list_bindings ()
      |> List.map (fun b -> b.Runtime.relation_name)
      |> List.sort String.compare
    in
    "Function predicates: "
    ^
    match names with [] -> "<none>" | _ -> String.concat ", " names

  let execute (storage : Storage.t) (db : Management.Database.t)
      (stmt : Ast.statement) : (Management.Database.t * string, error) result =
    match stmt with
    | Ast.LoadLibrary path -> (
        match Runtime.load_library path with
        | Ok () -> Ok (db, "Library loaded: " ^ path)
        | Error e -> Error (RuntimeError e))
    | Ast.DefineFunctionPredicate spec -> (
        match Plugin_api.find spec.symbol with
        | None -> Error (UnknownPluginSymbol spec.symbol)
        | Some impl ->
            let schema =
              List.fold_left
                (fun s (attr, dom) -> Schema.add attr dom s)
                Schema.empty spec.schema
            in
            let name = Qualified_name.(parse spec.name |> to_key) in
            let cardinality = convert_cardinality spec.cardinality in
            let generator = Runtime.make_generator name schema impl [] in
            let membership = Runtime.make_membership_criteria schema impl in
            let relation_result =
              Ops.create_immutable_relation storage db ~name ~schema
                ~generator ~membership_criteria:membership ~cardinality
            in
            let db_result = Result.map_error (fun e -> RelationError e) relation_result in
            Result.map
              (fun (new_db, _) ->
                Runtime.bind
                  {
                    Runtime.relation_name = name;
                    symbol = spec.symbol;
                    purity = convert_purity spec.purity;
                    cardinality;
                  };
                (new_db, "Function predicate created: " ^ name))
              db_result)
    | Ast.RetractFunctionPredicate name ->
        let name = Qualified_name.(parse name |> to_key) in
        let relation = Ops.get_relation db ~name in
        let relation = Option.to_result ~none:(RelationNotFound name) relation in
        Result.bind relation (fun _ ->
            let db' = Ops.retract_relation storage db ~name in
            let db' = Result.map_error (fun e -> RelationError e) db' in
            Result.map
              (fun updated_db ->
                Runtime.unbind name;
                (updated_db, "Function predicate retracted: " ^ name))
              db')
    | Ast.ListFunctionPredicates -> Ok (db, list_names_msg ())
end

module Memory = Make (Management.Physical.Memory)
