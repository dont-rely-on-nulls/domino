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

  let ( let* ) = Result.bind
  let map_rel_error r = Result.map_error (fun e -> RelationError e) r

  let require_relation (db : Management.Database.t) rel_name =
    match Management.Database.get_relation db rel_name with
    | Some rel -> Ok rel
    | None -> Error (RelationNotFound rel_name)

  let execute_load_library storage db path =
    match Runtime.load_library path with
    | Error e -> Error (RuntimeError e)
    | Ok () -> (
        let* ll_rel =
          require_relation db Prelude.Catalog.loaded_library_rel_name
        in
        let tuple = Prelude.Catalog.build_loaded_library_tuple path in
        match Ops.create_tuple storage db ll_rel tuple with
        | Ok (new_db, _, _) -> Ok (Sublanguage_types.Transition new_db)
        | Error (Error.DuplicateTuple _) -> Ok (Sublanguage_types.Transition db)
        | Error e -> Error (RelationError e))

  let execute_define_function_predicate storage db
      (spec : Ast.function_predicate) =
    match Sakura_prl_api.find spec.symbol with
    | None -> Error (UnknownPluginSymbol spec.symbol)
    | Some impl ->
        let schema =
          List.fold_left
            (fun s (a, d) -> Schema.add a d s)
            Schema.empty spec.schema
        in
        let name = Qualified_name.(parse spec.name |> to_key) in
        let generator = Runtime.make_generator name schema impl [] in
        let producer = Runtime.make_producer name schema impl in
        let membership_criteria =
          Runtime.make_membership_criteria schema impl
        in
        let* new_db, _ =
          Ops.create_immutable_relation storage db ~name ~schema ~generator
            ~membership_criteria ~cardinality:spec.cardinality
            ~producer:(Some producer)
          |> map_rel_error
        in
        let* fp_rel =
          require_relation new_db Prelude.Catalog.function_predicate_rel_name
        in
        let fp_tuple =
          Prelude.Catalog.build_function_predicate_tuple ~name
            ~symbol:spec.symbol ~cardinality:spec.cardinality
            ~purity:spec.purity
        in
        let* final_db =
          Ops.create_tuple storage new_db fp_rel fp_tuple
          |> map_rel_error
          |> Result.map (fun (db, _, _) -> db)
        in
        Ok (Sublanguage_types.Transition final_db)

  let execute_list_function_predicates db =
    require_relation db Prelude.Catalog.function_predicate_rel_name
    |> Result.map (fun rel -> Sublanguage_types.Query rel)

  let execute storage db = function
    | Ast.LoadLibrary path -> execute_load_library storage db path
    | Ast.DefineFunctionPredicate spec ->
        execute_define_function_predicate storage db spec
    | Ast.ListFunctionPredicates -> execute_list_function_predicates db
end

module Memory = Make (Management.Physical.Memory)
