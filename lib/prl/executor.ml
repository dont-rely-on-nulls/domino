module Make (NT : Nt.S) = struct
  type error =
    | ParseError of string
    | RuntimeError of string
    | NtError of Nt.error
    | UnknownPluginSymbol of string
    | RelationNotFound of string

  let sexp_of_error e =
    let open Sexplib.Sexp in
    match e with
    | ParseError s -> List [ Atom "parse-error"; Atom s ]
    | RuntimeError s -> List [ Atom "runtime-error"; Atom s ]
    | NtError e -> List [ Atom "nt-error"; Atom (Nt.string_of_error e) ]
    | UnknownPluginSymbol s -> List [ Atom "unknown-plugin-symbol"; Atom s ]
    | RelationNotFound s -> List [ Atom "relation-not-found"; Atom s ]

  let ( let* ) = Result.bind
  let map_nt r = Result.map_error (fun e -> NtError e) r

  let require_relation (db : Management.Multigroup.multigroup) rel_name =
    match NT.get_relation db rel_name with
    | Some rel -> Ok rel
    | None -> Error (RelationNotFound rel_name)

  let execute_load_library _bh db path =
    match Runtime.load_library path with
    | Error e -> Error (RuntimeError e)
    | Ok () -> (
        let* ll_rel =
          require_relation db Prelude.Catalog.loaded_library_rel_name
        in
        let tuple = Prelude.Catalog.build_loaded_library_tuple path in
        match
          NT.create_tuple ~branch_name:db#name
            ~rel_name:ll_rel#name tuple
        with
        | Ok _ -> Ok (Sublanguage_types.Transition db)
        | Error (Nt.NotSupported _) -> Ok (Sublanguage_types.Transition db)
        | Error e -> Error (NtError e))

  let execute_define_function_predicate bh db
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
        let* _bh, new_db, _ =
          NT.create_immutable_relation bh db ~name ~schema ~generator
            ~membership_criteria ~cardinality:spec.cardinality
            ~producer:(Some producer)
          |> map_nt
        in
        let* fp_rel =
          require_relation new_db Prelude.Catalog.function_predicate_rel_name
        in
        let fp_tuple =
          Prelude.Catalog.build_function_predicate_tuple ~name
            ~symbol:spec.symbol ~cardinality:spec.cardinality
            ~purity:spec.purity
        in
        let* _ =
          NT.create_tuple ~branch_name:new_db#name ~rel_name:fp_rel#name
            fp_tuple
          |> map_nt
        in
        Ok (Sublanguage_types.Transition new_db)

  let execute_list_function_predicates db =
    require_relation db Prelude.Catalog.function_predicate_rel_name
    |> Result.map (fun rel -> Sublanguage_types.Query (rel :> Relation.relation))

  let execute bh db = function
    | Ast.LoadLibrary path -> execute_load_library bh db path
    | Ast.DefineFunctionPredicate spec ->
        execute_define_function_predicate bh db spec
    | Ast.ListFunctionPredicates -> execute_list_function_predicates db
end
