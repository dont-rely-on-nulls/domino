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

  let list_names_msg () =
    let rec generator (position : int option) : Generator.result =
      let relation_data =
        Runtime.list_bindings ()
        |> Array.of_list
      in
      match position with
      | Some n when n <= Array.length relation_data ->
          let elem = relation_data.(n) in
          let open Prelude.Standard in
          let attributes =
            Tuple.AttributeMap.of_list [ ("name",   mk elem.relation_name); 
                                         ("cardinality",  mk elem.cardinality);
                                         ("symbol", mk elem.symbol);
                                         ("purity", mk elem.purity); ]
          in
          Generator.Value
            ( Tuple.make_materialized ~relation:"less_than_or_equal" ~attributes,
              generator )
      | Some _ -> Generator.Done
      | None -> Error "Cannot enumerate less_than_or_equal randomly."
    in
    let name = "function_predicate_binding" in
    let schema =
      Schema.empty
      |> Schema.add "name" "string"
      |> Schema.add "cardinality" "string"
      |> Schema.add "symbol" "string"
      |> Schema.add "purity" "string"      
    in
    (* TODO: To determine the actual finite cardinality here, 
       we gotta do a DB dirty read, which is just not worth it right now. *)
    let cardinality = Conventions.Cardinality.ConstrainedFinite in
    (* :  *)
    let membership_criteria _: Tuple.t -> bool = function
      | Tuple.Materialized {attributes; _} -> begin
        (* TODO: Convert the runtime to be a list of string and values,
           or add a hash to each binding with the contents of each elem.
           The hash would be a simple operation and it's much more efficient. *)
        (List.find_opt (fun (bindings: Runtime.binding) ->
          let attr = (Tuple.AttributeMap.bindings attributes) in
          List.for_all (function (attr_name, ({value;}: Attribute.materialized)) -> 
            if attr_name = "cardinality" then Obj.magic (Conventions.Cardinality.show bindings.cardinality) = value
            else if attr_name = "name" then Obj.magic bindings.relation_name = value
            else if attr_name = "symbol" then Obj.magic bindings.symbol = value
            else if attr_name = "purity" then Obj.magic bindings.purity = value
            else false) attr
        ) @@ Runtime.list_bindings ())
        |> function Some _ -> true | None -> false
      end
      | Tuple.NonMaterialized _ -> false
    in
    Relation.make ~hash:None ~name ~schema ~tree:None ~constraints:None
    ~cardinality ~generator:(Some generator) ~membership_criteria
    ~provenance:(Relation.Provenance.Base name)
    ~lineage:(Relation.Lineage.Base name)

  type externality =
  | Relation of Relation.t
  | LibraryLoad

  let execute (storage : Storage.t) (db : Management.Database.t)
      (stmt : Ast.statement) : (externality, error) result =
    match stmt with
    | Ast.LoadLibrary path -> begin
        match Runtime.load_library path with
        | Ok () -> Ok LibraryLoad
        | Error e -> Error (RuntimeError e)
    end
    | Ast.DefineFunctionPredicate spec -> begin
        match Plugin_api.find spec.symbol with
        | None -> Error (UnknownPluginSymbol spec.symbol)
        | Some impl ->
            let schema =
              List.fold_left
                (fun s (attr, dom) -> Schema.add attr dom s)
                Schema.empty spec.schema
            in
            let name = Qualified_name.(parse spec.name |> to_key) in
            let cardinality = spec.cardinality in
            let generator = Runtime.make_generator name schema impl [] in
            let membership = Runtime.make_membership_criteria schema impl in
            Result.map
              (fun (_, rel) -> Relation rel) 
              (Ops.create_immutable_relation storage db ~name ~schema ~generator ~membership_criteria:membership ~cardinality)
            |> Result.map_error (fun e -> RelationError e)
            end
    | Ast.ListFunctionPredicates -> begin
      
      Ok LibraryLoad
    end
end

module Memory = Make (Management.Physical.Memory)
