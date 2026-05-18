module Make (NT : Nt.S) = struct
  type error =
    | ParseError of string
    | NtError of Nt.error
    | RelationNotFound of string
    | MultigroupNotFound of string
    | UnqualifiedName of string

  let sexp_of_error e =
    let open Sexplib.Sexp in
    match e with
    | ParseError s -> List [ Atom "parse-error"; Atom s ]
    | NtError e -> List [ Atom "nt-error"; Atom (Nt.string_of_error e) ]
    | RelationNotFound s -> List [ Atom "relation-not-found"; Atom s ]
    | MultigroupNotFound s -> List [ Atom "multigroup-not-found"; Atom s ]
    | UnqualifiedName s -> List [ Atom "unqualified-name"; Atom s ]

  let ( let* ) = Result.bind
  let wrap_nt r = Result.map_error (fun e -> NtError e) r

  let parse_fqn (s : string) : (Qualified_name.t, error) result =
    Qualified_name.try_parse s |> Result.map_error (fun s -> UnqualifiedName s)

  let lookup_mg (ctx : Sublanguage_context.t) (mg_name : string) :
      (Management.Multigroup.multigroup, error) result =
    match ctx.branch#mg_of mg_name with
    | Some mg -> Ok mg
    | None -> Error (MultigroupNotFound mg_name)

  let convert_cardinality : Ast.cardinality_spec -> Conventions.Cardinality.t =
    function
    | Ast.Finite n -> Conventions.Cardinality.Finite n
    | Ast.AlephZero -> Conventions.Cardinality.AlephZero
    | Ast.Continuum -> Conventions.Cardinality.Continuum
    | Ast.ConstrainedFinite -> Conventions.Cardinality.ConstrainedFinite

  (* Every relation-touching statement names exactly one mg via its FQN
     prefix.  The executor parses the FQN, resolves the mg in the branch
     cache, advances it through NT, and returns a single-element delta. *)
  let execute (ctx : Sublanguage_context.t) (stmt : Ast.statement) :
      (Sublanguage_types.transition_delta * string, error) result =
    let bh = ctx.write_handle in
    let branch_name = ctx.branch#name in
    match stmt with
    | Ast.CreateMultigroup name ->
        ctx.branch#add_multigroup ~name;
        (match ctx.branch#mg_of name with
         | Some mg -> Ok ([ (name, mg) ], "Multigroup created: " ^ name)
         | None    -> Error (MultigroupNotFound name))
    | Ast.CreateRelation { name; schema = schema_pairs } ->
        let* fqn = parse_fqn name in
        let* mg = lookup_mg ctx fqn.mg in
        let schema =
          List.fold_left
            (fun s (attr, dom) -> Schema.add attr dom s)
            Schema.empty schema_pairs
        in
        let* _bh, new_mg =
          NT.create_relation bh mg
            ~branch_name ~mg_name:fqn.mg ~name:fqn.name ~schema
          |> wrap_nt
        in
        ctx.branch#set_mg ~name:fqn.mg new_mg;
        Ok ([ (fqn.mg, new_mg) ], "Relation created: " ^ name)
    | Ast.RetractRelation name ->
        let* fqn = parse_fqn name in
        let* mg = lookup_mg ctx fqn.mg in
        let* _bh, new_mg =
          NT.retract_relation bh mg ~name:fqn.name |> wrap_nt
        in
        ctx.branch#set_mg ~name:fqn.mg new_mg;
        Ok ([ (fqn.mg, new_mg) ], "Relation retracted: " ^ name)
    | Ast.ClearRelation name ->
        let* fqn = parse_fqn name in
        let* mg = lookup_mg ctx fqn.mg in
        (match mg#get_relation fqn.name with
         | None -> Error (RelationNotFound name)
         | Some rel ->
             let* _bh, new_mg =
               NT.clear_relation bh mg
                 ~branch_name ~mg_name:fqn.mg
                 (rel :> Relation.relation) |> wrap_nt
             in
             ctx.branch#set_mg ~name:fqn.mg new_mg;
             Ok ([ (fqn.mg, new_mg) ], "Relation cleared: " ^ name))
    | Ast.RegisterDomain { name; cardinality } ->
        let* fqn = parse_fqn name in
        let* mg = lookup_mg ctx fqn.mg in
        let domain : Relation.domain =
          new Relation.domain ~name:fqn.name
            ~generator:(fun _ -> Generator.Error "not enumerable via DDL")
            ~membership_criteria:(fun _ -> true)
            ~cardinality:(convert_cardinality cardinality)
            ~schema:Schema.empty ~provenance:None ~lineage:None
            ~constraints:None
        in
        let* _bh, new_mg = NT.register_domain bh mg domain |> wrap_nt in
        ctx.branch#set_mg ~name:fqn.mg new_mg;
        Ok ([ (fqn.mg, new_mg) ], "Domain registered: " ^ name)
end
