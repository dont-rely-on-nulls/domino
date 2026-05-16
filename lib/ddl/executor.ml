module Make (NT : Nt.S) = struct
  type error =
    | ParseError of string
    | NtError of Nt.error
    | RelationNotFound of string

  let sexp_of_error e =
    let open Sexplib.Sexp in
    match e with
    | ParseError s -> List [ Atom "parse-error"; Atom s ]
    | NtError e -> List [ Atom "nt-error"; Atom (Nt.string_of_error e) ]
    | RelationNotFound s -> List [ Atom "relation-not-found"; Atom s ]

  let ( let* ) = Result.bind
  let wrap_nt r = Result.map_error (fun e -> NtError e) r

  let convert_cardinality : Ast.cardinality_spec -> Conventions.Cardinality.t =
    function
    | Ast.Finite n -> Conventions.Cardinality.Finite n
    | Ast.AlephZero -> Conventions.Cardinality.AlephZero
    | Ast.Continuum -> Conventions.Cardinality.Continuum
    | Ast.ConstrainedFinite -> Conventions.Cardinality.ConstrainedFinite

  (* Returns the updated schema_cache (reflecting the new snapshot state) and
     a human-readable message.  The write target branch is ctx.schema_cache#name. *)
  let execute (ctx : Sublanguage_context.t) (stmt : Ast.statement) :
      (Management.Multigroup.multigroup * string, error) result =
    let bh = ctx.write_handle in
    let db = ctx.schema_cache in
    match stmt with
    | Ast.CreateMultigroup name ->
        let* _bh, new_db =
          NT.create_multigroup "" name |> wrap_nt
        in
        Ok (new_db, "Multigroup created: " ^ name)
    | Ast.CreateRelation { name; schema = schema_pairs } ->
        let schema =
          List.fold_left
            (fun s (attr, dom) -> Schema.add attr dom s)
            Schema.empty schema_pairs
        in
        let* _bh, new_db =
          NT.create_relation bh db ~branch_name:db#name ~name ~schema
          |> wrap_nt
        in
        Ok (new_db, "Relation created: " ^ name)
    | Ast.RetractRelation name ->
        let* _bh, new_db =
          NT.retract_relation bh db ~name |> wrap_nt
        in
        Ok (new_db, "Relation retracted: " ^ name)
    | Ast.ClearRelation name -> (
        match NT.get_relation db name with
        | None -> Error (RelationNotFound name)
        | Some rel ->
            let* _bh, new_db =
              NT.clear_relation bh db (rel :> Relation.relation) |> wrap_nt
            in
            Ok (new_db, "Relation cleared: " ^ name))
    | Ast.RegisterDomain { name; cardinality } ->
        let domain : Relation.domain =
          new Relation.domain ~name
            ~generator:(fun _ -> Generator.Error "not enumerable via DDL")
            ~membership_criteria:(fun _ -> true)
            ~cardinality:(convert_cardinality cardinality)
            ~schema:Schema.empty ~provenance:None ~lineage:None
            ~constraints:None
        in
        let* _bh, new_db = NT.register_domain bh db domain |> wrap_nt in
        Ok (new_db, "Domain registered: " ^ name)
end
