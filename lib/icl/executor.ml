module Make (NT : Nt.S) = struct
  type error =
    | ParseError of string
    | NtError of Nt.error
    | ConversionError of string
    | MultigroupNotFound of string
    | UnqualifiedName of string

  let sexp_of_error e =
    let open Sexplib.Sexp in
    match e with
    | ParseError s -> List [ Atom "parse-error"; Atom s ]
    | NtError e -> List [ Atom "nt-error"; Atom (Nt.string_of_error e) ]
    | ConversionError s -> List [ Atom "conversion-error"; Atom s ]
    | MultigroupNotFound s -> List [ Atom "multigroup-not-found"; Atom s ]
    | UnqualifiedName s -> List [ Atom "unqualified-name"; Atom s ]

  let ( let* ) = Result.bind
  let wrap_nt = Result.map_error (fun e -> NtError e)

  let parse_fqn (s : string) : (Qualified_name.t, error) result =
    Qualified_name.try_parse s |> Result.map_error (fun s -> UnqualifiedName s)

  let lookup_mg (ctx : Sublanguage_context.t) (mg_name : string) :
      (Management.Multigroup.multigroup, error) result =
    match ctx.branch#mg_of mg_name with
    | Some mg -> Ok mg
    | None -> Error (MultigroupNotFound mg_name)

  let convert_binding_expr : Ast.binding_expr -> Constraint.binding_expr =
    function
    | Ast.Var name -> Constraint.Var name
    | Ast.Const value -> Constraint.Const (Drl.Ast.value_to_abstract value)

  let convert_binding (pairs : (string * Ast.binding_expr) list) :
      Constraint.binding =
    List.fold_left
      (fun acc (key, expr) ->
        Constraint.BindingMap.add key (convert_binding_expr expr) acc)
      Constraint.BindingMap.empty pairs

  let rec convert_body : Ast.constraint_body -> Constraint.t = function
    | Ast.MemberOf { target; binding } ->
        Constraint.member_of ~target ~binding:(convert_binding binding)
    | Ast.Not { body; universe } ->
        Constraint.not_ ~universe (convert_body body)
    | Ast.And bodies -> Constraint.and_ (List.map convert_body bodies)
    | Ast.Or bodies -> Constraint.or_ (List.map convert_body bodies)
    | Ast.Exists { variable; quantifier; body } ->
        Constraint.exists ~variable ~quantifier (convert_body body)
    | Ast.Forall { variable; quantifier; body } ->
        Constraint.forall ~variable ~quantifier (convert_body body)

  let execute (ctx : Sublanguage_context.t) (stmt : Ast.statement) :
      (Sublanguage_types.transition_delta * string, error) result =
    match stmt with
    | Ast.RegisterConstraint { constraint_name; relation_name; body } ->
        let* fqn = parse_fqn relation_name in
        let* mg  = lookup_mg ctx fqn.mg in
        let runtime_body = convert_body body in
        let* _bh, new_mg =
          NT.register_constraint ctx.write_handle mg
            ~constraint_name ~relation_name:fqn.name ~body:runtime_body
          |> wrap_nt
        in
        ctx.branch#set_mg ~name:fqn.mg new_mg;
        Ok ([ (fqn.mg, new_mg) ], "Constraint registered: " ^ constraint_name)
end

module Memory = Make (Nt.Memory)
