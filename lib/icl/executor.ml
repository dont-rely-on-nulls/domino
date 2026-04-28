module Make (Storage : Management.Physical.S) = struct
  module Ops = Manipulation.Make (Storage)

  type error =
    | ParseError of string
    | ManipulationError of Error.t
    | ConversionError of string

  let sexp_of_error e =
    let open Sexplib.Sexp in
    match e with
    | ParseError s -> List [ Atom "parse-error"; Atom s ]
    | ManipulationError e -> Error.sexp_of_error e
    | ConversionError s -> List [ Atom "conversion-error"; Atom s ]

  let wrap_manip = Result.map_error (fun e -> ManipulationError e)

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

  let execute (storage : Storage.t) (db : Management.Database.t)
      (stmt : Ast.statement) : (Management.Database.t * string, error) result =
    match stmt with
    | Ast.RegisterConstraint { constraint_name; relation_name; body } -> (
        let runtime_body = convert_body body in
        match
          Ops.register_constraint storage db ~constraint_name ~relation_name
            ~body:runtime_body
          |> wrap_manip
        with
        | Ok new_db -> Ok (new_db, "Constraint registered: " ^ constraint_name)
        | Error e -> Error e)
end

module Memory = Make (Management.Physical.Memory)
