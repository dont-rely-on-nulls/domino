module Make (NT : Nt.S) = struct
  type error =
    | ParseError of string
    | NtError of Nt.error

  let sexp_of_error e =
    let open Sexplib.Sexp in
    match e with
    | ParseError s -> List [ Atom "parse-error"; Atom s ]
    | NtError e    -> List [ Atom "nt-error"; Atom (Nt.string_of_error e) ]

  let execute (ctx : Sublanguage_context.t) (stmt : Ast.statement) :
      (Sublanguage_types.result, error) result =
    match stmt with
    | Ast.Use branch_name ->
        match ctx.switch_branch branch_name with
        | Ok delta     -> Ok (Sublanguage_types.Transition delta)
        | Error e      -> Error (NtError e)

  let _ = (module NT : Nt.S)
end
