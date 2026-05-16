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
  let _map_nt r = Result.map_error (fun e -> NtError e) r

  (* TODO: PRL function predicates are OCaml shared-library callbacks that
     produce tuples at runtime.  To make their output joinable inside the
     Tarski VM they need to be materialised into a temporary relation in the
     InMemoryBackend and then SCANned like any other stored relation.  This
     requires a `rnt_register_ephemeral_relation` C API call that does not
     yet exist.  Disable PRL execution until that bridge is in place. *)

  let execute (_ctx : Sublanguage_context.t) (_stmt : Ast.statement) :
      (Sublanguage_types.result, error) result =
    Error (RuntimeError
      "PRL execution is temporarily disabled pending VM callback-cursor support")

  let _execute_load_library _ctx _path =
    Error (RuntimeError "PRL disabled")

  let _execute_define_function_predicate _ctx (_spec : Ast.function_predicate) =
    let* _ = Ok () in
    Error (RuntimeError "PRL disabled")

  let _execute_list_function_predicates _ctx =
    Error (RuntimeError "PRL disabled")
end
