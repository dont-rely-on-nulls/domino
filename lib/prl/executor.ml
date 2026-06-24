module Make (NT : Nt.S) = struct
  module Error = struct
    open Condition

    (* TODO: more structure *)
    let parse_error msg = condition "parse-error" msg empty
    let runtime_error msg = condition "runtime-error" msg empty

    let unknown_plugin_symbol name =
      condition "unknown-plugin-symbol" "Unknown plugin" ("name" |=| of_string name)

    let relation_not_found name =
      condition "relation-not-found" "Relation not found" ("name" |=| of_string name)
  end

  let ( let* ) = Result.bind

  (* TODO: PRL function predicates are OCaml shared-library callbacks that
     produce tuples at runtime.  To make their output joinable inside the
     Tarski VM they need to be materialised into a temporary relation in the
     InMemoryBackend and then SCANned like any other stored relation.  This
     requires a `rnt_register_ephemeral_relation` C API call that does not
     yet exist.  Disable PRL execution until that bridge is in place. *)

  let execute (_ctx : Sublanguage_context.t) (_stmt : Ast.statement) :
      (Sublanguage_types.result, Condition.t) result =
    Error
      (Error.runtime_error
         "PRL execution is temporarily disabled pending VM callback-cursor support" )

  let _execute_load_library _ctx _path = Error (Error.runtime_error "PRL disabled")

  let _execute_define_function_predicate _ctx (_spec : Ast.function_predicate) =
    let* _ = Ok () in
    Error (Error.runtime_error "PRL disabled")

  let _execute_list_function_predicates _ctx = Error (Error.runtime_error "PRL disabled")
end
