module type CONFIGURABLE = sig
  type configuration

  val parse : Sexplib.Sexp.t -> (configuration, Condition.t) result
end

module Error = struct
  open Condition
  let unknown_section key = condition "unknown-section" ("key" |=| (of_string key))
  let duplicate_section key = condition "duplicate-section" ("key" |=| (of_string key))
  let empty_section key = condition "empty-section" ("key" |=| (of_string key))
  let multiple_values_in_section key = condition "multiple-values-in-section" ("key" |=| (of_string key))
  let invalid_toplevel toplevel = condition "invalid-toplevel" ("expression" |=| (of_sexp toplevel))
  let malformed_section expr = condition "malformed-section" ("expression" |=| (of_sexp expr))
  let missing_section name = condition "missing-section" ("name" |=| (of_string name))
  let invalid_tag section tag valid_options = condition "invalid-tag"
                                                ("section" |=| (of_string section) &
                                                 "tag" |=| (of_string tag) &
                                                 "valid-options" |=| (of_list of_string valid_options))
  let failed_to_load_file path msg = condition "failed-to-load-file"
                                       ("path" |=| (of_string path) &
                                        "message" |=| (of_string msg))
  let syntax_error path msg = condition "syntax-error"
                                ("path" |=| (of_string path) &
                                 "message" |=| (of_string msg))
end

type t = Sexplib.Sexp.t Utilities.StringMap.t
(** Section name -> raw sexp subtree. *)

let find_section (key : string) (config : t) : Sexplib.Sexp.t option =
  Utilities.StringMap.find_opt key config

(** Check that [key] is expected and not duplicate, then add it. *)
let insert_section ~expected acc key body =
  let ( let* ) = Result.bind in
  let* () =
    if Utilities.StringSet.mem key expected then Ok ()
    else Error (Error.unknown_section key)
  in
  let* () =
    if Utilities.StringMap.mem key acc then
      Error (Error.duplicate_section key)
    else Ok ()
  in
  match body with
  | [ subtree ] -> Ok (Utilities.StringMap.add key subtree acc)
  | [] ->
     Error (Error.empty_section key)
  | _ ->
     Error (Error.multiple_values_in_section key)

(** [(server ...)] sexp -> section map. Rejects unknown/duplicate keys. *)
let parse_server ~expected_keys (sexp : Sexplib.Sexp.t) : (t, Condition.t) result =
  let open Sexplib.Sexp in
  match sexp with
  | List (Atom "server" :: sections) ->
      let expected =
        List.fold_right Utilities.StringSet.add expected_keys
          Utilities.StringSet.empty
      in
      let rec go acc = function
        | [] -> Ok acc
        | List (Atom key :: body) :: rest ->
            Result.bind (insert_section ~expected acc key body) (fun acc ->
                go acc rest)
        | bad :: _ ->
            Error (Error.malformed_section bad)
      in
      go Utilities.StringMap.empty sections
  | _ -> Error (Error.invalid_toplevel sexp)

(** Read a file from disk and run [parse_server] on it. *)
let load ~expected_keys (path : string) : (t, Condition.t) result =
  match Sexplib.Sexp.load_sexp path with
  | sexp -> parse_server ~expected_keys sexp
  | exception Sys_error msg ->
     Error (Error.failed_to_load_file path msg)
  | exception Failure msg ->
     Error (Error.syntax_error path msg)

(** [(tag field1 ...)] -> [(tag, List [field1; ...])]. *)
let extract_tagged_section (sexp : Sexplib.Sexp.t) :
    (string * Sexplib.Sexp.t, Condition.t) result =
  let open Sexplib.Sexp in
  match sexp with
  | List (Atom tag :: body) -> Ok (tag, List body)
  | _ -> Error (Error.malformed_section sexp)

(** Look up a section by name, extract its tag, and check the tag is allowed. *)
let require_section ~(name : string) ~(valid_tags : string list) (config : t) :
    (string * Sexplib.Sexp.t, Condition.t) result =
  let ( let* ) = Result.bind in
  let* sexp =
    find_section name config
    |> Option.to_result ~none:(Error.missing_section name)
  in
  let* tag, body = extract_tagged_section sexp in
  let* () =
    if List.mem tag valid_tags then Ok () else Error (Error.invalid_tag name tag valid_tags)
  in
  Ok (tag, body)
