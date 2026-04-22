module type CONFIGURABLE = sig
  type configuration

  val parse : Sexplib.Sexp.t -> (configuration, string) result
end

type t = Sexplib.Sexp.t Utilities.StringMap.t
(** Section name -> raw sexp subtree. *)

let find_section (key : string) (config : t) : Sexplib.Sexp.t option =
  Utilities.StringMap.find_opt key config

let errorf fmt = Printf.ksprintf (fun s -> Error s) fmt

(** Check that [key] is expected and not duplicate, then add it. *)
let insert_section ~expected acc key body =
  let ( let* ) = Result.bind in
  let* () =
    if Utilities.StringSet.mem key expected then Ok ()
    else errorf "Unknown configuration section: %s" key
  in
  let* () =
    if Utilities.StringMap.mem key acc then
      errorf "Duplicate configuration section: %s" key
    else Ok ()
  in
  match body with
  | [ subtree ] -> Ok (Utilities.StringMap.add key subtree acc)
  | [] -> errorf "Configuration section %s is empty (expected a tagged value)" key
  | _ ->
      errorf "Configuration section %s has multiple values (expected exactly one)"
        key

(** [(server ...)] sexp -> section map. Rejects unknown/duplicate keys. *)
let parse_server ~expected_keys (sexp : Sexplib.Sexp.t) : (t, string) result =
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
            errorf "Malformed configuration section: %s" (to_string bad)
      in
      go Utilities.StringMap.empty sections
  | _ -> Error "Configuration must be a (server ...) s-expression"

(** Read a file from disk and run [parse_server] on it. *)
let load ~expected_keys (path : string) : (t, string) result =
  match Sexplib.Sexp.load_sexp path with
  | sexp -> parse_server ~expected_keys sexp
  | exception Sys_error msg ->
      errorf "Failed to load configuration file %s: %s" path msg
  | exception Failure msg ->
      errorf "Failed to parse configuration file %s: %s" path msg

(** [(tag field1 ...)] -> [(tag, List [field1; ...])]. *)
let extract_tagged_section (sexp : Sexplib.Sexp.t) :
    (string * Sexplib.Sexp.t, string) result =
  let open Sexplib.Sexp in
  match sexp with
  | List (Atom tag :: body) -> Ok (tag, List body)
  | _ -> errorf "Expected (tag ...) but got: %s" (to_string sexp)

(** Parse a list of names from a sexp like [((name1) (name2) ...)].
    Returns the list of name strings. *)
let parse_name_list (sexp : Sexplib.Sexp.t) : (string list, string) result =
  let open Sexplib.Sexp in
  match sexp with
  | List items ->
      let rec go acc = function
        | [] -> Ok (List.rev acc)
        | List [ Atom name ] :: rest -> go (name :: acc) rest
        | Atom name :: rest -> go (name :: acc) rest
        | bad :: _ -> errorf "Expected (name) but got: %s" (to_string bad)
      in
      go [] items
  | Atom name -> Ok [ name ]

(** Look up a section by name, extract its tag, and check the tag is allowed. *)
let require_section ~(name : string) ~(valid_tags : string list) (config : t) :
    (string * Sexplib.Sexp.t, string) result =
  let ( let* ) = Result.bind in
  let* sexp =
    find_section name config
    |> Option.to_result
         ~none:(Printf.sprintf "Missing (%s ...) section in configuration" name)
  in
  let* tag, body = extract_tagged_section sexp in
  let* () =
    if List.mem tag valid_tags then Ok () else errorf "Unknown %s: %s" name tag
  in
  Ok (tag, body)
