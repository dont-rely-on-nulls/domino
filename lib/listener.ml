module type LISTENER = functor
  (T : Transport.TRANSPORT)
  (S : Management.Physical.S with type error = string)
  -> sig
  val run : T.t -> Catalog.t -> S.t -> unit
end

type connection_context = { mutable current_multigroup : string }

module Make : LISTENER =
functor
  (T : Transport.TRANSPORT)
  (S : Management.Physical.S with type error = string)
  ->
  struct
    module type SubS = Sublanguage.S with type storage = S.t

    module BranchOps = Management.Branch.Make (S)
    module AlgebraOps = Algebra.Make (S)
    module CatalogOps = Catalog.Make (S)

    let read_command input =
      try Ok (Sexplib.Sexp.input_sexp input)
      with Sexplib.Sexp.Parse_error { err_msg; _ } ->
        Error (Error.SyntaxError err_msg)

    let sublanguages =
      List.fold_right
        (fun (module Language : SubS) ->
          Utilities.StringMap.add Language.name (module Language : SubS))
        [
          (module Drl.Sublanguage.Make (S) : SubS);
          (module Ddl.Sublanguage.Make (S) : SubS);
          (module Dml.Sublanguage.Make (S) : SubS);
          (module Icl.Sublanguage.Make (S) : SubS);
          (module Dcl.Sublanguage.Make (S) : SubS);
          (module Prl.Sublanguage.Make (S) : SubS);
          (module Scl.Sublanguage.Make (S) : SubS);
        ]
        Utilities.StringMap.empty

    let fmap f m = Result.bind m f

    let find_language tag =
      Utilities.StringMap.find_opt tag sublanguages
      |> Option.to_result ~none:(Error.UnrecognizedSublanguage tag)

    let execute storage db expr (module Language : SubS) =
      Language.parse_sexp expr
      |> fmap (Language.execute storage db)
      |> Result.map_error (fun e ->
          Error.SublanguageError (Language.sexp_of_error e))

    let execute_command storage db = function
      | Sexplib.Sexp.(List [ Atom tag; expr ]) ->
          find_language tag |> fmap (execute storage db expr)
      | s -> Error (Error.MalformedExpression s)

    let advance_head_branch storage new_hash =
      match BranchOps.get_head storage with
      | Ok (Some branch_name) ->
          ignore (BranchOps.update_tip storage ~name:branch_name ~tip:new_hash)
      | _ -> ()

    let perform storage catalog ctx db_head old_db result =
      match result with
      | Sublanguage.Transition (new_db, _) ->
          if Atomic.compare_and_set db_head old_db new_db then (
            advance_head_branch storage new_db.hash;
            Ok result)
          else Error (Error.Conflict { old_db; new_db })
      | Sublanguage.SessionSwitch multigroup -> (
          match CatalogOps.get catalog multigroup with
          | None -> Error (Error.MultigroupNotFound multigroup)
          | Some _ ->
              ctx.current_multigroup <- multigroup;
              Ok result)
      | Sublanguage.CreateMultigroup name -> (
          match
            CatalogOps.add catalog storage
              ~prelude_relations:Prelude.Standard.prelude_relations name
          with
          | Error e -> Error (Error.SyntaxError e)
          | Ok _ -> Ok result)
      | _ -> Ok result

    let get_branch storage =
      match BranchOps.get_head storage with Ok (Some name) -> name | _ -> "--"

    let current_limit = 16 (* TODO: make the limit per-connection? *)

    let materialize_generator gen limit =
      let rec go gen pos acc count =
        if count >= limit then (List.rev acc, true)
        else
          match gen (Some pos) with
          | Generator.Done -> (List.rev acc, false)
          | Generator.Error _ -> (List.rev acc, false)
          | Generator.Value (t, next) -> (
              let mat =
                match t with
                | Tuple.Materialized m -> Some m
                | Tuple.NonMaterialized _ -> None
              in
              match mat with
              | None -> go next (pos + 1) acc count
              | Some m -> go next (pos + 1) (m :: acc) (count + 1))
      in
      go gen 0 [] 0

    let materialize_relation storage (rel : Relation.t) limit =
      let gen = AlgebraOps.to_generator storage rel in
      materialize_generator gen limit

    let tuple_to_sexp (t : Tuple.materialized) =
      let open Sexplib.Sexp in
      List
        (Tuple.AttributeMap.bindings t.Tuple.attributes
        |> List.map (fun (k, attr) ->
            List
              [
                Atom k; Conventions.AbstractValue.sexp_of_t attr.Attribute.value;
              ]))

    (* TODO: does our network protocol make sense? *)
    let serialize storage (db : Management.Database.t) =
      let open Sexplib.Sexp in
      function
      | Error e -> List [ Atom "error"; Error.sexp_of_error e ]
      (* TODO: forsake the global cursor register in favour of per-connection bookkeeping *)
      | Ok (Sublanguage.Cursor { cursor_id; rows; has_more }) ->
          let row_sexps = List.map tuple_to_sexp rows in
          let rows_sexp = List row_sexps in
          List
            [
              Atom "cursor";
              List [ Atom "id"; Atom cursor_id ];
              List [ Atom "rows"; rows_sexp ];
              List
                [
                  Atom "row_count"; Atom (string_of_int (List.length row_sexps));
                ];
              List [ Atom "has_more"; Atom (string_of_bool has_more) ];
              List [ Atom "db_hash"; Atom db.hash ];
              List [ Atom "db_name"; Atom db.name ];
              List [ Atom "branch"; Atom (get_branch storage) ];
            ]
      | Ok (Sublanguage.Query rel) ->
          let tuples, truncated =
            materialize_relation storage rel current_limit
          in
          let schema_sexp =
            List
              (List.map
                 (fun (a, d) -> List [ Atom a; Atom d ])
                 rel.Relation.schema)
          in
          let rows_sexp = List (List.map tuple_to_sexp tuples) in
          List
            [
              Atom "relation";
              List [ Atom "name"; Atom rel.Relation.name ];
              List [ Atom "schema"; schema_sexp ];
              List [ Atom "rows"; rows_sexp ];
              List
                [ Atom "row_count"; Atom (string_of_int (List.length tuples)) ];
              List [ Atom "truncated"; Atom (string_of_bool truncated) ];
              List [ Atom "db_hash"; Atom db.hash ];
              List [ Atom "db_name"; Atom db.name ];
              List [ Atom "branch"; Atom (get_branch storage) ];
            ]
      | Ok (Sublanguage.Transition (new_db, message)) ->
          List
            [
              Atom "ok";
              List [ Atom "message"; Atom message ];
              List [ Atom "db_hash"; Atom new_db.hash ];
              List [ Atom "db_name"; Atom new_db.name ];
              (* TODO: `serialize' shouldn't need access to the storage layer *)
              List [ Atom "branch"; Atom (get_branch storage) ];
            ]
      | Ok (Sublanguage.SessionSwitch multigroup) ->
          List
            [
              Atom "ok";
              List [ Atom "message"; Atom ("Switched to multigroup " ^ multigroup) ];
            ]
      | Ok (Sublanguage.CreateMultigroup name) ->
          List
            [
              Atom "ok";
              List [ Atom "message"; Atom ("Multigroup " ^ name ^ " created") ];
            ]

    let print_with_time str =
      (* TODO: a proper logger *)
      let now = Unix.gettimeofday () in
      let tm = Unix.localtime now in
      let orange = "\027[38;5;208m" in
      let reset = "\027[0m" in
      let formatted_time =
        Printf.sprintf "%04d-%02d-%02d %02d:%02d:%02d" (tm.Unix.tm_year + 1900)
          (tm.Unix.tm_mon + 1) tm.Unix.tm_mday tm.Unix.tm_hour tm.Unix.tm_min
          tm.Unix.tm_sec
      in
      print_endline
      @@ Printf.sprintf "%s[%s]%s %s" orange formatted_time reset str

    let output_response out_ch sexp =
      let response = Sexplib.Sexp.to_string sexp in
      output_string out_ch response;
      output_string out_ch "\n";
      flush out_ch;
      print_with_time response

    let resolve_db_ref catalog ctx =
      match CatalogOps.get catalog ctx.current_multigroup with
      | Some db_ref -> Ok db_ref
      | None -> Error (Error.MultigroupNotFound ctx.current_multigroup)

    let fallback_db ctx = Management.Database.empty ~name:ctx.current_multigroup

    let send_serialized storage out_ch db result =
      serialize storage db result |> output_response out_ch

    let handle_sublanguage storage catalog ctx out_ch sexp =
      match resolve_db_ref catalog ctx with
      | Error e -> send_serialized storage out_ch (fallback_db ctx) (Error e)
      | Ok db_ref ->
          let db = Atomic.get db_ref in
          Ok sexp
          |> fmap (execute_command storage db)
          |> fmap (perform storage catalog ctx db_ref db)
          |> send_serialized storage out_ch db

    let handle_client connection catalog (storage : S.t) =
      let input = T.input connection in
      let output = T.output connection in
      let ctx = { current_multigroup = catalog.Catalog.default_multigroup } in
      try
        while true do
          match read_command input with
          | Error e ->
              send_serialized storage output (fallback_db ctx) (Error e)
          | Ok sexp -> handle_sublanguage storage catalog ctx output sexp
        done
      with
      | End_of_file -> ()
      | e ->
          Printf.eprintf "Error handling connection: %s" (Printexc.to_string e)

    let spawn_handler catalog storage connection =
      Stdlib.Domain.spawn (fun () -> handle_client connection catalog storage)

    let run transport catalog storage =
      T.listen transport;
      while true do
        T.accept transport |> spawn_handler catalog storage |> ignore
      done
  end
