module Make =
functor
  (T : Transport.TRANSPORT)
  (NT : Nt.S)
  ->
  struct
    module type SubS = Sublanguage.S

    (* Per-connection state: always active from the moment the client connects.
       Authentication and the default "master" branch are opened automatically. *)
    type conn_state = {
      claims        : Nt.claims;
      branch_handle : Nt.branch_handle;
      multigroup    : Management.Multigroup.multigroup;
    }

    let read_command input =
      try Ok (Sexplib.Sexp.input_sexp input)
      with Sexplib.Sexp.Parse_error { err_msg; _ } ->
        Error (Error.SyntaxError err_msg)

    let sublanguages =
      List.fold_right
        (fun (module Language : SubS) ->
          Utilities.StringMap.add Language.name (module Language : SubS))
        [
          (module Drl.Sublanguage.Make (NT) : SubS);
          (module Ddl.Sublanguage.Make (NT) : SubS);
          (module Dml.Sublanguage.Make (NT) : SubS);
          (module Icl.Sublanguage.Make (NT) : SubS);
          (module Prl.Sublanguage.Make (NT) : SubS);
          (module Scl.Sublanguage.Make (NT) : SubS);
          (module Vcl.Sublanguage.Make (NT) : SubS);
        ]
        Utilities.StringMap.empty

    let fmap f m = Result.bind m f

    let find_language tag =
      Utilities.StringMap.find_opt tag sublanguages
      |> Option.to_result ~none:(Error.UnrecognizedSublanguage tag)

    let execute_sublanguage bh db expr (module Language : SubS) =
      Language.parse_sexp expr
      |> fmap (Language.execute bh db)
      |> Result.map_error (fun e ->
          Error.SublanguageError (Language.sexp_of_error e))

    let execute_command bh db = function
      | Sexplib.Sexp.(List [ Atom tag; expr ]) ->
          find_language tag |> fmap (execute_sublanguage bh db expr)
      | s -> Error (Error.MalformedExpression s)

    let perform handle db result =
      match result with
      | Sublanguage_types.Transition new_db -> Ok (handle, new_db, result)
      | _ -> Ok (handle, db, result)

    let tuple_to_sexp (t : Tuple.materialized) =
      let open Sexplib.Sexp in
      List
        (Tuple.AttributeMap.bindings t.Tuple.attributes
        |> List.map (fun (k, attr) ->
            List
              [
                Atom k; Conventions.AbstractValue.sexp_of_t attr.Attribute.value;
              ]))

    let print_with_time str =
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

    let send_error out_ch e =
      output_response out_ch
        Sexplib.Sexp.(List [ Atom "error"; Error.sexp_of_error e ])

    let send_ok_message out_ch msg =
      output_response out_ch
        Sexplib.Sexp.(List [ Atom "ok"; List [ Atom "message"; Atom msg ] ])

    let serialize (db : Management.Multigroup.multigroup) =
      let open Sexplib.Sexp in
      function
      | Error e -> List [ Atom "error"; Error.sexp_of_error e ]
      | Ok (Sublanguage_types.Cursor { cursor_id; rows; has_more }) ->
          let row_sexps = List.map tuple_to_sexp rows in
          let rows_sexp = List row_sexps in
          List
            [
              Atom "cursor";
              List [ Atom "id"; Atom cursor_id ];
              List [ Atom "rows"; rows_sexp ];
              List [ Atom "row_count"; Atom (string_of_int (List.length row_sexps)) ];
              List [ Atom "has_more"; Atom (string_of_bool has_more) ];
              List [ Atom "db_hash"; Atom db#hash ];
              List [ Atom "db_name"; Atom db#name ];
            ]
      | Ok (Sublanguage_types.Query _rel) ->
          ignore db;
          List [ Atom "error"; Atom "unexpected Query result: use Cursor path" ]
      | Ok (Sublanguage_types.Transition new_db) ->
          List
            [
              Atom "ok";
              List [ Atom "db_hash"; Atom new_db#hash ];
              List [ Atom "db_name"; Atom new_db#name ];
            ]
      | Ok (Sublanguage_types.SessionSwitch multigroup) ->
          List
            [
              Atom "ok";
              List [ Atom "message"; Atom ("Switched to multigroup " ^ multigroup) ];
            ]
      | Ok (Sublanguage_types.CreateMultigroup name) ->
          List
            [
              Atom "ok";
              List [ Atom "message"; Atom ("Multigroup " ^ name ^ " created") ];
            ]

    (* Close the current branch and open a new one by name.
       On failure to open the target, re-opens "master" as a fallback to keep
       the connection alive. *)
    let switch_branch output state name =
      let { claims; branch_handle = old_handle; _ } = !state in
      match NT.close_branch old_handle with
      | Error e ->
          send_error output (Error.SyntaxError (Nt.string_of_error e))
      | Ok () ->
          (match NT.open_branch claims name with
           | Error e ->
               (match NT.open_branch claims "master" with
                | Ok (bh, mg) ->
                    state := { !state with branch_handle = bh; multigroup = mg }
                | Error _ -> ());
               send_error output (Error.SyntaxError (Nt.string_of_error e))
           | Ok (bh, mg) ->
               state := { claims; branch_handle = bh; multigroup = mg };
               send_ok_message output ("using branch: " ^ name))

    let handle_sublanguage output state sexp =
      let handle = !state.branch_handle in
      let db = !state.multigroup in
      match
        Ok sexp
        |> fmap (execute_command handle db)
        |> fmap (perform handle db)
      with
      | Error e -> send_error output e
      | Ok (_, _, Sublanguage_types.SessionSwitch name) ->
          switch_branch output state name
      | Ok (new_handle, new_db, r) ->
          state := { !state with branch_handle = new_handle; multigroup = new_db };
          serialize new_db (Ok r) |> output_response output

    let handle_client connection =
      let input = T.input connection in
      let output = T.output connection in
      let claims =
        match NT.authenticate Nt.PlainText with
        | Ok c -> c
        | Error e ->
            Printf.eprintf "Auth failed: %s\n%!" (Nt.string_of_error e); ""
      in
      (match NT.open_branch claims "master" with
       | Error e ->
           send_error output (Error.SyntaxError (Nt.string_of_error e))
       | Ok (bh, mg) ->
           let state = ref { claims; branch_handle = bh; multigroup = mg } in
           (try
             while true do
               match read_command input with
               | Error e -> send_error output e
               | Ok sexp -> handle_sublanguage output state sexp
             done
           with
           | End_of_file -> ignore (NT.close_branch !state.branch_handle)
           | e ->
               ignore (NT.close_branch !state.branch_handle);
               Printf.eprintf "Error handling connection: %s" (Printexc.to_string e)))

    let spawn_handler connection =
      Stdlib.Domain.spawn (fun () -> handle_client connection)

    let run transport =
      T.listen transport;
      while true do
        T.accept transport |> spawn_handler |> ignore
      done
  end
