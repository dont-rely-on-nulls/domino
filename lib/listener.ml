module type LISTENER = functor
  (T : Transport.TRANSPORT)
  (S : Management.Physical.S with type error = string)
  -> sig
  val run : T.t -> S.t -> unit
end

module Make : LISTENER =
functor
  (T : Transport.TRANSPORT)
  (S : Management.Physical.S with type error = string)
  ->
  struct
    module type SubS = Sublanguage.S with type storage = S.t

    module AlgebraOps = Algebra.Make (S)

    (* Per-connection state: the client must issue (use "name") before
       any sublanguage command is accepted. *)
    type conn_state =
      | NoMultigroup
      | Active of Nt.handle * Management.Multigroup.multigroup

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
          (* (module Dcl.Sublanguage.Make (S) : SubS); *)
          (module Prl.Sublanguage.Make (S) : SubS);
          (module Scl.Sublanguage.Make (S) : SubS);
        ]
        Utilities.StringMap.empty

    let fmap f m = Result.bind m f

    let find_language tag =
      Utilities.StringMap.find_opt tag sublanguages
      |> Option.to_result ~none:(Error.UnrecognizedSublanguage tag)

    let execute_sublanguage storage db expr (module Language : SubS) =
      Language.parse_sexp expr
      |> fmap (Language.execute storage db)
      |> Result.map_error (fun e ->
          Error.SublanguageError (Language.sexp_of_error e))

    let execute_command storage db = function
      | Sexplib.Sexp.(List [ Atom tag; expr ]) ->
          find_language tag |> fmap (execute_sublanguage storage db expr)
      | s -> Error (Error.MalformedExpression s)

    (* Thread handle and db through the connection after a sublanguage result. *)
    let perform handle db result =
      match result with
      | Sublanguage_types.Transition new_db -> Ok (handle, new_db, result)
      | Sublanguage_types.SessionSwitch _ ->
          Error (Error.SyntaxError "SessionSwitch not supported")
      | Sublanguage_types.CreateMultigroup _ ->
          Error (Error.SyntaxError "CreateMultigroup not supported")
      | _ -> Ok (handle, db, result)

    let current_limit = 16

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

    let materialize_relation storage (rel : Relation.relation) limit =
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

    let send_error out_ch e =
      output_response out_ch
        Sexplib.Sexp.(List [ Atom "error"; Error.sexp_of_error e ])

    let send_ok_message out_ch msg =
      output_response out_ch
        Sexplib.Sexp.(List [ Atom "ok"; List [ Atom "message"; Atom msg ] ])

    let serialize storage (db : Management.Multigroup.multigroup) =
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
              List
                [
                  Atom "row_count"; Atom (string_of_int (List.length row_sexps));
                ];
              List [ Atom "has_more"; Atom (string_of_bool has_more) ];
              List [ Atom "db_hash"; Atom db#hash ];
              List [ Atom "db_name"; Atom db#name ];
              (* TODO: forsake the global cursor register in favour of per-connection bookkeeping *)
            ]
      | Ok (Sublanguage_types.Query rel) ->
          let tuples, truncated =
            materialize_relation storage rel current_limit
          in
          let schema_sexp =
            List
              (List.map
                 (fun (a, d) -> List [ Atom a; Atom d ])
                 rel#schema)
          in
          let rows_sexp = List (List.map tuple_to_sexp tuples) in
          List
            [
              Atom "relation";
              List [ Atom "name"; Atom rel#name ];
              List [ Atom "schema"; schema_sexp ];
              List [ Atom "rows"; rows_sexp ];
              List
                [ Atom "row_count"; Atom (string_of_int (List.length tuples)) ];
              List [ Atom "truncated"; Atom (string_of_bool truncated) ];
              List [ Atom "db_hash"; Atom db#hash ];
              List [ Atom "db_name"; Atom db#name ];
            ]
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
              List
                [
                  Atom "message"; Atom ("Switched to multigroup " ^ multigroup);
                ];
            ]
      | Ok (Sublanguage_types.CreateMultigroup name) ->
          List
            [
              Atom "ok";
              List [ Atom "message"; Atom ("Multigroup " ^ name ^ " created") ];
            ]

    (* Open a handle to a multigroup by name via the NT object layer. The claims
       from Firewall become the connection_context for access checks. *)
    let open_multigroup (_storage : S.t) claims name =
      match
        Nt.open_handle () () ~path:[ "multigroups"; name ]
          ~connection_context:claims
      with
      | Error e -> Error (Error.SyntaxError e)
      | Ok handle -> (
          match Nt.deserialize () () Nt.Multigroup handle.Nt.obj with
          | Error e -> Error (Error.SyntaxError e)
          | Ok (Nt.Multigroup mg) -> Ok (handle, mg)
          | Ok _ -> Error (Error.SyntaxError "unexpected object type at path"))

    let handle_use storage claims output state name =
      match open_multigroup storage claims name with
      | Error e -> send_error output e
      | Ok (handle, mg) ->
          state := Active (handle, mg);
          send_ok_message output ("using multigroup: " ^ name)

    let handle_sublanguage storage output state sexp handle db =
      match
        Ok sexp
        |> fmap (execute_command storage db)
        |> fmap (perform handle db)
      with
      | Error e -> send_error output e
      | Ok (new_handle, new_db, r) ->
          state := Active (new_handle, new_db);
          serialize storage new_db (Ok r) |> output_response output

    let dispatch_command storage claims output state sexp =
      match sexp with
      | Sexplib.Sexp.(List [ Atom "use"; Atom name ]) ->
          handle_use storage claims output state name
      | _ -> (
          match !state with
          | NoMultigroup ->
              send_error output
                (Error.SyntaxError "no multigroup selected; send (use \"name\")")
          | Active (handle, db) ->
              handle_sublanguage storage output state sexp handle db)

    let handle_client connection (storage : S.t) =
      let input = T.input connection in
      let output = T.output connection in
      (* Authenticate via NT PermissionsManager::Firewall before anything else. *)
      let claims =
        match Nt.firewall () with
        | Ok c -> c
        | Error e -> Printf.eprintf "Auth failed: %s\n%!" e; ""
      in
      let state = ref NoMultigroup in
      (try
        while true do
          match read_command input with
          | Error e -> send_error output e
          | Ok sexp -> dispatch_command storage claims output state sexp
        done
      with
      | End_of_file -> ()
      | e ->
          Printf.eprintf "Error handling connection: %s" (Printexc.to_string e))

    let spawn_handler storage connection =
      Stdlib.Domain.spawn (fun () -> handle_client connection storage)

    let run transport storage =
      T.listen transport;
      while true do
        T.accept transport |> spawn_handler storage |> ignore
      done
  end
