module Make =
functor
  (T : Transport.TRANSPORT)
  (NT : Nt.S)
  ->
  struct
    module type SubS = Sublanguage.S

    module Sess = Session.Make (NT)
    module B = Branch.Make (NT)
    module SC = Sublanguage_context.Make (NT)

    module Error = struct
      open Condition

      let syntax_error msg = condition "syntax-error" msg empty

      let unrecognized_sublanguage tag =
        condition "unrecognized-sublanguage" "Unrecognized sublanguage" ("tag" |=| of_string tag)

      let malformed_expression expr =
        condition "malformed-expression" "Malformed expression" ("expression" |=| of_sexp expr)
    end

    (** Per-connection state: session owns the active branch. The branch's
        [relation_path] becomes the context resolver, so all sublanguages
        address the full RNT namespace rather than a single multigroup. **)
    type conn_state = {claims: Nt.claims; session: Sess.session}

    (** Build a fresh execution context from the current session state. Called
        at the top of every handle_sublanguage so it always reflects the live
        branch after any prior VCL switch. **)
    let make_ctx ({session; claims} : conn_state) : Sublanguage_context.t =
      SC.make_ctx session claims

    let read_command input =
      try Ok (Sexplib.Sexp.input_sexp input)
      with Sexplib.Sexp.Parse_error {err_msg; _} -> Error (Error.syntax_error err_msg)

    let sublanguages =
      List.fold_right
        (fun (module Language : SubS) ->
          Utilities.StringMap.add Language.name (module Language : SubS) )
        [ (module Drl.Sublanguage.Make (NT) : SubS);
          (module Ddl.Sublanguage.Make (NT) : SubS);
          (module Dml.Sublanguage.Make (NT) : SubS);
          (module Icl.Sublanguage.Make (NT) : SubS);
          (module Prl.Sublanguage.Make (NT) : SubS);
          (module Scl.Sublanguage.Make (NT) : SubS);
          (module Vcl.Sublanguage.Make (NT) : SubS) ]
        Utilities.StringMap.empty

    let find_language tag =
      Utilities.StringMap.find_opt tag sublanguages
      |> Option.to_result ~none:(Error.unrecognized_sublanguage tag)

    let execute_sublanguage ctx expr (module Language : SubS) =
      Language.parse_sexp expr |> Utilities.Result.fmap (Language.execute ctx)

    let execute_command ctx = function
      | Sexplib.Sexp.(List [Atom tag; expr]) ->
          find_language tag |> Utilities.Result.fmap (execute_sublanguage ctx expr)
      | s -> Error (Error.malformed_expression s)

    let tuple_to_sexp (t : Tuple.materialized) =
      let open Sexplib.Sexp in
      List
        ( Tuple.AttributeMap.bindings t.Tuple.attributes
        |> List.map (fun (k, attr) ->
            List [Atom k; Conventions.AbstractValue.sexp_of_t attr.Attribute.value] ) )

    let output_response out_ch sexp =
      let response = Sexplib.Sexp.to_string sexp in
      output_string out_ch response;
      output_string out_ch "\n";
      flush out_ch;
      Utilities.print_with_time response

    let send_error out_ch e =
      output_response out_ch Sexplib.Sexp.(List [Atom "error"; Condition.to_sexp e])

    (* Serializes a sublanguage result.  [branch] is the write target whose
       tip and name surface in Cursor / Transition responses. *)
    let serialize (branch : B.branch) =
      let open Sexplib.Sexp in
      function
      | Error e -> List [Atom "error"; Condition.to_sexp e]
      | Ok (Sublanguage_types.Cursor {cursor_id; rows; has_more}) ->
          let row_sexps = List.map tuple_to_sexp rows in
          List
            [ Atom "cursor";
              List [Atom "id"; Atom cursor_id];
              List [Atom "rows"; List row_sexps];
              List [Atom "row_count"; Atom (string_of_int (List.length row_sexps))];
              List [Atom "has_more"; Atom (string_of_bool has_more)];
              List [Atom "snapshot"; Atom branch#tip];
              List [Atom "branch"; Atom branch#name] ]
      | Ok (Sublanguage_types.Query _rel) ->
          List [Atom "error"; Atom "unexpected Query result: use Cursor path"]
      | Ok (Sublanguage_types.Transition _new_cache) ->
          List
            [ Atom "ok";
              List [Atom "snapshot"; Atom branch#tip];
              List [Atom "branch"; Atom branch#name] ]
      | Ok (Sublanguage_types.SessionSwitch name) ->
          List [Atom "ok"; List [Atom "message"; Atom ("Switched to branch " ^ name)]]
      | Ok (Sublanguage_types.CreateMultigroup name) ->
          List [Atom "ok"; List [Atom "message"; Atom ("Multigroup " ^ name ^ " created")]]

    let handle_sublanguage output state sexp =
      let ctx = make_ctx !state in
      match execute_command ctx sexp with
      | Error e -> send_error output e
      | Ok result ->
          (* Sync updated schema_cache back into the branch mirror on any
             state-advancing result (DDL, ICL, VCL).  For VCL the session
             already holds the new branch; refresh_mg is a no-op on it but
             correct for DDL/ICL which stay on the same branch. *)
          ( match result with
          | Sublanguage_types.Transition delta -> !state.session#branch#apply_delta delta
          | _ -> () );
          serialize !state.session#branch (Ok result) |> output_response output

    let handle_client connection =
      let input = T.input connection in
      let output = T.output connection in
      match NT.authenticate Nt.PlainText with
      | Error e ->
          (* FIXME: why is this a syntax error? *)
          send_error output e
      | Ok claims -> (
        match Sess.open_session claims ~branch_name:"master" with
        | Error e -> send_error output e
        | Ok sess -> (
            let state = ref {claims; session= sess} in
            try
              while true do
                match read_command input with
                | Error e -> send_error output e
                | Ok sexp -> handle_sublanguage output state sexp
              done
            with
            | End_of_file -> ignore (!state.session#close ())
            | e ->
                ignore (!state.session#close ());
                Printf.eprintf "Error handling connection: %s" (Printexc.to_string e) ) )

    let spawn_handler connection = Stdlib.Domain.spawn (fun () -> handle_client connection)

    let run transport =
      T.listen transport;
      while true do
        T.accept transport |> spawn_handler |> ignore
      done
  end
