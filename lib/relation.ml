let write_and_retrieve () =
  let open Disk in
  let schema =
    Executor.StringMap.empty
    |> Executor.StringMap.add "user"
         [
           ("first_name", Executor.Text);
           ("last_name", Executor.Text);
           ("address", Executor.Relation "address");
         ]
    |> Executor.StringMap.add "address"
         [ ("street", Executor.Text); ("number", Executor.Integer32) ]
  in
  let commit : Executor.commit =
    {
      state = Executor._EMPTY_SHA_HASH_;
      files = Executor.StringMap.empty;
      references = Executor.StringMap.empty;
      schema;
    }
  in
  let locations : Executor.locations = Executor.StringMap.empty in
  let relation_name = "user" in
  (* let entity_id, _ = Executor.StringMap.find relation_name references |> Executor.IntMap.max_binding in *)
  (* let references =
     Executor.StringMap.empty
     |> Executor.StringMap.add "user" (Executor.StringMap.empty
                                       |> Executor.StringMap.add "entity_0" [handle11; handle12]
                                       |> Executor.StringMap.add "entity_1" [handle21; handle22]
                                       |> Executor.StringMap.add "entity_2" [handle31; handle32]) in *)
  let open Extensions.Result in
  let command_write1 : Command.t =
    {
      kind = Command.WRITE;
      timestamp = 10.0;
      hash = "";
      content = "Daisy";
      entity_id = Some 0L;
      filename = "user/first-name";
    }
  in
  let+ (commit, locations, references), Command.ComputedHash handle11 =
    Command.commit_and_perform commit locations command_write1
  in
  let command_write2 : Command.t =
    {
      kind = Command.WRITE;
      timestamp = 10.0;
      hash = "";
      content = "Blossom";
      entity_id = Some 0L;
      filename = "user/last-name";
    }
  in
  let+ (commit, locations, references), Command.ComputedHash handle12 =
    Command.commit_and_perform commit locations command_write2
  in
  let entity_id = 1L in
  let command_write3 : Command.t =
    {
      kind = Command.WRITE;
      timestamp = 10.0;
      hash = "";
      content = "Minnie";
      entity_id = Some 1L;
      filename = "user/first-name";
    }
  in
  let+ (commit, locations, references), Command.ComputedHash handle21 =
    Command.commit_and_perform commit locations command_write3
  in
  let command_write4 : Command.t =
    {
      kind = Command.WRITE;
      timestamp = 10.0;
      hash = "";
      content = "Mouse";
      entity_id = Some 1L;
      filename = "user/last-name";
    }
  in
  let+ (commit, locations, references), Command.ComputedHash handle22 =
    Command.commit_and_perform commit locations command_write4
  in
  let entity_id = 2L in
  let command_write5 : Command.t =
    {
      kind = Command.WRITE;
      timestamp = 10.0;
      hash = "";
      content = "Billie";
      entity_id = Some 2L;
      filename = "user/first-name";
    }
  in
  let+ (commit, locations, references), Command.ComputedHash handle31 =
    Command.commit_and_perform commit locations command_write5
  in
  let command_write6 : Command.t =
    {
      kind = Command.WRITE;
      timestamp = 10.0;
      hash = "";
      content = "Beans";
      entity_id = Some 2L;
      filename = "user/last-name";
    }
  in
  let+ (commit, locations, references), Command.ComputedHash handle32 =
    Command.commit_and_perform commit locations command_write6
  in
  let command_read : Command.t =
    {
      kind = Command.READ;
      timestamp = 0.0;
      hash = "";
      content = "";
      entity_id = None;
      filename = "user";
    }
  in
  let+ _, (Command.Read content as response) =
    Command.commit_and_perform commit locations command_read
  in
  Executor.StringMap.iter
    (fun k v ->
      print_endline ("→ " ^ k ^ ": ");
      Executor.IntMap.iter
        (fun k v ->
          print_endline ("  ↳ " ^ Int64.to_string k);
          List.iter (fun v -> print_endline ("    → " ^ v)) v)
        v;
      print_newline ())
    references;
  print_endline (Command.show_return response);
  Ok (commit, locations)
[@@warning "-8-27-26"]
(* Suppress pattern match incomplete warnings *)
