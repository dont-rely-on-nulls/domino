let print_references (references: (string * Disk.Executor.relational_type) list Disk.Executor.IntMap.t Disk.Executor.StringMap.t) =
  Disk.Executor.StringMap.iter
    (fun k v ->
      print_endline ("→ " ^ k ^ ": ");
      Disk.Executor.IntMap.iter
        (fun k v ->
          print_endline ("  ↳ " ^ Int64.to_string k);
          List.iter (fun (v, type') -> print_endline ("    → " ^ v ^ "[" ^ Disk.Executor.show_relational_type type' ^ "]")) v)
        v;
      print_newline ())
    references

let write_and_retrieve () =
  let open Disk in
  let open Executor in
  let schema =
    Executor.StringMap.empty
    |> Executor.StringMap.add "user"
         [
           ("address", Relation "address");
           ("last_name", Text);
           ("first_name", Text);
         ]
    |> Executor.StringMap.add "address"
         [ ("street", Text); ("number", Integer32) ]
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
  let command_write1 : Command.transaction =
    {
      timestamp = 10.0;
      content = Executor.LText "Daisy";
      entity_id = Some 0L;
      attribute = "user/first-name";
      type' = Executor.Text;
    }
  in
  let+ (commit, locations), Command.ComputedHash handle11 =
    Command.transact commit locations command_write1
  in
  print_references commit.references;
  let command_write2 : Command.transaction =
    {
      timestamp = 10.0;
      content = Executor.LText "Blossom";
      entity_id = Some 0L;
      attribute = "user/last-name";
      type' = Executor.Text;
    }
  in
  let+ (commit, locations), Command.ComputedHash handle12 =
    Command.transact commit locations command_write2
  in
  print_references commit.references;
  let command_write1_address : Command.transaction =
    {
      timestamp = 10.0;
      content = Executor.LText "Saint James St.";
      entity_id = Some 0L;
      attribute = "address/street";
      type' = Executor.Text;
    }
  in
  let+ (commit, locations), Command.ComputedHash handle12 =
    Command.transact commit locations command_write1_address
  in
  print_references commit.references;
  let command_write2_address : Command.transaction =
    {
      timestamp = 10.0;
      content = Executor.LInteger32 41l;
      entity_id = Some 0L;
      attribute = "address/number";
      type' = Executor.Text;
    }
  in
  let+ (commit, locations), Command.ComputedHash handle12 =
    Command.transact commit locations command_write2_address
  in
  print_references commit.references;
  let command_write2_5 : Command.transaction =
    {
      timestamp = 10.0;
      content = Executor.LRelation 0L;
      entity_id = Some 0L;
      attribute = "user/address";
      type' = Executor.Relation "address";
    }
  in
  let+ (commit, locations), Command.ComputedHash handle12 =
    Command.transact commit locations command_write2_5
  in
  print_references commit.references;
  (* let entity_id = 1L in *)
  (* let command_write3 : Command.t = *)
  (*   { *)
  (*     kind = Command.WRITE; *)
  (*     timestamp = 10.0; *)
  (*     hash = ""; *)
  (*     content = "Minnie"; *)
  (*     entity_id = Some 1L; *)
  (*     filename = "user/first-name"; *)
  (*   } *)
  (* in *)
  (* let+ (commit, locations, references), Command.ComputedHash handle21 = *)
  (*   Command.commit_and_perform commit locations command_write3 *)
  (* in *)
  (* let command_write4 : Command.t = *)
  (*   { *)
  (*     kind = Command.WRITE; *)
  (*     timestamp = 10.0; *)
  (*     hash = ""; *)
  (*     content = "Mouse"; *)
  (*     entity_id = Some 1L; *)
  (*     filename = "user/last-name"; *)
  (*   } *)
  (* in *)
  (* let+ (commit, locations, references), Command.ComputedHash handle22 = *)
  (*   Command.commit_and_perform commit locations command_write4 *)
  (* in *)
  (* let entity_id = 2L in *)
  (* let command_write5 : Command.t = *)
  (*   { *)
  (*     kind = Command.WRITE; *)
  (*     timestamp = 10.0; *)
  (*     hash = ""; *)
  (*     content = "Billie"; *)
  (*     entity_id = Some 2L; *)
  (*     filename = "user/first-name"; *)
  (*   } *)
  (* in *)
  (* let+ (commit, locations, references), Command.ComputedHash handle31 = *)
  (*   Command.commit_and_perform commit locations command_write5 *)
  (* in *)
  (* let command_write6 : Command.t = *)
  (*   { *)
  (*     kind = Command.WRITE; *)
  (*     timestamp = 10.0; *)
  (*     hash = ""; *)
  (*     content = "Beans"; *)
  (*     entity_id = Some 2L; *)
  (*     filename = "user/last-name"; *)
  (*   } *)
  (* in *)
  (* let+ (commit, locations, references), Command.ComputedHash handle32 = *)
  (*   Command.commit_and_perform commit locations command_write6 *)
  (* in *)
  let+ _, (Command.Read content as response) =
    Command.perform commit locations (SequentialRead { relation_name = "user" })
  in
  print_endline (Command.show_return response);
  Ok (commit, locations)
[@@warning "-8-27-26"]
(* Suppress pattern match incomplete warnings *)
