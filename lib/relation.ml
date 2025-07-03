let print_references (references: (string * Disk.Executor.relational_type * string) list Disk.Executor.IntMap.t Disk.Executor.StringMap.t) =
  Disk.Executor.StringMap.iter
    (fun k v ->
      print_endline ("→ " ^ k ^ ": ");
      Disk.Executor.IntMap.iter
        (fun k v ->
          print_endline ("  ↳ " ^ Int64.to_string k);
          List.iter (fun (v, type', name) -> print_endline ("    → " ^ v ^ "[" ^ name ^ "::" ^ Disk.Executor.show_relational_type type' ^ "]")) v)
        v;
      print_newline ())
    references

let prepare () =
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
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LText "Daisy";
      entity_id = Some 0L;
      attribute = "user/first-name";
      type' = Executor.Text;
    }
    |> Command.transact commit locations
  in
  print_references commit.references;
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LText "Duck";
      entity_id = Some 0L;
      attribute = "user/last-name";
      type' = Executor.Text;
    }
    |> Command.transact commit locations
  in
  print_references commit.references;
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LText "Saint James St.";
      entity_id = Some 0L;
      attribute = "address/street";
      type' = Executor.Text;
    }
    |> Command.transact commit locations
  in
  print_references commit.references;
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LInteger32 41l;
      entity_id = Some 0L;
      attribute = "address/number";
      type' = Executor.Text;
    }
    |> Command.transact commit locations
  in
  print_references commit.references;
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LRelation (0L, "address");
      entity_id = Some 0L;
      attribute = "user/address";
      type' = Executor.Relation "address";
    }
    |> Command.transact commit locations
  in
  print_references commit.references;
  let entity_id = 1L in
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LText "Minnie";
      entity_id = Some 1L;
      attribute = "user/first-name";
      type' = Executor.Text;
    }
    |> Command.transact commit locations
  in
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LText "Mouse";
      entity_id = Some 1L;
      attribute = "user/last-name";
      type' = Executor.Text;
    }
    |> Command.transact commit locations
  in
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LRelation (0L, "address");
      entity_id = Some 1L;
      attribute = "user/address";
      type' = Executor.Relation "address";
    }
    |> Command.transact commit locations
  in
  let entity_id = 2L in
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LText "Scrooge";
      entity_id = Some 2L;
      attribute = "user/first-name";
      type' = Executor.Text;
    }
    |> Command.transact commit locations
  in
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LText "McDuck";
      entity_id = Some 2L;
      attribute = "user/last-name";
      type' = Executor.Text;
    }
    |> Command.transact commit locations
  in
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LText "President Julian St.";
      entity_id = Some 1L;
      attribute = "address/street";
      type' = Executor.Text;
    }
    |> Command.transact commit locations
  in
  print_references commit.references;
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LInteger32 768l;
      entity_id = Some 1L;
      attribute = "address/number";
      type' = Executor.Text;
    }
    |> Command.transact commit locations
  in
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LRelation (1L, "address");
      entity_id = Some 2L;
      attribute = "user/address";
      type' = Executor.Relation "address";
    }
    |> Command.transact commit locations
  in
  Ok (commit, locations)
  [@@warning "-8-27-26"]

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
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LText "Daisy";
      entity_id = Some 0L;
      attribute = "user/first-name";
      type' = Executor.Text;
    }
    |> Command.transact commit locations
  in
  print_references commit.references;
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LText "Duck";
      entity_id = Some 0L;
      attribute = "user/last-name";
      type' = Executor.Text;
    }
    |> Command.transact commit locations
  in
  print_references commit.references;
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LText "Saint James St.";
      entity_id = Some 0L;
      attribute = "address/street";
      type' = Executor.Text;
    }
    |> Command.transact commit locations
  in
  print_references commit.references;
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LInteger32 41l;
      entity_id = Some 0L;
      attribute = "address/number";
      type' = Executor.Text;
    }
    |> Command.transact commit locations
  in
  print_references commit.references;
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LRelation (0L, "address");
      entity_id = Some 0L;
      attribute = "user/address";
      type' = Executor.Relation "address";
    }
    |> Command.transact commit locations
  in
  print_references commit.references;
  let entity_id = 1L in
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LText "Minnie";
      entity_id = Some 1L;
      attribute = "user/first-name";
      type' = Executor.Text;
    }
    |> Command.transact commit locations
  in
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LText "Mouse";
      entity_id = Some 1L;
      attribute = "user/last-name";
      type' = Executor.Text;
    }
    |> Command.transact commit locations
  in
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LRelation (0L, "address");
      entity_id = Some 1L;
      attribute = "user/address";
      type' = Executor.Relation "address";
    }
    |> Command.transact commit locations
  in
  let entity_id = 2L in
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LText "Scrooge";
      entity_id = Some 2L;
      attribute = "user/first-name";
      type' = Executor.Text;
    }
    |> Command.transact commit locations
  in
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LText "McDuck";
      entity_id = Some 2L;
      attribute = "user/last-name";
      type' = Executor.Text;
    }
    |> Command.transact commit locations
  in
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LText "President Julian St.";
      entity_id = Some 1L;
      attribute = "address/street";
      type' = Executor.Text;
    }
    |> Command.transact commit locations
  in
  print_references commit.references;
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LInteger32 768l;
      entity_id = Some 1L;
      attribute = "address/number";
      type' = Executor.Text;
    }
    |> Command.transact commit locations
  in
  let+ (commit, locations), _ =
    {
      timestamp = 10.0;
      content = Executor.LRelation (1L, "address");
      entity_id = Some 2L;
      attribute = "user/address";
      type' = Executor.Relation "address";
    }
    |> Command.transact commit locations
  in
  let+ _, (Command.Read content as response) =
    Command.perform commit locations (SequentialRead { relation_name = "user" })
  in
  print_endline (Command.show_return response);
  (* Ok (commit, locations) *)
  Ok response
[@@warning "-8-27-26"]
(* Suppress pattern match incomplete warnings *)
