module type Configuration = sig
  val base_dir : string
  val storage : string
  val transactions : string
  val assure : unit -> unit
end

module DevelopmentConfiguration : Configuration = struct
  let base_dir = "/tmp/relational-engine/"
  let storage = base_dir ^ "storage/"
  let transactions = base_dir ^ "transactions/"

  let assure () =
    if Sys.file_exists base_dir then Sys.remove base_dir;
    Sys.mkdir base_dir 0o700;
    Sys.mkdir storage 0o700;
    Sys.mkdir transactions 0o700
end

module type FileSystem = sig
  type target = Storage | Transaction

  val append : target -> bytes -> (int * int64, string) result
  val read : target -> int64 -> int -> (bytes, string) result
end

module FileSystem (C : Configuration) : FileSystem = struct
  type target = Storage | Transaction

  let target_to_string = function
    | Storage -> C.storage ^ "storage.re"
    | Transaction -> C.transactions ^ "transaction.re"

  (** [append target content] writes content to file in append only format,
      returning [content_length * offset_written] *)
  let append target content =
    let register channel =
      let offset_written =
        try
          let stats = Unix.stat (target_to_string target) in
          Int64.of_int stats.Unix.st_size
        with _ -> 0L
      in
      (* Out_channel.seek channel offset_written; *)
      (* let offset_written = Out_channel.length channel in *)
      Out_channel.output_bytes channel content;
      Out_channel.flush channel;
      Out_channel.close channel;
      Ok (Bytes.length content, offset_written)
    in
    try
      Out_channel.with_open_gen
        [ Open_append; Open_binary; Open_creat ]
        0o666 (target_to_string target) register
    with e ->
      Error
        (Printf.sprintf "Error on appending to target '%s': %s"
           (target_to_string target) (Printexc.to_string e))

  let read target (offset : int64) size =
    print_endline ("READ_OFFSET: " ^ Int64.to_string offset);
    print_endline ("READ_SIZE: " ^ string_of_int (Int64.to_int offset + size));
    let register (channel : in_channel) =
      let buffer = Bytes.create size in
      In_channel.seek channel offset;
      let _ = In_channel.really_input channel buffer 0 size in
      print_bytes buffer;
      print_newline ();
      In_channel.close channel;
      Ok buffer
    in
    try
      In_channel.with_open_gen
        [ Open_binary; Open_rdonly ]
        0o666 (target_to_string target) register
    with e ->
      Error
        (Printf.sprintf "Error on reading from target '%s': %s"
           (target_to_string target) (Printexc.to_string e))
end

module Executor = struct
  module StringMap = Map.Make (String)
  module IntMap = Map.Make (Int64)
  module FS = FileSystem (DevelopmentConfiguration)

  module Location = struct
    type t = { offset : int64; size : int; hash : string } [@@deriving show]
  end

  module Hashes = struct
    type t = { values : string list; hash : string } [@@deriving show]
    type history = t list [@@deriving show]
  end

  type relational_type =
    | Text
    | Integer32
    | Integer64
    | Boolean
    | DiscriminatedUnion of (string * relational_type option) list * string
      (* [("MemberA", None); ("MemberB", Some Integer32)], "NameOfDU" *)
    | Relation of string

  type schema = (string * relational_type) list StringMap.t
  type references = string list IntMap.t StringMap.t

  type commit = {
    state : string;
    files : Hashes.history StringMap.t;
    references : references;
    schema : schema;
  }

  type history = commit list
  type locations = Location.t StringMap.t

  let _EMPTY_SHA_HASH_ =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"

  (** Initializes the virtual file or opens it. *)
  let init_file (files : Hashes.history StringMap.t) filename :
      Hashes.history StringMap.t =
    match StringMap.find_opt filename files with
    | None ->
        let default : Hashes.t = { values = []; hash = _EMPTY_SHA_HASH_ } in
        let new_files = StringMap.add filename [ default ] files in
        new_files
    | Some _ -> files

  let compose_new_state files =
    let history = StringMap.to_list files in
    let all_hashes =
      List.concat
      @@ List.map
           (fun (_, (history : Hashes.history)) ->
             List.map (fun ({ hash; _ } : Hashes.t) -> hash) history)
           history
    in
    Interop.Merkle.merkle_generate_root all_hashes

  let write ({ files; references; schema; _ } as commit : commit)
      (locations : locations) ~filename ?hash_to_replace (content : Bytes.t) =
    let files = init_file files filename in
    let computed_hash : string = Interop.Sha256.compute_hash content in
    match StringMap.find_opt computed_hash locations with
    | None -> (
        match hash_to_replace with
        | None ->
            (* This instead should store a list of list to keep the history *)
            let files =
              StringMap.update filename
                (function
                  | Some (file_hashes : Hashes.history) ->
                      let { values; _ } : Hashes.t = List.hd file_hashes in
                      let new_state =
                        Interop.Merkle.merkle_generate_root
                          (computed_hash :: values)
                      in
                      Some
                        ({ values = computed_hash :: values; hash = new_state }
                        :: file_hashes)
                  | None ->
                      Some
                        [ { values = [ computed_hash ]; hash = computed_hash } ])
                files
            in
            let open Extensions.Result in
            let+ size, offset = FS.append Storage content in
            let () = print_endline @@ "OFFSET: " ^ Int64.to_string offset in
            let locations =
              StringMap.add computed_hash
                ({ size; offset; hash = computed_hash } : Location.t)
                locations
            in
            let commit =
              { state = compose_new_state files; files; references; schema }
            in
            Ok ((commit, locations), Some computed_hash)
        | Some hash_to_replace ->
            let update_fun = function
              | Some (file_hashes : Hashes.history) ->
                  let { values; _ } : Hashes.t = List.hd file_hashes in
                  let updated_entry =
                    List.map
                      (fun hash ->
                        if hash = hash_to_replace then computed_hash else hash)
                      values
                  in
                  let new_entry : Hashes.t =
                    {
                      values = updated_entry;
                      hash = Interop.Merkle.merkle_generate_root updated_entry;
                    }
                  in
                  Some (new_entry :: file_hashes)
              | None ->
                  Some [ { values = [ computed_hash ]; hash = computed_hash } ]
            in
            let files = StringMap.update filename update_fun files in
            let open Extensions.Result in
            let+ size, offset = FS.append Storage content in
            let () = print_endline @@ "OFFSET: " ^ Int64.to_string offset in
            let locations =
              StringMap.add computed_hash
                ({ size; offset; hash = computed_hash } : Location.t)
                locations
            in
            let commit =
              { state = compose_new_state files; files; references; schema }
            in
            Ok ((commit, locations), Some computed_hash))
    | Some _ -> (
        match hash_to_replace with
        | Some hash_to_replace ->
            let update_fun = function
              | Some (file_hashes : Hashes.history) ->
                  let { values; _ } : Hashes.t = List.hd file_hashes in
                  let updated_entry =
                    List.map
                      (fun hash ->
                        if hash = hash_to_replace then computed_hash else hash)
                      values
                  in
                  let new_entry : Hashes.t =
                    {
                      values = updated_entry;
                      hash = Interop.Merkle.merkle_generate_root updated_entry;
                    }
                  in
                  Some (new_entry :: file_hashes)
              | None ->
                  Some [ { values = [ computed_hash ]; hash = computed_hash } ]
            in
            (* let location_update_fun = function
                 | Some (location: Location.t) ->
                    Some { location with references = references@location.references }
                 | None -> None (* This case is a bit weird to exist. Can we ever already add and get nothing? Refactor with the location passed on the `Some _` already *)
               in *)
            let files = StringMap.update filename update_fun files in
            let commit =
              { state = compose_new_state files; files; references; schema }
            in
            (* let locations = StringMap.update computed_hash location_update_fun locations in *)
            Ok ((commit, locations), Some computed_hash)
        | None -> Ok ((commit, locations), None))

  let read ({ files; _ } as _commit : commit) (locations : locations) ~filename
      =
    let open Extensions.Option in
    let+ history : Hashes.history = StringMap.find_opt filename files in
    let+ { values = location_hashes; _ } : Hashes.t =
      match history with [] -> None | history -> Some (List.hd history)
    in
    let physical_locations =
      List.map (fun hash -> StringMap.find hash locations) location_hashes
    in
    (* Terrible! Here I read everything opening a new IO every time. We should be able to read with only one call, but changing the offset every time *)
    let content =
      List.map
        (fun ({ offset; size; _ } : Location.t) ->
          match FS.read FS.Storage offset size with
          | Ok x -> x
          | Error err -> failwith err)
        physical_locations
    in
    (*Bytes.concat Bytes.empty @@*)
    Some (List.rev content)

  let read_location ~hash (locations : locations) =
    match StringMap.find_opt hash locations with
    | Some ({ offset; size; _ } : Location.t) -> (
        match FS.read FS.Storage offset size with
        | Ok x -> x
        | Error err -> failwith err)
    | None -> failwith "Location could not be found."
end

module Startup = struct
  (* TODO: Weird behavior with a system error on the dir /tmp/relational-engine opening. Disabling for now *)
  (* let () = DevelopmentConfiguration.assure() *)
end

module Command = struct
  module FS = FileSystem (DevelopmentConfiguration)
  open Data_encoding

  type command_kind = OPEN | WRITE | READ
  (* | CREATE_RELATION *)

  let command_kind_encoding =
    union
      [
        case ~title:"open" (Tag 0) Data_encoding.empty
          (function OPEN -> Some () | _ -> None)
          (function () -> OPEN);
        case ~title:"write" (Tag 1) Data_encoding.empty
          (function WRITE -> Some () | _ -> None)
          (function () -> WRITE);
        case ~title:"read" (Tag 2) Data_encoding.empty
          (function READ -> Some () | _ -> None)
          (function () -> READ);
        (* case ~title:"create_relation" (Tag 3) *)
        (* Data_encoding.empty *)
        (* (function CREATE_RELATION -> Some () | _ -> None) *)
        (* (function () -> CREATE_RELATION); *)
      ]

  type t = {
    kind : command_kind;
    timestamp : float;
    hash : string;
    filename : string;
    entity_id : int64 option;
    (* branch must be added here later *)
    content : string;
  }

  let command_encoding =
    conv
      (fun { kind; timestamp; hash; filename; entity_id; content } ->
        (kind, timestamp, hash, filename, entity_id, content))
      (fun (kind, timestamp, hash, filename, entity_id, content) ->
        { kind; timestamp; hash; filename; entity_id; content })
      Data_encoding.(
        tup6 command_kind_encoding float string string (option int64) string)

  let parse_command ~data =
    let contract_encoding =
      Data_encoding.(
        tup5 command_kind_encoding string string (option int64) string)
    in
    match Binary.of_bytes_opt contract_encoding data with
    | Some (kind, hash, filename, entity_id, content) ->
        Ok
          { kind; timestamp = Unix.time (); hash; filename; entity_id; content }
    | None -> Error "Failed to parse command"

  type return =
    | ComputedHash of string
    | Read of (int64 * bytes list) list
    | Nothing
  [@@deriving show]

  (** TODO: This does not write atomically. If the system crashes while it
      attempts to write, it will corrupt. Solve this with a temporary file and
      move later. *)
  let commit_and_perform (commit : Executor.commit)
      (locations : Executor.locations) command =
    let open Extensions.Result in
    let+ serialized_command =
      Result.map_error (fun _ ->
          "Failed to serialize command to binary format.")
      @@ Binary.to_bytes command_encoding command
    in
    let+ _ = FS.append FS.Transaction serialized_command in
    match command.kind with
    | WRITE -> (
        let open Extensions.Result in
        let+ (commit, locations), computed_hash_handle =
          Executor.write commit locations ~filename:command.filename
          @@ Bytes.of_string command.content
        in
        match (computed_hash_handle, command.entity_id) with
        | Some computed_hash_handle, Some entity_id ->
            print_endline "WRITE A";
            let references =
              let relation_name =
                List.hd @@ String.split_on_char '/' command.filename
              in
              Executor.StringMap.update relation_name
                (function
                  | Some entity ->
                      print_endline "WRITE SOME 1";
                      Some
                        (Executor.IntMap.update entity_id
                           (function
                             | Some locations ->
                                 print_endline "WRITE SOME 2";
                                 Some (computed_hash_handle :: locations)
                             | None -> Some [ computed_hash_handle ])
                           entity)
                  | None ->
                      Some
                        (Executor.IntMap.empty
                        |> Executor.IntMap.add entity_id
                             [ computed_hash_handle ]))
                commit.references
            in
            Ok
              ( (commit, locations, references),
                ComputedHash computed_hash_handle )
        | Some _, None ->
            Error "Cannot write an entity without its referential."
        | None, _ -> Ok ((commit, locations, commit.references), Nothing))
    | READ ->
        let relation_name =
          List.hd @@ String.split_on_char '/' command.filename
        in
        print_endline "READ";
        print_endline relation_name;
        let entities : string list Executor.IntMap.t =
          print_endline command.filename;
          Executor.StringMap.find_opt command.filename commit.references
          |> function
          | Some entities ->
              print_endline "-----> A";
              entities
          | None ->
              print_endline "-----> B";
              Executor.IntMap.empty
        in
        let content =
          Executor.IntMap.fold
            (fun key hashes acc ->
              ( key,
                List.map
                  (fun location ->
                    Executor.read_location ~hash:location locations)
                  hashes )
              :: acc)
            entities []
        in
        (* print_string "CONTENT: "; *)
        (* print_endline @@ Bytes.to_string content; *)
        Ok ((commit, locations, commit.references), Read content)
    | _ -> Error "Unimplemented method"
end
