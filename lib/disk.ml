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
  val wipe_and_write : string -> bytes -> (int * int64, string) result
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

  let wipe_and_write path content =
    let offset_written =
      try
        let stats = Unix.stat path in
        Int64.of_int stats.Unix.st_size
      with _ -> 0L
    in
    let register channel =
      Out_channel.output_bytes channel content;
      Out_channel.flush channel;
      Ok (Bytes.length content, offset_written)
    in
    try
      Out_channel.with_open_gen
        [ Open_trunc; Open_binary; Open_creat; Open_wronly ]
        0o666 path register
    with e ->
      Error (
          Printf.sprintf "Error on appending to path '%s': %s"
            path (Printexc.to_string e)
        )

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
  module StringMap = Map.Make (String) [@@deriving show]
  module IntMap = Map.Make (Int64) [@@deriving show]
  module FS = FileSystem (DevelopmentConfiguration)

  module Location = struct
    type t = { offset : int64; size : int; hash : string } [@@deriving show]
    let encoding =
      let open Data_encoding in
      conv
        (fun { offset; size; hash } -> (offset, size, hash))
        (fun (offset, size, hash) -> { offset; size; hash })
        (tup3 int64 int31 string)
  end

  module Hashes = struct
    type t = { values : string list; hash : string } [@@deriving show]
    type history = t list [@@deriving show]

    let encoder: t Data_encoding.t =
      let open Data_encoding in
      conv
        (fun {values; hash} -> (values, hash))
        (fun (values, hash) -> {values; hash})
        (tup2 (list string) string)
  end

  open Sexplib0.Sexp_conv
  open Protocol_conv_xml

  type relational_type =
    | Text
    | Integer32
    | Integer64
    | Boolean
    (* | DiscriminatedUnion of (string * relational_type option) list * string *)
      (* [("MemberA", None); ("MemberB", Some Integer32)], "NameOfDU" *)
    | Relation of string
  [@@deriving show, sexp, protocol ~driver:(module Xml_light)]

  type relational_literal =
    | LText of string
    | LInteger32 of int32
    | LInteger64 of int64
    | LBoolean of bool
    | LRelation of int64*string
    | LRelationNested of int64*string*((string*relational_literal) list)
  [@@deriving show, sexp, protocol ~driver:(module Xml_light)]

  type schema = (string * relational_type) list StringMap.t
  (* entity * type * attribute_name *)
  type references = (string * relational_type * string) list IntMap.t StringMap.t

  type commit = {
    state : string;
    files : Hashes.history StringMap.t;
    references : references;
    schema : schema;
  }

  type history = commit list
  type locations = Location.t StringMap.t

  let relational_type_encoding =
    let open Data_encoding in
    union
      [
        case ~title:"text" (Tag 0) Data_encoding.empty
          (function Text -> Some () | _ -> None)
          (function () -> Text);
        case ~title:"integer32" (Tag 1) Data_encoding.empty
          (function Integer32 -> Some () | _ -> None)
          (function () -> Integer32);
        case ~title:"integer64" (Tag 2) Data_encoding.empty
          (function Integer64 -> Some () | _ -> None)
          (function () -> Integer64);
        case ~title:"boolean" (Tag 3) Data_encoding.empty
          (function Boolean -> Some () | _ -> None)
          (function () -> Boolean);
        case ~title:"relation" (Tag 4) Data_encoding.string
          (function Relation reference -> Some reference | _ -> None)
          (function reference -> Relation reference);
      ]

  let relational_literal_encoding =
    let open Data_encoding in
    union
      [
        case ~title:"text" (Tag 0) Data_encoding.string
          (function LText x -> Some x | _ -> None)
          (function x -> LText x);
        case ~title:"integer32" (Tag 1) Data_encoding.int32
          (function LInteger32 x -> Some x | _ -> None)
          (function x -> LInteger32 x);
        case ~title:"integer64" (Tag 2) Data_encoding.int64
          (function LInteger64 x -> Some x | _ -> None)
          (function x -> LInteger64 x);
        case ~title:"boolean" (Tag 3) Data_encoding.bool
          (function LBoolean x -> Some x | _ -> None)
          (function x -> LBoolean x);
        case ~title:"relation" (Tag 4) (Data_encoding.tup2 Data_encoding.int64 Data_encoding.string)
          (function LRelation (reference, name) -> Some (reference, name) | _ -> None)
          (function (reference, name) -> LRelation (reference, name));
      ]
  
  let encode_commit =
    let open Data_encoding in
    (* let references_encoder = (list (list (tup2 string (list (tup2 int64 (tup3 string relational_type_encoding string)))))) in *)
    let references_encoder = list (tup2 string (list (tup2 int64 (list (tup3 string relational_type_encoding string))))) in
    let schema_encoder = list (tup2 string (list (tup2 string relational_type_encoding))) in
    (* let schema_encoder = list (tup2 string (list (tup2 string relational_type_encoding))) in *)
    let files_encoder = (list (tup2 string (list Hashes.encoder))) in
    conv
      (fun {state; files; references; schema} -> (state, StringMap.to_list files, (let x = List.map (fun (k, v) -> k, IntMap.to_list v) (StringMap.to_list references) in x), StringMap.to_list schema))
      (fun (state, files, references, schema) -> {state; files = StringMap.of_list files; references = StringMap.of_list (List.map (fun (k, v) -> k, IntMap.of_list v) references); schema = StringMap.of_list schema})
      (tup4 string files_encoder references_encoder schema_encoder)


  let encode_locations =
    let open Data_encoding in
    conv
      StringMap.to_list
      StringMap.of_list
      (list (tup2 string Location.encoding))
  
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
        print_endline "-----------NOT EXISTS-----------";
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
        print_endline "-----------EXISTS-----------";
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

  let cast_to_type (type': relational_type) (content: bytes): relational_literal =
    match type' with
    | Text -> LText (Bytes.to_string content)
    | Integer32 -> LInteger32 (Data_encoding.Binary.of_bytes_exn Data_encoding.int32 content)
    | Relation _ ->
       begin
         let (reference, name) = (Data_encoding.Binary.of_bytes_exn (Data_encoding.tup2 Data_encoding.int64 Data_encoding.string) content) in
         LRelation (reference, name)
       end
    | _ -> failwith "Not implemented"

  let relational_literal_to_bytes (literal: relational_literal): bytes =
    match literal with
    | LText x -> Bytes.of_string x
    | LInteger32 x -> Data_encoding.Binary.to_bytes_exn Data_encoding.int32 x
    | LInteger64 x -> Data_encoding.Binary.to_bytes_exn Data_encoding.int64 x
    | LRelation (x,y) ->
       begin match Data_encoding.Binary.to_bytes (Data_encoding.tup2 Data_encoding.int64 Data_encoding.string) (x, y) with
       | Error x -> let open Data_encoding.Binary in
                    begin match x with
                    | Size_limit_exceeded -> print_endline "A"; Bytes.empty;
                    | No_case_matched -> print_endline "B"; Bytes.empty;
                    | Invalid_int _ -> print_endline "C"; Bytes.empty;
                    | Invalid_float _ -> print_endline "D"; Bytes.empty;
                    | Invalid_bytes_length _ -> print_endline "E"; Bytes.empty;
                    | Invalid_string_length _ -> print_endline "F"; Bytes.empty;
                    | Invalid_natural -> print_endline "G"; Bytes.empty;
                    | List_invalid_length -> print_endline "H"; Bytes.empty;
                    | Array_invalid_length -> print_endline "I"; Bytes.empty;
                    | Exception_raised_in_user_function _x -> print_endline "J"; Bytes.empty;
                    end
       | Ok x -> x
       end
    | LRelationNested _ -> Bytes.empty
    | LBoolean x -> Data_encoding.Binary.to_bytes_exn Data_encoding.bool x

  let read_location ~hash (locations: locations) (cast_type: relational_type): relational_literal =
    match StringMap.find_opt hash locations with
    | Some ({ offset; size; _ } : Location.t) ->
       begin
         match FS.read FS.Storage offset size with
         | Ok x -> cast_to_type cast_type x
         | Error err -> failwith err
        end
    | None -> failwith "Location could not be found."
end

module Startup = struct
  (* TODO: Weird behavior with a system error on the dir /tmp/relational-engine opening. Disabling for now *)
  (* let () = DevelopmentConfiguration.assure() *)
end

module Command = struct
  module FS = FileSystem (DevelopmentConfiguration)
  open Data_encoding

  type transaction = {
    (* kind : command_kind; *)
    timestamp : float;
    (* hash : string; *)
    attribute : string;
    entity_id : int64 option;
    (* branch must be added here later *)
    content : Executor.relational_literal;
    type': Executor.relational_type;
  }

  open Sexplib0.Sexp_conv
  open Protocol_conv_xml

  type t =
    | State
    | SequentialRead of { relation_name : string }
    | SpecifyRelation of { relation_name: string;
                           attributes: (string*Executor.relational_type) list }
    | NaturalJoin of {left_relation: string;
                      right_relation: string}
    | Nest of {relation_name: string}
  [@@deriving sexp, protocol ~driver:(module Xml_light)]
  
  let command_encoding =
    conv
      (fun { timestamp; attribute; entity_id; content; type' } ->
        (timestamp, attribute, entity_id, content, type'))
      (fun (timestamp, attribute, entity_id, content, type') ->
        { timestamp; attribute; entity_id; content; type' })
      Data_encoding.(tup5 float string (option int64) Executor.relational_literal_encoding Executor.relational_type_encoding)

  let parse_command ~data =
    let contract_encoding =
      Data_encoding.(tup4 string (option int64) Executor.relational_literal_encoding Executor.relational_type_encoding)
    in
    match Binary.of_bytes_opt contract_encoding data with
    | Some (attribute, entity_id, content, type') ->
        Ok { timestamp = Unix.time (); attribute; entity_id; content; type' }
    | None -> Error "Failed to parse command"

  type return =
    | ComputedHash of string
    | Read of (int64 * (string*Executor.relational_literal) list) list
    | Schema of (string * (string * Executor.relational_type) list) list
    | Nothing
  [@@deriving show, sexp, protocol ~driver:(module Xml_light)]

  (** Writes the commit and location to the disk, but cleans it before.
      Needless to say this is absolutely just a dirty trick to test persistency of the structures.
      To implement this properly, we need to mark the new appended objects, flush the dirty ones to
      the disk without touching the rest, and do it atomically with unix file swaps (for example). **)
  let write_context commit locations =
    let open Extensions.Result in
    let+ commit = Result.map_error (fun _ -> "Failed to serialize 'commit'") @@ Data_encoding.Binary.to_bytes Executor.encode_commit commit in
    let+ locations = Result.map_error (fun _ -> "Failed to serialize 'locations'") @@ Data_encoding.Binary.to_bytes Executor.encode_locations locations in
    let+ _ = Result.map_error (fun msg -> Printf.sprintf "Failed to write 'commit' to the disk: %s" msg) @@ FS.wipe_and_write "/tmp/relational-engine/commit" commit in
    let+ _ = Result.map_error (fun msg -> Printf.sprintf "Failed to write 'locations' to the disk: %s" msg) @@ FS.wipe_and_write "/tmp/relational-engine/locations" locations in
    Ok ()
  
  (** TODO: This does not write atomically. If the system crashes while it
      attempts to write, it will corrupt. Solve this with a temporary file and
      move later. *)
  let transact (commit : Executor.commit) (locations : Executor.locations)
      (command : transaction) =
    let open Extensions.Result in
    let+ serialized_command =
      Result.map_error (fun _ ->
          "Failed to serialize command to binary format.")
      @@ Binary.to_bytes command_encoding command
    in
    let+ _ = FS.append FS.Transaction serialized_command in
    let open Extensions.Result in
    let+ () = write_context commit locations in
    let+ (commit, locations), computed_hash_handle =
      Executor.write commit locations ~filename:command.attribute
      @@ Executor.relational_literal_to_bytes command.content
    in
    match (computed_hash_handle, command.entity_id) with 
    | Some computed_hash_handle, Some entity_id ->
        let references: Executor.references =
          let relation_name::[attribute_name] =
            String.split_on_char '/' command.attribute
          [@@warning "-8"] in
          Executor.StringMap.update relation_name
            (function
              | Some entity ->
                  Some
                    (Executor.IntMap.update entity_id
                       (function
                         | Some locations ->
                             Some ((computed_hash_handle, command.type', attribute_name) :: locations)
                         | None -> Some [ (computed_hash_handle, command.type', attribute_name) ])
                       entity)
              | None ->
                  Some
                    (Executor.IntMap.empty
                    |> Executor.IntMap.add entity_id [ (computed_hash_handle, command.type', attribute_name) ]))
            commit.references
        in
        Ok
          ( ({ commit with references }, locations),
            ComputedHash computed_hash_handle )
    | Some _, None -> Error "Cannot write an entity without its referential."
    | None, _ -> Ok ((commit, locations), Nothing)
end
