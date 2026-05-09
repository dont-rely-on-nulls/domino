open Ctypes

type tree_handle = unit

module Key : sig
  type t

  val size : int
  val of_string : string -> t option
  val to_string : t -> string
end = struct
  type t = string

  let size = 32

  let of_string value =
    if String.length value = size then Some value else None

  let to_string key = key
end

type t = { handle : tree_handle ptr; mutable closed : bool }

type error =
  | Not_found
  | Invalid_argument
  | Out_of_memory
  | Io_error
  | Corrupt_file
  | Closed
  | Unknown of int

let tree_ptr = ptr void

let status_ok = 0

let error_of_status = function
  | 1 -> Not_found
  | 2 -> Invalid_argument
  | 3 -> Out_of_memory
  | 4 -> Io_error
  | 5 -> Corrupt_file
  | status -> Unknown status

let status_to_result = function
  | status when status = status_ok -> Ok ()
  | status -> Error (error_of_status status)

module Raw = struct
  open Foreign

  let load_library () =
    let default_names =
      if Sys.win32 then
        [ "sakura_shabptree.dll" ]
      else if Sys.os_type = "Unix" then
        [ "libsakura_shabptree.so" ]
      else
        [ "libsakura_shabptree.dylib" ]
    in
    let candidates =
      match Sys.getenv_opt "SAKURA_BPLUSTREE_DLL" with
      | Some filename -> filename :: default_names
      | None -> default_names
    in
    let rec try_load = function
      | [] -> None
      | filename :: rest ->
          (try Some (Dl.dlopen ~filename ~flags:[ Dl.RTLD_NOW ]) with
           | Dl.DL_error _ -> try_load rest)
    in
    try_load candidates

  let library = lazy (load_library ())

  let bind name signature =
    match Lazy.force library with
    | Some library -> foreign ~from:library name signature
    | None -> foreign name signature

  let create = bind "sakura_bptree_create" (void @-> returning tree_ptr)
  let free = bind "sakura_bptree_free" (tree_ptr @-> returning void)
  let len = bind "sakura_bptree_len" (tree_ptr @-> returning uint64_t)

  let put =
    bind "sakura_bptree_put"
      (tree_ptr @-> ptr uint8_t @-> ptr void @-> uint64_t @-> returning int)

  let get =
    bind "sakura_bptree_get"
      (tree_ptr @-> ptr uint8_t @-> ptr (ptr void) @-> ptr uint64_t @-> returning int)

  let remove =
    bind "sakura_bptree_remove" (tree_ptr @-> ptr uint8_t @-> returning int)

  let save = bind "sakura_bptree_save" (tree_ptr @-> string @-> returning int)
  let load = bind "sakura_bptree_load" (string @-> ptr tree_ptr @-> returning int)

  let serialize =
    bind "sakura_bptree_serialize"
      (tree_ptr @-> ptr (ptr uint8_t) @-> ptr uint64_t @-> returning int)

  let deserialize =
    bind "sakura_bptree_deserialize"
      (ptr uint8_t @-> uint64_t @-> ptr tree_ptr @-> returning int)

  let free_buf = bind "free" (ptr uint8_t @-> returning void)

  let status_string =
    bind "sakura_bptree_status_string" (int @-> returning string)
end

let uint8_array_of_string value =
  let len = String.length value in
  let array = CArray.make uint8_t len in
  for i = 0 to len - 1 do
    CArray.set array i (Unsigned.UInt8.of_int (Char.code value.[i]))
  done;
  array

let with_key_ptr key f =
  let c_key = uint8_array_of_string (Key.to_string key) in
  f (CArray.start c_key)

let with_value_ptr value f =
  let len = String.length value in
  if len = 0 then f null 0
  else
    let c_value = uint8_array_of_string value in
    f (to_voidp (CArray.start c_value)) len

let handle tree =
  if tree.closed then Error Closed else Ok tree.handle

let free tree =
  if not tree.closed then begin
    Raw.free tree.handle;
    tree.closed <- true
  end

let close tree =
  free tree;
  Ok ()

let create () =
  let handle = Raw.create () in
  if is_null handle then Error Out_of_memory
  else
    let tree = { handle; closed = false } in
    Gc.finalise free tree;
    Ok tree

let length tree =
  match handle tree with
  | Error error -> Error error
  | Ok handle -> Ok (Unsigned.UInt64.to_int (Raw.len handle))

let put tree key value =
  match handle tree with
  | Error error -> Error error
  | Ok handle ->
      with_key_ptr key @@ fun key_ptr ->
      with_value_ptr value @@ fun value_ptr value_len ->
      Raw.put handle key_ptr value_ptr (Unsigned.UInt64.of_int value_len)
      |> status_to_result

let get tree key =
  match handle tree with
  | Error error -> Error error
  | Ok handle ->
      with_key_ptr key @@ fun key_ptr ->
      let value_ptr = allocate (ptr void) null in
      let len_ptr = allocate uint64_t Unsigned.UInt64.zero in
      match Raw.get handle key_ptr value_ptr len_ptr |> status_to_result with
      | Error error -> Error error
      | Ok () ->
          let len = Unsigned.UInt64.to_int !@len_ptr in
          let value = from_voidp uint8_t !@value_ptr in
          Ok (String.init len (fun i -> Char.chr (Unsigned.UInt8.to_int !@(value +@ i))))

let remove tree key =
  match handle tree with
  | Error error -> Error error
  | Ok handle ->
      with_key_ptr key @@ fun key_ptr -> Raw.remove handle key_ptr |> status_to_result

let save tree path =
  match handle tree with
  | Error error -> Error error
  | Ok handle -> Raw.save handle path |> status_to_result

let load path =
  let out = allocate tree_ptr null in
  match Raw.load path out |> status_to_result with
  | Ok () ->
      let tree = { handle = !@out; closed = false } in
      Gc.finalise free tree;
      Ok tree
  | Error error -> Error error

let serialize tree =
  match handle tree with
  | Error error -> Error error
  | Ok handle ->
      let out_ptr = allocate (ptr uint8_t) (from_voidp uint8_t null) in
      let out_len = allocate uint64_t Unsigned.UInt64.zero in
      (match Raw.serialize handle out_ptr out_len |> status_to_result with
       | Error error -> Error error
       | Ok () ->
           let len = Unsigned.UInt64.to_int !@out_len in
           let p = !@out_ptr in
           let bytes = Bytes.init len (fun i -> Char.chr (Unsigned.UInt8.to_int !@(p +@ i))) in
           Raw.free_buf p;
           Ok (Bytes.to_string bytes))

let deserialize data =
  let len = String.length data in
  let c_data = uint8_array_of_string data in
  let out = allocate tree_ptr null in
  match
    Raw.deserialize (CArray.start c_data) (Unsigned.UInt64.of_int len) out
    |> status_to_result
  with
  | Error error -> Error error
  | Ok () ->
      let tree = { handle = !@out; closed = false } in
      Gc.finalise free tree;
      Ok tree

let error_message error =
  match error with
  | Closed -> "tree is closed"
  | Unknown status -> Raw.status_string status
  | _ ->
      let status =
        match error with
        | Not_found -> 1
        | Invalid_argument -> 2
        | Out_of_memory -> 3
        | Io_error -> 4
        | Corrupt_file -> 5
        | Closed -> 2
        | Unknown status -> status
      in
      Raw.status_string status
