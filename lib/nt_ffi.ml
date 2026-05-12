(* Low-level ctypes bindings to RNT_C_API.h.
   This module is internal to Nt — never import it anywhere else. *)

open Ctypes
open Foreign

(* Load the shared library.  Set RNT_LIBRARY_PATH to the full path of
   libRNT.dylib / libRNT.so when the library is not on the default search
   path. *)
let lib =
  let path =
    match Sys.getenv_opt "RNT_LIBRARY_PATH" with
    | Some p -> p
    | None   -> "libRNT.dylib"
  in
  Dl.(dlopen ~filename:path ~flags:[ RTLD_NOW; RTLD_GLOBAL ])

let fn name typ = foreign ~from:lib name typ

(* Runtime ----------------------------------------------------------------- *)
let rnt_init =
  fn "rnt_init" (string @-> string @-> returning int)

(* Auth -------------------------------------------------------------------- *)
let rnt_firewall =
  fn "rnt_firewall" (string @-> ptr (ptr char) @-> returning int)

(* Handle lifecycle -------------------------------------------------------- *)
let rnt_open_handle =
  fn "rnt_open_handle" (string @-> ptr void @-> returning (ptr void))

let rnt_close_handle =
  fn "rnt_close_handle" (ptr void @-> returning int)

(* Branch payload ---------------------------------------------------------- *)
let rnt_branch_payload =
  fn "rnt_branch_payload"
    (ptr void @-> ptr (ptr uint8_t) @-> ptr size_t @-> returning int)

let rnt_branch_set_payload =
  fn "rnt_branch_set_payload"
    (ptr void @-> ptr uint8_t @-> size_t @-> returning int)

(* Object registration ----------------------------------------------------- *)
let rnt_register_relation =
  fn "rnt_register_relation" (string @-> returning int)

let rnt_register_branch =
  fn "rnt_register_branch"
    (string @-> ptr uint8_t @-> size_t @-> returning int)

(* Tuple storage ----------------------------------------------------------- *)
let rnt_link_tuple =
  fn "rnt_link_tuple"
    (string @-> string @-> ptr (ptr char) @-> returning int)

let rnt_unlink_tuple =
  fn "rnt_unlink_tuple" (string @-> string @-> returning int)

let rnt_clear_relation =
  fn "rnt_clear_relation" (string @-> returning int)

let rnt_relation_root =
  fn "rnt_relation_root"
    (string @-> ptr (ptr char) @-> returning int)

let rnt_set_relation_root =
  fn "rnt_set_relation_root" (string @-> string @-> returning int)

(* Cursor and VM ----------------------------------------------------------- *)
let rnt_cursor_open =
  fn "rnt_cursor_open" (ptr void @-> returning (ptr void))

let rnt_cursor_next =
  fn "rnt_cursor_next" (ptr void @-> ptr (ptr char) @-> returning int)

let rnt_cursor_close =
  fn "rnt_cursor_close" (ptr void @-> returning int)

(* VM plan builder --------------------------------------------------------- *)
let rnt_plan_scan =
  fn "rnt_plan_scan" (string @-> returning (ptr void))

let rnt_plan_join =
  fn "rnt_plan_join" (ptr void @-> ptr void @-> returning (ptr void))

let rnt_plan_take =
  fn "rnt_plan_take" (ptr void @-> size_t @-> returning (ptr void))

let rnt_plan_free =
  fn "rnt_plan_free" (ptr void @-> returning void)

let rnt_vm_execute_plan =
  fn "rnt_vm_execute_plan" (ptr void @-> returning (ptr void))

let rnt_vm_cursor_next =
  fn "rnt_vm_cursor_next" (ptr void @-> ptr (ptr char) @-> returning int)

let rnt_vm_cursor_close =
  fn "rnt_vm_cursor_close" (ptr void @-> returning int)

(* Memory management ------------------------------------------------------- *)
let rnt_free_string =
  fn "rnt_free_string" (ptr char @-> returning void)

let rnt_free_bytes =
  fn "rnt_free_bytes" (ptr uint8_t @-> returning void)

(* --------------------------------------------------------------------------
   Helpers shared by Nt (not exported from the library module)
   -------------------------------------------------------------------------- *)

let null_char_ptr : char ptr = from_voidp char null

(* Reads a null-terminated C string from a char pointer, then frees it. *)
let consume_cstring (p : char ptr) : string =
  let n = ref 0 in
  while !@(p +@ !n) <> '\000' do incr n done;
  let s = string_from_ptr p ~length:!n in
  rnt_free_string p;
  s

(* Allocates a char* out-parameter, calls [f], returns the rc and string. *)
let with_out_string (f : char ptr ptr -> int) : int * string option =
  let pp = allocate (ptr char) null_char_ptr in
  let rc = f pp in
  let p  = !@ pp in
  if is_null p then (rc, None)
  else (rc, Some (consume_cstring p))

(* Converts a void* (returned from open_handle / cursor_open) to nativeint.
   Returns None when the pointer is NULL (failure). *)
let ptr_to_opt (p : unit ptr) : nativeint option =
  if is_null p then None
  else Some (raw_address_of_ptr p)

(* Converts a stored nativeint back to a void* for API calls. *)
let nint_to_ptr (n : nativeint) : unit ptr =
  ptr_of_raw_address n

(* Converts an OCaml bytes value to a uint8_t CArray for passing to C. *)
let bytes_to_uint8_array (b : bytes) =
  let len = Bytes.length b in
  if len = 0 then (from_voidp uint8_t null, Unsigned.Size_t.zero)
  else
    let arr = CArray.make uint8_t len in
    for i = 0 to len - 1 do
      CArray.set arr i
        (Unsigned.UInt8.of_int (Char.code (Bytes.get b i)))
    done;
    (CArray.start arr, Unsigned.Size_t.of_int len)

(* Reads [len] bytes from a uint8_t pointer into an OCaml bytes value and
   frees the C buffer. *)
let consume_uint8_array p (len : Unsigned.size_t) : bytes =
  let n = Unsigned.Size_t.to_int len in
  let b = Bytes.create n in
  for i = 0 to n - 1 do
    Bytes.set b i
      (Char.chr (Unsigned.UInt8.to_int !@(p +@ i)))
  done;
  rnt_free_bytes p;
  b
