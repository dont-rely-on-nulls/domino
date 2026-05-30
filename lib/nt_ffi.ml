(* Low-level ctypes bindings to RNT_C_API.h.
   This module is internal to Nt — never import it anywhere else. *)

open Ctypes
open Foreign

let fn name typ = foreign name typ

(* Runtime ----------------------------------------------------------------- *)
let rnt_init =
  fn "rnt_init" (string @-> string @-> returning int)

(* Auth -------------------------------------------------------------------- *)
let rnt_firewall =
  fn "rnt_firewall" (string @-> ptr (ptr char) @-> returning int)

(* Session lifecycle ------------------------------------------------------- *)
let rnt_session_open =
  fn "rnt_session_open" (ptr void @-> ptr (ptr char) @-> returning int)

let rnt_session_close =
  fn "rnt_session_close" (string @-> returning int)

let rnt_session_set_branch =
  fn "rnt_session_set_branch"
    (string @-> string @-> string @-> returning int)

(* Handle lifecycle -------------------------------------------------------- *)
let rnt_open_handle =
  fn "rnt_open_handle" (string @-> ptr void @-> returning (ptr void))

let rnt_close_handle =
  fn "rnt_close_handle" (ptr void @-> returning int)

(* Branch hash-pointer ----------------------------------------------------- *)
let rnt_branch_target =
  fn "rnt_branch_target"
    (ptr void @-> ptr (ptr char) @-> returning int)

let rnt_branch_advance =
  fn "rnt_branch_advance" (string @-> string @-> returning int)

(* Object registration ----------------------------------------------------- *)
let rnt_register_relation =
  fn "rnt_register_relation" (string @-> returning int)

let rnt_register_branch =
  fn "rnt_register_branch" (string @-> string @-> returning int)

(* Branch / snapshot relation enumeration ----------------------------------- *)
let rnt_list_relations =
  fn "rnt_list_relations" (string @-> ptr (ptr char) @-> returning int)

let rnt_list_branch_multigroups =
  fn "rnt_list_branch_multigroups" (string @-> ptr (ptr char) @-> returning int)

let rnt_list_snapshot_relations =
  fn "rnt_list_snapshot_relations" (string @-> ptr (ptr char) @-> returning int)

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

let rnt_plan_project =
  fn "rnt_plan_project" (ptr void @-> ptr (ptr char) @-> returning (ptr void))

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

let with_strings_as_char_array
      (strings : string list)
      (body : char ptr ptr -> 'a)
    : 'a =
  let arr =
    CArray.make
      ~initial:(from_voidp char null)
      (ptr char)
      (1 + List.length strings)
  in
  List.iteri
    (fun i s -> CArray.set arr i (Ctypes_std_views.char_ptr_of_string s))
    strings;
  body (CArray.start arr)
  (* let pp = allocate (ptr (ptr char)) (from_voidp (ptr char) null) in *)


(* Converts a void* (returned from open_handle / cursor_open) to nativeint.
   Returns None when the pointer is NULL (failure). *)
let ptr_to_opt (p : unit ptr) : nativeint option =
  if is_null p then None
  else Some (raw_address_of_ptr p)

(* Converts a stored nativeint back to a void* for API calls. *)
let nint_to_ptr (n : nativeint) : unit ptr =
  ptr_of_raw_address n
