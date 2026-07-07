(* Low-level ctypes bindings to RNT_C_API.h.
   This module is internal to Nt — never import it anywhere else. *)

open Ctypes
open Foreign

let fn name typ = foreign name typ

(* Runtime ----------------------------------------------------------------- *)
let rnt_init = fn "rnt_init" (string @-> string @-> returning int)

(* Auth -------------------------------------------------------------------- *)
let rnt_firewall = fn "rnt_firewall" (string @-> ptr (ptr char) @-> returning int)

(* Session lifecycle ------------------------------------------------------- *)
let rnt_session_open = fn "rnt_session_open" (ptr void @-> ptr (ptr char) @-> returning int)
let rnt_session_close = fn "rnt_session_close" (string @-> returning int)

let rnt_session_set_branch =
  fn "rnt_session_set_branch" (string @-> string @-> string @-> returning int)

(* Ephemeral relations ------------------------------------------------------ *)

(* rnt_generator_fn: (ctx, args, offset, limit, sink) -> int.
   OCaml closures passed here cross into C; the closure must stay reachable
   for as long as RNT may invoke it (Nt retains them, see
   Nt.retained_generators). *)
let rnt_generator_fn =
  funptr (ptr void @-> string @-> size_t @-> size_t @-> ptr void @-> returning int)

let rnt_sink_emit = fn "rnt_sink_emit" (ptr void @-> string @-> returning int)

let rnt_register_ephemeral_relation =
  fn "rnt_register_ephemeral_relation"
    ( string
    @-> int
    @-> string
    @-> rnt_generator_fn
    @-> ptr void
    @-> int
    @-> string
    @-> string
    @-> string
    @-> ptr (ptr char)
    @-> returning int )

let rnt_drop_ephemeral_relation =
  fn "rnt_drop_ephemeral_relation" (string @-> string @-> returning int)

(* Handle lifecycle -------------------------------------------------------- *)
let rnt_open_handle = fn "rnt_open_handle" (string @-> ptr void @-> returning (ptr void))
let rnt_close_handle = fn "rnt_close_handle" (ptr void @-> returning int)

(* Branch hash-pointer ----------------------------------------------------- *)
let rnt_branch_target = fn "rnt_branch_target" (ptr void @-> ptr (ptr char) @-> returning int)
let rnt_branch_advance = fn "rnt_branch_advance" (string @-> string @-> returning int)

(* Object registration ----------------------------------------------------- *)
let rnt_register_relation = fn "rnt_register_relation" (string @-> returning int)
let rnt_register_branch = fn "rnt_register_branch" (string @-> string @-> returning int)

(* Branch / snapshot relation enumeration ----------------------------------- *)
let rnt_list_relations = fn "rnt_list_relations" (string @-> ptr (ptr char) @-> returning int)

let rnt_list_branch_multigroups =
  fn "rnt_list_branch_multigroups" (string @-> ptr (ptr char) @-> returning int)

let rnt_list_snapshot_relations =
  fn "rnt_list_snapshot_relations" (string @-> ptr (ptr char) @-> returning int)

(* Tuple storage ----------------------------------------------------------- *)
let rnt_link_tuple = fn "rnt_link_tuple" (string @-> string @-> ptr (ptr char) @-> returning int)
let rnt_unlink_tuple = fn "rnt_unlink_tuple" (string @-> string @-> returning int)
let rnt_clear_relation = fn "rnt_clear_relation" (string @-> returning int)
let rnt_relation_root = fn "rnt_relation_root" (string @-> ptr (ptr char) @-> returning int)

(* Cursor and VM ----------------------------------------------------------- *)
let rnt_cursor_open = fn "rnt_cursor_open" (ptr void @-> returning (ptr void))
let rnt_cursor_next = fn "rnt_cursor_next" (ptr void @-> ptr (ptr char) @-> returning int)
let rnt_cursor_close = fn "rnt_cursor_close" (ptr void @-> returning int)

(* VM plan builder --------------------------------------------------------- *)

(* For PlanAction in RNT_C_API.h, where one context struct per operator is laid
   side by side (no unions unfortunately as ctypes has no union support 🤡, and
   the C side reads only the member matching [operation]). Strings are passed
   as [ptr char]; the caller owns the backing CArray and must keep it alive across
   the rnt_plan_assemble call (the C side copies immediately). *)

type plan_args_scan

let plan_args_scan : plan_args_scan structure typ = structure "PlanArgsScan"
let scan_relation_path = field plan_args_scan "relation_path" (ptr char)
let () = seal plan_args_scan

type plan_args_join

let plan_args_join : plan_args_join structure typ = structure "PlanArgsJoin"
let join_left = field plan_args_join "left" (ptr void)
let join_right = field plan_args_join "right" (ptr void)
let join_attrs = field plan_args_join "attrs" (ptr (ptr char))
let () = seal plan_args_join

type plan_args_take

let plan_args_take : plan_args_take structure typ = structure "PlanArgsTake"
let take_source = field plan_args_take "source" (ptr void)
let take_limit = field plan_args_take "limit" size_t
let () = seal plan_args_take

type plan_args_project

let plan_args_project : plan_args_project structure typ = structure "PlanArgsProject"
let project_source = field plan_args_project "source" (ptr void)
let project_attrs = field plan_args_project "attrs" (ptr (ptr char))
let () = seal plan_args_project

type plan_action

let plan_action : plan_action structure typ = structure "PlanAction"
let action_operation = field plan_action "operation" int
let action_scan = field plan_action "scan" plan_args_scan
let action_join = field plan_action "join" plan_args_join
let action_take = field plan_action "take" plan_args_take
let action_project = field plan_action "project" plan_args_project
let () = seal plan_action

(* nt::Operation values, RNT include/VM.h *)
let fol_operation_scan = 1
let fol_operation_join = 2
let fol_operation_take = 3
let fol_operation_project = 4
let rnt_plan_assemble = fn "rnt_plan_assemble" (plan_action @-> returning (ptr void))
let rnt_plan_free = fn "rnt_plan_free" (ptr void @-> returning void)
let rnt_vm_execute_plan = fn "rnt_vm_execute_plan" (ptr void @-> returning (ptr void))
let rnt_vm_cursor_next = fn "rnt_vm_cursor_next" (ptr void @-> ptr (ptr char) @-> returning int)
let rnt_vm_cursor_close = fn "rnt_vm_cursor_close" (ptr void @-> returning int)

(* Memory management ------------------------------------------------------- *)
let rnt_free_string = fn "rnt_free_string" (ptr char @-> returning void)
let rnt_free_bytes = fn "rnt_free_bytes" (ptr uint8_t @-> returning void)

(* --------------------------------------------------------------------------
   Helpers shared by Nt (not exported from the library module)
   -------------------------------------------------------------------------- *)

let null_char_ptr : char ptr = from_voidp char null

let cstring (s : string) : char CArray.t =
  let n = String.length s in
  let arr = CArray.make char (n + 1) in
  String.iteri (CArray.set arr) s;
  CArray.set arr n '\000';
  arr

(* Reads a null-terminated C string from a char pointer, then frees it. *)
let consume_cstring (p : char ptr) : string =
  let n = ref 0 in
  while !@(p +@ !n) <> '\000' do
    incr n
  done;
  let s = string_from_ptr p ~length:!n in
  rnt_free_string p; s

(* Allocates a char* out-parameter, calls [f], returns the rc and string. *)
let with_out_string (f : char ptr ptr -> int) : int * string option =
  let pp = allocate (ptr char) null_char_ptr in
  let rc = f pp in
  let p = !@pp in
  if is_null p then rc, None else rc, Some (consume_cstring p)

(* Converts a void* (returned from open_handle / cursor_open) to nativeint.
   Returns None when the pointer is NULL (failure). *)
let ptr_to_opt (p : unit ptr) : nativeint option =
  if is_null p then None else Some (raw_address_of_ptr p)

(* Converts a stored nativeint back to a void* for API calls. *)
let nint_to_ptr (n : nativeint) : unit ptr = ptr_of_raw_address n
