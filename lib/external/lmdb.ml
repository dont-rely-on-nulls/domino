open Ctypes
open PosixTypes
open Foreign

type mdb_result = (unit, int) Result.t

let mdb_result_of_int = function
  | 0 -> Ok ()
  | x -> Error x

let int_of_mdb_result = function
  | Ok _ -> 0
  | Error x -> x

let mdb_mode_t = mode_t

let mdb_result = view ~read:mdb_result_of_int ~write:int_of_mdb_result int

type mdb_env
let mdb_env : mdb_env structure typ = structure "mdb_env"

type mdb_dbi = Unsigned.uint
let mdb_dbi : mdb_dbi typ = uint

type mdb_txn
let mdb_txn : mdb_txn structure typ = structure "mdb_txn"

type mdb_val
let mdb_val : mdb_val structure typ = structure "mdb_val"
let mv_size = field mdb_val "mv_size" size_t
let mv_data = field mdb_val "mv_data" (ptr void)
let () = seal mdb_val

let with_output_pointer output_type default body =
  let open Utilities.Result in
  let output_ptr = allocate output_type default in
  let* () = body output_ptr in
  Ok (!@ output_ptr)

let mdb_env_create = foreign "mdb_env_create" (ptr (ptr mdb_env) @-> returning mdb_result)
let mdb_env_create' () = with_output_pointer
                           (ptr mdb_env)
                           (from_voidp mdb_env null)
                           mdb_env_create

let mdb_env_open = foreign "mdb_env_open" (ptr mdb_env @-> string @-> uint @-> mdb_mode_t @-> returning mdb_result)
let mdb_env_close = foreign "mdb_env_close" (ptr mdb_env @-> returning void)

let mdb_dbi_open = foreign "mdb_dbi_open" (ptr mdb_txn @-> ptr char @-> uint @-> ptr mdb_dbi @-> returning mdb_result)
let mdb_dbi_open' txn flags = with_output_pointer
                                mdb_dbi
                                (Unsigned.UInt.of_int 0)
                                (mdb_dbi_open txn (from_voidp char null) flags)

let mdb_txn_begin = foreign "mdb_txn_begin" (ptr mdb_env @-> ptr mdb_txn @-> uint @-> ptr (ptr mdb_txn) @-> returning mdb_result)
let mdb_txn_begin' env parent flags = with_output_pointer
                                        (ptr mdb_txn)
                                        (from_voidp mdb_txn null)
                                        (mdb_txn_begin env parent flags)

let mdb_txn_commit = foreign "mdb_txn_commit" (ptr mdb_txn @-> returning mdb_result)
let mdb_txn_abort = foreign "mdb_txn_abort" (ptr mdb_txn @-> returning mdb_result)

let carray_of_bytes (b : bytes) =
  let buffer = CArray.make char (Bytes.length b) in
  Bytes.iteri (CArray.set buffer) b;
  buffer

let bytes_of_carray arr =
  let buffer = Bytes.make (CArray.length arr) (Char.chr 0) in
  CArray.iteri (Bytes.set buffer) arr;
  buffer

let mdb_val_ptr_of_bytes (b : bytes) =
  let buf = carray_of_bytes b in
  let s = make mdb_val in
  setf s mv_size (Unsigned.Size_t.of_int (Bytes.length b));
  setf s mv_data (to_voidp (CArray.start buf));
  addr s

let bytes_of_mdb_val (s : mdb_val structure) =
  let buf = CArray.from_ptr
              (from_voidp char (getf s mv_data))
              (Unsigned.Size_t.to_int (getf s mv_size)) in
  bytes_of_carray buf

let mdb_get = foreign "mdb_get" (ptr mdb_txn @-> mdb_dbi @-> ptr mdb_val @-> ptr mdb_val @-> returning mdb_result)
let mdb_get' txn dbi key = with_output_pointer
                             mdb_val
                             (make mdb_val)
                             (mdb_get txn dbi (mdb_val_ptr_of_bytes key))
                           |> Result.map bytes_of_mdb_val

let mdb_put = foreign "mdb_put" (ptr mdb_txn @-> mdb_dbi @-> ptr mdb_val @-> ptr mdb_val @-> uint @-> returning mdb_result)
let mdb_put' txn dbi key data flags = mdb_put txn dbi (mdb_val_ptr_of_bytes key) (mdb_val_ptr_of_bytes data) flags
