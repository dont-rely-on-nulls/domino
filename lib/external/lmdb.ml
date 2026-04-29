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

type mdb_env = unit ptr
let mdb_env = ptr Void

type mdb_dbi = Unsigned.uint
let mdb_dbi = uint

type mdb_txn = unit ptr
let mdb_txn = ptr Void

let mdb_env_create = foreign "mdb_env_create" (ptr ptr mdb_env @-> returning mdb_result)
let mdb_env_create' () =
  let open Utilities.Result in
  let env_ptr = allocate mdb_env null in
  let* () = mdb_env_create env_ptr in
  Ok (!@ env_ptr)

let mdb_env_open = foreign "mdb_env_open" (ptr mdb_env @-> string @-> uint @-> mdb_mode_t @-> returning mdb_result)
let mdb_env_close = foreign "mdb_env_close" (ptr mdb_env @-> returning Void)

let mdb_dbi_open = foreign "mdb_dbi_open" (ptr mdb_txn @-> string @-> uint @-> ptr mdb_dbi @-> returning mdb_result)
let mdb_dbi_open' txn name flags =
  let open Utilities.Result in
  let dbi_ptr = allocate mdb_dbi (Unsigned.UInt.of_int 0) in
  let* () = mdb_dbi_open txn name flags dbi_ptr in
  Ok (!@ dbi_ptr)
