open Ctypes
open PosixTypes
open Foreign

type mdb_result = Success | Failure of int

let mdb_result_of_int = function
  | 0 -> Success
  | x -> Failure x

let int_of_mdb_result = function
  | Success -> 0
  | Failure x -> x

let mdb_mode_t = mode_t

let mdb_result = view ~read:mdb_result_of_int ~write:int_of_mdb_result int

let mdb_env = ptr Void

let mdb_env_create = foreign "mdb_env_create" (ptr mdb_env @-> returning mdb_result)

let mdb_env_open = foreign "mdb_env_open" (ptr mdb_env @-> string @-> uint @-> mdb_mode_t @-> returning mdb_result)
let mdb_env_close = foreign "mdb_env_close" (ptr mdb_env @-> returning Void)
