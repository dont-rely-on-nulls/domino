let not_implemented = Obj.magic ()

module Backend : Physical.BACKEND = struct
  open External.Lmdb
  open Ctypes

  module MDB_Errors = struct
    let mdb_notfound = -30798
  end

  type configuration = { path : string; name : string }
  type connection = { env : mdb_env structure ptr; dbi : mdb_dbi }
  type error = int

  type _ Effect.t +=
     | Current: mdb_txn structure ptr Effect.t
     | Rollback: unit Effect.t

  let parse (expr : Sexplib.Sexp.t) =
    let open Utilities.Result in
    let* keys = Configuration.map_of_sexp expr in
    let* path = Utilities.StringMap.find_opt "path" keys
                |> Option.to_result ~none:"Missing key: path"
                |> fmap (Configuration.atom_of_sexp)
    in
    let* name = Utilities.StringMap.find_opt "name" keys
                |> Option.to_result ~none:"Missing key: name"
                |> fmap (Configuration.atom_of_sexp)
    in
    Ok { path; name }

  let current_tx () = Effect.perform Current
  let current_tx' () = try current_tx () with Effect.Unhandled Current -> (from_voidp mdb_txn null)

  let with_transaction' env body =
    let open Utilities.Result in
    let success = ref false in
    let* tx = mdb_txn_begin' env (current_tx' ()) (Unsigned.UInt.of_int 0) in
    Fun.protect
      (fun () ->
        match body () with
        | x ->
           let* () = mdb_txn_commit tx in
           success := true;
           Ok (Some x)
        | effect Current, k -> Effect.Deep.continue k tx
        (* Should a transaction abort be an error value? *)
        | effect Rollback, _ ->
           let* () = mdb_txn_abort tx in
           success := true;
           Ok None)
      ~finally:(fun () -> if not !success then mdb_txn_abort tx |> ignore)

  let with_transaction { env; _ } body = with_transaction' env body

  let connect { path; name } =
    let open Utilities.Result in
    let* env = mdb_env_create' () in
    match mdb_env_open env path (Unsigned.UInt.of_int 0) PosixTypes.Mode.zero with
    | Error x ->
       mdb_env_close env;
       Error x
    | Ok () ->
       Result.bind
         (with_transaction' env
            (fun () ->
              let* dbi = mdb_dbi_open' (current_tx ()) name (Unsigned.UInt.of_int 0) in
              Ok { env; dbi }))
         Option.get

  let disconnect { env; _ } =
    mdb_env_close env

  let begin_transaction _conn = not_implemented

  let commit _conn = not_implemented

  let rollback _conn = not_implemented

  let get { dbi; _ } hash =
    match mdb_get' (current_tx ()) dbi (Bytes.of_string hash) with
    | Ok x -> Ok (Some x)
    | Error e when e = MDB_Errors.mdb_notfound -> Ok (None)
    | Error e -> Error e

  let put _conn _hash _value = not_implemented

  let exists _conn _hash = not_implemented

  let get_many _conn _hashes = not_implemented

  let put_many _conn _values = not_implemented
end
[@warning "-38-32"]
