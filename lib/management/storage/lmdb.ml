module B : Physical.BACKEND with type error = string = struct
  open External.Lmdb
  open Ctypes

  module MDB_Errors = struct
    let mdb_notfound = -30798
  end

  type configuration = { path : string; name : string }
  type connection = { env : mdb_env structure ptr; dbi : mdb_dbi }
  type error = string (* int *) (* FIXME *)

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
    let* tx = mdb_txn_begin' env (current_tx' ()) Unsigned.UInt.zero in
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

  let with_transaction { env; _ } body =
    with_transaction' env body
    |> Result.map_error Int.to_string

  let abort _ = Effect.perform Rollback

  let connect { path; name } =
    begin
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
    end
    |> Result.map_error Int.to_string

  let disconnect { env; _ } =
    mdb_env_close env

  let get { dbi; _ } hash =
    begin
      match mdb_get' (current_tx ()) dbi (Bytes.of_string hash) with
      | Ok x -> Ok (Some x)
      | Error e when e = MDB_Errors.mdb_notfound -> Ok (None)
      | Error e -> Error e
    end
    |> Result.map_error Int.to_string

  let put { dbi; _ } hash value =
    mdb_put' (current_tx ()) dbi (Bytes.of_string hash) value Unsigned.UInt.zero
    |> Result.map_error Int.to_string

  let exists conn hash =
    get conn hash
    |> Result.map Option.is_some

  let lift_result r =
    BatEnum.fold
      (fun acc x ->
        Result.bind acc
          (fun xs -> Result.bind x (fun x -> Ok (x::xs))))
      (Ok [])
      r
    |> Result.map BatList.enum

  let flatten_result = function
    | Error e -> Error e
    | Ok (Error e) -> Error e
    | Ok (Ok x) -> Ok x

  let get_many conn hashes = BatEnum.map (get conn) hashes |> lift_result

  let put_many conn values =
    with_transaction conn
      (fun () ->
        BatEnum.map (fun (k, v) -> put conn k v) values
        |> lift_result
        |> Result.map ignore)
    |> Result.map Option.get
    |> flatten_result

end
