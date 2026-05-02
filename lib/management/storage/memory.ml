(** In-memory backend for testing and development *)
module MemoryBackend :
  Physical.BACKEND with type error = string and type configuration = unit = struct
  type configuration = unit

  type connection = {
    data : (string, bytes) Hashtbl.t;
    mutable in_transaction : bool;
    mutable transaction_buffer : (string * bytes) list;
  }

  type error = string

  let parse sexp =
    match sexp with
    | Sexplib.Sexp.List [] -> Ok ()
    | _ ->
        Error
          (Printf.sprintf "memory backend takes no configuration, got: %s"
             (Sexplib.Sexp.to_string sexp))

  let connect () =
    Ok
      {
        data = Hashtbl.create 1024;
        in_transaction = false;
        transaction_buffer = [];
      }

  let disconnect _ = ()
  let get conn hash = Ok (Hashtbl.find_opt conn.data hash)

  let put conn hash value =
    if conn.in_transaction then begin
      conn.transaction_buffer <- (hash, value) :: conn.transaction_buffer;
      Ok ()
    end
    else begin
      Hashtbl.replace conn.data hash value;
      Ok ()
    end

  let exists conn hash = Ok (Hashtbl.mem conn.data hash)

  let begin_transaction conn =
    if conn.in_transaction then Error "Already in transaction"
    else begin
      conn.in_transaction <- true;
      conn.transaction_buffer <- [];
      Ok ()
    end

  let commit conn =
    if not conn.in_transaction then Error "Not in transaction"
    else begin
      List.iter
        (fun (hash, value) -> Hashtbl.replace conn.data hash value)
        conn.transaction_buffer;
      conn.in_transaction <- false;
      conn.transaction_buffer <- [];
      Ok ()
    end

  let rollback conn =
    if not conn.in_transaction then Error "Not in transaction"
    else begin
      conn.in_transaction <- false;
      conn.transaction_buffer <- [];
      Ok ()
    end

  let get_many conn hashes =
    Ok (List.map (fun h -> Hashtbl.find_opt conn.data h) hashes)

  let put_many conn pairs =
    List.iter
      (fun (hash, value) ->
        if conn.in_transaction then
          conn.transaction_buffer <- (hash, value) :: conn.transaction_buffer
        else Hashtbl.replace conn.data hash value)
      pairs;
    Ok ()
end
