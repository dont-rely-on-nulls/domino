(** Physical storage abstraction.

    This module defines a functor-based storage interface that can be backed by
    any key-value store (etcd, Redis, DynamoDB, PostgreSQL, filesystem, etc.).

    All data is content-addressed: keys are hashes, values are serialized
    objects. The storage is append-only - objects are never modified, only new
    versions are written with new hashes. *)

(** Backend module signature that any storage driver must implement *)
module type BACKEND = sig
  include Configuration.CONFIGURABLE

  type connection
  type error

  val connect : configuration -> (connection, error) result
  (** Connect to the storage backend *)

  val disconnect : connection -> unit
  (** Disconnect from the backend *)

  val get : connection -> Conventions.Hash.t -> (bytes option, error) result
  (** Get a value by its hash. Returns None if not found. *)

  val put : connection -> Conventions.Hash.t -> bytes -> (unit, error) result
  (** Store a value at its hash *)

  val exists : connection -> Conventions.Hash.t -> (bool, error) result
  (** Check if a hash exists *)

  val begin_transaction : connection -> (unit, error) result
  (** Begin a transaction *)

  val with_transaction : connection -> (unit -> 'a) -> ('a option, error) result
  (** Execute a function within a transaction *)

  val get_many :
    connection -> Conventions.Hash.t BatEnum.t -> (bytes option BatEnum.t, error) result
  (** Batch get multiple values *)

  val put_many :
    connection -> (Conventions.Hash.t * bytes) BatEnum.t -> (unit, error) result
  (** Batch put multiple values *)
end

(** Storage operations built on top of a backend *)
module type S = sig
  include Configuration.CONFIGURABLE

  type t
  type error

  val create : configuration -> (t, error) result
  (** Create a storage instance from a backend connection *)

  val close : t -> unit
  (** Close the storage *)

  val store_attribute :
    t -> Conventions.AbstractValue.t -> (Conventions.Hash.t, error) result
  (** Store an attribute value, returns its hash *)

  val load_attribute :
    t ->
    Conventions.Hash.t ->
    (Conventions.AbstractValue.t option, error) result
  (** Load an attribute value by hash *)

  val store_raw : t -> Conventions.Hash.t -> bytes -> (unit, error) result
  (** Store raw bytes at a hash *)

  val load_raw : t -> Conventions.Hash.t -> (bytes option, error) result
  (** Load raw bytes by hash *)

  val exists : t -> Conventions.Hash.t -> (bool, error) result
  (** Check if a hash exists *)

  val with_transaction : t -> (unit -> 'a) -> ('a option, error) result
  (** Execute a function within a transaction *)
end

(** Functor to create storage operations from a backend *)
module Make (B : BACKEND) :
  S with type error = B.error and type configuration = B.configuration = struct
  type configuration = B.configuration
  type t = B.connection
  type error = B.error

  let parse = B.parse
  let create config = B.connect config
  let close = B.disconnect

  let store_attribute conn value =
    let bytes = Marshal.to_bytes value [] in
    let hash = Conventions.AbstractValue.hash value in
    match B.put conn hash bytes with Ok () -> Ok hash | Error e -> Error e

  let load_attribute conn hash =
    match B.get conn hash with
    | Ok (Some bytes) -> Ok (Some (Marshal.from_bytes bytes 0))
    | Ok None -> Ok None
    | Error e -> Error e

  let store_raw conn hash bytes = B.put conn hash bytes
  let load_raw conn hash = B.get conn hash
  let exists conn hash = B.exists conn hash

  let with_transaction = B.with_transaction
end

(** In-memory backend for testing and development *)
module MemoryBackend :
  BACKEND with type error = string and type configuration = unit = struct
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

module Memory = Make (MemoryBackend)
(** Default in-memory storage for development *)
