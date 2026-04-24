(** System assembly: registry-based configuration dispatch. This module is the
    composition root. It owns the mapping from configuration tags to concrete
    implemenations and assembles a server from a configuration file. *)

type storage_parcel =
  | StorageParcel :
      (module Management.Physical.S with type t = 't and type error = string)
      * 't
      -> storage_parcel

type transport_parcel =
  | TransportParcel :
      (module Transport.TRANSPORT with type t = 't) * 't
      -> transport_parcel

type storage_provider = Sexplib.Sexp.t -> (storage_parcel, string) result
type transport_provider = Sexplib.Sexp.t -> (transport_parcel, string) result

type registry = {
  storage : storage_provider Utilities.StringMap.t;
  transport : transport_provider Utilities.StringMap.t;
}

let registry : registry =
  let open Utilities.StringMap in
  { storage =
      empty
      |> add "memory" (fun sexp ->
             let ( let* ) = Result.bind in
             let* config = Management.Physical.MemoryBackend.parse sexp in
             let* storage = Management.Physical.Memory.create config in
             Ok (StorageParcel ((module Management.Physical.Memory), storage)))
  ; transport =
      empty
      |> add "tcp" (fun sexp ->
             let ( let* ) = Result.bind in
             let* config = Transport.TCP.parse sexp in
             let transport = Transport.TCP.create config in
             Ok (TransportParcel ((module Transport.TCP), transport))) }


let assemble (config : Configuration.t) : (unit -> unit, string) result =
  let open Utilities.Result in
  let* storage_tag, storage_body =
    Configuration.require_section ~name:"storage"
      ~valid_tags:(Utilities.StringMap.bindings registry.storage |> List.map fst)
      config
  in
  let* storage_provider =
    Utilities.StringMap.find_opt storage_tag registry.storage
    |> Option.to_result
         ~none:(Printf.sprintf "Unknown storage backend: %s" storage_tag)
  in
  let* packed_storage = storage_provider storage_body in
  let* transport_tag, transport_body =
    Configuration.require_section ~name:"transport"
      ~valid_tags:
        (Utilities.StringMap.bindings registry.transport |> List.map fst)
      config
  in
  let* transport_provider =
    Utilities.StringMap.find_opt transport_tag registry.transport
    |> Option.to_result
         ~none:(Printf.sprintf "Unknown transport backend: %s" transport_tag)
  in
  let* packed_transport = transport_provider transport_body in
  let (StorageParcel ((module S), storage)) = packed_storage in
  let (TransportParcel ((module T), transport)) = packed_transport in
  let module C = Catalog.Make (S) in
  let* catalog =
    C.create storage ~prelude_relations:Prelude.Standard.prelude_relations
  in
  let module L = Listener.Make (T) (S) in
  Ok (fun () -> Scl.Executor.set_sessions (Session.create ());
                L.run transport catalog storage)

let run_from_config (path : string) : (unit -> unit, string) result =
  let ( let* ) = Result.bind in
  let* config =
    Configuration.load ~expected_keys:[ "storage"; "transport" ] path
  in
  assemble config
