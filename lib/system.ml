(** System assembly: registry-based configuration dispatch.
    This module is the composition root. It owns the mapping from configuration
    tags to concrete implementations and assembles a server from a config file. *)

type nt_parcel = NtParcel : (module Nt.S) -> nt_parcel

type transport_parcel =
  | TransportParcel :
      (module Transport.TRANSPORT with type t = 't) * 't
      -> transport_parcel

type nt_provider        = Sexplib.Sexp.t -> (nt_parcel, string) result
type transport_provider = Sexplib.Sexp.t -> (transport_parcel, string) result

type registry = {
  nt        : nt_provider        Utilities.StringMap.t;
  transport : transport_provider Utilities.StringMap.t;
}

let registry : registry =
  let open Utilities.StringMap in
  {
    nt =
      empty
      |> add "memory" (fun _sexp ->
          Ok (NtParcel (module Nt.Memory : Nt.S)))
      |> add "sqlite" (fun _sexp ->
          Ok (NtParcel (module Nt.Sqlite : Nt.S)));
    transport =
      empty
      |> add "tcp" (fun sexp ->
          let ( let* ) = Result.bind in
          let* config = Transport.TCP.parse sexp in
          let transport = Transport.TCP.create config in
          Ok (TransportParcel ((module Transport.TCP), transport)));
  }

let assemble (config : Configuration.t) : (unit -> unit, string) result =
  let open Utilities.Result in
  let* nt_tag, nt_body =
    Configuration.require_section ~name:"nt"
      ~valid_tags:(Utilities.StringMap.bindings registry.nt |> List.map fst)
      config
  in
  let* nt_provider =
    Utilities.StringMap.find_opt nt_tag registry.nt
    |> Option.to_result
         ~none:(Printf.sprintf "Unknown NT backend: %s" nt_tag)
  in
  let* packed_nt = nt_provider nt_body in
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
  let (NtParcel (module NT)) = packed_nt in
  let (TransportParcel ((module T), transport)) = packed_transport in
  let module L = Listener.Make (T) (NT) in
  Ok (fun () -> L.run transport)

let run_from_config (path : string) : (unit -> unit, string) result =
  let ( let* ) = Result.bind in
  let* config =
    Configuration.load ~expected_keys:[ "nt"; "transport" ] path
  in
  assemble config
