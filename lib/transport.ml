module type TRANSPORT = sig
  include Configuration.CONFIGURABLE

  type t
  type connection

  val create : configuration -> t
  val listen : t -> unit
  val accept : t -> connection
  val input : connection -> in_channel
  val output : connection -> out_channel
end

module TCP : TRANSPORT = struct
  type configuration = {
    address_family : Unix.socket_domain;
    sockaddr : Unix.sockaddr;
  }

  type t = Unix.file_descr
  type connection = Unix.file_descr

  let sockaddr_of_fields = function
    | `Inet (addr, port) ->
        Ok
          {
            address_family = Unix.PF_INET;
            sockaddr = Unix.ADDR_INET (Unix.inet_addr_of_string addr, port);
          }
    | `Unix path ->
        Ok { address_family = Unix.PF_UNIX; sockaddr = Unix.ADDR_UNIX path }

  let parse sexp =
    let open Sexplib.Sexp in
    let errorf fmt = Printf.ksprintf (fun s -> Error s) fmt in
    let rec go addr port path = function
      | [] -> (
          match (addr, port, path) with
          | Some a, Some p, None -> sockaddr_of_fields (`Inet (a, p))
          | None, None, Some s -> sockaddr_of_fields (`Unix s)
          | None, None, None -> errorf "transport/tcp: no address fields"
          | _ ->
              errorf "transport/tcp: use (address + port) or (path), not both")
      | List [ Atom "address"; Atom a ] :: rest -> go (Some a) port path rest
      | List [ Atom "port"; Atom p ] :: rest -> (
          match int_of_string_opt p with
          | Some n -> go addr (Some n) path rest
          | None -> errorf "transport/tcp: invalid port: %s" p)
      | List [ Atom "path"; Atom p ] :: rest -> go addr port (Some p) rest
      | bad :: _ -> errorf "transport/tcp: unexpected: %s" (to_string bad)
    in
    match sexp with
    | List children -> go None None None children
    | _ -> Error "transport/tcp: expected a list of fields"

  let string_of_sockaddr = function
    | Unix.ADDR_INET (host, port) ->
        Printf.sprintf "%s:%d" (Unix.string_of_inet_addr host) port
    | Unix.ADDR_UNIX path -> path

  let create { address_family; sockaddr } =
    let socket = Unix.socket address_family Unix.SOCK_STREAM 0 in
    Unix.setsockopt socket Unix.SO_REUSEADDR true;
    Unix.bind socket sockaddr;
    socket

  let listen fd =
    Unix.listen fd 5;
    Printf.printf "Listening on %s\n%!"
      (string_of_sockaddr (Unix.getsockname fd))

  let accept fd =
    let conn, _ = Unix.accept fd in
    conn

  let input = Unix.in_channel_of_descr
  let output = Unix.out_channel_of_descr
end
