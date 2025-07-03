open Lwt
open Unix

let host = Unix.inet_addr_loopback
let port = 7524

let create_socket () : Lwt_unix.file_descr t =
  let sock = Lwt_unix.socket PF_INET SOCK_STREAM 0 in
  Lwt_unix.set_close_on_exec sock;
  Lwt_unix.setsockopt sock Unix.SO_REUSEADDR true;
  let () = Lwt_unix.set_blocking sock false in
  Lwt_io.write_line Lwt_io.stdout "🔍 Relational Engine Server Started"
  >>= fun () ->
  Lwt_unix.bind sock @@ ADDR_INET (host, port) >>= fun () ->
  Lwt_unix.listen sock 10;
  return sock

let client_read sock maxlen =
  let str = Bytes.create maxlen in
  let rec _read sock acc =
    Lwt_unix.read sock str 0 maxlen >>= fun recvlen ->
    Lwt_io.write_line Lwt_io.stdout
      (Printf.sprintf "[LOG] Received %d bytes from client" recvlen)
    >>= fun () ->
    let acc = acc ^ Bytes.to_string (Bytes.sub str 0 recvlen) in
    if recvlen <= maxlen then Lwt.return acc else _read sock acc
  in
  _read sock String.empty >>= fun x ->
  print_string "RECEIVED: ";
  print_endline x;
  let command = Protocol.command_of_sexp (Sexplib.Sexp.of_string x) in
  (* let command = Protocol.command_of_xml_light_exn (Protocol_conv_xml.Xml_light.of_string x) in *)
  let Ok (commit, locations) = Relation.prepare () in
  let Ok (_, (Disk.Command.Read response)) =
    Planner.Executor.perform commit locations command
  in return @@ Sexplib.Sexp.to_string (Protocol.sexp_of_facts response)
  (* in return @@ Xml.to_string (Protocol.facts_to_xml_light response) *)
  (* match *)
  (* Relation.write_and_retrieve () *)
  (* with *)
  (* | Ok (Disk.Command.Read response) -> *)
      (* return @@ Xml.to_string (Protocol.facts_to_xml_light response) *)
      (* return @@ Sexplib0.Sexp_conv.string_of_sexp (Protocol.sexp_of_facts response) *)
  (* | Error err -> return err *)
[@@warning "-27-8-26"]

let rec socket_read sock =
  (* (Int32.of_int (String.length results))) *)
  Lwt_unix.accept sock >>= fun (client, _) ->
  client_read client 512
  (* >>= fun results -> Lwt_unix.send client (Data_encoding.Binary.to_bytes_exn Data_encoding.Encoding.int32 5l) 0 1 [] *)
  >>= fun results ->
  Lwt_unix.send client (Bytes.of_string results) 0 (String.length results) []
  >>= fun _ ->
  Lwt_unix.wait_write client >>= fun () ->
  Lwt_unix.close client >>= fun () -> socket_read sock

let start () =
  Stdlib.exit
  @@ Lwt_main.run
       ( Lwt_exit.wrap_and_exit (create_socket ()) >>= fun sock ->
         let clean_callback =
           Lwt_exit.register_clean_up_callback ~loc:__LOC__ (fun _ ->
               Lwt_io.write_line Lwt_io.stdout "💥 Shutting down" >>= fun () ->
               Lwt_unix.close sock)
         in
         Lwt_exit.wrap_and_exit (socket_read sock) >>= fun () ->
         let () = Lwt_exit.unregister_clean_up_callback clean_callback in
         Lwt_exit.exit_and_wait 0 )
