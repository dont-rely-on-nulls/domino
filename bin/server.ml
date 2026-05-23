open Relational_engine

let () =
  if Array.length Sys.argv <> 2 then begin
    Printf.eprintf "Usage: %s <config-file>\n%!" Sys.argv.(0);
    exit 1
  end;
  match System.run_from_config Sys.argv.(1) with
  | Error e ->
      Printf.eprintf "Couldn't initialize: %s\n%!" e.name;
      exit 1
  | Ok run -> run ()
