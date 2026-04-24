let () =
  let rec repl () =
    print_string "> "; flush stdout;
    match String.split_on_char ' ' (read_line ()) with
    | ["load"; path] ->
        (try 
          Dynlink.loadfile path; 
          print_endline "Library loaded." 
        with Dynlink.Error e -> print_endline (Dynlink.error_message e));
        repl ()
    | ["call"; name; arg] ->
        (try
          let f = Hashtbl.find Plugin_intf.registry name in
          print_endline ("Result: " ^ f arg)
        with Not_found -> print_endline "Function not found.");
        repl ()
    | ["quit"] -> ()
    | _ -> print_endline "Usage: load <path> | call <name> <arg> | quit"; repl ()
  in
  print_endline "Host Ready. Use 'load' then 'call'.";
  repl ()