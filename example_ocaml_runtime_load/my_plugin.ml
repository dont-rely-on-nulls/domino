let () =
  Plugin_intf.register "greet" (fun name -> "Hello, " ^ name ^ "!")