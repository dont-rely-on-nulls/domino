open Relational_engine
let () =
  Prl.Plugin_api.register "demo.ones"
    {
      Prl.Plugin_api.check =
        Some
          (fun row ->
            match List.assoc_opt "x" row with
            | Some v -> Ok ((Obj.obj v : int) = 1)
            | None -> Ok false);
      open_cursor =
        Some
          (fun () ->
            let emitted = ref false in
            Ok
              {
                Prl.Plugin_api.next =
                  (fun () ->
                    if !emitted then Ok None
                    else (
                      emitted := true;
                      Ok (Some [ ("x", Obj.repr 1) ])));
                close = (fun () -> ());
              });
    }