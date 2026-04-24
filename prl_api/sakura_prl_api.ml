type value = Obj.t
type row = (string * value) list

type implementation = {
  check : (row -> (bool, string) result) option;
  produce : (row -> (row list, string) result) option;
}

let registry : (string, implementation) Hashtbl.t = Hashtbl.create 32
let mutex = Mutex.create ()

let register symbol impl =
  Mutex.protect mutex (fun () -> Hashtbl.replace registry symbol impl)

let find symbol = Mutex.protect mutex (fun () -> Hashtbl.find_opt registry symbol)

let symbols () =
  Mutex.protect mutex (fun () -> Hashtbl.to_seq_keys registry |> List.of_seq)

let implementation_of_rows ?check (rows : row list) : implementation =
  { check; produce = Some (fun _bindings -> Ok rows) }

let implementation_of_producer ?check
    (produce : row -> (row list, string) result) : implementation =
  { check; produce = Some produce }

let implementation_of_unit_producer ?check
    (produce : unit -> (row list, string) result) : implementation =
  { check; produce = Some (fun _bindings -> produce ()) }
