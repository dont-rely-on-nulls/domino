type value = Obj.t
type tuple = (string * value) list

type implementation = {
  membership_criteria : (tuple -> (bool, string) result) option;
  produce : (tuple -> (tuple list, string) result) option;
}

(* TODO: Rethink this instead in terms of a value.
   We need to store the references as a normal relation
   on the multigroup relation catalog. *)
let registry : (string, implementation) Hashtbl.t = Hashtbl.create 32
let mutex = Mutex.create ()

let register symbol impl =
  Mutex.protect mutex (fun () -> Hashtbl.replace registry symbol impl)

let find symbol = Mutex.protect mutex (fun () -> Hashtbl.find_opt registry symbol)

let symbols () =
  Mutex.protect mutex (fun () -> Hashtbl.to_seq_keys registry |> List.of_seq)

let implementation_of_rows ?membership_criteria (rows : tuple list) : implementation =
  { membership_criteria; produce = Some (fun _bindings -> Ok rows) }

let implementation_of_producer ?membership_criteria
    (produce : tuple -> (tuple list, string) result) : implementation =
  { membership_criteria; produce = Some produce }

let implementation_of_unit_producer ?membership_criteria
    (produce : unit -> (tuple list, string) result) : implementation =
  { membership_criteria; produce = Some (fun _bindings -> produce ()) }
