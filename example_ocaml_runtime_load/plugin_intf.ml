(* A directory of functions: Name -> (Argument -> Result) *)
let registry : (string, (string -> string)) Hashtbl.t = Hashtbl.create 10

let register name f = Hashtbl.replace registry name f