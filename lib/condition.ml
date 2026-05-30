
class type serializable = object
  method as_sexp : Sexplib.Sexp.t
  method as_string : string
end

class literal (expr : Sexplib.Sexp.t) : serializable = object
  method as_sexp = expr
  method as_string = Sexplib.Sexp.to_string_hum expr
end

let of_sexp (expr : Sexplib.Sexp.t) = new literal expr
let of_string (s : string) = new literal (Sexplib.Sexp.Atom s)
let of_int (n : int) = of_string (Int.to_string n)
let of_list (f : 'a -> serializable) (l : 'a list) : serializable =
  new literal (Sexplib.Sexp.List (List.map (fun e -> (f e)#as_sexp) l))

type t = { name : string;
           message : string;
           backtrace : Printexc.raw_backtrace;
           properties : (string, serializable) BatMap.t }

let condition name message ?parent props =
  { name;
    message;
    backtrace = Printexc.get_callstack 64;
    properties =
      parent
      |> Option.map (fun { properties; _ } -> properties)
      |> Option.value ~default:BatMap.empty
      |> props }

let ( |=| ) name value = (fun p -> BatMap.add name value p)
let ( & ) l r = (fun p -> r (l p))
let empty = (fun p -> p)

let to_sexp { name; message; properties; _ } =
  let open Sexplib.Sexp in
  let properties' =
    properties
    |> BatMap.to_seq
    |> BatSeq.map (fun (k, v) -> List [Atom k; v#as_sexp])
    |> BatList.of_seq in
  List (Atom name :: Atom message :: properties')

let to_string { name; message; properties; backtrace } =
  let properties' =
    properties
    |> BatMap.to_seq
    |> BatSeq.to_string
         ~first:"" ~last:"" ~sep:"\n"
         (fun (k, v) -> "\t" ^ k ^ ": " ^ v#as_string)
  in
  let backtrace' = Printexc.raw_backtrace_to_string backtrace in
  name ^ ": " ^ message ^ "\n" ^ properties' ^ "\n\nBacktrace:\n" ^ backtrace'
