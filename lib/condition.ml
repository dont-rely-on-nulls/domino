
class type serializable = object
  method as_sexp : Sexplib.Sexp.t
end

class literal (expr : Sexplib.Sexp.t) : serializable = object
  method as_sexp = expr
end

let of_sexp (expr : Sexplib.Sexp.t) = new literal expr
let of_string (s : string) = new literal (Sexplib.Sexp.Atom s)
let of_list (f : 'a -> serializable) (l : 'a list) : serializable =
  new literal (Sexplib.Sexp.List (List.map (fun e -> (f e)#as_sexp) l))

type t = { name : string; properties : (string, serializable) BatMap.t }

let condition name ?parent props =
  { name;
    properties =
      parent
      |> Option.map (fun { properties; _ } -> properties)
      |> Option.value ~default:BatMap.empty
      |> props }

let ( |=| ) name value = (fun p -> BatMap.add name value p)
let ( & ) l r = (fun p -> r (l p))
let empty = (fun p -> p)

let to_sexp { name; properties } =
  let open Sexplib.Sexp in
  let properties' =
    BatMap.to_seq properties
    |> BatSeq.map (fun (k, v) -> List [Atom k; v#as_sexp])
    |> BatList.of_seq in
  List (Atom name :: properties')
