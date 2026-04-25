module Hash = struct
  type t = string

  let hash_text text = Sha256.to_hex (Sha256.string text)
  let compare = String.compare
  let sexp_of_t = Sexplib.Std.sexp_of_string
end

module Name = struct
  type t = string

  let sexp_of_t = Sexplib.Std.sexp_of_string
end

module Cardinality = struct
  open Sexplib.Std
  
  type t = Finite of int | ConstrainedFinite | AlephZero | Continuum
  [@@deriving sexp]

  let sexp_of_t = function
    | Finite n -> Sexplib.Sexp.(List [ Atom "Finite"; Atom (string_of_int n) ])
    | ConstrainedFinite -> Sexplib.Sexp.Atom "ConstrainedFinite"
    | AlephZero -> Sexplib.Sexp.Atom "AlephZero"
    | Continuum -> Sexplib.Sexp.Atom "Continuum"
end

module Purity = struct
  type t = Pure | IO
  [@@deriving sexp]
end

module AbstractValue = struct
  type t = Obj.t

  let hash (elem : t) =
    Sha256.to_hex (Sha256.string (Bytes.to_string (Marshal.to_bytes elem [])))

  let sexp_of_t (v : t) =
    let open Sexplib.Sexp in
    if Obj.is_int v then Atom (string_of_int (Obj.obj v : int))
    else
      let tag = Obj.tag v in
      if tag = Obj.string_tag then Atom (Obj.obj v : string)
      else if tag = Obj.double_tag then
        let f = (Obj.obj v : float) in
        if Float.is_nan f || Float.is_infinite f then Atom "nan"
        else Atom (string_of_float f)
      else Atom "<opaque>"
end
