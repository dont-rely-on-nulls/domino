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
end

module Purity = struct
  type t = Pure | IO [@@deriving sexp]
end

module AbstractValue = struct
  type t = Obj.t
  type value_type = Int | String | Float | Opaque

  let hash (elem : t) =
    Sha256.to_hex (Sha256.string (Bytes.to_string (Marshal.to_bytes elem [])))

  let type_of (elem : t) : value_type =
    let tag = Obj.tag elem in
    if Obj.is_int elem then Int
    else if tag = Obj.string_tag then String
    else if tag = Obj.double_tag then Float
    else Opaque

  let sexp_of_t (v : t) =
    let open Sexplib.Sexp in
    match type_of v with
    | Int -> Atom (string_of_int (Obj.obj v : int))
    | String -> Atom (Obj.obj v : string)
    | Float ->
       let f = (Obj.obj v : float) in
       if Float.is_nan f || Float.is_infinite f
       then Atom "nan"
       else Atom (string_of_float f)
    | Opaque -> Atom "<opaque>"

  let equals a b =
    match (type_of a, type_of b) with
    | (Int, Int) -> (Obj.obj a : int) = (Obj.obj b : int)
    | (String, String) -> (Obj.obj a : string) = (Obj.obj b : string)
    | (Float, Float) -> (Obj.obj a : float) = (Obj.obj b : float)
    | (Opaque, Opaque) -> false
    | (_, _) -> false
end
