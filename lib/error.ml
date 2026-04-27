
(** Error types for manipulation operations *)
type t =
  | RelationNotFound of string
  | RelationAlreadyExists of string
  | TupleNotFound of Conventions.Hash.t
  | DuplicateTuple of Conventions.Hash.t
  | ConstraintViolation of string
  | StorageError of string
  | UnrecognizedSublanguage of string
  | MalformedExpression of Sexplib.Sexp.t
  (* TODO: think of a better way to manage and serialize sublanguage errors *)
  | SublanguageError of Sexplib.Sexp.t
  | Conflict of { old_db : Management.Database.t; new_db : Management.Database.t}
  | SyntaxError of string
  | MultigroupNotFound of string

let sexp_of_error e =
  let open Sexplib.Sexp in
  let error e ps = List (Atom e :: ps) in
  let (<+>) key value = List [Atom key; value] in
  match e with
  (* TODO: think of a subset of common attributes that every error should have (message, etc)? *)
  | RelationNotFound s          -> error "relation-not-found"       ["relation"   <+> (Atom s)]
  | RelationAlreadyExists s     -> error "relation-already-exists"  ["relation"   <+> (Atom s)]
  | TupleNotFound h             -> error "tuple-not-found"          ["hash"       <+> (Atom h)]
  | DuplicateTuple h            -> error "duplicate-tuple"          ["hash"       <+> (Atom h)]
  | ConstraintViolation s       -> error "constraint-violation"     ["message"    <+> (Atom s)]
  | StorageError s              -> error "storage-error"            ["message"    <+> (Atom s)]
  | UnrecognizedSublanguage s   -> error "unrecognized-sublanguage" ["tag"        <+> (Atom s)]
  | MalformedExpression s       -> error "malformed-expression"     ["expression" <+> s]
  | SublanguageError s          -> error "sublanguage-error"        ["error"      <+> s]
  | SyntaxError s               -> error "syntax-error"             ["message"    <+> (Atom s)]
  | MultigroupNotFound s        -> error "multigroup-not-found"     ["name"       <+> (Atom s)]
  | Conflict { old_db; new_db } -> error "conflict"                 ["old-hash"   <+> (Atom old_db.hash);
                                                                     "new-hash"   <+> (Atom new_db.hash)]
