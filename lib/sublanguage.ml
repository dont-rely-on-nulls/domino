include Sublanguage_types
(** Effect classification for sublanguage operations.

    Categorically:
    - Query: a natural transformation between instance functors (DRL).
      Side-effect-free; the database state is unchanged.
    - Transition: a morphism in the category of database states (DDL, DML, ICL,
      DCL, and future TCL). Produces a new state via substitution (Harper, PFPL
      ch. 3-4).

    Future sublanguages:
    - TCL -> Transition (transaction boundaries are state morphisms with
      atomicity constraints)
    - PPL -> new effect class: potentially non-terminating computation
      (Turing-complete; breaks the termination guarantee of DRL)
    - ACL -> a functor restriction: narrows the admissible natural
      transformations based on authorization context *)

module type S = sig
  include Configuration.CONFIGURABLE

  type storage
  type ast
  type error

  val name : string
  val parse_sexp : Sexplib.Sexp.t -> (ast, error) Result.t

  val execute :
    storage -> Management.Multigroup.multigroup -> ast -> (result, error) Result.t

  val sexp_of_error : error -> Sexplib.Sexp.t
end
