module Make (Storage : Management.Physical.S) = struct
  module Alg = Algebra.Make (Storage)
  module Ops = Manipulation.Make (Storage)

  type error =
    | ParseError of string
    | RelationNotFound of string
    | IoPredicateNotAllowed of string
    | AlgebraError of Algebra.error

  let sexp_of_error e =
    let open Sexplib.Sexp in
    match e with
    | ParseError s -> List [ Atom "parse-error"; Atom s ]
    | RelationNotFound s -> List [ Atom "relation-not-found"; Atom s ]
    | IoPredicateNotAllowed s -> List [ Atom "io-predicate-not-allowed"; Atom s ]
    | AlgebraError (Algebra.StorageError s) ->
        List [ Atom "storage-error"; Atom s ]
    | AlgebraError (Algebra.GeneratorError s) ->
        List [ Atom "generator-error"; Atom s ]

  let wrap = Result.map_error (fun e -> AlgebraError e)
  let ast_value_to_abstract = Ast.value_to_abstract

  let select_semijoin storage source filter =
    let common =
      List.filter_map
        (fun (n, _) ->
          if List.exists (fun (m, _) -> m = n) filter.Relation.schema then
            Some n
          else None)
        source.Relation.schema
    in
    let source_attrs = List.map fst source.Relation.schema in
    Result.bind (wrap (Alg.equijoin storage common source filter)) (fun joined ->
        wrap (Alg.project storage source_attrs joined))

  let relation_of_materialized ~name ~schema (tuples : Tuple.materialized list) =
    let rec list_generator = function
      | [] -> fun _ -> Generator.Done
      | t :: rest ->
          fun _ ->
            Generator.Value (Tuple.Materialized t, list_generator rest)
    in
    Relation.make ~hash:None ~name ~schema ~tree:None ~constraints:None
      ~cardinality:(Conventions.Cardinality.Finite (List.length tuples))
      ~generator:(Some (list_generator tuples))
      ~membership_criteria:(fun _ _ -> true)
      ~provenance:Relation.Provenance.Undefined
      ~lineage:(Relation.Lineage.Base name)

  let bindings_of_tuple attrs (tuple : Tuple.materialized) : Prl.Plugin_api.row option
      =
    let rec go acc = function
      | [] -> Some (List.rev acc)
      | attr :: rest -> (
          match Tuple.AttributeMap.find_opt attr tuple.attributes with
          | None -> None
          | Some v -> go ((attr, v.Attribute.value) :: acc) rest)
    in
    go [] attrs

  let procedural_rows_from_bindings storage (rel : Relation.t) attrs
      (bindings_source : Relation.t) =
    Result.bind
      (Result.map_error (fun e -> AlgebraError e)
         (Alg.materialize storage bindings_source))
      (fun source_rows ->
        let rec loop acc = function
          | [] -> Ok (List.rev acc)
          | row :: rest -> (
              match bindings_of_tuple attrs row with
              | None -> loop acc rest
              | Some bindings ->
                  let seeded = Prl.Runtime.hydrate_relation_with_bindings rel bindings in
                  Result.bind
                    (Result.map_error (fun e -> AlgebraError e)
                       (Alg.materialize storage seeded))
                    (fun produced ->
                      loop (List.rev_append produced acc) rest))
        in
        loop [] source_rows)

  let rec execute (storage : Storage.t) (db : Management.Database.t)
      (q : Ast.query) : (Relation.t, error) Result.t =
    let ( >>= ) = Result.bind in
    match q with
    | Ast.Base name -> (
        match Prl.Runtime.find_binding name with
        | Some { Prl.Runtime.purity = Prl.Runtime.Io; _ } ->
            Error (IoPredicateNotAllowed name)
        | _ ->
        match Ops.get_relation db ~name with
        | None -> Error (RelationNotFound name)
        | Some rel -> Ok (Prl.Runtime.hydrate_relation rel))
    | Ast.Const pairs ->
        Ok
          (Alg.const_relation
             (List.map (fun (k, v) -> (k, ast_value_to_abstract v)) pairs))
    | Ast.Select (Ast.Const pairs, Ast.Base name) ->
        let filter =
          Alg.const_relation
            (List.map (fun (k, v) -> (k, ast_value_to_abstract v)) pairs)
        in
        let bindings = List.map (fun (k, v) -> (k, ast_value_to_abstract v)) pairs in
        let source_result =
          match Ops.get_relation db ~name with
          | None -> Error (RelationNotFound name)
          | Some rel -> (
              match Prl.Runtime.find_binding name with
              | Some { Prl.Runtime.purity = Prl.Runtime.Io; _ } ->
                  Error (IoPredicateNotAllowed name)
              | Some _ -> Ok (Prl.Runtime.hydrate_relation_with_bindings rel bindings)
              | None -> Ok rel)
        in
        source_result >>= fun source -> select_semijoin storage source filter
    | Ast.Select (filter_q, source_q) ->
        execute storage db filter_q >>= fun filter ->
        execute storage db source_q >>= fun source ->
        select_semijoin storage source filter
    | Ast.Join (attrs, q1, q2) ->
        let try_left_bound_join name other_q =
          match Ops.get_relation db ~name with
          | None -> Error (RelationNotFound name)
          | Some rel -> (
              match Prl.Runtime.find_binding name with
              | Some { Prl.Runtime.purity = Prl.Runtime.Io; _ } ->
                  Error (IoPredicateNotAllowed name)
              | None ->
                  execute storage db (Ast.Base name) >>= fun left_rel ->
                  execute storage db other_q >>= fun right_rel ->
                  wrap (Alg.equijoin storage attrs left_rel right_rel)
              | Some _ ->
                  execute storage db other_q >>= fun other_rel ->
                  procedural_rows_from_bindings storage rel attrs other_rel
                  >>= fun produced_rows ->
                  let left_rel =
                    relation_of_materialized
                      ~name:(name ^ "_bound") ~schema:rel.Relation.schema
                      produced_rows
                  in
                  wrap (Alg.equijoin storage attrs left_rel other_rel))
        in
        let try_right_bound_join other_q name =
          match Ops.get_relation db ~name with
          | None -> Error (RelationNotFound name)
          | Some rel -> (
              match Prl.Runtime.find_binding name with
              | Some { Prl.Runtime.purity = Prl.Runtime.Io; _ } ->
                  Error (IoPredicateNotAllowed name)
              | None ->
                  execute storage db other_q >>= fun left_rel ->
                  execute storage db (Ast.Base name) >>= fun right_rel ->
                  wrap (Alg.equijoin storage attrs left_rel right_rel)
              | Some _ ->
                  execute storage db other_q >>= fun other_rel ->
                  procedural_rows_from_bindings storage rel attrs other_rel
                  >>= fun produced_rows ->
                  let right_rel =
                    relation_of_materialized
                      ~name:(name ^ "_bound") ~schema:rel.Relation.schema
                      produced_rows
                  in
                  wrap (Alg.equijoin storage attrs other_rel right_rel))
        in
        (match (q1, q2) with
        | Ast.Base name, other_q when attrs <> [] -> try_left_bound_join name other_q
        | other_q, Ast.Base name when attrs <> [] -> try_right_bound_join other_q name
        | _ ->
            execute storage db q1 >>= fun r1 ->
            execute storage db q2 >>= fun r2 ->
            wrap (Alg.equijoin storage attrs r1 r2))
    | Ast.Project (attrs, q) ->
        execute storage db q >>= fun rel -> wrap (Alg.project storage attrs rel)
    | Ast.Rename (renames, q) ->
        execute storage db q >>= fun rel ->
        wrap (Alg.rename storage renames rel)
    | Ast.Cartesian (q1, q2) ->
        execute storage db q1 >>= fun r1 ->
        execute storage db q2 >>= fun r2 ->
        (* Cartesian product = join on no attrs *)
        wrap (Alg.equijoin storage [] r1 r2)
    | Ast.Union (q1, q2) ->
        execute storage db q1 >>= fun r1 ->
        execute storage db q2 >>= fun r2 -> wrap (Alg.union storage r1 r2)
    | Ast.Diff (q1, q2) ->
        execute storage db q1 >>= fun r1 ->
        execute storage db q2 >>= fun r2 -> wrap (Alg.diff storage r1 r2)
    | Ast.Take (n, q) ->
        execute storage db q >>= fun rel -> wrap (Alg.take storage n rel)
end

module Memory = Make (Management.Physical.Memory)
