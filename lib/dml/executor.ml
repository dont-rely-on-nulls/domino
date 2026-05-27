module Make (NT : Nt.S) = struct
  module DrlExec = Drl.Executor.Make (NT)

  module Error = struct
    open Condition
    (* TODO: more structure *)
    let parse_error msg = condition "parse-error" msg empty
    let relation_not_found name = condition "relation-not-found" "Relation not found" ("name" |=| (of_string name))
    let multigroup_not_found name = condition "multigroup-not-found" "Multigroup not found" ("name" |=| (of_string name))
  end

  let ( let* ) = Result.bind

  let parse_fqn (s : string) : (Qualified_name.t, Condition.t) result =
    Qualified_name.try_parse s

  let lookup_mg (ctx : Sublanguage_context.t) (mg_name : string) :
      (Management.Multigroup.multigroup, Condition.t) result =
    match ctx.branch#mg_of mg_name with
    | Some mg -> Ok mg
    | None -> Error (Error.multigroup_not_found mg_name)

  let get_rel (ctx : Sublanguage_context.t) (fqn : Qualified_name.t) =
    let* mg = lookup_mg ctx fqn.mg in
    match NT.get_relation mg fqn.name with
    | Some r -> Ok r
    | None -> Error (Error.relation_not_found (Qualified_name.to_string fqn))

  let retarget target (t : Tuple.materialized) =
    { t with Tuple.relation = target }

  let build_tuple ~relation (attributes : Ast.attr_value list) : Tuple.materialized =
    let attr_map =
      List.fold_left
        (fun acc (name, value) ->
          Tuple.AttributeMap.add name
            { Attribute.value = Drl.Ast.value_to_abstract value }
            acc)
        Tuple.AttributeMap.empty attributes
    in
    { Tuple.relation; attributes = attr_map }

  (* Drain all tuples from a DRL query using ctx.resolve for path resolution.
     Cross-multigroup reads work transparently here. *)
  let drain_query (ctx : Sublanguage_context.t) query =
    let* plan = DrlExec.compile ctx.resolve query in
    let* stream =
      NT.execute_query plan ~rel_name:"dml_drain"
    in
    let rec drain acc =
      match NT.stream_next stream with
      | Error _ -> List.rev acc
      | Ok None -> List.rev acc
      | Ok (Some t) -> drain (t :: acc)
    in
    let tuples = drain [] in
    ignore (NT.stream_close stream);
    Ok tuples

  let attr_val_eq a b =
    Stdlib.( = ) a.Attribute.value b.Attribute.value

  let tuple_matches_pred common (target_t : Tuple.materialized)
      (pred_t : Tuple.materialized) =
    List.for_all (fun attr ->
      match
        ( Tuple.AttributeMap.find_opt attr target_t.Tuple.attributes,
          Tuple.AttributeMap.find_opt attr pred_t.Tuple.attributes )
      with
      | Some a, Some b -> attr_val_eq a b
      | _ -> false)
      common

  let semijoin common target_tuples pred_tuples =
    List.filter
      (fun t -> List.exists (tuple_matches_pred common t) pred_tuples)
      target_tuples

  (* DML mutates tuples in a single mg per statement.  Returns a
     single-element transition delta keyed by that mg's name. *)
  let execute (ctx : Sublanguage_context.t) (stmt : Ast.statement) :
      (Sublanguage_types.transition_delta, Condition.t) result =
    let bh = ctx.write_handle in
    let branch_name = ctx.branch#name in
    let after fqn = Result.bind (lookup_mg ctx fqn.Qualified_name.mg)
        (fun mg -> Ok [ (fqn.Qualified_name.mg, mg) ])
    in
    match stmt with
    | Ast.InsertTuple { relation; attributes } ->
        let* fqn = parse_fqn relation in
        let* rel = get_rel ctx fqn in
        let tuple = build_tuple ~relation:rel#name attributes in
        let* _ =
          NT.create_tuple ~branch_name ~mg_name:fqn.mg ~rel_name:rel#name tuple
        in
        after fqn
    | Ast.InsertTuples { relation; tuples } ->
        let* fqn = parse_fqn relation in
        let* rel = get_rel ctx fqn in
        let tuple_list = List.map (build_tuple ~relation:rel#name) tuples in
        let* _ =
          NT.create_tuples ~branch_name ~mg_name:fqn.mg ~rel_name:rel#name
            tuple_list
        in
        after fqn
    | Ast.DeleteTuple { relation; attributes } ->
        let* fqn = parse_fqn relation in
        let* rel = get_rel ctx fqn in
        let tuple = build_tuple ~relation:rel#name attributes in
        let hash = Hashing.hash_tuple tuple in
        let* () =
          NT.retract_tuple ~branch_name ~mg_name:fqn.mg ~rel_name:rel#name hash
        in
        after fqn
    | Ast.Assign { target; body } ->
        let* fqn = parse_fqn target in
        let* mg  = lookup_mg ctx fqn.mg in
        let* rel = get_rel ctx fqn in
        let* tuples = drain_query ctx body in
        let* _bh, new_mg =
          NT.clear_relation bh mg ~branch_name ~mg_name:fqn.mg
            (rel :> Relation.relation)
        in
        ctx.branch#set_mg ~name:fqn.mg new_mg;
        let* _ =
          NT.create_tuples ~branch_name ~mg_name:fqn.mg ~rel_name:rel#name
            (List.map (retarget rel#name) tuples)
        in
        Ok [ (fqn.mg, new_mg) ]
    | Ast.InsertFrom { target; source } ->
        let* fqn = parse_fqn target in
        let* rel = get_rel ctx fqn in
        let* tuples = drain_query ctx source in
        let* _ =
          NT.create_tuples ~branch_name ~mg_name:fqn.mg ~rel_name:rel#name
            (List.map (retarget rel#name) tuples)
        in
        after fqn
    | Ast.DeleteWhere { target; predicate } ->
        let* fqn = parse_fqn target in
        let* rel = get_rel ctx fqn in
        let* target_tuples = drain_query ctx (Drl.Ast.Base target) in
        let* pred_tuples   = drain_query ctx predicate in
        let attr_names_of = function
          | [] -> []
          | t :: _ ->
              List.map fst (Tuple.AttributeMap.bindings t.Tuple.attributes)
        in
        let target_attrs = attr_names_of target_tuples in
        let pred_attrs   = attr_names_of pred_tuples in
        let common = List.filter (fun n -> List.mem n pred_attrs) target_attrs in
        let to_delete = semijoin common target_tuples pred_tuples in
        let* () =
          List.fold_left
            (fun acc t ->
              let* () = acc in
              let hash = Hashing.hash_tuple (retarget rel#name t) in
              NT.retract_tuple ~branch_name ~mg_name:fqn.mg ~rel_name:rel#name
                hash)
            (Ok ()) to_delete
        in
        after fqn
end

module Memory = Make (Nt.Memory)
