open Relational_engine

module Test_Nt = Nt.Memory
module Test_Drl = Drl.Executor.Make (Test_Nt)
module Test_Branch = Branch.Make (Test_Nt)
module Test_Session = Session.Make (Test_Nt)
module Test_Ctx = Sublanguage_context.Make (Test_Nt)

let ( let* ) = Result.bind

let with_proper_attrs tuples =
  List.map (fun (name, value) -> (name, { Attribute.value = value })) tuples

let tuple_from_attrs name attrs =
  { Tuple.relation = name; attributes = Tuple.AttributeMap.of_list (with_proper_attrs attrs) }

let seed_fixture
      (branch : Test_Branch.branch)
      (multigroup : Management.Multigroup.multigroup)
    : (unit, Condition.t) result =
  let branch_handle = branch#branch_handle in
  let branch_name = branch#name in
  let mg_name = multigroup#name in
  let relation ~name ~schema ~tuples =
    (name, schema,
     List.map (tuple_from_attrs name) tuples)
  in
  let fixture =
    [
      relation
        ~name:"fruit"
        ~schema:(Schema.empty
                 |> Schema.add "name" "string"
                 |> Schema.add "flavour" "string")
        ~tuples:[
          [("name", Obj.magic "peach"); ("flavour", Obj.magic "sweet")];
          [("name", Obj.magic "grape"); ("flavour", Obj.magic "sweet")];
          [("name", Obj.magic "lemon"); ("flavour", Obj.magic "sour")]
        ];

      relation
        ~name:"drink"
        ~schema:(Schema.empty
                 |> Schema.add "name" "string"
                 |> Schema.add "fruit" "string")
        ~tuples:[
          [("name", Obj.magic "lemonade");    ("fruit", Obj.magic "lemon")];
          [("name", Obj.magic "wine");        ("fruit", Obj.magic "grape")];
          [("name", Obj.magic "grape juice"); ("fruit", Obj.magic "grape")]
        ]
    ]
  in
  let create (name, schema, tuples) =
    let* _ = Test_Nt.create_relation branch_handle multigroup ~branch_name ~name ~schema in
    let* _ = Test_Nt.create_tuples ~branch_name ~mg_name ~rel_name:name tuples in
    Ok ()
  in
  let sequencing a b = Result.bind a (fun _ -> b) in
  fixture
  |> List.map create
  |> List.fold_left sequencing (Ok ())
  |> Result.map ignore

let make_fixture () =
  let* _ = Test_Nt.initialize () in (* FIXME: this *leaks* on the C++ side! *)
  let branch_name = "fixture" in
  let multigroup_name = "fixture" in
  let claims = "fixture" in
  let* session = Test_Session.open_session claims ~branch_name in
  let branch = session#branch in
  let multigroup = begin
      branch#add_multigroup ~name:multigroup_name;
      branch#mg_of multigroup_name
      |> Option.get
    end in
  let* _ = seed_fixture branch multigroup in
  let ctx = Test_Ctx.make_ctx session claims in
  Ok ctx

module Error = struct
  open Condition

  let invalid_argument msg = condition "invalid-argument" msg empty
  let expected_tuple tuple_expr relation_expr =
    condition "expected-tuple" "An expected tuple is missing from the result"
      ("tuple" |=| (of_sexp tuple_expr) &
       "relation" |=| (of_sexp relation_expr))
end

let assert_ok = function
  | Ok x -> x
  | Error condition -> raise (Invalid_argument (Condition.to_string condition))

let drain { Sublanguage_types.rows; _ } = rows         (* TODO *)

let contains tuple ts =
  List.find_opt (Tuple.materialized_equal tuple) ts |> Option.is_some

let ensure_contains t ts =
  let tuple = tuple_from_attrs "" t in
  Result.bind ts (fun ts ->
      if contains tuple ts
      then Ok ts
      else Error (Error.expected_tuple
                    (Tuple.sexp_of_materialized tuple)
                    (Sexplib.Sexp.List (List.map Tuple.sexp_of_materialized ts))))

let with_cursor = function
  | Sublanguage_types.Cursor cursor -> Ok (drain cursor)
  | _ -> Error (Error.invalid_argument "Expected cursor")

let%test_unit "Ensure that `base` translates to a scan over its argument" =
  begin
    let* ctx = make_fixture () in
    let* result = Test_Drl.execute ctx (Drl.Ast.Base "fixture:fruit") in
    with_cursor result
    |> ensure_contains [("name", Obj.magic "peach"); ("flavour", Obj.magic "sweet")]
    |> ensure_contains [("name", Obj.magic "grape"); ("flavour", Obj.magic "sweet")]
    |> ensure_contains [("name", Obj.magic "lemon"); ("flavour", Obj.magic "sour")]
    |> Result.map ignore
  end
  |> assert_ok

let%test_unit "Ensure that `project` returns a projection of it's argument" =
  begin
    let* ctx = make_fixture () in
    let* result = Test_Drl.execute ctx (Drl.Ast.Project (["fruit"], Drl.Ast.Base "fixture:drink")) in
    with_cursor result
    |> ensure_contains [("fruit", Obj.magic "grape")]
    |> ensure_contains [("fruit", Obj.magic "lemon")]
    |> Result.map ignore
  end
  |> assert_ok
