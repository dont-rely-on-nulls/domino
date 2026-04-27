type t = {
  loaded_libraries : (string, unit) Hashtbl.t;
  mutex : Mutex.t;
}

let state : t =
  { loaded_libraries = Hashtbl.create 16; mutex = Mutex.create () }

let load_library path : (unit, string) result =
  Mutex.protect state.mutex (fun () ->
      if Hashtbl.mem state.loaded_libraries path then Ok ()
      else
        try
          Dynlink.loadfile path;
          Hashtbl.replace state.loaded_libraries path ();
          Ok ()
        with Dynlink.Error e -> Error (Dynlink.error_message e))

let relation_row_of_tuple (schema : Schema.t) (tuple : Tuple.materialized) :
    Sakura_prl_api.tuple option =
  let rec go acc = function
    | [] -> Some (List.rev acc)
    | (attr_name, _) :: rest -> (
        match Tuple.AttributeMap.find_opt attr_name tuple.attributes with
        | None -> None
        | Some attr -> go ((attr_name, attr.Attribute.value) :: acc) rest)
  in
  go [] schema

let materialized_of_row relation_name (schema : Schema.t)
    (row : Sakura_prl_api.tuple) : Tuple.materialized option =
  let rec go acc = function
    | [] -> Some { Tuple.relation = relation_name; attributes = acc }
    | (attr_name, _) :: rest -> (
        match List.assoc_opt attr_name row with
        | None -> None
        | Some value ->
            go (Tuple.AttributeMap.add attr_name { Attribute.value = value } acc)
              rest)
  in
  go Tuple.AttributeMap.empty schema

let make_generator relation_name schema (impl : Sakura_prl_api.implementation)
    (bindings : Sakura_prl_api.tuple) : Generator.t =
  match impl.produce with
  | None -> fun _ -> Generator.Done
  | Some produce ->
      let rows_cache = lazy (produce bindings) in
      let rec gen pos =
        let i = Option.value ~default:0 pos in
        match Lazy.force rows_cache with
        | Error e -> Generator.Error e
        | Ok rows ->
            if i < 0 || i >= List.length rows then Generator.Done
            else
              match
                materialized_of_row relation_name schema (List.nth rows i)
              with
              | None ->
                  Generator.Error
                    "Plugin row does not conform to predicate schema."
              | Some tuple -> Generator.Value (Tuple.Materialized tuple, gen)
      in
      gen

let make_producer relation_name schema (impl : Sakura_prl_api.implementation) :
    Relation.producer =
  fun bindings -> make_generator relation_name schema impl bindings

let make_membership_criteria schema (impl : Sakura_prl_api.implementation) :
    (string -> Merkle.t option) -> Tuple.t -> bool =
  match impl.membership_criteria with
  | None -> fun _ _ -> false
  | Some check ->
      fun _tree_of -> function
        | Tuple.NonMaterialized _ -> false
        | Tuple.Materialized m -> (
            match relation_row_of_tuple schema m with
            | None -> false
            | Some row -> ( match check row with Ok b -> b | Error _ -> false))
