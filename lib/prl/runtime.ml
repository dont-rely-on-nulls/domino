type purity = Pure | Io

type binding = {
  relation_name : string;
  symbol : string;
  purity : purity;
  cardinality : Conventions.Cardinality.t;
}

type t = {
  loaded_libraries : (string, unit) Hashtbl.t;
  bindings : (string, binding) Hashtbl.t;
  mutex : Mutex.t;
}

let state : t =
  {
    loaded_libraries = Hashtbl.create 16;
    bindings = Hashtbl.create 64;
    mutex = Mutex.create ();
  }

let normalize_name name = Qualified_name.(parse name |> to_key)

let with_lock f = Mutex.protect state.mutex f

let convert_dynlink_error (e : Dynlink.error) = Dynlink.error_message e

let load_library path : (unit, string) result =
  with_lock (fun () ->
      if Hashtbl.mem state.loaded_libraries path then Ok ()
      else
        try
          Dynlink.loadfile path;
          Hashtbl.replace state.loaded_libraries path ();
          Ok ()
        with Dynlink.Error e -> Error (convert_dynlink_error e))

let bind (binding : binding) : unit =
  with_lock (fun () ->
      Hashtbl.replace state.bindings binding.relation_name binding)

let unbind relation_name : unit =
  with_lock (fun () -> Hashtbl.remove state.bindings (normalize_name relation_name))

let find_binding relation_name : binding option =
  with_lock (fun () -> Hashtbl.find_opt state.bindings (normalize_name relation_name))

let list_bindings () : binding list =
  with_lock (fun () -> Hashtbl.to_seq_values state.bindings |> List.of_seq)

let relation_row_of_tuple (schema : Schema.t) (tuple : Tuple.materialized) :
    Plugin_api.row option =
  let rec go acc = function
    | [] -> Some (List.rev acc)
    | (attr_name, _) :: rest -> (
        match Tuple.AttributeMap.find_opt attr_name tuple.attributes with
        | None -> None
        | Some attr -> go ((attr_name, attr.Attribute.value) :: acc) rest)
  in
  go [] schema

let materialized_of_row relation_name (schema : Schema.t) (row : Plugin_api.row) :
    Tuple.materialized option =
  let rec go acc = function
    | [] -> Some { Tuple.relation = relation_name; attributes = acc }
    | (attr_name, _) :: rest -> (
        match List.assoc_opt attr_name row with
        | None -> None
        | Some value ->
            let attrs =
              Tuple.AttributeMap.add attr_name { Attribute.value = value } acc
            in
            go attrs rest)
  in
  go Tuple.AttributeMap.empty schema

let make_generator relation_name schema (impl : Plugin_api.implementation)
    (bindings : Plugin_api.row) : Generator.t =
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
              let row = List.nth rows i in
              match materialized_of_row relation_name schema row with
              | None ->
                  Generator.Error
                    "Plugin row does not conform to predicate schema."
              | Some tuple ->
                  Generator.Value (Tuple.Materialized tuple, gen)
      in
      gen

let make_membership_criteria schema (impl : Plugin_api.implementation) :
    (string -> Merkle.t option) -> Tuple.t -> bool =
  match impl.check with
  | None -> fun _ _ -> false
  | Some check ->
      fun _tree_of tuple ->
        match tuple with
        | Tuple.NonMaterialized _ -> false
        | Tuple.Materialized m -> (
            match relation_row_of_tuple schema m with
            | None -> false
            | Some row -> (
                match check row with Ok b -> b | Error _ -> false))

let hydrate_relation (relation : Relation.t) : Relation.t =
  let empty_bindings : Plugin_api.row = [] in
  match find_binding relation.Relation.name with
  | None -> relation
  | Some binding -> (
      match Plugin_api.find binding.symbol with
      | None -> relation
      | Some impl ->
          {
            relation with
            generator =
              Some
                (make_generator relation.Relation.name relation.Relation.schema
                   impl empty_bindings);
            membership_criteria =
              make_membership_criteria relation.Relation.schema impl;
            cardinality = binding.cardinality;
          })

let hydrate_relation_with_bindings (relation : Relation.t)
    (bindings : Plugin_api.row) : Relation.t =
  match find_binding relation.Relation.name with
  | None -> relation
  | Some binding -> (
      match Plugin_api.find binding.symbol with
      | None -> relation
      | Some impl ->
          {
            relation with
            generator =
              Some
                (make_generator relation.Relation.name relation.Relation.schema
                   impl bindings);
            membership_criteria =
              make_membership_criteria relation.Relation.schema impl;
            cardinality = binding.cardinality;
          })
