type t = {loaded_libraries: (string, unit) Hashtbl.t; mutex: Mutex.t}

let state : t = {loaded_libraries= Hashtbl.create 16; mutex= Mutex.create ()}

let load_library path : (unit, string) result =
  Mutex.protect state.mutex (fun () ->
      if Hashtbl.mem state.loaded_libraries path then Ok ()
      else
        try
          Dynlink.loadfile path;
          Hashtbl.replace state.loaded_libraries path ();
          Ok ()
        with Dynlink.Error e -> Error (Dynlink.error_message e) )

(* Convert an engine materialized tuple to the plugin's (string * Obj.t) list format. *)
let to_plugin_row (schema : Schema.t) (tuple : Tuple.materialized) : Sakura_prl_api.tuple option =
  let rec go acc = function
    | [] -> Some (List.rev acc)
    | (attr_name, _) :: rest -> (
      match Tuple.AttributeMap.find_opt attr_name tuple.attributes with
      | None -> None
      | Some attr -> go ((attr_name, attr.Attribute.value) :: acc) rest )
  in
  go [] schema

(* Convert a plugin row back to an engine materialized tuple. *)
let of_plugin_row relation_name (schema : Schema.t) (row : Sakura_prl_api.tuple) :
    Tuple.materialized option =
  let rec go acc = function
    | [] -> Some {Tuple.relation= relation_name; attributes= acc}
    | (attr_name, _) :: rest -> (
      match List.assoc_opt attr_name row with
      | None -> None
      | Some value -> go (Tuple.AttributeMap.add attr_name {Attribute.value} acc) rest )
  in
  go Tuple.AttributeMap.empty schema

(* Wrap a list of plugin rows as a generator. Uses an array for O(1) indexing. *)
let rows_to_generator relation_name schema (rows : Sakura_prl_api.tuple list) : Generator.t =
  let arr = Array.of_list rows in
  let n = Array.length arr in
  let rec gen i _pos =
    if i >= n then Generator.Done
    else
      match of_plugin_row relation_name schema arr.(i) with
      | None -> Generator.Error "Plugin row does not conform to predicate schema."
      | Some tuple -> Generator.Value (Tuple.Materialized tuple, gen (i + 1))
  in
  gen 0

let make_generator relation_name schema (impl : Sakura_prl_api.implementation)
    (bindings : Sakura_prl_api.tuple) : Generator.t =
  match impl.produce with
  | None -> fun _ -> Generator.Done
  | Some produce -> (
      let rows_result = lazy (produce bindings) in
      fun _pos ->
        match Lazy.force rows_result with
        | Error e -> Generator.Error e
        | Ok rows -> rows_to_generator relation_name schema rows _pos )

let make_producer relation_name schema (impl : Sakura_prl_api.implementation) : Relation.producer =
 fun bindings -> make_generator relation_name schema impl bindings

let make_membership_criteria schema (impl : Sakura_prl_api.implementation) : Tuple.t -> bool =
  match impl.membership_criteria with
  | None -> fun _ -> false
  | Some check -> (
      (* fun _tree_of -> *)
      function
      | Tuple.NonMaterialized _ -> false
      | Tuple.Materialized m -> (
        match to_plugin_row schema m with
        | None -> false
        | Some row -> ( match check row with Ok b -> b | Error _ -> false ) ) )
