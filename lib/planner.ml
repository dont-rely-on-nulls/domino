module type Action = sig
  (* Names of the attributes & The tuples *)
  type tuples = string list * bytes list list

  val execute : tuples -> int
end

module Scan = struct
  open Disk

  let execute commit locations (entity : string) =
    Executor.read commit locations ~filename:("schema~" ^ entity) |> function
    | Some binary -> (
        let relation =
          Result.map_error (fun _ ->
              "Failed to deserialize relation from binary format.")
          @@ Data_encoding.Binary.of_bytes Schema.Protocol.relation_encoding
               (Bytes.concat Bytes.empty binary)
        in
        match relation with
        | Ok relation ->
            let _attrs =
              List.map
                (fun (a : Schema.Protocol.attribute) ->
                  relation.name ^ "/" ^ a.name)
                relation.attributes
            in
            Executor.read commit locations ~filename:("schema~" ^ entity)
        | Error err -> failwith err)
    | None -> failwith ""
end

module Join = struct
  (* open Disk
     let execute (left_attributes: bytes list) (right_attributes: bytes list) locations =
       (* Redundant operations that are already in the locations, but this is easier to implement now *)
       (* Fix this when we persist the indexes *)
       let left_computed_hashes: (int * string) list = List.mapi (fun index elem -> (index, Interop.Sha256.compute_hash elem)) left_attributes in
       let right_computed_hashes: (int * string) list = List.mapi (fun index elem -> (index, Interop.Sha256.compute_hash elem)) right_attributes in
       let references_locations_right = List.filter_map (fun (index, rhash) -> Option.map (fun (elem: Executor.Location.t) -> index, rhash, elem.references)
                                                                               @@ Executor.StringMap.find_opt rhash locations) right_computed_hashes in
       let indexes =
         List.filter_map (fun (lindex, lhash) -> Option.map (fun (rindex, rhash, _) -> lindex, rindex, lhash, rhash)
                                                 @@ List.find_opt (fun (_rindex, _rhash, references) -> List.exists ((=) lhash) references) references_locations_right) left_computed_hashes
       in List.map (fun (lindex, rindex, _, _) -> List.nth left_attributes lindex, List.nth right_attributes rindex) indexes *)

end

module Filter = struct end
