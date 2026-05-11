(** Cursor registry for streaming tuple consumption.

    Each cursor wraps a [Generator.t] produced by a DRL query and tracks the
    current position so that successive [fetch] calls return the next batch of
    rows without replaying the query. *)

type cursor = {
  id : string;
  mutable generator : Generator.t;
  mutable position : int;
  db_snapshot : Management.Multigroup.multigroup;
  query_sexp : string;
  created_at : float;
}

type t = { tbl : (string, cursor) Hashtbl.t; mutable counter : int }

let create () : t = { tbl = Hashtbl.create 16; counter = 0 }

let register (reg : t) ~(db : Management.Multigroup.multigroup) ~(query : string)
    ~(generator : Generator.t) : cursor =
  let raw = string_of_int reg.counter ^ query ^ db#hash in
  let id = Conventions.Hash.hash_text raw in
  reg.counter <- reg.counter + 1;
  let cur =
    {
      id;
      generator;
      position = 0;
      db_snapshot = db;
      query_sexp = query;
      created_at = Unix.gettimeofday ();
    }
  in
  Hashtbl.replace reg.tbl id cur;
  cur

let fetch (reg : t) ~(id : string) ~(limit : int) :
    (Tuple.materialized list * bool, string) result =
  match Hashtbl.find_opt reg.tbl id with
  | None ->
      Error
        ("The cursor with identifier `" ^ id
       ^ "` was not found in the session registry.")
  | Some cur ->
      let rec go gen pos acc count =
        if count >= limit then (List.rev acc, gen, pos, true)
        else
          match gen (Some pos) with
          | Generator.Done -> (List.rev acc, gen, pos, false)
          (* Generator errors are treated as exhaustion: the partial batch is
           returned with has_more = false and the cursor is auto-removed.
           The caller receives no indication that the stream terminated
           abnormally. When fetch gains a richer return type this arm
           should surface the error message instead. *)
          | Generator.Error _ -> (List.rev acc, gen, pos, false)
          | Generator.Value (t, next) ->
              begin match t with
              | Tuple.Materialized m -> go next (pos + 1) (m :: acc) (count + 1)
              | Tuple.NonMaterialized _ -> go next (pos + 1) acc count
              end
      in
      let rows, gen', pos', has_more = go cur.generator cur.position [] 0 in
      cur.generator <- gen';
      cur.position <- pos';
      if not has_more then Hashtbl.remove reg.tbl id;
      Ok (rows, has_more)

let close (reg : t) ~(id : string) : unit = Hashtbl.remove reg.tbl id

let gc (reg : t) ~(max_age : float) : unit =
  let now = Unix.gettimeofday () in
  let to_remove =
    Hashtbl.fold
      (fun k cur acc ->
        if now -. cur.created_at >= max_age then k :: acc else acc)
      reg.tbl []
  in
  List.iter (Hashtbl.remove reg.tbl) to_remove
