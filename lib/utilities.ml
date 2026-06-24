module StringMap = Map.Make (String)
module StringSet = Set.Make (String)

module Result = struct
  let ( let* ) = Result.bind
  let fmap f m = Result.bind m f
end

module Option = struct
  let ( let* ) = Option.bind
end

let print_with_time str =
  let now = Unix.gettimeofday () in
  let tm = Unix.localtime now in
  let orange = "\027[38;5;208m" in
  let reset = "\027[0m" in
  let formatted_time =
    Printf.sprintf "%04d-%02d-%02d %02d:%02d:%02d" (tm.Unix.tm_year + 1900) (tm.Unix.tm_mon + 1)
      tm.Unix.tm_mday tm.Unix.tm_hour tm.Unix.tm_min tm.Unix.tm_sec
  in
  print_endline @@ Printf.sprintf "%s[%s]%s %s" orange formatted_time reset str
