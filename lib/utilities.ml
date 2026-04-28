module StringMap = Map.Make (String)
module StringSet = Set.Make (String)

module Result = struct
  let ( let* ) = Result.bind
end

module Option = struct
  let ( let* ) = Option.bind
end
