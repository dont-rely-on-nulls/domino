module StringMap = Map.Make (String)
module StringSet = Set.Make (String)

module Result = struct
  let ( let* ) = Result.bind
  let fmap f m = Result.bind m f
end

module Option = struct
  let ( let* ) = Option.bind
end
