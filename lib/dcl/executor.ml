module Make (Storage : Management.Physical.S with type error = string) = struct
  module Ops = Manipulation.Make (Storage)
  module Branch = Management.Branch.Make (Storage)
  module MergeOps = Management.Merge.Make (Storage) (Ops)

  type error =
    | ParseError of string
    | BranchError of Error.t
    | BranchNotFound of string
    | NoDatabaseAtHash of Conventions.Hash.t
    | StorageError of string
    | MergeError of Error.t
    | Conflict of string

  let sexp_of_error e =
    let open Sexplib.Sexp in
    match e with
    | ParseError s -> List [ Atom "parse-error"; Atom s ]
    | BranchError s -> List [ Atom "branch-error"; Error.sexp_of_error s ]
    | BranchNotFound s -> List [ Atom "branch-not-found"; Atom s ]
    | NoDatabaseAtHash s -> List [ Atom "no-database-at-hash"; Atom s ]
    | StorageError s -> List [ Atom "storage-error"; Atom s ]
    | MergeError s -> List [ Atom "merge-error"; Error.sexp_of_error s ]
    | Conflict s -> List [ Atom "conflict"; Atom s ]

  let convert_strategy : Ast.merge_strategy -> Management.Merge.strategy =
    function
    | Ast.PreferLeft -> Management.Merge.PreferLeft
    | Ast.PreferRight -> Management.Merge.PreferRight
    | Ast.RevertToAncestor -> Management.Merge.RevertToAncestor

  type exec_result =
    | DbResult of Management.Database.t * string
    | Switch of string
    | NewMultigroup of string

  let execute (storage : Storage.t) (db : Management.Database.t)
      (stmt : Ast.statement) : (exec_result, error) result =
    match stmt with
    | Ast.CreateBranch { name; hash } -> (
        let tip =
          match hash with Some h -> h | None -> db.Management.Database.hash
        in
        match Branch.create storage ~name ~tip with
        | Error e -> Error (StorageError e)
        | Ok () -> Ok (DbResult (db, "Branch " ^ name ^ " created")))
    | Ast.Checkout branch_name -> (
        match Branch.get_tip storage branch_name with
        | Error e -> Error (StorageError e)
        | Ok None -> Error (BranchNotFound branch_name)
        | Ok (Some tip) -> (
            match Branch.checkout storage branch_name with
            | Error e -> Error (StorageError e)
            | Ok () -> (
                match Ops.load_database storage tip with
                | Error e -> Error (BranchError e)
                | Ok None -> Error (NoDatabaseAtHash tip)
                | Ok (Some loaded_db) ->
                    Ok (DbResult (loaded_db, "HEAD:" ^ branch_name)))))
    | Ast.GetHead -> (
        match Branch.get_head storage with
        | Error e -> Error (StorageError e)
        | Ok None -> Ok (DbResult (db, "HEAD is unset"))
        | Ok (Some name) -> Ok (DbResult (db, "HEAD:" ^ name)))
    | Ast.GetBranchTip name -> (
        match Branch.get_tip storage name with
        | Error e -> Error (StorageError e)
        | Ok None -> Error (BranchNotFound name)
        | Ok (Some hash) -> Ok (DbResult (db, "branch:" ^ name ^ "=" ^ hash)))
    | Ast.UpdateBranchTip { name; hash } -> (
        match Branch.update_tip storage ~name ~tip:hash with
        | Error e -> Error (StorageError e)
        | Ok () -> Ok (DbResult (db, "Branch " ^ name ^ " updated")))
    | Ast.Merge { left; right; strategy } -> (
        match (Branch.get_tip storage left, Branch.get_tip storage right) with
        | Error e, _ | _, Error e -> Error (StorageError e)
        | Ok None, _ -> Error (BranchNotFound left)
        | _, Ok None -> Error (BranchNotFound right)
        | Ok (Some left_tip), Ok (Some right_tip) -> (
            let strat = convert_strategy strategy in
            match
              MergeOps.merge ~storage ~strategy:strat ~left_tip ~right_tip
            with
            | Error e -> Error (MergeError e)
            | Ok (Management.Merge.Failed conflicts) ->
                let msgs =
                  List.map
                    (function
                      | Management.Merge.TupleConflict { relation; hash } ->
                          "tuple conflict in " ^ relation ^ " (" ^ hash ^ ")"
                      | Management.Merge.SchemaConflict r ->
                          "schema conflict in " ^ r
                      | Management.Merge.ConstraintConflict r ->
                          "constraint conflict in " ^ r)
                    conflicts
                in
                Error (Conflict ("Merge failed: " ^ String.concat "; " msgs))
            | Ok (Management.Merge.Clean merged_db) ->
                let _ =
                  Branch.update_tip storage ~name:left
                    ~tip:merged_db.Management.Database.hash
                in
                Ok (DbResult (merged_db, "Merged:" ^ right ^ "→" ^ left))))
    | Ast.Use multigroup -> Ok (Switch multigroup)
    | Ast.CreateMultigroup name -> Ok (NewMultigroup name)
end

module Memory = Make (Management.Physical.Memory)
