%%% @doc Query Planner and Prepared Statement Execution
%%%
%%% Provides a Lisp-inspired tuple-based DSL for constructing and executing
%%% relational algebra queries. Queries are represented as nested Erlang tuples
%%% that can be validated, optimized, and compiled into iterator pipelines.
%%%
%%% == Query DSL Syntax ==
%%%
%%% The DSL uses nested tuples to represent relational operations:
%%%
%%% ```
%%% % Scan relation (base case)
%%% {scan, employees}
%%%
%%% % Reference relation (synonym for scan)
%%% {relation, employees}
%%%
%%% % Selection (filter)
%%% {select, {scan, employees}, fun(T) -> maps:get(age, T) > 30 end}
%%%
%%% % Projection
%%% {project, {scan, employees}, [name, age]}
%%%
%%% % Equijoin (natural join on common attribute)
%%% {join, {scan, employees}, {scan, departments}, dept_id}
%%%
%%% % Theta join (join with arbitrary predicate)
%%% {theta_join,
%%%   {scan, employees},
%%%   {scan, departments},
%%%   fun(L, R) -> maps:get(dept_id, L) =:= maps:get(id, R) end}
%%%
%%% % Sort
%%% {sort, {scan, employees}, fun(A, B) -> maps:get(age, A) =< maps:get(age, B) end}
%%%
%%% % Take (limit)
%%% {take, {scan, employees}, 10}
%%%
%%% % Rename attribute
%%% {rename, old_name, new_name, {scan, employees}}
%%%
%%% % Composite query
%%% {project,
%%%   {select,
%%%     {join,
%%%       {scan, employees},
%%%       {scan, departments},
%%%       dept_id},
%%%     fun(T) -> maps:get(age, T) > 30 end},
%%%   [name, dept_name]}
%%% ```
%%%
%%% == Usage ==
%%%
%%% ```
%%% % Prepare a query plan
%%% Plan = query_planner:prepare({select, {scan, employees},
%%%                                fun(T) -> maps:get(age, T) > 30 end}).
%%%
%%% % Execute and get iterator
%%% Iter = query_planner:execute(DB, Plan).
%%%
%%% % Collect all results
%%% Results = query_planner:collect(DB, Plan).
%%% ```
%%%
%%% @author Nekoma Team
%%% @copyright 2025

-module(query_planner).

-export([prepare/1,
         execute/2,
         collect/2,
         explain/1]).

-include("operations.hrl").

%%% Types

-type query_plan() ::
    {scan, atom()}                                          % Scan relation
  | {relation, atom()}                                      % Reference relation
  | {select, query_plan(), fun((map()) -> boolean())}      % Filter
  | {project, query_plan(), [atom()]}                      % Projection
  | {join, query_plan(), query_plan(), atom()}             % Equijoin
  | {theta_join, query_plan(), query_plan(),
     fun((map(), map()) -> boolean())}                     % Theta join
  | {sort, query_plan(), fun((map(), map()) -> boolean())} % Sort
  | {take, query_plan(), pos_integer()}                    % Limit
  | {rename, atom(), atom(), query_plan()}                 % Rename attribute
  | {materialize, query_plan()}                            % Materialize all
  | {materialize, query_plan(), pos_integer()}.            % Materialize N tuples

-export_type([query_plan/0]).

%%% Public API

%% @doc Prepare and validate a query plan.
%%
%% Takes a query plan tuple and validates its structure. Returns the
%% plan unchanged if valid, or throws an error if invalid.
%%
%% @param Plan The query plan to prepare
%% @returns The validated query plan
%% @throws {invalid_plan, Reason}
-spec prepare(query_plan()) -> query_plan().
prepare(Plan) ->
    case validate_plan(Plan) of
        ok -> Plan;
        {error, Reason} -> error({invalid_plan, Reason})
    end.

%% @doc Execute a prepared query plan against a database.
%%
%% Compiles the query plan into an iterator pipeline and returns
%% the iterator handle. The caller must consume the iterator using
%% operations:next_tuple/1 and close it with operations:close_iterator/1.
%%
%% @param DB The database state
%% @param Plan The prepared query plan
%% @returns Iterator process PID
-spec execute(term(), query_plan()) -> pid().
execute(DB, Plan) ->
    compile_to_iterator(DB, Plan).

%% @doc Execute a query plan and collect all results.
%%
%% Convenience function that executes the plan and collects all
%% tuples into a list. Automatically closes the iterator.
%%
%% If the iterator times out (e.g., when sorting an infinite relation),
%% returns partial results with a warning.
%%
%% @param DB The database state
%% @param Plan The prepared query plan
%% @returns List of result tuples, or partial results on timeout
-spec collect(term(), query_plan()) -> [map()].
collect(DB, Plan) ->
    Iter = execute(DB, Plan),
    Results = operations:collect_all(Iter),
    operations:close_iterator(Iter),
    case Results of
        {error, timeout, PartialResults} ->
            io:format("~nWarning: Query timed out. Returning partial results (~b tuples).~n",
                     [length(PartialResults)]),
            io:format("Tip: For infinite relations, use 'take' before operations like 'sort'.~n~n"),
            PartialResults;
        {error, Reason, PartialResults} ->
            io:format("~nWarning: Query failed (~p). Returning partial results (~b tuples).~n~n",
                     [Reason, length(PartialResults)]),
            PartialResults;
        _ ->
            Results
    end.

%% @doc Explain a query plan in human-readable form.
%%
%% Converts the query plan to a readable string representation
%% showing the operation tree.
%%
%% @param Plan The query plan to explain
%% @returns String representation of the plan
-spec explain(query_plan()) -> string().
explain(Plan) ->
    explain_plan(Plan, 0).

%%% Internal Functions

%% Validate query plan structure
-spec validate_plan(query_plan()) -> ok | {error, term()}.
validate_plan({scan, Name}) when is_atom(Name) ->
    ok;
validate_plan({relation, Name}) when is_atom(Name) ->
    ok;
validate_plan({select, SubPlan, Pred}) when is_function(Pred, 1) ->
    validate_plan(SubPlan);
validate_plan({project, SubPlan, Attrs}) when is_list(Attrs) ->
    case lists:all(fun is_atom/1, Attrs) of
        true -> validate_plan(SubPlan);
        false -> {error, {invalid_attributes, Attrs}}
    end;
validate_plan({join, Left, Right, Attr}) when is_atom(Attr) ->
    case {validate_plan(Left), validate_plan(Right)} of
        {ok, ok} -> ok;
        {Error, _} -> Error;
        {_, Error} -> Error
    end;
validate_plan({theta_join, Left, Right, Pred}) when is_function(Pred, 2) ->
    case {validate_plan(Left), validate_plan(Right)} of
        {ok, ok} -> ok;
        {Error, _} -> Error;
        {_, Error} -> Error
    end;
validate_plan({sort, SubPlan, Comp}) when is_function(Comp, 2) ->
    validate_plan(SubPlan);
validate_plan({take, SubPlan, N}) when is_integer(N), N > 0 ->
    validate_plan(SubPlan);
validate_plan({rename, OldAttr, NewAttr, SubPlan}) when is_atom(OldAttr), is_atom(NewAttr) ->
    validate_plan(SubPlan);
validate_plan({materialize, SubPlan}) ->
    validate_plan(SubPlan);
validate_plan({materialize, SubPlan, N}) when is_integer(N), N > 0 ->
    validate_plan(SubPlan);
validate_plan(Plan) ->
    {error, {unknown_operator, Plan}}.

%% Compile query plan to iterator pipeline
-spec compile_to_iterator(term(), query_plan()) -> pid().
compile_to_iterator(DB, {scan, Name}) ->
    operations:get_tuples_iterator(DB, Name);

compile_to_iterator(DB, {relation, Name}) ->
    operations:get_tuples_iterator(DB, Name);

compile_to_iterator(DB, {select, SubPlan, Predicate}) ->
    SubIter = compile_to_iterator(DB, SubPlan),
    operations:select_iterator(SubIter, Predicate);

compile_to_iterator(DB, {project, SubPlan, Attributes}) ->
    SubIter = compile_to_iterator(DB, SubPlan),
    operations:project_iterator(SubIter, Attributes);

compile_to_iterator(DB, {join, LeftPlan, RightPlan, Attribute}) ->
    LeftIter = compile_to_iterator(DB, LeftPlan),
    RightIter = compile_to_iterator(DB, RightPlan),
    operations:equijoin_iterator(LeftIter, RightIter, Attribute);

compile_to_iterator(DB, {theta_join, LeftPlan, RightPlan, Predicate}) ->
    LeftIter = compile_to_iterator(DB, LeftPlan),
    RightIter = compile_to_iterator(DB, RightPlan),
    operations:theta_join_iterator(LeftIter, RightIter, Predicate);

compile_to_iterator(DB, {sort, SubPlan, Comparator}) ->
    SubIter = compile_to_iterator(DB, SubPlan),
    operations:sort_iterator(SubIter, Comparator);

compile_to_iterator(DB, {take, SubPlan, N}) ->
    SubIter = compile_to_iterator(DB, SubPlan),
    operations:take_iterator(SubIter, N);

compile_to_iterator(DB, {rename, OldAttr, NewAttr, SubPlan}) ->
    SubIter = compile_to_iterator(DB, SubPlan),
    rename_iterator(SubIter, OldAttr, NewAttr);

compile_to_iterator(DB, {materialize, SubPlan}) ->
    % Materialize returns all tuples as a "static" iterator
    SubIter = compile_to_iterator(DB, SubPlan),
    Results = operations:collect_all(SubIter),
    operations:close_iterator(SubIter),
    static_iterator(Results);

compile_to_iterator(DB, {materialize, SubPlan, Limit}) ->
    % Materialize with limit
    SubIter = compile_to_iterator(DB, SubPlan),
    LimitedIter = operations:take_iterator(SubIter, Limit),
    Results = operations:collect_all(LimitedIter),
    operations:close_iterator(LimitedIter),
    static_iterator(Results).

%% Static iterator - serves pre-materialized tuples
-spec static_iterator([map()]) -> pid().
static_iterator(Tuples) ->
    spawn(fun() -> static_loop(Tuples) end).

static_loop(Tuples) ->
    receive
        {next, Caller} ->
            case Tuples of
                [] ->
                    Caller ! done,
                    static_loop([]);
                [Tuple | Rest] ->
                    Caller ! {tuple, Tuple},
                    static_loop(Rest)
            end;
        {close, Caller} ->
            Caller ! ok
    end.

%% Rename iterator - renames an attribute in each tuple
-spec rename_iterator(pid(), atom(), atom()) -> pid().
rename_iterator(SourceIter, OldAttr, NewAttr) ->
    spawn(fun() -> rename_loop(SourceIter, OldAttr, NewAttr) end).

rename_loop(SourceIter, OldAttr, NewAttr) ->
    receive
        {next, Caller} ->
            case operations:next_tuple(SourceIter) of
                done ->
                    Caller ! done,
                    rename_loop(SourceIter, OldAttr, NewAttr);
                {ok, Tuple} ->
                    RenamedTuple = case maps:is_key(OldAttr, Tuple) of
                        true ->
                            Value = maps:get(OldAttr, Tuple),
                            Tuple1 = maps:remove(OldAttr, Tuple),
                            maps:put(NewAttr, Value, Tuple1);
                        false ->
                            Tuple
                    end,
                    Caller ! {tuple, RenamedTuple},
                    rename_loop(SourceIter, OldAttr, NewAttr)
            end;
        {close, Caller} ->
            operations:close_iterator(SourceIter),
            Caller ! ok
    end.

%% Pretty-print query plan
-spec explain_plan(query_plan(), non_neg_integer()) -> string().
explain_plan({scan, Name}, Indent) ->
    indent(Indent) ++ "Scan: " ++ atom_to_list(Name) ++ "\n";

explain_plan({relation, Name}, Indent) ->
    indent(Indent) ++ "Relation: " ++ atom_to_list(Name) ++ "\n";

explain_plan({select, SubPlan, _Pred}, Indent) ->
    indent(Indent) ++ "Select (filter)\n" ++
    explain_plan(SubPlan, Indent + 2);

explain_plan({project, SubPlan, Attrs}, Indent) ->
    AttrStr = string:join([atom_to_list(A) || A <- Attrs], ", "),
    indent(Indent) ++ "Project [" ++ AttrStr ++ "]\n" ++
    explain_plan(SubPlan, Indent + 2);

explain_plan({join, LeftPlan, RightPlan, Attr}, Indent) ->
    indent(Indent) ++ "Join on " ++ atom_to_list(Attr) ++ "\n" ++
    explain_plan(LeftPlan, Indent + 2) ++
    explain_plan(RightPlan, Indent + 2);

explain_plan({theta_join, LeftPlan, RightPlan, _Pred}, Indent) ->
    indent(Indent) ++ "Theta Join\n" ++
    explain_plan(LeftPlan, Indent + 2) ++
    explain_plan(RightPlan, Indent + 2);

explain_plan({sort, SubPlan, _Comp}, Indent) ->
    indent(Indent) ++ "Sort\n" ++
    explain_plan(SubPlan, Indent + 2);

explain_plan({take, SubPlan, N}, Indent) ->
    indent(Indent) ++ "Take " ++ integer_to_list(N) ++ "\n" ++
    explain_plan(SubPlan, Indent + 2);

explain_plan({rename, OldAttr, NewAttr, SubPlan}, Indent) ->
    indent(Indent) ++ "Rename " ++ atom_to_list(OldAttr) ++ " -> " ++ atom_to_list(NewAttr) ++ "\n" ++
    explain_plan(SubPlan, Indent + 2);

explain_plan({materialize, SubPlan}, Indent) ->
    indent(Indent) ++ "Materialize (all)\n" ++
    explain_plan(SubPlan, Indent + 2);

explain_plan({materialize, SubPlan, N}, Indent) ->
    indent(Indent) ++ "Materialize (" ++ integer_to_list(N) ++ " tuples)\n" ++
    explain_plan(SubPlan, Indent + 2).

%% Helper: indent string
-spec indent(non_neg_integer()) -> string().
indent(N) -> lists:duplicate(N, $\s).
