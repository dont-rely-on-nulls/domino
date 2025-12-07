%%% @doc Query Planner and Prepared Statement Execution
%%%
%%% Provides a Lisp-inspired tuple-based DSL for constructing and executing
%%% relational algebra queries. Queries are represented as nested Erlang tuples
%%% that can be validated, optimized, and compiled into iterator pipelines.
%%%
%%% == Query DSL Syntax ==
%%%
%%% The DSL uses nested tuples to represent relational operations:
%%% <pre>
%%% % Scan relation (base case)
%%% {scan, employees}
%%%
%%% % Reference relation (synonym for scan)
%%% {relation, employees}
%%%
%%% % Selection (filter)
%%% {select, {scan, employees}, fun(T) -&gt; maps:get(age, T) &gt; 30 end}
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
%%%   fun(L, R) -&gt; maps:get(dept_id, L) =:= maps:get(id, R) end}
%%%
%%% % Sort
%%% {sort, {scan, employees}, fun(A, B) -&gt; maps:get(age, A) =&lt; maps:get(age, B) end}
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
%%%     fun(T) -&gt; maps:get(age, T) &gt; 30 end},
%%%   [name, dept_name]}
%%% </pre>
%%%
%%% == Usage ==
%%% <pre>
%%% % Prepare a query plan
%%% Plan = query_planner:prepare({select, {scan, employees},
%%%                                fun(T) -&gt; maps:get(age, T) &gt; 30 end}).
%%%
%%% % Execute and get iterator
%%% Iter = query_planner:execute(DB, Plan).
%%%
%%% % Collect all results
%%% Results = query_planner:collect(DB, Plan).
%%% </pre>
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
    {scan, atom()}                                         % Scan relation
  | {relation, atom()}                                     % Reference relation
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
%% Returns an ephemeral relation for top-level operations that can spawn
%% multiple independent iterators. The relation's generator creates fresh
%% iterator pipelines on demand, allowing reproducible results.
%%
%% @param DB The database state
%% @param Plan The prepared query plan
%% @returns Ephemeral relation record
-spec execute(term(), query_plan()) -> #relation{}.
execute(DB, Plan) ->
    create_ephemeral_relation(DB, Plan).

%% @doc Execute a query plan and collect all results.
%%
%% Convenience function that executes the plan and collects all
%% tuples into a list. Uses the ephemeral relation to create an
%% iterator and automatically closes it.
%%
%% If the iterator times out (e.g., when sorting an infinite relation),
%% returns partial results with a warning.
%%
%% @param DB The database state
%% @param Plan The prepared query plan
%% @returns List of result tuples, or partial results on timeout
-spec collect(term(), query_plan()) -> [map()].
collect(DB, Plan) ->
    EphemeralRelation = execute(DB, Plan),
    % Create iterator from the ephemeral relation's generator
    Iter = (EphemeralRelation#relation.generator)(#{}),
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

%% @doc Create an ephemeral relation for the top-level query plan.
%%
%% Uses pure relational operators from relational_operators module to construct
%% the query result. Returns an ephemeral relation with a generator closure that
%% can spawn fresh iterators, allowing reproducible query results.
%%
%% @param DB The database state (for looking up base relations only)
%% @param Plan The query plan
%% @returns Ephemeral relation record
-spec create_ephemeral_relation(term(), query_plan()) -> #relation{}.
create_ephemeral_relation(DB, Plan) ->
    % Compile plan to ephemeral relation using pure operators
    compile_to_relation(DB, Plan).

%% @doc Compute the schema for a query plan.
%%
%% Recursively analyzes the query plan structure to determine the output schema.
%% The schema maps attribute names to their domain/type information.
%%
%% @param DB The database state
%% @param Plan The query plan
%% @returns Schema map
-spec compute_schema(term(), query_plan()) -> map().
compute_schema(DB, {scan, RelationName}) ->
    case maps:get(RelationName, DB#database_state.relations, undefined) of
        undefined ->
            % Relation not found, return empty schema
            #{};
        RelationHash ->
            [Relation] = mnesia:dirty_read(relation, RelationHash),
            Relation#relation.schema
    end;

compute_schema(DB, {relation, RelationName}) ->
    % Same as scan
    compute_schema(DB, {scan, RelationName});

compute_schema(DB, {select, SubPlan, _Predicate}) ->
    % Select doesn't change schema, just filters tuples
    compute_schema(DB, SubPlan);

compute_schema(DB, {project, SubPlan, Attributes}) ->
    % Project filters the schema to only include selected attributes
    SubSchema = compute_schema(DB, SubPlan),
    maps:with(Attributes, SubSchema);

compute_schema(DB, {join, LeftPlan, RightPlan, _Attribute}) ->
    % Join combines schemas from both sides, handling attribute conflicts
    LeftSchema = compute_schema(DB, LeftPlan),
    RightSchema = compute_schema(DB, RightPlan),
    merge_join_schemas(LeftSchema, RightSchema);

compute_schema(DB, {theta_join, LeftPlan, RightPlan, _Predicate}) ->
    % Theta join also combines schemas from both sides, handling attribute conflicts
    LeftSchema = compute_schema(DB, LeftPlan),
    RightSchema = compute_schema(DB, RightPlan),
    merge_join_schemas(LeftSchema, RightSchema);

compute_schema(DB, {sort, SubPlan, _Comparator}) ->
    % Sort doesn't change schema
    compute_schema(DB, SubPlan);

compute_schema(DB, {take, SubPlan, _N}) ->
    % Take doesn't change schema, just limits tuples
    compute_schema(DB, SubPlan);

compute_schema(DB, {rename, OldAttr, NewAttr, SubPlan}) ->
    % Rename changes one attribute name in the schema
    SubSchema = compute_schema(DB, SubPlan),
    case maps:is_key(OldAttr, SubSchema) of
        true ->
            AttrType = maps:get(OldAttr, SubSchema),
            SubSchema1 = maps:remove(OldAttr, SubSchema),
            maps:put(NewAttr, AttrType, SubSchema1);
        false ->
            % Attribute doesn't exist, return unchanged schema
            SubSchema
    end;

compute_schema(DB, {materialize, SubPlan}) ->
    % Materialize doesn't change schema
    compute_schema(DB, SubPlan);

compute_schema(DB, {materialize, SubPlan, _N}) ->
    % Materialize with limit doesn't change schema
    compute_schema(DB, SubPlan);

compute_schema(_DB, _UnknownPlan) ->
    % Unknown plan type, return empty schema
    #{}.

%% @doc Merge schemas for join operations, handling attribute conflicts elegantly.
%%
%% Instead of prefixing conflicting attribute names, this approach maintains
%% both provenances in the schema value. For attributes that exist in both
%% relations, the schema value becomes a list of the original values.
%% This is especially important for natural joins where the join attribute
%% logically represents the same entity from both sides.
%%
%% @param LeftSchema Schema from left relation
%% @param RightSchema Schema from right relation  
%% @returns Merged schema map with provenance lists for conflicts
-spec merge_join_schemas(map(), map()) -> map().
merge_join_schemas(LeftSchema, RightSchema) ->
    maps:fold(
        fun(Key, RightValue, Acc) ->
            case maps:get(Key, Acc, undefined) of
                undefined ->
                    % No conflict, add the right attribute
                    maps:put(Key, RightValue, Acc);
                LeftValue when LeftValue =:= RightValue ->
                    % Same type/domain, keep single value (common in natural joins)
                    maps:put(Key, LeftValue, Acc);
                LeftValue when is_list(LeftValue) ->
                    % Left value is already a list, append if not already present
                    case lists:member(RightValue, LeftValue) of
                        true -> maps:put(Key, LeftValue, Acc);
                        false -> maps:put(Key, LeftValue ++ [RightValue], Acc)
                    end;
                LeftValue ->
                    % Conflict with different types - create provenance list
                    maps:put(Key, [LeftValue, RightValue], Acc)
            end
        end,
        LeftSchema,
        RightSchema
    ).

%% @doc Compute provenance for a query plan.
%%
%% Recursively analyzes the query plan structure to determine the provenance,
%% following the provenance types defined in operations.hrl.
%%
%% @param Plan The query plan
%% @returns Provenance term
-spec compute_provenance(query_plan()) -> term().
compute_provenance({scan, RelationName}) ->
    {base, RelationName};

compute_provenance({relation, RelationName}) ->
    {base, RelationName};

compute_provenance({select, SubPlan, _Predicate}) ->
    % For select, we'd need to represent the predicate constraints
    % For now, simplified without constraint details
    SubProvenance = compute_provenance(SubPlan),
    {select, SubProvenance, #{}};

compute_provenance({project, SubPlan, Attributes}) ->
    SubProvenance = compute_provenance(SubPlan),
    {project, SubProvenance, Attributes};

compute_provenance({join, LeftPlan, RightPlan, _Attribute}) ->
    LeftProvenance = compute_provenance(LeftPlan),
    RightProvenance = compute_provenance(RightPlan),
    {join, LeftProvenance, RightProvenance};

compute_provenance({theta_join, LeftPlan, RightPlan, _Predicate}) ->
    LeftProvenance = compute_provenance(LeftPlan),
    RightProvenance = compute_provenance(RightPlan),
    {join, LeftProvenance, RightProvenance};

compute_provenance({sort, SubPlan, _Comparator}) ->
    % Sort doesn't change provenance
    compute_provenance(SubPlan);

compute_provenance({take, SubPlan, N}) ->
    SubProvenance = compute_provenance(SubPlan),
    {take, SubProvenance, N};

compute_provenance({rename, _OldAttr, _NewAttr, SubPlan}) ->
    % Rename preserves underlying provenance
    compute_provenance(SubPlan);

compute_provenance({materialize, SubPlan}) ->
    % Materialize doesn't change provenance
    compute_provenance(SubPlan);

compute_provenance({materialize, SubPlan, _N}) ->
    % Materialize with limit doesn't change provenance
    compute_provenance(SubPlan);

compute_provenance(_UnknownPlan) ->
    % Unknown plan type
    undefined.

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
        {{error, _} = Error, _} -> Error;
        {_, {error, _} = Error} -> Error
    end;
validate_plan({theta_join, Left, Right, Pred}) when is_function(Pred, 2) ->
    case {validate_plan(Left), validate_plan(Right)} of
        {ok, ok} -> ok;
        {{error, _} = Error, _} -> Error;
        {_, {error, _} = Error} -> Error
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
    %% For iterator-based compilation, use take_iterator
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

%% Helper: get relation from database
-spec get_relation_from_db(term(), atom()) -> #relation{}.
get_relation_from_db(DB, RelationName) ->
    case maps:get(RelationName, DB#database_state.relations, undefined) of
        undefined ->
            erlang:error({relation_not_found, RelationName});
        RelationHash ->
            [Relation] = mnesia:dirty_read(relation, RelationHash),
            Relation
    end.

%% @doc Compile query plan to ephemeral relation using pure relational operators.
%%
%% This is the new approach: build a relation tree using pure operators,
%% then return the final ephemeral relation. No iterators are created until
%% the generator is called.
-spec compile_to_relation(term(), query_plan()) -> #relation{}.
compile_to_relation(DB, {scan, RelationName}) ->
    get_relation_from_db(DB, RelationName);

compile_to_relation(DB, {relation, RelationName}) ->
    get_relation_from_db(DB, RelationName);

compile_to_relation(DB, {select, SubPlan, Predicate}) ->
    SubRelation = compile_to_relation(DB, SubPlan),
    relational_operators:select(SubRelation, Predicate);

compile_to_relation(DB, {project, SubPlan, Attributes}) ->
    SubRelation = compile_to_relation(DB, SubPlan),
    relational_operators:project(SubRelation, Attributes);

compile_to_relation(DB, {join, LeftPlan, RightPlan, Attribute}) ->
    LeftRelation = compile_to_relation(DB, LeftPlan),
    RightRelation = compile_to_relation(DB, RightPlan),
    relational_operators:join(LeftRelation, RightRelation, Attribute);

compile_to_relation(DB, {theta_join, LeftPlan, RightPlan, Predicate}) ->
    LeftRelation = compile_to_relation(DB, LeftPlan),
    RightRelation = compile_to_relation(DB, RightPlan),
    relational_operators:theta_join(LeftRelation, RightRelation, Predicate);

compile_to_relation(DB, {sort, SubPlan, Comparator}) ->
    SubRelation = compile_to_relation(DB, SubPlan),
    relational_operators:sort(SubRelation, Comparator);

compile_to_relation(DB, {take, SubPlan, N}) ->
    SubRelation = compile_to_relation(DB, SubPlan),
    relational_operators:take(SubRelation, N);

compile_to_relation(DB, {rename, OldAttr, NewAttr, SubPlan}) ->
    SubRelation = compile_to_relation(DB, SubPlan),
    relational_operators:rename(SubRelation, #{OldAttr => NewAttr});

compile_to_relation(DB, {materialize, SubPlan}) ->
    % Materialize is different - it creates an iterator and collects results
    % For now, fall back to iterator-based compilation
    % TODO: Create a pure relational materialize operator
    GeneratorFun = fun(_Constraints) ->
        compile_to_iterator(DB, SubPlan)
    end,
    EphemeralName = list_to_atom("materialized_" ++ integer_to_list(erlang:unique_integer([positive]))),
    #relation{
        hash = crypto:hash(sha256, term_to_binary({materialize, SubPlan})),
        name = EphemeralName,
        tree = undefined,
        schema = compute_schema(DB, SubPlan),
        constraints = #{},
        cardinality = unknown,
        generator = GeneratorFun,
        membership_criteria = #{},
        mutability = immutable,
        provenance = compute_provenance(SubPlan)
    };

compile_to_relation(DB, {materialize, SubPlan, N}) ->
    % Materialize with limit
    SubRelation = compile_to_relation(DB, SubPlan),
    TakeRelation = relational_operators:take(SubRelation, N),
    % Then wrap in a materialized form (for now just return take)
    TakeRelation.
