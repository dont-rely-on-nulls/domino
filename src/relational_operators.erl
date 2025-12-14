%%% @doc Pure Relational Operators
%%%
%%% This module provides pure relational algebra operators that take relations
%%% as input and return new ephemeral relations as output. No database state
%%% is modified. Each operator returns a relation with a generator closure that
%%% can spawn fresh iterators on demand, allowing reproducible query results.
%%%
%%% == Architecture ==
%%%
%%% Relational Operators (Pure Functions):
%%% - Input: One or more #relation{} records
%%% - Output: New ephemeral #relation{} with generator closure
%%% - No side effects, no DB modification
%%% - Generator can be called multiple times for same results
%%%
%%% Iterator Operators (Data Transport):
%%% - Input: Iterator PIDs
%%% - Output: Iterator PID
%%% - Used inside generator closures
%%% - Examples: select_iterator/2, project_iterator/2, take_iterator/2
%%%
%%% @author Nekoma Team
%%% @copyright 2025

-module(relational_operators).

-include("../include/operations.hrl").

-export([
    take/2,
    select/2,
    project/2,
    join/3,
    theta_join/3,
    rename/2,
    sort/2
]).

%%% ============================================================================
%%% Internal Helper Functions
%%% ============================================================================

%% @private
%% Generate a random name for ephemeral relations.
generate_ephemeral_name() ->
    {UUID, _} = uuid:get_v1(uuid:new(self(), erlang)),
    [Base64UUID] = io_lib:format("~s", [base64:encode(UUID)]),
    Base64UUID.

%%% ============================================================================
%%% Relational Operators
%%% ============================================================================

%% @doc Take operator (τ): Returns ephemeral relation with first N tuples.
%%
%% Creates an ephemeral relation containing N arbitrary elements from the
%% source relation. The generator closure captures the source relation and N,
%% spawning fresh iterators on each call.
%%
%% == Example ==
%% <pre>
%% NaturalRel = get_relation(DB, natural),
%% Some100 = relational_operators:take(NaturalRel, 100),
%%
%% %% Can call generator multiple times
%% Iter1 = operations:get_iterator_from_generator(ephemeral, Some100#relation.generator),
%% Results1 = operations:collect_all(Iter1),
%%
%% Iter2 = operations:get_iterator_from_generator(ephemeral, Some100#relation.generator),
%% Results2 = operations:collect_all(Iter2),
%% %% Results1 =:= Results2
%% </pre>
%%
%% @param SourceRelation Source #relation{} record
%% @param N Number of tuples to take
%% @returns Ephemeral #relation{} with generator
-spec take(#relation{}, pos_integer()) -> #relation{}.
take(SourceRelation, N) when is_record(SourceRelation, relation), is_integer(N), N > 0 ->
    %% Compute result cardinality
    ResultCardinality = case SourceRelation#relation.cardinality of
        {finite, M} when M < N -> {finite, M};
        {finite, _} -> {finite, N};
        _ -> {finite, N}
    end,
    Name = generate_ephemeral_name(),
    SourceGen = SourceRelation#relation.generator,
    GeneratorFun = fun() ->
        SourceIter = SourceGen(),
        operations:take_iterator(SourceIter, N)
    end,
    #relation{
        hash = <<>>,
        name = Name,
        tree = undefined,
        schema = SourceRelation#relation.schema,
        constraints = SourceRelation#relation.constraints,
        cardinality = ResultCardinality,
        generator = GeneratorFun,
        membership_criteria = #{},
        provenance = SourceRelation#relation.provenance,
        lineage = {take, N, SourceRelation#relation.lineage}
    }.

%% @doc Select operator (σ): Returns ephemeral relation filtered by predicate.
%%
%% Creates an ephemeral relation containing only tuples that satisfy the
%% predicate. The generator closure captures the source relation and predicate.
%%
%% @param SourceRelation Source #relation{} record
%% @param Predicate Function that takes a tuple and returns boolean
%% @returns Ephemeral #relation{} with generator
-spec select(#relation{}, fun((map()) -> boolean())) -> #relation{}.
select(SourceRelation, Predicate) when is_record(SourceRelation, relation), is_function(Predicate, 1) ->
    Name = generate_ephemeral_name(),

    SourceGen = SourceRelation#relation.generator,
    GeneratorFun = fun() ->
        SourceIter = SourceGen(),
        operations:select_iterator(SourceIter, Predicate)
    end,

    #relation{
        hash = <<>>,
        name = Name,
        tree = undefined,
        schema = SourceRelation#relation.schema,
        constraints = SourceRelation#relation.constraints,
        cardinality = unknown,  % Cannot determine without evaluation
        generator = GeneratorFun,
        membership_criteria = #{},
        provenance = SourceRelation#relation.provenance,
        lineage = {select, Predicate, SourceRelation#relation.lineage}
    }.

%% @doc Project operator (π): Returns ephemeral relation with selected attributes.
%%
%% Creates an ephemeral relation containing only the specified attributes.
%% The generator closure captures the source relation and attribute list.
%%
%% @param SourceRelation Source #relation{} record
%% @param Attributes List of attribute names to keep
%% @returns Ephemeral #relation{} with generator
-spec project(#relation{}, [atom()]) -> #relation{}.
project(SourceRelation, Attributes) when is_record(SourceRelation, relation), is_list(Attributes) ->
    Name = generate_ephemeral_name(),

    %% Compute projected schema
    ProjectedSchema = maps:with(Attributes, SourceRelation#relation.schema),

    SourceGen = SourceRelation#relation.generator,
    GeneratorFun = fun() ->
        SourceIter = SourceGen(),
        operations:project_iterator(SourceIter, Attributes)
    end,

    #relation{
        hash = <<>>,
        name = Name,
        tree = undefined,
        schema = ProjectedSchema,
        constraints = SourceRelation#relation.constraints,
        cardinality = SourceRelation#relation.cardinality,
        generator = GeneratorFun,
        membership_criteria = #{},
        provenance = SourceRelation#relation.provenance,
        lineage = {project, Attributes, SourceRelation#relation.lineage}
    }.

%% @doc Join operator (⋈): Returns ephemeral relation from equijoin.
%%
%% Creates an ephemeral relation by joining two relations on a common attribute.
%% The generator closure captures both source relations and the join attribute.
%%
%% @param LeftRelation Left #relation{} record
%% @param RightRelation Right #relation{} record
%% @param JoinAttribute Attribute name to join on
%% @returns Ephemeral #relation{} with generator
-spec join(#relation{}, #relation{}, atom()) -> #relation{}.
join(LeftRelation, RightRelation, JoinAttribute)
    when is_record(LeftRelation, relation),
         is_record(RightRelation, relation),
         is_atom(JoinAttribute) ->

    Name = list_to_atom(
        "join_" ++ atom_to_list(LeftRelation#relation.name) ++ "_" ++
        atom_to_list(RightRelation#relation.name) ++ "_" ++
        integer_to_list(erlang:unique_integer([positive]))
    ),

    %% Merge schemas
    MergedSchema = maps:merge(LeftRelation#relation.schema, RightRelation#relation.schema),

    LeftGen = LeftRelation#relation.generator,
    RightGen = RightRelation#relation.generator,
    GeneratorFun = fun() ->
        LeftIter = LeftGen(),
        RightIter = RightGen(),
        operations:equijoin_iterator(LeftIter, RightIter, JoinAttribute)
    end,

    #relation{
        hash = <<>>,
        name = Name,
        tree = undefined,
        schema = MergedSchema,
        constraints = #{},
        cardinality = unknown,
        generator = GeneratorFun,
        membership_criteria = #{},
        provenance = {join, LeftRelation#relation.provenance, RightRelation#relation.provenance},
        lineage = {join, JoinAttribute, LeftRelation#relation.lineage, RightRelation#relation.lineage}
    }.

%% @doc Theta Join operator (⋈θ): Returns ephemeral relation from theta join.
%%
%% Creates an ephemeral relation by joining two relations with an arbitrary
%% predicate. The generator closure captures both source relations and predicate.
%%
%% @param LeftRelation Left #relation{} record
%% @param RightRelation Right #relation{} record
%% @param Predicate Function that takes two tuples and returns boolean
%% @returns Ephemeral #relation{} with generator
-spec theta_join(#relation{}, #relation{}, fun((map(), map()) -> boolean())) -> #relation{}.
theta_join(LeftRelation, RightRelation, Predicate)
    when is_record(LeftRelation, relation),
         is_record(RightRelation, relation),
         is_function(Predicate, 2) ->

    Name = list_to_atom(
        "theta_join_" ++ atom_to_list(LeftRelation#relation.name) ++ "_" ++
        atom_to_list(RightRelation#relation.name) ++ "_" ++
        integer_to_list(erlang:unique_integer([positive]))
    ),

    MergedSchema = maps:merge(LeftRelation#relation.schema, RightRelation#relation.schema),

    LeftGen = LeftRelation#relation.generator,
    RightGen = RightRelation#relation.generator,
    GeneratorFun = fun() ->
        LeftIter = LeftGen(),
        RightIter = RightGen(),
        operations:theta_join_iterator(LeftIter, RightIter, Predicate)
    end,

    #relation{
        hash = <<>>,
        name = Name,
        tree = undefined,
        schema = MergedSchema,
        constraints = #{},
        cardinality = unknown,
        generator = GeneratorFun,
        membership_criteria = #{},
        provenance = {join, LeftRelation#relation.provenance, RightRelation#relation.provenance},
        lineage = {theta_join, Predicate, LeftRelation#relation.lineage, RightRelation#relation.lineage}
    }.

%% @doc Rename operator (ρ): Returns ephemeral relation with renamed attributes.
%%
%% Creates an ephemeral relation with attributes renamed according to the mapping.
%% The generator closure captures the source relation and rename mappings.
%%
%% @param SourceRelation Source #relation{} record
%% @param RenameMappings Map of old_name => new_name (atoms or strings)
%% @returns Ephemeral #relation{} with generator
-spec rename(#relation{}, #{atom() => atom()}) -> #relation{}.
rename(SourceRelation, RenameMappings) when is_record(SourceRelation, relation), is_map(RenameMappings) ->
    Name = generate_ephemeral_name(),

    %% Compute renamed schema
    RenamedSchema = maps:fold(
        fun(OldAttr, NewAttr, SchemaAcc) ->
            case maps:get(OldAttr, SchemaAcc, undefined) of
                undefined -> SchemaAcc;
                Type ->
                    SchemaAcc1 = maps:remove(OldAttr, SchemaAcc),
                    maps:put(NewAttr, Type, SchemaAcc1)
            end
        end,
        SourceRelation#relation.schema,
        RenameMappings
    ),

    SourceGen = SourceRelation#relation.generator,
    GeneratorFun = fun() ->
        SourceIter = SourceGen(),
        rename_iterator(SourceIter, RenameMappings)
    end,

    #relation{
        hash = <<>>,
        name = Name,
        tree = undefined,
        schema = RenamedSchema,
        constraints = SourceRelation#relation.constraints,
        cardinality = SourceRelation#relation.cardinality,
        generator = GeneratorFun,
        membership_criteria = #{},
        provenance = SourceRelation#relation.provenance,
        lineage = {rename, RenameMappings, SourceRelation#relation.lineage}
    }.

%% @doc Sort operator: Returns ephemeral relation with sorted tuples.
%%
%% Creates an ephemeral relation with tuples sorted by the comparator.
%% WARNING: This is a blocking operation - it materializes all tuples.
%%
%% @param SourceRelation Source #relation{} record
%% @param Comparator Function that compares two tuples
%% @returns Ephemeral #relation{} with generator
-spec sort(#relation{}, fun((map(), map()) -> boolean())) -> #relation{}.
sort(SourceRelation, Comparator) when is_record(SourceRelation, relation), is_function(Comparator, 2) ->
    Name = generate_ephemeral_name(),

    SourceGen = SourceRelation#relation.generator,
    GeneratorFun = fun() ->
        SourceIter = SourceGen(),
        operations:sort_iterator(SourceIter, Comparator)
    end,

    #relation{
        hash = <<>>,
        name = Name,
        tree = undefined,
        schema = SourceRelation#relation.schema,
        constraints = SourceRelation#relation.constraints,
        cardinality = SourceRelation#relation.cardinality,
        generator = GeneratorFun,
        membership_criteria = #{},
        provenance = SourceRelation#relation.provenance,
        lineage = {sort, Comparator, SourceRelation#relation.lineage}
    }.

%%% Helper Functions

%% @private
%% @doc Rename iterator - applies rename mapping to each tuple
rename_iterator(SourceIter, RenameMappings) ->
    spawn(fun() -> rename_loop(SourceIter, RenameMappings) end).

rename_loop(SourceIter, RenameMappings) ->
    receive
        {next, Caller} ->
            case operations:next_tuple(SourceIter) of
                done ->
                    Caller ! done;
                {ok, Tuple} ->
                    RenamedTuple = maps:fold(
                        fun(OldAttr, NewAttr, TupleAcc) ->
                            case maps:get(OldAttr, TupleAcc, undefined) of
                                undefined -> TupleAcc;
                                Value ->
                                    TupleAcc1 = maps:remove(OldAttr, TupleAcc),
                                    maps:put(NewAttr, Value, TupleAcc1)
                            end
                        end,
                        Tuple,
                        RenameMappings
                    ),
                    Caller ! {tuple, RenamedTuple},
                    rename_loop(SourceIter, RenameMappings)
            end;
        {close, Caller} ->
            operations:close_iterator(SourceIter),
            Caller ! ok
    end.
