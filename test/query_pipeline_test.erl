-module(query_pipeline_test).
-include_lib("eunit/include/eunit.hrl").
-include("operations.hrl").

%%% Test Suite for Query Pipeline (Volcano Iterator Model)

query_pipeline_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun test_select_iterator/1,
      fun test_project_iterator/1,
      fun test_take_iterator/1,
      fun test_sort_iterator/1,
      fun test_composed_pipeline/1,
      fun test_materialize_finite/1,
      fun test_materialize_infinite/1,
      fun test_complete_query_pipeline/1
     ]}.

%%% Setup and Cleanup

setup() ->
    operations:setup(),
    DB = operations:create_database(test_db),

    %% Create employees relation with test data
    {DB1, _} = operations:create_relation(DB, employees, #{
        name => string,
        age => integer,
        department => string
    }),

    {DB2, _} = operations:create_tuple(DB1, employees, #{name => "Alice", age => 35, department => "Engineering"}),
    {DB3, _} = operations:create_tuple(DB2, employees, #{name => "Bob", age => 28, department => "Sales"}),
    {DB4, _} = operations:create_tuple(DB3, employees, #{name => "Carol", age => 42, department => "Engineering"}),
    {DB5, _} = operations:create_tuple(DB4, employees, #{name => "Dave", age => 31, department => "Marketing"}),
    {DB6, _} = operations:create_tuple(DB5, employees, #{name => "Eve", age => 29, department => "Engineering"}),

    DB6.

cleanup(_DB) ->
    ok.

%%% Tests

test_select_iterator(DB) ->
    %% Filter employees age > 30
    BaseIter = operations:get_tuples_iterator(DB, employees, #{}),
    FilteredIter = operations:select_iterator(BaseIter,
        fun(E) -> maps:get(age, E) > 30 end),
    Results = operations:collect_all(FilteredIter),

    %% Should get 3 employees: Alice (35), Carol (42), Dave (31)
    [
     ?_assertEqual(3, length(Results)),
     ?_assert(lists:all(fun(E) -> maps:get(age, E) > 30 end, Results))
    ].

test_project_iterator(DB) ->
    %% Project to [name, age] only
    BaseIter = operations:get_tuples_iterator(DB, employees, #{}),
    ProjectedIter = operations:project_iterator(BaseIter, [name, age]),
    Results = operations:collect_all(ProjectedIter),

    %% Should have only name and age fields
    [
     ?_assertEqual(5, length(Results)),
     ?_assert(lists:all(fun(E) ->
                           maps:is_key(name, E) andalso
                           maps:is_key(age, E) andalso
                           not maps:is_key(department, E)
                       end, Results))
    ].

test_take_iterator(DB) ->
    %% Take first 3 employees
    BaseIter = operations:get_tuples_iterator(DB, employees, #{}),
    LimitedIter = operations:take_iterator(BaseIter, 3),
    Results = operations:collect_all(LimitedIter),

    [
     ?_assertEqual(3, length(Results))
    ].

test_sort_iterator(DB) ->
    %% Sort by age ascending
    BaseIter = operations:get_tuples_iterator(DB, employees, #{}),
    SortedIter = operations:sort_iterator(BaseIter,
        fun(A, B) -> maps:get(age, A) =< maps:get(age, B) end),
    Results = operations:collect_all(SortedIter),

    Ages = [maps:get(age, E) || E <- Results],

    [
     ?_assertEqual(5, length(Results)),
     ?_assertEqual([28, 29, 31, 35, 42], Ages)
    ].

test_composed_pipeline(DB) ->
    %% Complex pipeline: filter age > 30, sort by age, take 2, project [name]
    Iter1 = operations:get_tuples_iterator(DB, employees, #{}),
    Iter2 = operations:select_iterator(Iter1, fun(E) -> maps:get(age, E) > 30 end),
    Iter3 = operations:sort_iterator(Iter2, fun(A, B) -> maps:get(age, A) =< maps:get(age, B) end),
    Iter4 = operations:take_iterator(Iter3, 2),
    Pipeline = operations:project_iterator(Iter4, [name]),

    Results = operations:collect_all(Pipeline),

    %% Should get 2 oldest employees over 30: Dave (31), Alice (35)
    Names = [maps:get(name, R) || R <- Results],

    [
     ?_assertEqual(2, length(Results)),
     ?_assertEqual(["Dave", "Alice"], Names),
     ?_assert(lists:all(fun(R) -> not maps:is_key(age, R) end, Results))
    ].

test_materialize_finite(DB) ->
    %% Build pipeline and materialize
    Iter1 = operations:get_tuples_iterator(DB, employees, #{}),
    Iter2 = operations:select_iterator(Iter1, fun(E) ->
                maps:get(department, E) =:= "Engineering"
            end),
    Pipeline = operations:project_iterator(Iter2, [name, age]),

    {DB1, EngineersRel} = operations:materialize(DB, Pipeline, engineers),

    %% Verify new relation
    [
     ?_assert(is_record(EngineersRel, relation)),
     ?_assertEqual(engineers, EngineersRel#relation.name),
     ?_assertEqual({finite, 3}, EngineersRel#relation.cardinality),

     %% Can query the materialized relation
     ?_assertMatch([_,_,_],
        operations:collect_all(operations:get_tuples_iterator(DB1, engineers, #{})))
    ].

test_materialize_infinite(DB) ->
    %% Naturals are built-in, no need to create them

    %% Take 100 and materialize
    Pipeline = operations:get_tuples_iterator(DB, natural, #{value => {range, 0, 99}}),
    {_DB2, Naturals100} = operations:materialize(DB, Pipeline, naturals_100),

    [
     ?_assertEqual({finite, 100}, Naturals100#relation.cardinality),
     ?_assertEqual(naturals_100, Naturals100#relation.name)
    ].

test_complete_query_pipeline(DB) ->
    %% Realistic query:
    %% SELECT name, age FROM employees
    %% WHERE age > 28
    %% ORDER BY age DESC
    %% LIMIT 3

    Iter1 = operations:get_tuples_iterator(DB, employees, #{}),
    Iter2 = operations:select_iterator(Iter1, fun(E) -> maps:get(age, E) > 28 end),
    Iter3 = operations:sort_iterator(Iter2, fun(A, B) ->
                maps:get(age, A) >= maps:get(age, B)  % DESC
            end),
    Iter4 = operations:take_iterator(Iter3, 3),
    Pipeline = operations:project_iterator(Iter4, [name, age]),

    {DB1, TopEmployees} = operations:materialize(DB, Pipeline, top_3_employees),

    %% Verify result - NOTE: Relations are unordered sets!
    %% After materialization, tuples come back in hash order, not sort order
    Results = operations:collect_all(operations:get_tuples_iterator(DB1, top_3_employees, #{})),
    Ages = lists:sort(fun(A, B) -> A >= B end, [maps:get(age, R) || R <- Results]),

    [
     ?_assertEqual(3, length(Results)),
     ?_assertEqual([42, 35, 31], Ages),  % Verify we have the right ages (sorted DESC for check)
     ?_assert(lists:all(fun(R) ->
                           Age = maps:get(age, R),
                           lists:member(Age, [42, 35, 31])
                       end, Results)),
     ?_assertEqual({finite, 3}, TopEmployees#relation.cardinality)
    ].
