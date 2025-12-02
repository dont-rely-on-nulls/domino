-module(infinite_relations_test).
-include_lib("eunit/include/eunit.hrl").
-include("operations.hrl").

%%% Test Suite for Infinite Relations

infinite_relations_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun test_create_naturals/1,
      fun test_naturals_with_constraints/1,
      fun test_integers_generator/1,
      fun test_rationals_generator/1,
      fun test_take_operator/1,
      fun test_finite_relation_cardinality/1
     ]}.

%%% Setup and Cleanup

setup() ->
    operations:setup(),
    operations:create_database(test_db).

cleanup(_DB) ->
    ok.

%%% Helper Functions

%% Strip metadata from tuples for comparison
strip_meta(Tuple) when is_map(Tuple) ->
    maps:remove(meta, Tuple);
strip_meta(Tuples) when is_list(Tuples) ->
    [strip_meta(T) || T <- Tuples].

%%% Tests

test_create_naturals(DB) ->
    %% Naturals are now built-in to every database
    %% Just retrieve the relation to verify it exists
    {ok, NaturalsHash} = operations:get_relation_hash(DB, naturals),
    [NaturalsRel] = mnesia:dirty_read(relation, NaturalsHash),

    [
     ?_assert(is_record(NaturalsRel, relation)),
     ?_assertEqual(naturals, NaturalsRel#relation.name),
     ?_assertEqual(aleph_zero, NaturalsRel#relation.cardinality),
     ?_assertEqual({generators, naturals}, NaturalsRel#relation.generator)
    ].

test_naturals_with_constraints(DB) ->
    %% Naturals are built-in, just query them
    %% Query with range constraint [0, 9]
    Iterator = operations:get_tuples_iterator(DB, naturals, #{value => {range, 0, 9}}),
    Tuples = operations:collect_all(Iterator),
    DataOnly = strip_meta(Tuples),

    %% Should get exactly 10 tuples: {0, 1, 2, ..., 9}
    [
     ?_assertEqual(10, length(Tuples)),
     ?_assert(lists:member(#{value => 0}, DataOnly)),
     ?_assert(lists:member(#{value => 5}, DataOnly)),
     ?_assert(lists:member(#{value => 9}, DataOnly))
    ].

test_integers_generator(DB) ->
    %% Integers are built-in
    {ok, IntegersHash} = operations:get_relation_hash(DB, integers),
    [Integers] = mnesia:dirty_read(relation, IntegersHash),

    %% Query with range constraint [-5, 5]
    Iterator = operations:get_tuples_iterator(DB, integers, #{value => {range, -5, 5}}),
    Tuples = operations:collect_all(Iterator),
    DataOnly = strip_meta(Tuples),

    %% Should get 11 tuples: {-5, -4, ..., 0, ..., 4, 5}
    [
     ?_assertEqual(aleph_zero, Integers#relation.cardinality),
     ?_assertEqual(11, length(Tuples)),
     ?_assert(lists:member(#{value => 0}, DataOnly)),
     ?_assert(lists:member(#{value => -5}, DataOnly)),
     ?_assert(lists:member(#{value => 5}, DataOnly))
    ].

test_rationals_generator(DB) ->
    %% Rationals are built-in
    {ok, RationalsHash} = operations:get_relation_hash(DB, rationals),
    [Rationals] = mnesia:dirty_read(relation, RationalsHash),

    %% Take first 10 rationals using constraints
    Iterator = operations:get_tuples_iterator(DB, rationals, #{}),

    %% Just get a few tuples to verify generator works
    {ok, First} = operations:next_tuple(Iterator),
    {ok, Second} = operations:next_tuple(Iterator),
    operations:close_iterator(Iterator),

    [
     ?_assertEqual(aleph_zero, Rationals#relation.cardinality),
     ?_assert(is_map(First)),
     ?_assert(maps:is_key(numerator, First)),
     ?_assert(maps:is_key(denominator, First)),
     ?_assert(is_map(Second))
    ].

test_take_operator(DB) ->
    %% Naturals are built-in, just use them
    %% Take 100 naturals
    {_DB2, Naturals100} = operations:take(DB, naturals, 100),

    %% Verify result is finite
    [
     ?_assertEqual({finite, 100}, Naturals100#relation.cardinality),
     ?_assertEqual({take, naturals, 100}, Naturals100#relation.generator)
    ].

test_finite_relation_cardinality(DB) ->
    %% Create finite relation
    {DB1, _} = operations:create_relation(DB, users, #{name => string, age => integer}),

    %% Add tuple
    {_DB2, UpdatedRel} = operations:create_tuple(DB1, users, #{name => "Alice", age => 30}),

    %% Cardinality should be 1
    [
     ?_assertEqual({finite, 1}, UpdatedRel#relation.cardinality)
    ].
