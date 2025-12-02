-module(function_relations_test).
-include_lib("eunit/include/eunit.hrl").
-include("operations.hrl").

%%% Function Relations Test Suite
%%%
%%% Testing infinite relations that represent arithmetic operations.
%%% These work like functions but backwards too (Prolog-style)
%%%
%%% - Plus: {(a, b, sum) | sum = a + b }
%%% - Times: {(a, b, product) | product = a * b}
%%% - Minus: {(a, b, difference) | difference = a - b}
%%% - Divide: {(a, b, quotient, remainder) | a = b*quotient + remainder}

function_relations_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun test_plus_both_operands_constrained/1,
      fun test_plus_a_and_sum_constrained/1,
      fun test_plus_b_and_sum_constrained/1,
      fun test_plus_only_sum_constrained/1,
      fun test_plus_sum_range_constrained/1,
      fun test_plus_insufficient_constraints/1,
      fun test_times_both_operands_constrained/1,
      fun test_times_insufficient_constraints/1,
      fun test_minus_both_operands_constrained/1,
      fun test_divide_both_operands_constrained/1,
      fun test_divide_avoids_zero/1,
      fun test_plus_join_with_finite/1,
      fun test_materialized_plus_relation/1
     ]}.

%%% Setup and Cleanup

setup() ->
    operations:setup(),
    DB = operations:create_database(test_db),

    % Integers and naturals are now built-in to every database
    % Just create the function relations using the new format

    % Create function relations - note how their schemas reference the domain relations
    {DB1, _Plus} = operations:create_infinite_relation(DB, #infinite_relation{
        name = plus,
        schema = #{a => integers, b => integers, sum => integers},
        generator = {generators, plus},
        membership_criteria = #{},
        cardinality = aleph_zero
    }),

    {DB2, _Times} = operations:create_infinite_relation(DB1, #infinite_relation{
        name = times,
        schema = #{a => integers, b => integers, product => integers},
        generator = {generators, times},
        membership_criteria = #{},
        cardinality = aleph_zero
    }),

    {DB3, _Minus} = operations:create_infinite_relation(DB2, #infinite_relation{
        name = minus,
        schema = #{a => integers, b => integers, difference => integers},
        generator = {generators, minus},
        membership_criteria = #{},
        cardinality = aleph_zero
    }),

    {DB4, _Divide} = operations:create_infinite_relation(DB3, #infinite_relation{
        name = divide,
        schema = #{a => integers, b => integers, quotient => integers, remainder => naturals},
        generator = {generators, divide},
        membership_criteria = #{},
        cardinality = aleph_zero
    }),

    DB4.

cleanup(_DB) ->
    ok.

%%% Helper Functions

%% Strip metadata from tuples for comparison
strip_meta(Tuple) when is_map(Tuple) ->
    maps:remove(meta, Tuple);
strip_meta(Tuples) when is_list(Tuples) ->
    [strip_meta(T) || T <- Tuples].

%%% Plus Relation Tests

test_plus_both_operands_constrained(DB) ->
    % Both operands bounded: a ∈ [1,3], b ∈ [2,4]
    % TODO: eventually this should be a proper join like Integer[A], Integer[B], Plus[A,B,Sum]
    %       but for now just pass constraints directly
    Iterator = operations:get_tuples_iterator(DB, plus, #{
        a => {range, 1, 3},
        b => {range, 2, 4}
    }),
    Results = operations:collect_all(Iterator),
    DataOnly = strip_meta(Results),

    % should get 9 tuples total (3 × 3 = 9)
    % like (1,2,3), (1,3,4), (1,4,5), (2,2,4), ..., (3,4,7)
    [
     ?_assertEqual(9, length(Results)),
     ?_assert(lists:member(#{a => 1, b => 2, sum => 3}, DataOnly)),
     ?_assert(lists:member(#{a => 2, b => 3, sum => 5}, DataOnly)),
     ?_assert(lists:member(#{a => 3, b => 4, sum => 7}, DataOnly)),
     ?_assert(lists:all(fun(#{a := A, b := B, sum := S}) ->
                           S =:= A + B
                       end, DataOnly))
    ].

test_plus_a_and_sum_constrained(DB) ->
    % Test reversibility - given A and Sum, compute B
    % a ∈ [1,3], sum ∈ [5,7], so b = sum - a
    % This is like asking: Plus[A, ?, Sum]
    Iterator = operations:get_tuples_iterator(DB, plus, #{
        a => {range, 1, 3},
        sum => {range, 5, 7}
    }),
    Results = operations:collect_all(Iterator),
    DataOnly = strip_meta(Results),

    % Expected tuples: (1,4,5), (1,5,6), (1,6,7), (2,3,5), (2,4,6), (2,5,7), (3,2,5), (3,3,6), (3,4,7)
    % (filtered to only keep where b >= 0)
    [
     ?_assert(length(Results) > 0),
     ?_assert(lists:all(fun(#{a := A, b := B, sum := S}) ->
                           S =:= A + B andalso B >= 0
                       end, DataOnly)),
     ?_assert(lists:member(#{a => 1, b => 4, sum => 5}, DataOnly)),
     ?_assert(lists:member(#{a => 3, b => 4, sum => 7}, DataOnly))
    ].

test_plus_b_and_sum_constrained(DB) ->
    % Another reversibility test - now given B and Sum, find A
    % b ∈ [2,4], sum ∈ [5,6]  →  a = sum - b
    % Like: Plus[?, B, Sum]
    Iterator = operations:get_tuples_iterator(DB, plus, #{
        b => {range, 2, 4},
        sum => {range, 5, 6}
    }),
    Results = operations:collect_all(Iterator),
    DataOnly = strip_meta(Results),

    [
     ?_assert(length(Results) > 0),
     ?_assert(lists:all(fun(#{a := A, b := B, sum := S}) ->
                           S =:= A + B andalso A >= 0
                       end, DataOnly)),
     ?_assert(lists:member(#{a => 3, b => 2, sum => 5}, DataOnly)),
     ?_assert(lists:member(#{a => 2, b => 4, sum => 6}, DataOnly))
    ].

test_plus_only_sum_constrained(DB) ->
    % Only constrain the sum - should give us all ways to add up to 5
    % Plus[?, ?, 5]  generates: (0,5), (1,4), (2,3), (3,2), (4,1), (5,0)
    Iterator = operations:get_tuples_iterator(DB, plus, #{
        sum => {eq, 5}
    }),
    Results = operations:collect_all(Iterator),
    DataOnly = strip_meta(Results),

    [
     ?_assertEqual(6, length(Results)),
     ?_assert(lists:member(#{a => 0, b => 5, sum => 5}, DataOnly)),
     ?_assert(lists:member(#{a => 2, b => 3, sum => 5}, DataOnly)),
     ?_assert(lists:member(#{a => 5, b => 0, sum => 5}, DataOnly)),
     ?_assert(lists:all(fun(#{a := A, b := B, sum := S}) ->
                           S =:= A + B andalso S =:= 5
                       end, DataOnly))
    ].

test_plus_sum_range_constrained(DB) ->
    % sum in a range [2,3] instead of single value
    Iterator = operations:get_tuples_iterator(DB, plus, #{
        sum => {range, 2, 3}
    }),
    Results = operations:collect_all(Iterator),
    DataOnly = strip_meta(Results),

    % For sum=2: (0,2), (1,1), (2,0) → 3 pairs
    % For sum=3: (0,3), (1,2), (2,1), (3,0) → 4 pairs
    % Total = 7
    [
     ?_assertEqual(7, length(Results)),
     ?_assert(lists:all(fun(#{sum := S}) -> S >= 2 andalso S =< 3 end, DataOnly)),
     ?_assert(lists:member(#{a => 0, b => 2, sum => 2}, DataOnly)),
     ?_assert(lists:member(#{a => 3, b => 0, sum => 3}, DataOnly))
    ].

test_plus_insufficient_constraints(DB) ->
    % Try querying with zero constraints - should fail since we'd get infinite results
    Iterator = operations:get_tuples_iterator(DB, plus, #{}),
    Result = operations:next_tuple(Iterator),

    [
     ?_assertMatch({error, {unbounded_plus, _}}, Result)
    ].

%%% Times (multiplication) Tests

test_times_both_operands_constrained(DB) ->
    % Basic multiplication with both inputs bounded
    Iterator = operations:get_tuples_iterator(DB, times, #{
        a => {range, 2, 4},
        b => {range, 3, 5}
    }),
    Results = operations:collect_all(Iterator),
    DataOnly = strip_meta(Results),

    % 3×3 = 9 tuples
    [
     ?_assertEqual(9, length(Results)),
     ?_assert(lists:member(#{a => 2, b => 3, product => 6}, DataOnly)),
     ?_assert(lists:member(#{a => 3, b => 4, product => 12}, DataOnly)),
     ?_assert(lists:member(#{a => 4, b => 5, product => 20}, DataOnly)),
     ?_assert(lists:all(fun(#{a := A, b := B, product := P}) ->
                           P =:= A * B
                       end, DataOnly))
    ].

test_times_insufficient_constraints(DB) ->
    % Times doesn't support full reversibility yet (would need factorization)
    % So constraining just one operand should error
    Iterator = operations:get_tuples_iterator(DB, times, #{a => {range, 1, 5}}),
    Result = operations:next_tuple(Iterator),

    [
     ?_assertMatch({error, {unbounded_times, _}}, Result)
    ].

%%% Minus (subtraction) Tests

test_minus_both_operands_constrained(DB) ->
    % subtraction with both operands bounded
    Iterator = operations:get_tuples_iterator(DB, minus, #{
        a => {range, 5, 7},
        b => {range, 2, 3}
    }),
    Results = operations:collect_all(Iterator),
    DataOnly = strip_meta(Results),

    % 3 × 2 = 6 tuples
    [
     ?_assertEqual(6, length(Results)),
     ?_assert(lists:member(#{a => 5, b => 2, difference => 3}, DataOnly)),
     ?_assert(lists:member(#{a => 7, b => 3, difference => 4}, DataOnly)),
     ?_assert(lists:all(fun(#{a := A, b := B, difference := D}) ->
                           D =:= A - B
                       end, DataOnly))
    ].

%%% Divide (integer division) Tests

test_divide_both_operands_constrained(DB) ->
    % Integer division with quotient and remainder
    Iterator = operations:get_tuples_iterator(DB, divide, #{
        a => {range, 10, 12},
        b => {range, 3, 4}
    }),
    Results = operations:collect_all(Iterator),
    DataOnly = strip_meta(Results),

    % 3 × 2 = 6 tuples
    [
     ?_assertEqual(6, length(Results)),
     ?_assert(lists:member(#{a => 10, b => 3, quotient => 3, remainder => 1}, DataOnly)),
     ?_assert(lists:member(#{a => 12, b => 4, quotient => 3, remainder => 0}, DataOnly)),
     ?_assert(lists:all(fun(#{a := A, b := B, quotient := Q, remainder := R}) ->
                           A =:= B * Q + R andalso R >= 0 andalso R < abs(B)
                       end, DataOnly))
    ].

test_divide_avoids_zero(DB) ->
    % Make sure we don't try to divide by zero
    % even if the range includes 0
    Iterator = operations:get_tuples_iterator(DB, divide, #{
        a => {range, 5, 7},
        b => {range, 0, 2}
    }),
    Results = operations:collect_all(Iterator),

    % 3 × 2 = 6 (skips b=0, only uses b=1,2)
    [
     ?_assertEqual(6, length(Results)),
     ?_assert(lists:all(fun(#{b := B}) -> B =/= 0 end, Results))
    ].

%%% Integration Tests

test_plus_join_with_finite(DB) ->
    % Simulate joining an infinite relation with a finite one
    {DB1, _Employees} = operations:create_relation(DB, employees, #{
        name => string,
        age => integer
    }),

    {DB2, _} = operations:create_tuple(DB1, employees, #{name => "Alice", age => 30}),
    {DB3, _} = operations:create_tuple(DB2, employees, #{name => "Bob", age => 35}),

    EmployeeIter = operations:get_tuples_iterator(DB3, employees, #{}),
    Employees = operations:collect_all(EmployeeIter),

    % For each employee's age, find all the ways to add up to it
    % (simulating: Employee JOIN Plus ON Plus.sum = Employee.age)
    Results = lists:flatmap(fun(#{age := Age}) ->
        PlusIter = operations:get_tuples_iterator(DB3, plus, #{
            a => {range, 0, 15},
            b => {range, 0, 15},
            sum => {eq, Age}
        }),
        Tuples = operations:collect_all(PlusIter),
        [#{age => Age, plus_tuple => T} || T <- Tuples]
    end, Employees),

    % verify we got some results
    [
     ?_assert(length(Results) > 0),
     ?_assert(lists:any(fun(#{age := Age, plus_tuple := #{sum := S}}) ->
                           Age =:= S
                       end, Results))
    ].

test_materialized_plus_relation(DB) ->
    % Take an infinite relation, constrain it, and materialize into a finite one
    PlusIter = operations:get_tuples_iterator(DB, plus, #{
        a => {range, 0, 2},
        b => {range, 0, 2}
    }),

    % Turn it into an actual stored relation
    {DB1, SmallPlus} = operations:materialize(DB, PlusIter, small_plus),

    % check that it worked
    ResultIter = operations:get_tuples_iterator(DB1, small_plus, #{}),
    Results = operations:collect_all(ResultIter),

    [
     ?_assertEqual({finite, 9}, SmallPlus#relation.cardinality),
     ?_assertEqual(9, length(Results)),
     ?_assert(lists:all(fun(#{a := A, b := B, sum := S}) ->
                           S =:= A + B andalso A >= 0 andalso A =< 2 andalso B >= 0 andalso B =< 2
                       end, Results))
    ].
