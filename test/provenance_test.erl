-module(provenance_test).
-include_lib("eunit/include/eunit.hrl").
-include("operations.hrl").

%%% Provenance & Lineage Tracking Test Suite
%%%
%%% Tests for tuple metadata tracking in relational operations.
%%% Metadata is ALWAYS tracked and included in every tuple via the nested 'meta' field:
%%% - Provenance: Tracks the immediate source of each attribute
%%% - Lineage: Tracks the complete operational history of tuple creation
%%%
%%% NOTE ON TUPLE ORDERING:
%%% These tests observe tuples in a specific order (e.g., Alice before Bob) because
%%% collect_all/1 currently returns results in Merkle tree traversal order for finite
%%% relations. This ordering is an IMPLEMENTATION DETAIL and is NOT guaranteed.
%%% Relations are unordered sets - the current order is based on how tuples were
%%% inserted and hashed into the Merkle tree. Future implementations may return tuples
%%% in different orders. Tests use lists:all/2 to verify properties hold for ALL tuples
%%% rather than relying on specific ordering.

provenance_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun test_base_provenance/1,
      fun test_join_provenance/1,
      fun test_join_with_conflicts_provenance/1,
      fun test_equijoin_preserves_both_sources/1,
      fun test_base_lineage/1,
      fun test_join_lineage/1,
      fun test_select_lineage/1,
      fun test_project_lineage/1,
      fun test_sort_lineage/1,
      fun test_take_lineage/1,
      fun test_multi_operation_pipeline_lineage/1,
      fun test_theta_join_lineage/1,
      fun test_complex_nested_lineage/1
     ]}.

%%% Setup and Cleanup

setup() ->
    operations:setup(),
    DB = operations:create_database(test_db),

    % Create employees relation
    {DB1, _} = operations:create_relation(DB, employees, #{
        id => integer,
        name => string,
        dept_id => integer
    }),

    {DB2, _} = operations:create_tuple(DB1, employees, #{id => 1, name => "Alice", dept_id => 10}),
    {DB3, _} = operations:create_tuple(DB2, employees, #{id => 2, name => "Bob", dept_id => 20}),

    % Create departments relation
    {DB4, _} = operations:create_relation(DB3, departments, #{
        dept_id => integer,
        dept_name => string,
        budget => integer
    }),

    {DB5, _} = operations:create_tuple(DB4, departments, #{dept_id => 10, dept_name => "Engineering", budget => 100000}),
    {DB6, _} = operations:create_tuple(DB5, departments, #{dept_id => 20, dept_name => "Sales", budget => 80000}),

    DB6.

cleanup(_DB) ->
    ok.

%%% Base Provenance Tests

test_base_provenance(DB) ->
    % Query tuples (metadata always included)
    Iter = operations:get_tuples_iterator(DB, employees, #{}),
    Results = operations:collect_all(Iter),

    % All tuples should have correct provenance structure
    [
     ?_assertEqual(2, length(Results)),
     ?_assert(lists:all(fun(T) ->
                           maps:is_key(meta, T) andalso
                           begin
                               Meta = maps:get(meta, T),
                               maps:is_key(provenance, Meta)
                           end
                       end, Results)),
     % Check ALL tuples have correct provenance mapping
     ?_assert(lists:all(fun(T) ->
                           Meta = maps:get(meta, T),
                           Prov = maps:get(provenance, Meta),
                           % All attributes should map to {employees, attr_name}
                           maps:get(id, Prov) =:= {employees, id} andalso
                           maps:get(name, Prov) =:= {employees, name} andalso
                           maps:get(dept_id, Prov) =:= {employees, dept_id}
                       end, Results))
    ].

%%% Join Provenance Tests

test_join_provenance(DB) ->
    % Create iterators (metadata always included)
    EmpIter = operations:get_tuples_iterator(DB, employees, #{}),
    DeptIter = operations:get_tuples_iterator(DB, departments, #{}),

    % Join them
    JoinIter = operations:equijoin_iterator(EmpIter, DeptIter, dept_id),
    Results = operations:collect_all(JoinIter),

    % All results should have metadata with provenance
    [
     ?_assertEqual(2, length(Results)),
     ?_assert(lists:all(fun(T) -> maps:is_key(meta, T) end, Results)),
     % Check that ALL tuples have correct provenance structure
     ?_assert(lists:all(fun(T) ->
                           Meta = maps:get(meta, T),
                           Prov = maps:get(provenance, Meta),
                           % Employee attributes should come from employees
                           maps:get(id, Prov) =:= {employees, id} andalso
                           maps:get(name, Prov) =:= {employees, name} andalso
                           % Department attributes should come from departments
                           maps:get(dept_name, Prov) =:= {departments, dept_name} andalso
                           maps:get(budget, Prov) =:= {departments, budget}
                       end, Results))
    ].

test_join_with_conflicts_provenance(DB) ->
    % Create relations with conflicting attribute names
    {DB1, _} = operations:create_relation(DB, table_a, #{
        id => integer,
        value => integer
    }),
    {DB2, _} = operations:create_tuple(DB1, table_a, #{id => 1, value => 100}),

    {DB3, _} = operations:create_relation(DB2, table_b, #{
        id => integer,
        value => integer  % Conflict!
    }),
    {DB4, _} = operations:create_tuple(DB3, table_b, #{id => 1, value => 200}),

    % Create iterators with provenance
    AIter = operations:get_tuples_iterator(DB4, table_a, #{}),
    BIter = operations:get_tuples_iterator(DB4, table_b, #{}),

    JoinIter = operations:equijoin_iterator(AIter, BIter, id),
    Results = operations:collect_all(JoinIter),

    % Should be exactly one result tuple
    ?_assertEqual(1, length(Results)),

    % Check provenance handles conflicts correctly
    [Result] = Results,
    Meta = maps:get(meta, Result),
    Prov = maps:get(provenance, Meta),

    [
     % Left 'value' provenance
     ?_assertEqual({table_a, value}, maps:get(value, Prov)),
     % Right 'value' provenance (prefixed)
     ?_assertEqual({table_b, value}, maps:get(right_value, Prov)),
     % Join attribute from left side
     ?_assertEqual({table_a, id}, maps:get(id, Prov)),
     % Join attribute from right side (prefixed)
     ?_assertEqual({table_b, id}, maps:get(right_id, Prov))
    ].

test_equijoin_preserves_both_sources(DB) ->
    % Even though equijoin only keeps one dept_id value,
    % provenance should track both sources
    EmpIter = operations:get_tuples_iterator(DB, employees, #{}),
    DeptIter = operations:get_tuples_iterator(DB, departments, #{}),

    JoinIter = operations:equijoin_iterator(EmpIter, DeptIter, dept_id),
    Results = operations:collect_all(JoinIter),

    % Check that ALL tuples have both dept_id sources in provenance
    [
     ?_assert(lists:all(fun(T) ->
                           Meta = maps:get(meta, T),
                           Prov = maps:get(provenance, Meta),
                           % Left side dept_id
                           maps:is_key(dept_id, Prov) andalso
                           % Right side dept_id (prefixed)
                           maps:is_key(right_dept_id, Prov)
                       end, Results)),
     % Verify the sources are correct for ALL tuples
     ?_assert(lists:all(fun(T) ->
                           Meta = maps:get(meta, T),
                           Prov = maps:get(provenance, Meta),
                           maps:get(dept_id, Prov) =:= {employees, dept_id} andalso
                           maps:get(right_dept_id, Prov) =:= {departments, dept_id}
                       end, Results))
    ].

%%% Lineage Tests

test_base_lineage(DB) ->
    % Query tuples (metadata always included)
    Iter = operations:get_tuples_iterator(DB, employees, #{}),
    Results = operations:collect_all(Iter),

    % All tuples should have lineage showing base relation
    [
     ?_assertEqual(2, length(Results)),
     ?_assert(lists:all(fun(T) ->
                           Meta = maps:get(meta, T),
                           Lineage = maps:get(lineage, Meta),
                           Lineage =:= {base, employees}
                       end, Results))
    ].

test_join_lineage(DB) ->
    % Create iterators (metadata always included)
    EmpIter = operations:get_tuples_iterator(DB, employees, #{}),
    DeptIter = operations:get_tuples_iterator(DB, departments, #{}),

    % Join them
    JoinIter = operations:equijoin_iterator(EmpIter, DeptIter, dept_id),
    Results = operations:collect_all(JoinIter),

    % All results should have lineage showing join operation
    [
     ?_assertEqual(2, length(Results)),
     ?_assert(lists:all(fun(T) ->
                           Meta = maps:get(meta, T),
                           Lineage = maps:get(lineage, Meta),
                           % Should be a join of two base relations
                           case Lineage of
                               {join, dept_id, {base, employees}, {base, departments}} -> true;
                               _ -> false
                           end
                       end, Results))
    ].

%%% Comprehensive Lineage Tests

test_select_lineage(DB) ->
    % Test lineage through select (filter) operation
    Iter = operations:get_tuples_iterator(DB, employees, #{}),

    Predicate = fun(T) -> maps:get(id, T, 0) =:= 1 end,
    FilteredIter = operations:select_iterator(Iter, Predicate),
    Results = operations:collect_all(FilteredIter),

    % Should have lineage: select wrapping base
    [
     ?_assertEqual(1, length(Results)),
     ?_assert(lists:all(fun(T) ->
                           Meta = maps:get(meta, T),
                           Lineage = maps:get(lineage, Meta),
                           case Lineage of
                               {select, Pred, {base, employees}} when is_function(Pred) -> true;
                               _ -> false
                           end
                       end, Results))
    ].

test_project_lineage(DB) ->
    % Test lineage through project operation
    Iter = operations:get_tuples_iterator(DB, employees, #{}),
    ProjectedIter = operations:project_iterator(Iter, [name, dept_id]),
    Results = operations:collect_all(ProjectedIter),

    % Should have lineage: project wrapping base
    [
     ?_assertEqual(2, length(Results)),
     ?_assert(lists:all(fun(T) ->
                           Meta = maps:get(meta, T),
                           Lineage = maps:get(lineage, Meta),
                           case Lineage of
                               {project, [name, dept_id], {base, employees}} -> true;
                               _ -> false
                           end
                       end, Results))
    ].

test_sort_lineage(DB) ->
    % Test lineage through sort operation
    Iter = operations:get_tuples_iterator(DB, employees, #{}),

    CompareFun = fun(A, B) -> maps:get(id, A, 0) =< maps:get(id, B, 0) end,
    SortedIter = operations:sort_iterator(Iter, CompareFun),
    Results = operations:collect_all(SortedIter),

    % Should have lineage: sort wrapping base
    [
     ?_assertEqual(2, length(Results)),
     ?_assert(lists:all(fun(T) ->
                           Meta = maps:get(meta, T),
                           Lineage = maps:get(lineage, Meta),
                           case Lineage of
                               {sort, Cmp, {base, employees}} when is_function(Cmp) -> true;
                               _ -> false
                           end
                       end, Results))
    ].

test_take_lineage(DB) ->
    % Test lineage through take (limit) operation
    Iter = operations:get_tuples_iterator(DB, employees, #{}),
    TakeIter = operations:take_iterator(Iter, 1),
    Results = operations:collect_all(TakeIter),

    % Should have lineage: take wrapping base
    [
     ?_assertEqual(1, length(Results)),
     ?_assert(lists:all(fun(T) ->
                           Meta = maps:get(meta, T),
                           Lineage = maps:get(lineage, Meta),
                           case Lineage of
                               {take, 1, {base, employees}} -> true;
                               _ -> false
                           end
                       end, Results))
    ].

test_multi_operation_pipeline_lineage(DB) ->
    % Test complex pipeline: employees → select(id=1) → join(departments) → project([name, dept_name])

    % 1. Start with employees base relation
    EmpIter = operations:get_tuples_iterator(DB, employees, #{}),

    % 2. Filter to Alice (id=1)
    Predicate = fun(T) -> maps:get(id, T, 0) =:= 1 end,
    FilteredEmp = operations:select_iterator(EmpIter, Predicate),

    % 3. Join with departments
    DeptIter = operations:get_tuples_iterator(DB, departments, #{}),
    JoinedIter = operations:equijoin_iterator(FilteredEmp, DeptIter, dept_id),

    % 4. Project to [name, dept_name]
    ProjectedIter = operations:project_iterator(JoinedIter, [name, dept_name]),

    Results = operations:collect_all(ProjectedIter),

    % Expected lineage:
    % {project, [name, dept_name],
    %   {join, dept_id,
    %     {select, Pred, {base, employees}},
    %     {base, departments}}}

    [
     ?_assertEqual(1, length(Results)),  % Only Alice
     ?_assert(lists:all(fun(T) ->
                           Meta = maps:get(meta, T),
                           Lineage = maps:get(lineage, Meta),
                           case Lineage of
                               {project, [name, dept_name],
                                {join, dept_id,
                                 {select, Pred, {base, employees}},
                                 {base, departments}}} when is_function(Pred) -> true;
                               _ -> false
                           end
                       end, Results))
    ].

test_theta_join_lineage(DB) ->
    % Test lineage for theta-join operation
    EmpIter = operations:get_tuples_iterator(DB, employees, #{}),
    DeptIter = operations:get_tuples_iterator(DB, departments, #{}),

    Predicate = fun(Emp, Dept) ->
        maps:get(dept_id, Emp) =:= maps:get(dept_id, Dept)
    end,

    JoinIter = operations:theta_join_iterator(EmpIter, DeptIter, Predicate),
    Results = operations:collect_all(JoinIter),

    % Should have theta_join lineage
    [
     ?_assertEqual(2, length(Results)),
     ?_assert(lists:all(fun(T) ->
                           Meta = maps:get(meta, T),
                           Lineage = maps:get(lineage, Meta),
                           case Lineage of
                               {theta_join, Pred, {base, employees}, {base, departments}}
                                   when is_function(Pred) -> true;
                               _ -> false
                           end
                       end, Results))
    ].

test_complex_nested_lineage(DB) ->
    % Test deeply nested lineage:
    % employees → select(id=1) → project([name, dept_id]) → join(departments → select(budget>50000)) → take(1)

    % Left side: employees → select → project
    EmpIter = operations:get_tuples_iterator(DB, employees, #{}),
    FilteredEmp = operations:select_iterator(EmpIter, fun(T) -> maps:get(id, T, 0) =:= 1 end),
    ProjectedEmp = operations:project_iterator(FilteredEmp, [name, dept_id]),

    % Right side: departments → select
    DeptIter = operations:get_tuples_iterator(DB, departments, #{}),
    FilteredDept = operations:select_iterator(DeptIter, fun(D) -> maps:get(budget, D, 0) > 50000 end),

    % Join both sides
    JoinedIter = operations:equijoin_iterator(ProjectedEmp, FilteredDept, dept_id),

    % Take first result
    TakeIter = operations:take_iterator(JoinedIter, 1),

    Results = operations:collect_all(TakeIter),

    % Expected lineage should be deeply nested
    [
     ?_assertEqual(1, length(Results)),
     ?_assert(lists:all(fun(T) ->
                           Meta = maps:get(meta, T),
                           Lineage = maps:get(lineage, Meta),
                           case Lineage of
                               {take, 1,
                                {join, dept_id,
                                 {project, [name, dept_id],
                                  {select, _, {base, employees}}},
                                 {select, _, {base, departments}}}} -> true;
                               _ -> false
                           end
                       end, Results))
    ].
