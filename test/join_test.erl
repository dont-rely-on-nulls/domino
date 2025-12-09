-module(join_test).
-include_lib("eunit/include/eunit.hrl").
-include("operations.hrl").

%%% Join Operators Test Suite
%%%
%%% Tests for relational join operations:
%%% - Equijoin (natural join on attribute equality)
%%% - Theta-join (join with arbitrary predicate)

join_test_() ->
    {foreach,
     fun setup/0,
     fun cleanup/1,
     [
      fun test_equijoin_simple/1,
      fun test_equijoin_multiple_matches/1,
      fun test_equijoin_no_matches/1,
      fun test_equijoin_with_conflicts/1,
      fun test_theta_join_greater_than/1,
      fun test_theta_join_complex_predicate/1,
      fun test_join_with_infinite_relation/1,
      fun test_join_materialize_pipeline/1
     ]}.

%%% Setup and Cleanup

setup() ->
    main:setup(),
    DB = operations:create_database(test_db),

    % Create employees relation
    {DB1, _} = operations:create_relation(DB, employees, #{
        id => integer,
        name => string,
        dept_id => integer,
        age => integer
    }),

    {DB2, _} = operations:create_tuple(DB1, employees, #{id => 1, name => "Alice", dept_id => 10, age => 30}),
    {DB3, _} = operations:create_tuple(DB2, employees, #{id => 2, name => "Bob", dept_id => 20, age => 35}),
    {DB4, _} = operations:create_tuple(DB3, employees, #{id => 3, name => "Carol", dept_id => 10, age => 28}),
    {DB5, _} = operations:create_tuple(DB4, employees, #{id => 4, name => "Dave", dept_id => 30, age => 42}),

    % Create departments relation
    {DB6, _} = operations:create_relation(DB5, departments, #{
        dept_id => integer,
        dept_name => string,
        budget => integer
    }),

    {DB7, _} = operations:create_tuple(DB6, departments, #{dept_id => 10, dept_name => "Engineering", budget => 100000}),
    {DB8, _} = operations:create_tuple(DB7, departments, #{dept_id => 20, dept_name => "Sales", budget => 80000}),
    {DB9, _} = operations:create_tuple(DB8, departments, #{dept_id => 30, dept_name => "Marketing", budget => 90000}),

    DB9.

cleanup(_DB) ->
    ok.

%%% Equijoin Tests

test_equijoin_simple(DB) ->
    % Join employees with departments on dept_id
    EmpIter = operations:get_tuples_iterator(DB, employees),
    DeptIter = operations:get_tuples_iterator(DB, departments),

    JoinIter = operations:equijoin_iterator(EmpIter, DeptIter, dept_id),
    Results = operations:collect_all(JoinIter),

    % Should get 4 joined tuples (one for each employee)
    [
     ?_assertEqual(4, length(Results)),
     % Check Alice joined with Engineering
     ?_assert(lists:any(fun(T) ->
                           maps:get(name, T, undefined) =:= "Alice" andalso
                           maps:get(dept_name, T, undefined) =:= "Engineering"
                       end, Results)),
     % Check Bob joined with Sales
     ?_assert(lists:any(fun(T) ->
                           maps:get(name, T, undefined) =:= "Bob" andalso
                           maps:get(dept_name, T, undefined) =:= "Sales"
                       end, Results)),
     % All results should have both employee and department attributes
     ?_assert(lists:all(fun(T) ->
                           maps:is_key(name, T) andalso
                           maps:is_key(dept_name, T) andalso
                           maps:is_key(dept_id, T)
                       end, Results))
    ].

test_equijoin_multiple_matches(DB) ->
    % Multiple employees in same department should produce multiple results
    EmpIter = operations:get_tuples_iterator(DB, employees),
    DeptIter = operations:get_tuples_iterator(DB, departments),

    JoinIter = operations:equijoin_iterator(EmpIter, DeptIter, dept_id),
    Results = operations:collect_all(JoinIter),

    % Engineering dept (10) has 2 employees (Alice and Carol)
    EngineeringResults = lists:filter(fun(T) ->
        maps:get(dept_name, T, undefined) =:= "Engineering"
    end, Results),

    [
     ?_assertEqual(2, length(EngineeringResults)),
     ?_assert(lists:any(fun(T) -> maps:get(name, T) =:= "Alice" end, EngineeringResults)),
     ?_assert(lists:any(fun(T) -> maps:get(name, T) =:= "Carol" end, EngineeringResults))
    ].

test_equijoin_no_matches(DB) ->
    % Create a department with no employees
    {DB1, _} = operations:create_tuple(DB, departments, #{dept_id => 40, dept_name => "HR", budget => 70000}),

    % Filter to only dept_id=40 which has no employees
    DeptIter = operations:get_tuples_iterator(DB1, departments),
    FilteredDept = operations:select_iterator(DeptIter, fun(D) -> maps:get(dept_id, D) =:= 40 end),

    EmpIter = operations:get_tuples_iterator(DB1, employees),

    JoinIter = operations:equijoin_iterator(EmpIter, FilteredDept, dept_id),
    Results = operations:collect_all(JoinIter),

    % No employees in HR, so no results
    [
     ?_assertEqual(0, length(Results))
    ].

test_equijoin_with_conflicts(DB) ->
    % Create relations with conflicting attribute names
    {DB1, _} = operations:create_relation(DB, table_a, #{
        id => integer,
        value => integer
    }),
    {DB2, _} = operations:create_tuple(DB1, table_a, #{id => 1, value => 100}),

    {DB3, _} = operations:create_relation(DB2, table_b, #{
        id => integer,
        value => integer  % Same name as table_a.value
    }),
    {DB4, _} = operations:create_tuple(DB3, table_b, #{id => 1, value => 200}),

    AIter = operations:get_tuples_iterator(DB4, table_a),
    BIter = operations:get_tuples_iterator(DB4, table_b),

    JoinIter = operations:equijoin_iterator(AIter, BIter, id),
    Results = operations:collect_all(JoinIter),

    % Should have one result with both values
    % Right side 'value' should be prefixed as 'right_value'
    [
     ?_assertEqual(1, length(Results)),
     ?_assertMatch([#{id := 1, value := 100, right_value := 200}], Results)
    ].

%%% Theta-Join Tests

test_theta_join_greater_than(DB) ->
    % Join where employee.age > department.budget/1000
    % (contrived example for testing)

    EmpIter = operations:get_tuples_iterator(DB, employees),
    DeptIter = operations:get_tuples_iterator(DB, departments),

    Predicate = fun(Emp, Dept) ->
        Age = maps:get(age, Emp),
        Budget = maps:get(budget, Dept),
        Age > (Budget div 2000)  % Age > Budget/2000
    end,

    JoinIter = operations:theta_join_iterator(EmpIter, DeptIter, Predicate),
    Results = operations:collect_all(JoinIter),

    % Should get some results based on predicate
    [
     ?_assert(length(Results) > 0),
     % Verify predicate holds for all results
     ?_assert(lists:all(fun(T) ->
                           Age = maps:get(age, T),
                           Budget = maps:get(budget, T),
                           Age > (Budget div 2000)
                       end, Results))
    ].

test_theta_join_complex_predicate(DB) ->
    % Join with multiple conditions
    EmpIter = operations:get_tuples_iterator(DB, employees),
    DeptIter = operations:get_tuples_iterator(DB, departments),

    % Join where employee is in department AND age > 30
    Predicate = fun(Emp, Dept) ->
        maps:get(dept_id, Emp) =:= maps:get(dept_id, Dept) andalso
        maps:get(age, Emp) > 30
    end,

    JoinIter = operations:theta_join_iterator(EmpIter, DeptIter, Predicate),
    Results = operations:collect_all(JoinIter),

    % Should get Bob (35) and Dave (42), not Alice (30) or Carol (28)
    [
     ?_assertEqual(2, length(Results)),
     ?_assert(lists:all(fun(T) -> maps:get(age, T) > 30 end, Results)),
     ?_assert(lists:any(fun(T) -> maps:get(name, T) =:= "Bob" end, Results)),
     ?_assert(lists:any(fun(T) -> maps:get(name, T) =:= "Dave" end, Results))
    ].

%%% Integration Tests

test_join_with_infinite_relation(DB) ->
    % Integers are built-in, no need to create them

    % Get small set of integers
    IntIter = operations:get_tuples_iterator(DB, integer, #{value => {range, 1, 5}}),

    % Get employees
    EmpIter = operations:get_tuples_iterator(DB, employees),

    % Join where employee.id matches integer.value
    Predicate = fun(Emp, Int) ->
        maps:get(id, Emp) =:= maps:get(value, Int)
    end,

    JoinIter = operations:theta_join_iterator(EmpIter, IntIter, Predicate),
    Results = operations:collect_all(JoinIter),

    % Should get employees with id in [1,5] (all 4 employees)
    [
     ?_assertEqual(4, length(Results)),
     ?_assert(lists:all(fun(T) ->
                           Id = maps:get(id, T),
                           Value = maps:get(value, T),
                           Id =:= Value andalso Id >= 1 andalso Id =< 5
                       end, Results))
    ].

test_join_materialize_pipeline(DB) ->
    % Complex pipeline: join, filter, project, materialize

    % 1. Join employees with departments
    EmpIter = operations:get_tuples_iterator(DB, employees),
    DeptIter = operations:get_tuples_iterator(DB, departments),
    JoinIter = operations:equijoin_iterator(EmpIter, DeptIter, dept_id),

    % 2. Filter to high-budget departments
    FilteredIter = operations:select_iterator(JoinIter, fun(T) ->
        maps:get(budget, T) >= 90000
    end),

    % 3. Project to [name, dept_name, budget]
    ProjectedIter = operations:project_iterator(FilteredIter, [name, dept_name, budget]),

    % 4. Materialize
    {DB1, HighBudgetEmps} = operations:materialize(DB, ProjectedIter, high_budget_employees),

    % Verify materialized relation
    ResultIter = operations:get_tuples_iterator(DB1, high_budget_employees),
    Results = operations:collect_all(ResultIter),

    % Engineering (100k) has Alice and Carol
    % Marketing (90k) has Dave
    % Sales (80k) is excluded
    [
     ?_assertEqual({finite, 3}, HighBudgetEmps#relation.cardinality),
     ?_assertEqual(3, length(Results)),
     ?_assert(lists:all(fun(T) ->
                           maps:is_key(name, T) andalso
                           maps:is_key(dept_name, T) andalso
                           maps:is_key(budget, T) andalso
                           not maps:is_key(id, T) andalso  % Projected out
                           not maps:is_key(age, T)  % Projected out
                       end, Results)),
     ?_assert(lists:all(fun(T) -> maps:get(budget, T) >= 90000 end, Results))
    ].
