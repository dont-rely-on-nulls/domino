%%% @doc Interactive REPL Helper for Domino Query Planner
%%%
%%% Provides convenience functions for working with the query planner
%%% in the Erlang shell (REPL). Includes utilities for pretty-printing
%%% results and working with prepared statements.
%%%
%%% == Quick Start ==
%%% <pre>
%%% % Start the system
%%% repl:start().
%%%
%%% % Create a database with sample data
%%% DB = repl:example_db().
%%%
%%% % Execute a query
%%% repl:q(DB, {select, {relation, employees}, fun(T) -&gt; maps:get(age, T) &gt; 30 end}).
%%%
%%% % Explain a query plan
%%% repl:explain({project, {relation, employees}, [name, age]}).
%%% </pre>
%%%
%%% == Query Syntax Examples ==
%%% <pre>
%%% % Base relation (just use the name)
%%% employees
%%%
%%% % Filter
%%% {select, employees, fun(T) -&gt; maps:get(age, T) &gt; 30 end}
%%%
%%% % Project
%%% {project, employees, [name, age]}
%%%
%%% % Join
%%% {join, employees, departments, dept_id}
%%%
%%% % Rename
%%% {rename, age, years, employees}
%%%
%%% % Complex pipeline
%%% {take,
%%%   {sort,
%%%     {project,
%%%       {select,
%%%         {join, employees, departments, dept_id},
%%%         fun(T) -&gt; maps:get(age, T) &gt; 30 end},
%%%       [name, dept_name, age]},
%%%     fun(A, B) -&gt; maps:get(age, A) =&lt; maps:get(age, B) end},
%%%   5}
%%% </pre>
%%%
%%% @author Nekoma Team
%%% @copyright 2025

-module(repl).

-export([start/0,
         example_db/0,
         q/2,
         q/3,
         query/2,
         show/1,
         show/2,
         explain/1,
         pretty/1,
         modes/0,
         help/0]).

-include("operations.hrl").

%%% Public API

%% @doc Start the Domino system and initialize Mnesia.
%%
%% Call this first when starting a new REPL session.
%%
%% @returns ok
-spec start() -> ok.
start() ->
    operations:setup(),
    io:format("~n~s~n", [banner()]),
    io:format("Domino REPL started. Type repl:help() for usage.~n~n"),
    ok.

%% @doc Create an example database with sample data.
%%
%% Creates a database with employees and departments relations
%% for testing and experimentation.
%%
%% @returns Database state
-spec example_db() -> term().
example_db() ->
    DB = operations:create_database(example_db),

    % Create employees relation
    {DB1, _} = operations:create_relation(DB, employees, #{
        id => integer,
        name => string,
        dept_id => integer,
        age => integer,
        salary => integer
    }),

    {DB2, _} = operations:create_tuple(DB1, employees, #{id => 1, name => "Alice", dept_id => 10, age => 30, salary => 80000}),
    {DB3, _} = operations:create_tuple(DB2, employees, #{id => 2, name => "Bob", dept_id => 20, age => 35, salary => 90000}),
    {DB4, _} = operations:create_tuple(DB3, employees, #{id => 3, name => "Carol", dept_id => 10, age => 28, salary => 75000}),
    {DB5, _} = operations:create_tuple(DB4, employees, #{id => 4, name => "Dave", dept_id => 30, age => 42, salary => 95000}),
    {DB6, _} = operations:create_tuple(DB5, employees, #{id => 5, name => "Eve", dept_id => 20, age => 25, salary => 70000}),

    % Create departments relation
    {DB7, _} = operations:create_relation(DB6, departments, #{
        dept_id => integer,
        dept_name => string,
        budget => integer
    }),

    {DB8, _} = operations:create_tuple(DB7, departments, #{dept_id => 10, dept_name => "Engineering", budget => 100000}),
    {DB9, _} = operations:create_tuple(DB8, departments, #{dept_id => 20, dept_name => "Sales", budget => 80000}),
    {DB10, _} = operations:create_tuple(DB9, departments, #{dept_id => 30, dept_name => "Marketing", budget => 90000}),

    %% Note: natural, integer, rational, and boolean are now built-in to every database

    io:format("Example database created with:~n"),
    io:format("  - employees (5 tuples)~n"),
    io:format("  - departments (3 tuples)~n"),
    io:format("  - Built-in immutable relations: boolean, natural, integer, rational~n~n"),

    DB10.

%% @doc Execute a query and pretty-print results (default table mode).
%%
%% Convenience function that prepares, executes, and displays
%% query results in a readable format. Returns `ok' to avoid
%% duplicate output in the Erlang shell.
%%
%% If you need the results for further processing, use `query/2'.
%%
%% @param DB Database state
%% @param Plan Query plan tuple
%% @returns ok
-spec q(term(), query_planner:query_plan()) -> ok.
q(DB, Plan) ->
    q(DB, Plan, table).

%% @doc Execute a query with specified visualization mode.
%%
%% Displays query results using the specified visualization mode.
%% Returns `ok' to avoid duplicate output in the Erlang shell.
%%
%% If you need the results for further processing, use `query/2'.
%%
%% @param DB Database state
%% @param Plan Query plan tuple
%% @param Mode Visualization mode (table, tree, nested_table, etc.)
%% @returns ok
-spec q(term(), query_planner:query_plan(), visualizer:render_mode()) -> ok.
q(DB, Plan, Mode) ->
    PreparedPlan = query_planner:prepare(Plan),
    Results = query_planner:collect(DB, PreparedPlan),
    visualizer:render(Results, Mode).

%% @doc Execute a query without printing results.
%%
%% This function executes a query and returns the raw results without
%% any visualization. Use this when you want to work with the results
%% programmatically or decide later how to display them.
%%
%% @param DB Database state
%% @param Plan Query plan tuple
%% @returns List of result tuples
%%
%% == Example ==
%% <pre>
%% Results = repl:query(DB, {scan, employees}).
%% FilteredResults = lists:filter(fun(T) -&gt; maps:get(age, T) &gt; 30 end, Results).
%% repl:show(FilteredResults, tree).
%% </pre>
-spec query(term(), query_planner:query_plan()) -> [map()].
query(DB, Plan) ->
    PreparedPlan = query_planner:prepare(Plan),
    query_planner:collect(DB, PreparedPlan).

%% @doc Display results using default visualization mode (table).
%%
%% Takes a list of tuples (e.g., from query/2) and displays them
%% using the table visualization mode.
%%
%% @param Results List of result tuples
%% @returns ok
-spec show([map()]) -> ok.
show(Results) ->
    show(Results, table).

%% @doc Display results using specified visualization mode.
%%
%% Takes a list of tuples (e.g., from query/2) and displays them
%% using the specified visualization mode.
%%
%% @param Results List of result tuples
%% @param Mode Visualization mode (table, tree, nested_table, etc.)
%% @returns ok
%%
%% == Example ==
%% <pre>
%% Results = repl:query(DB, {scan, employees}),
%% repl:show(Results, tree).
%% </pre>
-spec show([map()], visualizer:render_mode()) -> ok.
show(Results, Mode) ->
    visualizer:render(Results, Mode).

%% @doc Explain a query plan.
%%
%% Displays the query execution plan in a readable tree format.
%%
%% @param Plan Query plan tuple
%% @returns ok
-spec explain(query_planner:query_plan()) -> ok.
explain(Plan) ->
    Explanation = query_planner:explain(Plan),
    io:format("~nQuery Plan:~n~s~n", [Explanation]),
    ok.

%% @doc List available visualization modes.
%%
%% @returns List of available render modes
-spec modes() -> [visualizer:render_mode()].
modes() ->
    visualizer:modes().

%% @doc Pretty-print a list of tuples.
%%
%% Displays tuples using the visualizer module (default table mode).
%%
%% @param Results List of result tuples
%% @returns ok
-spec pretty([map()]) -> ok.
pretty(Results) ->
    visualizer:render(Results, table).

%% @doc Display help message with query examples.
%%
%% @returns ok
-spec help() -> ok.
help() ->
    io:format("~n~s~n~n", [banner()]),
    io:format("Domino Query REPL - Lisp-inspired tuple-based query language~n~n"),
    io:format("Quick Start:~n"),
    io:format("  repl:start().                  %% Initialize system~n"),
    io:format("  DB = repl:example_db().        %% Create example database~n"),
    io:format("  repl:q(DB, employees).         %% Run a query~n~n"),

    io:format("Query Operators (all lazy by default):~n"),
    io:format("  RelationName                              - Base relation~n"),
    io:format("  {select, Plan, Predicate}                 - Filter tuples~n"),
    io:format("  {project, Plan, [Attrs]}                  - Project attributes~n"),
    io:format("  {join, Left, Right, Attr}                 - Equijoin~n"),
    io:format("  {theta_join, Left, Right, Predicate}      - Theta join~n"),
    io:format("  {sort, Plan, Comparator}                  - Sort tuples~n"),
    io:format("  {take, Plan, N}                           - Limit results~n"),
    io:format("  {rename, OldAttr, NewAttr, Plan}          - Rename attribute~n"),
    io:format("  {materialize, Plan}                       - Force evaluation (all)~n"),
    io:format("  {materialize, Plan, N}                    - Force evaluation (N tuples)~n~n"),

    io:format("Lazy vs Eager:~n"),
    io:format("  All operators return lazy iterators (relations)~n"),
    io:format("  Use 'materialize' to force eager evaluation~n"),
    io:format("  Materialize can be nested anywhere in the query~n~n"),

    io:format("Query Execution:~n"),
    io:format("  repl:q(DB, Plan)           - Execute and display (returns ok)~n"),
    io:format("  repl:q(DB, Plan, Mode)     - Execute and display with mode (returns ok)~n"),
    io:format("  repl:query(DB, Plan)       - Execute without display (returns results)~n"),
    io:format("  repl:show(Results)         - Display results (returns ok)~n"),
    io:format("  repl:show(Results, Mode)   - Display results with mode (returns ok)~n~n"),

    io:format("Visualization Modes:~n"),
    io:format("  Modes: table, tree, nested_table, linked_tables, outline~n"),
    io:format("  repl:modes() - List all available modes~n~n"),

    io:format("Examples:~n"),
    io:format("  %% Query all employees~n"),
    io:format("  repl:q(DB, employees).~n~n"),

    io:format("  %% Filter employees over 30~n"),
    io:format("  repl:q(DB, {select, employees, ~n"),
    io:format("              fun(T) -> maps:get(age, T) > 30 end}).~n~n"),

    io:format("  %% Project name and age~n"),
    io:format("  repl:q(DB, {project, employees, [name, age]}).~n~n"),

    io:format("  %% Join employees with departments~n"),
    io:format("  repl:q(DB, {join, employees, departments, dept_id}).~n~n"),

    io:format("  %% Rename age to years~n"),
    io:format("  repl:q(DB, {rename, age, years, employees}).~n~n"),

    io:format("  %% Materialize first 10 tuples (eager evaluation)~n"),
    io:format("  repl:q(DB, {materialize, employees, 10}).~n~n"),

    io:format("  %% Nested materialize - select from materialized data~n"),
    io:format("  repl:q(DB, {select, {materialize, employees},~n"),
    io:format("              fun(T) -> maps:get(age, T) > 30 end}).~n~n"),

    io:format("  %% Use different visualization modes~n"),
    io:format("  repl:q(DB, employees, tree).~n"),
    io:format("  repl:q(DB, employees, nested_table).~n"),
    io:format("  repl:q(DB, employees, outline).~n~n"),

    io:format("  %% Query without printing, then show results~n"),
    io:format("  Results = repl:query(DB, employees).~n"),
    io:format("  Filtered = lists:filter(fun(T) -> maps:get(age, T) > 30 end, Results).~n"),
    io:format("  repl:show(Filtered, tree).~n~n"),

    io:format("  %% Explain a query plan~n"),
    io:format("  repl:explain({select, employees, ~n"),
    io:format("                fun(T) -> maps:get(age, T) > 30 end}).~n~n"),

    ok.

%%% Internal Functions

banner() ->
    "========================================\n"
    "  Domino - Extended Relational DB\n"
    "  Tasmania / RM/T Implementation\n"
    "========================================".
