%%% @doc Relation Visualizer - Multiple Rendering Modes
%%%
%%% Provides various visual representations for nested relational data.
%%% Supports different modes optimized for different use cases.
%%%
%%% == Modes ==
%%%
%%% - `table` - Classic flat table (default)
%%% - `nested_table` - Tables within table cells
%%% - `tree` - Hierarchical tree view with indentation
%%% - `linked_tables` - Separate linked tables for nested relations
%%% - `outline` - Indented outline format
%%%
%%% == Usage ==
%%%
%%% ```
%%% Results = query_planner:collect(DB, Plan),
%%% visualizer:render(Results, tree).
%%% visualizer:render(Results, nested_table).
%%% ```
%%%
%%% @author Nekoma Team
%%% @copyright 2025

-module(visualizer).

-export([render/1,
         render/2,
         modes/0]).

-type render_mode() :: table
                     | nested_table
                     | tree
                     | linked_tables
                     | outline.

-export_type([render_mode/0]).

%%% Public API

%% @doc Render tuples with default mode (table).
-spec render([map()]) -> ok.
render(Tuples) ->
    render(Tuples, table).

%% @doc Render tuples with specified visualization mode.
-spec render([map()], render_mode()) -> ok.
render([], _Mode) ->
    io:format("(no results)~n"),
    ok;
render(Tuples, table) ->
    render_table(Tuples);
render(Tuples, nested_table) ->
    render_nested_table(Tuples);
render(Tuples, tree) ->
    render_tree(Tuples);
render(Tuples, linked_tables) ->
    render_linked_tables(Tuples);
render(Tuples, outline) ->
    render_outline(Tuples).

%% @doc List available rendering modes.
-spec modes() -> [render_mode()].
modes() ->
    [table, nested_table, tree, linked_tables, outline].

%%% Rendering Implementations

%% Mode 1: Table with proper column width calculation
render_table(Tuples) ->
    AllKeys = get_all_keys(Tuples),

    % Calculate column widths based on content
    Widths = calculate_column_widths(Tuples, AllKeys),

    io:format("~n"),
    print_separator_with_widths(Widths),
    print_row_with_widths([atom_to_list(K) || K <- AllKeys], Widths),
    print_separator_with_widths(Widths),

    lists:foreach(
        fun(Tuple) ->
            Values = [format_value_for_table(maps:get(K, Tuple, undefined)) || K <- AllKeys],
            print_row_with_widths(Values, Widths)
        end,
        Tuples
    ),
    print_separator_with_widths(Widths),
    io:format("~n~b tuple(s)~n~n", [length(Tuples)]),
    ok.

calculate_column_widths(Tuples, Keys) ->
    lists:map(
        fun(Key) ->
            % Start with header width
            HeaderWidth = length(atom_to_list(Key)),

            % Find max value width in this column
            MaxValueWidth = lists:foldl(
                fun(Tuple, MaxW) ->
                    Value = maps:get(Key, Tuple, undefined),
                    ValueStr = format_value_for_table(Value),
                    max(MaxW, length(ValueStr))
                end,
                HeaderWidth,
                Tuples
            ),

            max(12, MaxValueWidth)  % Minimum width of 12
        end,
        Keys
    ).

format_value_for_table(V) when is_map(V) ->
    % Format nested map as inline representation
    Keys = [K || K <- maps:keys(V), K =/= meta],
    Parts = [atom_to_list(K) ++ ":" ++ format_value(maps:get(K, V)) || K <- Keys],
    "[" ++ string:join(Parts, ", ") ++ "]";
format_value_for_table(V) ->
    format_value(V).

print_separator_with_widths(Widths) ->
    io:format("+"),
    lists:foreach(
        fun(W) ->
            io:format("~s+", [lists:duplicate(W + 2, $-)])
        end,
        Widths
    ),
    io:format("~n").

print_row_with_widths(Values, Widths) ->
    io:format("| "),
    lists:foreach(
        fun({Val, Width}) ->
            io:format("~*s | ", [-Width, Val])
        end,
        lists:zip(Values, Widths)
    ),
    io:format("~n").

%% Mode 2: Nested tables (tables within cells)
render_nested_table(Tuples) ->
    io:format("~n=== Nested Table View ===~n~n"),
    lists:foreach(
        fun(Tuple) ->
            render_tuple_nested(Tuple, 0)
        end,
        Tuples
    ),
    io:format("~n~b tuple(s)~n~n", [length(Tuples)]),
    ok.

render_tuple_nested(Tuple, Indent) ->
    Keys = [K || K <- maps:keys(Tuple), K =/= meta],
    IndentStr = lists:duplicate(Indent, $\s),

    io:format("~s+-- Tuple~n", [IndentStr]),
    lists:foreach(
        fun(Key) ->
            Value = maps:get(Key, Tuple),
            case is_map(Value) andalso (Key =/= meta) of
                true ->
                    % Nested tuple - render as inner table
                    io:format("~s|  ~s:~n", [IndentStr, atom_to_list(Key)]),
                    io:format("~s|  +-------------------------+~n", [IndentStr]),
                    NestedKeys = maps:keys(Value),
                    lists:foreach(
                        fun(NK) ->
                            NV = format_value(maps:get(NK, Value)),
                            io:format("~s|  | ~-10s : ~-10s |~n",
                                     [IndentStr, atom_to_list(NK), NV])
                        end,
                        NestedKeys
                    ),
                    io:format("~s|  +-------------------------+~n", [IndentStr]);
                false ->
                    % Simple value
                    io:format("~s|  ~-15s : ~s~n",
                             [IndentStr, atom_to_list(Key), format_value(Value)])
            end
        end,
        Keys
    ),
    io:format("~s+--~n", [IndentStr]).

%% Mode 3: Tree view with hierarchy
render_tree(Tuples) ->
    io:format("~n=== Tree View ===~n~n"),
    io:format("+-- Relation (~b tuples)~n", [length(Tuples)]),
    render_tuples_as_tree(Tuples, "|  ", length(Tuples)),
    io:format("~n"),
    ok.

render_tuples_as_tree([], _Prefix, _Total) ->
    ok;
render_tuples_as_tree([Tuple], Prefix, Total) ->
    io:format("~s~n~s+-- Row ~b~n", [Prefix, Prefix, Total]),
    render_tuple_as_tree(Tuple, Prefix ++ "    ");
render_tuples_as_tree([Tuple | Rest], Prefix, Total) ->
    Index = Total - length(Rest),
    io:format("~s~n~s+-- Row ~b~n", [Prefix, Prefix, Index]),
    render_tuple_as_tree(Tuple, Prefix ++ "|   "),
    render_tuples_as_tree(Rest, Prefix, Total).

render_tuple_as_tree(Tuple, Prefix) ->
    Keys = [K || K <- maps:keys(Tuple), K =/= meta],
    render_attrs_as_tree(Keys, Tuple, Prefix).

render_attrs_as_tree([], _Tuple, _Prefix) ->
    ok;
render_attrs_as_tree([Key], Tuple, Prefix) ->
    Value = maps:get(Key, Tuple),
    case is_map(Value) andalso (Key =/= meta) of
        true ->
            io:format("~s+-- ~s:~n", [Prefix, atom_to_list(Key)]),
            render_tuple_as_tree(Value, Prefix ++ "    ");
        false ->
            io:format("~s+-- ~s: ~s~n",
                     [Prefix, atom_to_list(Key), format_value(Value)])
    end;
render_attrs_as_tree([Key | Rest], Tuple, Prefix) ->
    Value = maps:get(Key, Tuple),
    case is_map(Value) andalso (Key =/= meta) of
        true ->
            io:format("~s+-- ~s:~n", [Prefix, atom_to_list(Key)]),
            render_tuple_as_tree(Value, Prefix ++ "|   "),
            render_attrs_as_tree(Rest, Tuple, Prefix);
        false ->
            io:format("~s+-- ~s: ~s~n",
                     [Prefix, atom_to_list(Key), format_value(Value)]),
            render_attrs_as_tree(Rest, Tuple, Prefix)
    end.

%% Mode 4: Linked tables (parent + separate child tables)
render_linked_tables(Tuples) ->
    io:format("~n=== Linked Tables View ===~n~n"),

    % Collect nested relations with parent references
    {FlatTuples, NestedRels} = extract_nested_relations_with_refs(Tuples),

    % Render main table
    io:format("Main Relation:~n"),
    render_table(FlatTuples),

    % Render nested tables with parent references
    case NestedRels of
        [] -> ok;
        _ ->
            io:format("~nNested Relations:~n"),
            lists:foreach(
                fun({RowIdx, AttrName, NestedTuple}) ->
                    io:format("~n[~s for Row ~b]:~n", [atom_to_list(AttrName), RowIdx]),
                    render_table([NestedTuple])
                end,
                NestedRels
            )
    end,
    ok.

extract_nested_relations_with_refs(Tuples) ->
    % Process tuples with index tracking
    {FlatTuples, NestedRels} = lists:foldl(
        fun(Tuple, {FlatAcc, NestedAcc}) ->
            RowIdx = length(FlatAcc) + 1,

            % Flatten this tuple
            FlatTuple = maps:map(
                fun(K, V) when is_map(V), K =/= meta ->
                        lists:flatten(io_lib:format("[~b]", [RowIdx]));
                   (_K, V) -> V
                end,
                Tuple
            ),

            % Extract nested relations with parent reference
            Nested = lists:filtermap(
                fun(K) ->
                    case maps:get(K, Tuple) of
                        V when is_map(V), K =/= meta ->
                            {true, {RowIdx, K, V}};
                        _ ->
                            false
                    end
                end,
                maps:keys(Tuple)
            ),

            {[FlatTuple | FlatAcc], Nested ++ NestedAcc}
        end,
        {[], []},
        Tuples
    ),

    {lists:reverse(FlatTuples), lists:reverse(NestedRels)}.

%% Mode 5: Outline format (indented text)
render_outline(Tuples) ->
    io:format("~n=== Outline View ===~n~n"),
    lists:foreach(
        fun(Tuple) ->
            render_tuple_outline(Tuple, 0)
        end,
        Tuples
    ),
    io:format("~n~b tuple(s)~n~n", [length(Tuples)]),
    ok.

render_tuple_outline(Tuple, Indent) ->
    Keys = [K || K <- maps:keys(Tuple), K =/= meta],
    IndentStr = lists:duplicate(Indent, $\s),

    io:format("~s+-- Tuple~n", [IndentStr]),
    lists:foreach(
        fun(Key) ->
            Value = maps:get(Key, Tuple),
            case is_map(Value) andalso (Key =/= meta) of
                true ->
                    io:format("~s|   ~s:~n", [IndentStr, atom_to_list(Key)]),
                    render_tuple_outline(Value, Indent + 4);
                false ->
                    io:format("~s|   ~s: ~s~n",
                             [IndentStr, atom_to_list(Key), format_value(Value)])
            end
        end,
        Keys
    ),
    io:format("~s+--~n", [IndentStr]).

%%% Helper Functions

get_all_keys(Tuples) ->
    lists:usort(
        lists:flatmap(
            fun(Tuple) ->
                [K || K <- maps:keys(Tuple), K =/= meta]
            end,
            Tuples
        )
    ).

format_value(undefined) -> "(null)";
format_value(V) when is_integer(V) -> integer_to_list(V);
format_value(V) when is_float(V) -> float_to_list(V, [{decimals, 2}]);
format_value(V) when is_atom(V) -> atom_to_list(V);
format_value(V) when is_binary(V) -> binary_to_list(V);
format_value(V) when is_list(V) -> V;
format_value(V) when is_map(V) -> "(nested)";
format_value(_) -> "(complex)".
