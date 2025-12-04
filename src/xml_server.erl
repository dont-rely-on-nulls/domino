%%% @doc XML Server for Lazy Relation Consumption
%%%
%%% Provides a TCP server that accepts query plans as Erlang term strings
%%% and returns relation tuples as XML. The server maintains lazy iterators
%%% and allows clients to consume tuples on demand.
%%%
%%% == Protocol ==
%%%
%%% Client sends commands as line-delimited strings:
%%% <pre>
%%% QUERY &lt;erlang-term-string&gt;   - Execute a query plan, returns session ID
%%% NEXT &lt;session-id&gt; &lt;count&gt;    - Get next N tuples from iterator
%%% CLOSE &lt;session-id&gt;           - Close iterator and free resources
%%% </pre>
%%%
%%% Server responds with XML:
%%% <pre>
%%% &lt;?xml version="1.0"?&gt;
%%% &lt;response&gt;
%%%   &lt;session&gt;abc123&lt;/session&gt;
%%%   &lt;tuples&gt;
%%%     &lt;tuple&gt;
%%%       &lt;attribute name="id"&gt;1&lt;/attribute&gt;
%%%       &lt;attribute name="name"&gt;Alice&lt;/attribute&gt;
%%%     &lt;/tuple&gt;
%%%   &lt;/tuples&gt;
%%% &lt;/response&gt;
%%% </pre>
%%%
%%% == Usage ==
%%% <pre>
%%% % Start server on port 8080
%%% xml_server:start(8080).
%%%
%%% % Client connects and sends:
%%% % QUERY {scan, employees}\n
%%% % NEXT &lt;session-id&gt; 10\n
%%% % NEXT &lt;session-id&gt; 10\n
%%% % CLOSE &lt;session-id&gt;\n
%%% </pre>
%%%
%%% @author Nekoma Team
%%% @copyright 2025

-module(xml_server).

-export([start/1, start/2, stop/0]).
-export([init/2, handle_client/2]).

-include("operations.hrl").

%%% Public API

%% @doc Start the XML server on specified port with example database.
%%
%% @param Port TCP port number to listen on
%% @returns {ok, Pid} on success
-spec start(pos_integer()) -> {ok, pid()}.
start(Port) ->
    DB = repl:example_db(),
    start(Port, DB).

%% @doc Start the XML server on specified port with custom database.
%%
%% @param Port TCP port number to listen on
%% @param DB Database state to use for queries
%% @returns {ok, Pid} on success
-spec start(pos_integer(), term()) -> {ok, pid()}.
start(Port, DB) ->
    spawn(?MODULE, init, [Port, DB]),
    io:format("XML Server started on port ~p~n", [Port]),
    {ok, whereis(xml_server)}.

%% @doc Stop the XML server.
%%
%% @returns ok
-spec stop() -> ok.
stop() ->
    case whereis(xml_server) of
        undefined -> ok;
        Pid ->
            Pid ! stop,
            ok
    end.

%%% Internal Functions

%% @private
%% @doc Initialize server and start listening.
init(Port, DB) ->
    register(xml_server, self()),
    {ok, ListenSocket} = gen_tcp:listen(Port, [
        binary,
        {packet, line},
        {active, false},
        {reuseaddr, true}
    ]),
    io:format("Listening for connections...~n"),
    accept_loop(ListenSocket, DB).

%% @private
%% @doc Accept incoming client connections.
accept_loop(ListenSocket, DB) ->
    receive
        stop ->
            gen_tcp:close(ListenSocket),
            io:format("Server stopped~n"),
            ok
    after 0 ->
        case gen_tcp:accept(ListenSocket, 1000) of
            {ok, ClientSocket} ->
                io:format("Client connected: ~p~n", [ClientSocket]),
                spawn(?MODULE, handle_client, [ClientSocket, DB]),
                accept_loop(ListenSocket, DB);
            {error, timeout} ->
                accept_loop(ListenSocket, DB);
            {error, Reason} ->
                io:format("Accept error: ~p~n", [Reason]),
                accept_loop(ListenSocket, DB)
        end
    end.

%% @private
%% @doc Handle client session with command processing.
handle_client(Socket, DB) ->
    Sessions = #{},
    client_loop(Socket, DB, Sessions).

%% @private
%% @doc Main client command processing loop.
client_loop(Socket, DB, Sessions) ->
    case gen_tcp:recv(Socket, 0) of
        {ok, Data} ->
            Line = binary_to_list(Data),
            Command = string:trim(Line),
            io:format("Received command: ~s~n", [Command]),

            case parse_command(Command) of
                {query, QueryStr} ->
                    {NewSessions, Response} = handle_query(QueryStr, DB, Sessions),
                    send_response(Socket, Response),
                    client_loop(Socket, DB, NewSessions);

                {next, SessionId, Count} ->
                    {NewSessions, Response} = handle_next(SessionId, Count, Sessions),
                    send_response(Socket, Response),
                    client_loop(Socket, DB, NewSessions);

                {close, SessionId} ->
                    {NewSessions, Response} = handle_close(SessionId, Sessions),
                    send_response(Socket, Response),
                    client_loop(Socket, DB, NewSessions);

                {schema} ->
                    Response = handle_schema(DB),
                    send_response(Socket, Response),
                    client_loop(Socket, DB, Sessions);

                {error, Msg} ->
                    Response = error_response(Msg),
                    send_response(Socket, Response),
                    client_loop(Socket, DB, Sessions)
            end;

        {error, closed} ->
            io:format("Client disconnected~n"),
            cleanup_sessions(Sessions),
            ok;

        {error, Reason} ->
            io:format("Receive error: ~p~n", [Reason]),
            cleanup_sessions(Sessions),
            ok
    end.

%% @private
%% @doc Parse client command.
parse_command(Command) ->
    case string:split(Command, " ", leading) of
        ["QUERY", QueryStr] ->
            {query, QueryStr};
        ["NEXT", Rest] ->
            case string:split(Rest, " ", leading) of
                [SessionId, CountStr] ->
                    try
                        Count = list_to_integer(CountStr),
                        {next, SessionId, Count}
                    catch
                        _:_ -> {error, "Invalid count"}
                    end;
                _ ->
                    {error, "Invalid NEXT syntax"}
            end;
        ["CLOSE", SessionId] ->
            {close, SessionId};
        ["SCHEMA"] ->
            {schema};
        _ ->
            {error, "Unknown command"}
    end.

%% @private
%% @doc Handle QUERY command - execute query and create iterator.
handle_query(QueryStr, DB, Sessions) ->
    try
        % Parse and evaluate the query plan from string using erl_eval
        % This allows parsing of function literals
        {ok, Tokens, _} = erl_scan:string(QueryStr ++ "."),
        {ok, Exprs} = erl_parse:parse_exprs(Tokens),
        {value, QueryPlan, _} = erl_eval:exprs(Exprs, erl_eval:new_bindings()),

        % Prepare and execute query to get iterator
        PreparedPlan = query_planner:prepare(QueryPlan),
        Iterator = query_planner:execute(DB, PreparedPlan),

        % Generate session ID
        SessionId = generate_session_id(),

        % Store iterator in sessions
        NewSessions = maps:put(SessionId, Iterator, Sessions),

        % Return session ID
        Response = session_response(SessionId),
        {NewSessions, Response}
    catch
        Error:Reason:Stack ->
            io:format("Query error: ~p:~p~n~p~n", [Error, Reason, Stack]),
            {Sessions, error_response(io_lib:format("Query failed: ~p", [Reason]))}
    end.

%% @private
%% @doc Handle NEXT command - fetch next N tuples from iterator.
handle_next(SessionId, Count, Sessions) ->
    case maps:get(SessionId, Sessions, undefined) of
        undefined ->
            {Sessions, error_response("Invalid session ID")};
        Iterator ->
            try
                % Collect next Count tuples
                {Tuples, NewIterator} = collect_n_tuples(Iterator, Count),

                % Update session with new iterator state
                NewSessions = maps:put(SessionId, NewIterator, Sessions),

                % Return tuples as XML
                Response = tuples_response(Tuples),
                {NewSessions, Response}
            catch
                Error:Reason:Stack ->
                    io:format("Next error: ~p:~p~n~p~n", [Error, Reason, Stack]),
                    {Sessions, error_response(io_lib:format("Next failed: ~p", [Reason]))}
            end
    end.

%% @private
%% @doc Handle CLOSE command - close iterator and free resources.
handle_close(SessionId, Sessions) ->
    case maps:get(SessionId, Sessions, undefined) of
        undefined ->
            {Sessions, error_response("Invalid session ID")};
        Iterator ->
            operations:close_iterator(Iterator),
            NewSessions = maps:remove(SessionId, Sessions),
            Response = ok_response("Session closed"),
            {NewSessions, Response}
    end.

%% @private
%% @doc Handle SCHEMA command - return database schema with all relations and their attributes.
handle_schema(DB) ->
    try
        % Get all relation names
        RelationNames = operations:get_relations(DB),

        % For each relation, get its hash and then the relation record
        Relations = lists:map(fun(Name) ->
            case operations:get_relation_hash(DB, Name) of
                {ok, Hash} ->
                    % Read the relation record from Mnesia
                    case mnesia:dirty_read(relation, Hash) of
                        [Relation] ->
                            {Name, Relation#relation.schema, Relation#relation.cardinality,
                             Relation#relation.constraints, Relation#relation.provenance};
                        [] ->
                            {Name, #{}, unknown, #{}, undefined}
                    end;
                {error, _} ->
                    {Name, #{}, unknown, #{}, undefined}
            end
        end, RelationNames),

        schema_response(Relations)
    catch
        Error:Reason:Stack ->
            io:format("Schema error: ~p:~p~n~p~n", [Error, Reason, Stack]),
            error_response(io_lib:format("Schema failed: ~p", [Reason]))
    end.

%% @private
%% @doc Collect N tuples from iterator lazily.
collect_n_tuples(Iterator, Count) ->
    collect_n_tuples(Iterator, Count, []).

collect_n_tuples(Iterator, 0, Acc) ->
    {lists:reverse(Acc), Iterator};
collect_n_tuples(Iterator, Count, Acc) ->
    case operations:next_tuple(Iterator) of
        {ok, Tuple} ->
            collect_n_tuples(Iterator, Count - 1, [Tuple | Acc]);
        done ->
            {lists:reverse(Acc), Iterator}
    end.

%% @private
%% @doc Generate unique session ID.
generate_session_id() ->
    Timestamp = erlang:system_time(microsecond),
    Random = rand:uniform(1000000),
    Hash = crypto:hash(md5, term_to_binary({Timestamp, Random})),
    binary_to_list(base64:encode(Hash)).

%% @private
%% @doc Cleanup all sessions on disconnect.
cleanup_sessions(Sessions) ->
    maps:foreach(
        fun(_SessionId, Iterator) ->
            operations:close_iterator(Iterator)
        end,
        Sessions).

%%% XML Response Formatters

%% @private
%% @doc Format session response.
session_response(SessionId) ->
    [
        "<?xml version=\"1.0\"?>\n",
        "<response>\n",
        "  <status>ok</status>\n",
        "  <session>", SessionId, "</session>\n",
        "</response>\n"
    ].

%% @private
%% @doc Format schema response as XML.
schema_response(Relations) ->
    RelationsXML = lists:map(fun({Name, Schema, Cardinality, Constraints, Provenance}) ->
        % Convert cardinality to string
        CardStr = case Cardinality of
            {finite, N} -> io_lib:format("finite(~p)", [N]);
            aleph_zero -> "infinite";
            continuum -> "continuum";
            _ -> "unknown"
        end,

        % Convert schema map to attribute elements
        % Wrap each attribute in a list to keep it as a single element when reversed
        Attributes = maps:fold(fun(AttrName, Type, Acc) ->
            [
                ["      <attribute name=\"", atom_to_list(AttrName), "\" type=\"",
                 atom_to_list(Type), "\"/>\n"]
                | Acc
            ]
        end, [], Schema),

        % Convert constraints to XML
        ConstraintsXML = case maps:size(Constraints) of
            0 -> [];
            _ -> [
                "      <constraints>\n",
                maps:fold(fun(AttrName, Constraint, Acc) ->
                    ConstraintStr = format_constraint(Constraint),
                    [["        <constraint attribute=\"", atom_to_list(AttrName),
                      "\">", ConstraintStr, "</constraint>\n"] | Acc]
                end, [], Constraints),
                "      </constraints>\n"
            ]
        end,

        % Convert provenance to XML
        ProvenanceXML = case Provenance of
            undefined -> [];
            _ -> [
                "      <provenance>", format_provenance(Provenance), "</provenance>\n"
            ]
        end,

        [
            "    <relation name=\"", atom_to_list(Name), "\" cardinality=\"", CardStr, "\">\n",
            lists:reverse(Attributes),
            ConstraintsXML,
            ProvenanceXML,
            "    </relation>\n"
        ]
    end, Relations),

    [
        "<?xml version=\"1.0\"?>\n",
        "<response>\n",
        "  <status>ok</status>\n",
        "  <schema>\n",
        RelationsXML,
        "  </schema>\n",
        "</response>\n"
    ].

%% @private
%% @doc Format constraint as string.
format_constraint({eq, Value}) -> lists:flatten(io_lib:format("= ~p", [Value]));
format_constraint({neq, Value}) -> lists:flatten(io_lib:format("!= ~p", [Value]));
format_constraint({lt, Value}) -> lists:flatten(io_lib:format("&lt; ~p", [Value]));
format_constraint({lte, Value}) -> lists:flatten(io_lib:format("&lt;= ~p", [Value]));
format_constraint({gt, Value}) -> lists:flatten(io_lib:format("&gt; ~p", [Value]));
format_constraint({gte, Value}) -> lists:flatten(io_lib:format("&gt;= ~p", [Value]));
format_constraint({in, List}) -> lists:flatten(io_lib:format("in ~p", [List]));
format_constraint({range, Min, Max}) -> lists:flatten(io_lib:format("in [~p, ~p]", [Min, Max]));
format_constraint({member_of, Relation}) -> lists:flatten(io_lib:format("in ~p", [Relation]));
format_constraint(Other) -> lists:flatten(io_lib:format("~p", [Other])).

%% @private
%% @doc Format provenance as string.
format_provenance(undefined) -> "undefined";
format_provenance({base, Relation}) -> lists:flatten(io_lib:format("base(~p)", [Relation]));
format_provenance({join, P1, P2}) ->
    lists:flatten(io_lib:format("join(~s, ~s)", [format_provenance(P1), format_provenance(P2)]));
format_provenance({select, P, _Constraints}) ->
    lists:flatten(io_lib:format("select(~s)", [format_provenance(P)]));
format_provenance({project, P, Attrs}) ->
    lists:flatten(io_lib:format("project(~s, ~p)", [format_provenance(P), Attrs]));
format_provenance({take, P, N}) ->
    lists:flatten(io_lib:format("take(~s, ~p)", [format_provenance(P), N]));
format_provenance(Other) -> lists:flatten(io_lib:format("~p", [Other])).

%% @private
%% @doc Format tuples response.
tuples_response(Tuples) ->
    [
        "<?xml version=\"1.0\"?>\n",
        "<response>\n",
        "  <status>ok</status>\n",
        "  <tuples>\n",
        lists:map(fun tuple_to_xml/1, Tuples),
        "  </tuples>\n",
        "</response>\n"
    ].

%% @private
%% @doc Format single tuple as XML.
tuple_to_xml(Tuple) ->
    Attributes = maps:to_list(Tuple),
    [
        "    <tuple>\n",
        lists:map(fun({Name, Value}) ->
            EscapedValue = escape_xml(format_value(Value)),
            io_lib:format("      <attribute name=\"~s\">~s</attribute>\n",
                         [Name, EscapedValue])
        end, Attributes),
        "    </tuple>\n"
    ].

%% @private
%% @doc Format value for XML output.
format_value(Value) when is_integer(Value) -> integer_to_list(Value);
format_value(Value) when is_float(Value) -> float_to_list(Value);
format_value(Value) when is_atom(Value) -> atom_to_list(Value);
format_value(Value) when is_binary(Value) -> binary_to_list(Value);
format_value(Value) when is_list(Value) -> Value;
format_value(Value) -> io_lib:format("~p", [Value]).

%% @private
%% @doc Escape XML special characters.
escape_xml(Str) ->
    lists:flatten(
        lists:map(
            fun($<) -> "&lt;";
               ($>) -> "&gt;";
               ($&) -> "&amp;";
               ($") -> "&quot;";
               ($') -> "&apos;";
               (C) -> C
            end,
            lists:flatten(Str))).

%% @private
%% @doc Format error response.
error_response(Message) ->
    [
        "<?xml version=\"1.0\"?>\n",
        "<response>\n",
        "  <status>error</status>\n",
        "  <message>", escape_xml(Message), "</message>\n",
        "</response>\n"
    ].

%% @private
%% @doc Format OK response.
ok_response(Message) ->
    [
        "<?xml version=\"1.0\"?>\n",
        "<response>\n",
        "  <status>ok</status>\n",
        "  <message>", escape_xml(Message), "</message>\n",
        "</response>\n"
    ].

%% @private
%% @doc Send response to client.
send_response(Socket, Response) ->
    gen_tcp:send(Socket, Response).
