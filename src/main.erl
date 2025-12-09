-module(main).
-behaviour(application).

-export([start/2, stop/1]).

%% @doc Initialize Mnesia database schema and tables.
%%
%% Creates a fresh Mnesia schema with four tables for content-addressed storage:
%% <ul>
%%   <li>`attribute' - Hash → Value mapping for deduplication</li>
%%   <li>`tuple' - Hash → Tuple mapping with attribute references</li>
%%   <li>`relation' - Hash → Relation with merkle tree and schema</li>
%%   <li>`database_state' - Hash → Database with relation merkle tree</li>
%% </ul>
%%
%% <b>Warning:</b> This function deletes any existing schema and recreates it from scratch.
%%
%% @returns `ok' on success
%% @see operations:create_database/1
setup() ->
  mnesia:stop(),
  mnesia:delete_schema([node()]),
  mnesia:create_schema([node()]),
  mnesia:start(),
  mnesia:create_table(attribute, [
    {attributes, [hash, value]},
    {disc_copies, [node()]},
    {type, set}
  ]),
  mnesia:create_table(tuple, [
    {attributes, [hash, relation, attribute_map]},
    {disc_copies, [node()]},
    {type, set}
  ]),
  mnesia:create_table(relation, [
    {attributes, [hash, name, tree, schema, constraints, cardinality, generator, membership_criteria, mutability, provenance, lineage]},
    {disc_copies, [node()]},
    {type, set},
    {index, [name]}  % Index on name for efficient lookup
  ]),
  mnesia:create_table(database_state, [
    {attributes, [hash, name, tree, relations, timestamp]},
    {disc_copies, [node()]},
    {type, set}
  ]).

uuid() ->
  ok = application:load(quickrand),
  ok = quickrand:seed(),
  ok = quickrand_cache:init(),
  quickrand_cache:new().

start(_StartType, _StartArgs) ->
    %% Start Mnesia (or other children) here
    io:format("Starting app...~n"),

    %% Ensure Mnesia is started
    mnesia:start(),
    setup(),
    _ = uuid(),

    {ok, self()}.

stop(_State) ->
    io:format("Stopping app...~n"),
    ok.
