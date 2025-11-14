-module(operations).
-export([setup/0, 
	 store/3, 
	 hash/1, 
         create_database/1, 
	 create_relation/3, 
	 get_relations/1, 
	 get_relation_hash/2,
         get_tuple_hashes/1,
	 get_tuples_iterator/1,
	 next_tuple/1,
	 close_iterator/1, 
	 collect_all/1]).

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
        {attributes, [hash, name, tree, schema]},
        {disc_copies, [node()]},
        {type, set}
    ]),
    mnesia:create_table(database_state, [
        {attributes, [name, hash, tree, relations, timestamp]},
        {disc_copies, [node()]},
        {type, set}
    ]).

-record(database_state, {name, hash, tree, relations, timestamp}).
-record(relation, {hash, name, tree, schema}).
-record(tuple, {hash, relation, attribute_map}).
-record(attribute, {hash, value}).

hash(Value) ->
    crypto:hash(sha256, term_to_binary(Value)).

%% @doc Store a tuple into a relation and update database state
%% Database: #database_state{} record
%% Relation: relation name (atom)
%% Tuple: (map) #{name => "John", age => 18}
%% Returns: {UpdatedDatabase, UpdatedRelation}
store(Database, RelationName, Tuple) when is_map(Tuple), is_record(Database, database_state) ->
    F = fun() ->
        %% Step 1: Store each attribute value and build attribute_map
        AttributeMap = maps:map(fun(_AttrName, Value) ->
            ValueHash = hash(Value),
            mnesia:write(attribute, #attribute{hash = ValueHash, value = Value}, write),
            ValueHash
        end, Tuple),
        
        %% Step 2: Compute tuple hash and store tuple
        TupleHash = hash(Tuple),
        mnesia:write(tuple, #tuple{hash = TupleHash, relation = RelationName, attribute_map = AttributeMap}, write),
        
        %% Step 3: Get current relation hash from database
        CurrentRelationHash = maps:get(RelationName, Database#database_state.relations),
        
        %% Step 4: Read current relation
        [RelationRecord] = mnesia:read(relation, CurrentRelationHash),
        
        %% Step 5: Insert tuple hash into relation merkle tree
        NewRelationTree = merklet:insert({TupleHash, TupleHash}, RelationRecord#relation.tree),
        
        %% Step 6: Compute new relation hash and store
        NewRelationHash = hash(NewRelationTree),
        UpdatedRelation = #relation{
            hash = NewRelationHash, 
            name = RelationName, 
            tree = NewRelationTree, 
            schema = RelationRecord#relation.schema
        },
        mnesia:write(relation, UpdatedRelation, write),
        
        %% Step 7: Update database relations map
        NewRelations = maps:put(RelationName, NewRelationHash, Database#database_state.relations),
        
        %% Step 8: Update database tree (for diffing)
        RelationKey = atom_to_binary(RelationName, utf8),
        NewDatabaseTree = merklet:insert({RelationKey, NewRelationHash}, Database#database_state.tree),
        
        %% Step 9: Compute new database hash and store
        NewDatabaseHash = hash(NewDatabaseTree),
        UpdatedDatabase = #database_state{
            name = Database#database_state.name,
            hash = NewDatabaseHash,
            tree = NewDatabaseTree,
            relations = NewRelations,
            timestamp = erlang:timestamp()
        },
        mnesia:write(database_state, UpdatedDatabase, write),
        
        {UpdatedDatabase, UpdatedRelation}
    end,
    
    {atomic, Result} = mnesia:transaction(F),
    Result.

create_relation(Database, Name, Definition) when is_record(Database, database_state) ->
    F = fun() ->
        %% Create relation with empty tree
        RelationHash = hash({Name, Definition}),
        NewRelation = #relation{
            hash = RelationHash, 
            name = Name, 
            tree = undefined, 
            schema = Definition
        },
        mnesia:write(relation, NewRelation, write),
        
        %% Update database relations map
        NewRelations = maps:put(Name, RelationHash, Database#database_state.relations),
        
        %% Update database tree (for diffing)
        RelationKey = atom_to_binary(Name, utf8),
        NewTree = merklet:insert({RelationKey, RelationHash}, Database#database_state.tree),
        NewHash = hash(NewTree),
        
        UpdatedDatabase = #database_state{
            name = Database#database_state.name,
            hash = NewHash,
            tree = NewTree,
            relations = NewRelations,
            timestamp = erlang:timestamp()
        },
        mnesia:write(database_state, UpdatedDatabase, write),
        
        {UpdatedDatabase, NewRelation}
    end,
    {atomic, Result} = mnesia:transaction(F),
    Result.

create_database(Name) ->
    Entry = #database_state{
        name = Name, 
        hash = hash(undefined), 
        tree = undefined,
        relations = #{},
        timestamp = erlang:timestamp()
    },
    {atomic, ok} = mnesia:transaction(fun () -> 
        mnesia:write(database_state, Entry, write) 
    end),
    Entry.

%% @doc Get all relation names in a database
get_relations(Database) ->
    maps:keys(Database#database_state.relations).

%% @doc Get relation hash by name
get_relation_hash(Database, RelationName) ->
    case maps:find(RelationName, Database#database_state.relations) of
        error -> {error, not_found};
        X -> X
    end.

%% @doc Get all tuple hashes in a relation
get_tuple_hashes(Relation) when is_record(Relation, relation) ->
    case Relation#relation.tree of
        undefined -> [];
        Tree -> merklet:keys(Tree)
    end.

%% %% @doc Get all tuples from a relation (resolved with actual values)
%% %% Returns list of maps with actual attribute values
%% get_tuples(Relation) when is_record(Relation, relation) ->
%%     TupleHashes = get_tuple_hashes(Relation),
%%     F = fun() ->
%%         lists:map(fun(TupleHash) ->
%%             resolve_tuple(TupleHash)
%%         end, TupleHashes)
%%     end,
%%     {atomic, Tuples} = mnesia:transaction(F),
%%     Tuples.

%% @doc Resolve a tuple hash to actual values
%% Returns map with attribute names and actual values
resolve_tuple(TupleHash) ->
    F = fun() ->
        %% Read tuple record
        [#tuple{attribute_map = AttributeMap}] = mnesia:read(tuple, TupleHash),
        
        %% Resolve each attribute value
        maps:map(fun(_AttrName, ValueHash) ->
            [#attribute{value = Value}] = mnesia:read(attribute, ValueHash),
            Value
        end, AttributeMap)
    end,
    {atomic, ResolvedTuple} = mnesia:transaction(F),
    ResolvedTuple.

%% @doc Create an iterator for streaming tuples from a relation
%% Returns: Pid of iterator process
get_tuples_iterator(Relation) when is_record(Relation, relation) ->
    TupleHashes = get_tuple_hashes(Relation),
    spawn(fun() -> tuple_iterator_loop(TupleHashes) end).

%% @doc Get next tuple from iterator1
%% Returns: {ok, Tuple} | done | {error, timeout}
next_tuple(IteratorPid) ->
    IteratorPid ! {next, self()},
    receive
        {tuple, Tuple} -> {ok, Tuple};
        done -> done
    after 5000 ->
        {error, timeout}
    end.

%% @doc Close iterator process
close_iterator(IteratorPid) ->
    IteratorPid ! stop,
    ok.

%% Iterator loop: streams tuples one at a time
tuple_iterator_loop([]) ->
    receive
        {next, Caller} -> 
            Caller ! done;
        stop -> 
            ok
    end;
tuple_iterator_loop([TupleHash | Rest]) ->
    receive
        {next, Caller} ->
            %% Resolve tuple on demand
            ResolvedTuple = resolve_tuple(TupleHash),
            Caller ! {tuple, ResolvedTuple},
            tuple_iterator_loop(Rest);
        stop ->
            ok
    end.

%% @doc Helper to collect all tuples from iterator (for testing)
collect_all(IteratorPid) ->
    collect_all(IteratorPid, []).

collect_all(IteratorPid, Acc) ->
    case next_tuple(IteratorPid) of
        {ok, Tuple} -> collect_all(IteratorPid, [Tuple | Acc]);
        done -> 
            close_iterator(IteratorPid),
            lists:reverse(Acc);
        {error, Reason} -> 
            close_iterator(IteratorPid),
            {error, Reason, lists:reverse(Acc)}
    end.

%% @doc Infer type of a value
type_of(Value) when is_binary(Value) -> binary;
type_of(Value) when is_list(Value) -> list;
type_of(Value) when is_integer(Value) -> integer;
type_of(Value) when is_float(Value) -> float;
type_of(Value) when is_atom(Value) -> atom;
type_of(_) -> term.
