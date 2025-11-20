-module(operations).
-export([setup/0, 
	 hash/1, 
         create_database/1,
	 create_relation/3, 
	 create_tuple/3, 
	 get_relations/1, 
	 get_relation_hash/2,
         hashes_from_tuple/1,
	 get_tuples_iterator/2,
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
        {attributes, [hash, name, tree, relations, timestamp]},
        {disc_copies, [node()]},
        {type, set}
    ]).

-record(database_state, {hash, name, tree, relations, timestamp}).
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
create_tuple(Database, RelationName, Tuple) when is_map(Tuple), is_record(Database, database_state) ->
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
        %% We don't need to store the content of a tuple hash, just the key
        %% merklet can spot the difference in both keys and values
        %% sets:set follows the same strategy of an empty value
        NewRelationTree = merklet:insert({TupleHash, <<>>}, RelationRecord#relation.tree),
        
        %% Step 6: Compute new relation hash as the root of the new merkle tree and store
        {_,NewRelationHash,_,_} = NewRelationTree,
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
        {_,NewDatabaseHash,_,_} = NewDatabaseTree,
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

retract_tuple(Database, RelationHash, TupleHashes) 
  when is_record(Database, database_state) andalso is_list(TupleHashes) ->
    todo;
retract_tuple(Database, RelationHash, TupleHash) when is_record(Database, database_state) ->
    [Relation] = mnesia:dirty_read(relation, RelationHash),
    RelationTreeWithoutTuple = merklet:delete(TupleHash, Relation#relation.tree),
    {_,NewRelationHash,_,_} = RelationTreeWithoutTuple,
    DatabaseTreeWithoutTuple = merklet:delete(RelationHash, Database#database_state.tree),
    {_,NewDatabaseHash,_,_} = DatabaseTreeWithoutTuple,
    UpdatedDatabase =
	Database#database_state{timestamp = erlang:timestamp(),
				hash = NewDatabaseHash,
			        tree = DatabaseTreeWithoutTuple},
    UpdatedRelation =
	Relation#relation{hash = NewRelationHash,
			  tree = RelationTreeWithoutTuple},
    TX = fun () ->
		 mnesia:write(relation, UpdatedRelation, write),
		 mnesia:write(database_state, UpdatedDatabase, write)
	 end,
    case mnesia:is_transaction() of
	true -> TX();
	false -> mnesia:transaction(TX)
    end.

retract_relation(Database, Name) ->
    todo.

create_relation(Database, Name, Definition) when is_record(Database, database_state) ->
    F = fun() ->
        %% Create relation with empty tree
	%% The meaning of a relation needs to be attached to its name
        %% It may sound nominalistic but that is a physical concern.
        %% Two relations might share definitions but their name gives another interpretation
        %% and therefore different tuples
        RelationHash = hash({Name, Definition}),
        NewRelation = #relation{
            hash = RelationHash, 
            name = Name, 
            tree = undefined, 
            schema = Definition
        },
        mnesia:write(relation, NewRelation, write),
        
        %% Update database relations map
        %% TODO: maybe we can purge this later after including the name on the merkle trees
        NewRelations = maps:put(Name, RelationHash, Database#database_state.relations),
        
        %% Update database tree (for diffing)
	%% Note that the name here is important as a key because
        %% we gotta go from a name to a hash within a database state
        NewTree = merklet:insert({atom_to_binary(Name), RelationHash}, Database#database_state.tree),
        {_,NewHash,_,_} = NewTree,
        
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
        hash = <<>>, 
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

%% @doc Resolve a tuple hash to actual values
%% Returns map with attribute names and actual values
resolve_tuple(TupleHash) ->
    [#tuple{attribute_map = AttributeMap}] = mnesia:dirty_read(tuple, TupleHash),
    maps:map(fun(_AttrName, ValueHash) ->
        [#attribute{value = Value}] = mnesia:dirty_read(attribute, ValueHash),
        Value
    end, AttributeMap).

%% All reads can be dirty as it is all immutable
%% Writes need to be transactional
hashes_from_tuple(RelationHash) ->
    [Relation] = mnesia:dirty_read(relation, RelationHash),
    case Relation#relation.tree of
        undefined -> [];
        Tree -> merklet:keys(Tree)
    end.

%% @doc Create an iterator for streaming tuples from a relation
%% Returns: Pid of iterator process
get_tuples_iterator(Database, RelationName) when is_record(Database, database_state) ->
    case maps:get(RelationName, Database#database_state.relations, error) of
	error -> erlang:error({error_tuple_iterator_init, RelationName});
	RelationHash -> TupleHashes = hashes_from_tuple(RelationHash),
			spawn(fun() -> tuple_iterator_loop(TupleHashes) end)
    end.

%% X = (a, b, c, d)
%% Y = (a, x, y)
%% Z = project (a, c, y, natural_join(X, Y))

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
            Acc;
        {error, Reason} -> 
            close_iterator(IteratorPid),
            {error, Reason, Acc}
    end.

