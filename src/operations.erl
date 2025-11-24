%%% @doc Extended Relational Database Operations (RM/T)
%%%
%%% This module implements core database operations for Domino, an extended
%%% relational database engine based on E. F. Codd's RM/T (1979).
%%%
%%% == Architecture ==
%%%
%%% The system uses:
%%% <ul>
%%%   <li><b>Content-Addressed Storage</b> - All entities identified by SHA-256 hashes</li>
%%%   <li><b>Immutable Data Structures</b> - Operations create new versions</li>
%%%   <li><b>Merkle Trees</b> - Efficient versioning and diffing via merklet</li>
%%%   <li><b>Mnesia Backend</b> - ACID transactions and persistent storage</li>
%%%   <li><b>Volcano Iterator Model</b> - Lazy evaluation with process-based iterators</li>
%%% </ul>
%%%
%%% == Data Model ==
%%%
%%% The system maintains four tables in Mnesia:
%%% <ul>
%%%   <li>`attribute' - Stores atomic values with their hashes</li>
%%%   <li>`tuple' - Stores tuples as maps from attribute names to value hashes</li>
%%%   <li>`relation' - Stores relations with merkle trees of tuple hashes</li>
%%%   <li>`database_state' - Stores database snapshots with relation merkle trees</li>
%%% </ul>
%%%
%%% == RM/T Integrity Rules ==
%%%
%%% <ul>
%%%   <li><b>Rule 3 (Entity Integrity)</b> - E-relations accept insertions/deletions, not updates</li>
%%%   <li><b>Rule 4 (Property Integrity)</b> - Tuples require entity existence in E-relation</li>
%%% </ul>
%%%
%%% @author Nekoma Team
%%% @copyright 2025
%%% @version 0.1.0
%%% @reference E. F. Codd (1979). "Extending the Database Relational Model to Capture More Meaning"
%%% @reference E. F. Codd (1970). "A Relational Model of Data for Large Shared Data Banks"

-module(operations).
-include("operations.hrl").
-export([setup/0,
	 hash/1,
         create_database/1,
	 create_relation/3,
	 create_infinite_relation/2,
	 create_tuple/3,
	 retract_tuple/3,
	 retract_tuple/4,
	 clear_relation/2,
	 clear_relation/3,
	 retract_relation/2,
	 retract_relation/3,
	 get_relations/1,
	 get_relation_hash/2,
         hashes_from_tuple/1,
	 get_tuples_iterator/2,
	 get_tuples_iterator/3,
	 take/3,
	 next_tuple/1,
	 close_iterator/1,
	 collect_all/1,
	 %% Iterator operators (lazy)
	 select_iterator/2,
	 project_iterator/2,
	 sort_iterator/2,
	 take_iterator/2,
	 %% Join operators
	 equijoin_iterator/3,
	 theta_join_iterator/3,
	 %% Materialization (eager)
	 materialize/3]).

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
%% @see create_database/1
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
        {attributes, [hash, name, tree, schema, constraints, cardinality, generator, provenance]},
        {disc_copies, [node()]},
        {type, set}
    ]),
    mnesia:create_table(database_state, [
        {attributes, [hash, name, tree, relations, timestamp]},
        {disc_copies, [node()]},
        {type, set}
    ]).

%% @doc Compute SHA-256 hash of an Erlang term.
%%
%% This is the foundational hashing function used throughout the system for
%% content addressing. All entities (attributes, tuples, relations, databases)
%% are identified by hashes computed with this function.
%%
%% @param Value Any Erlang term to hash
%% @returns Binary SHA-256 hash (32 bytes)
%%
%% @see create_tuple/3
%% @see create_relation/3
hash(Value) ->
    crypto:hash(sha256, term_to_binary(Value)).

%% @doc Insert a tuple into a relation and update the database state.
%%
%% This is the primary operation for adding data to the database. It:
%% <ol>
%%   <li>Stores each attribute value with its hash (automatic deduplication)</li>
%%   <li>Creates a tuple record mapping attribute names to value hashes</li>
%%   <li>Inserts the tuple hash into the relation's merkle tree</li>
%%   <li>Computes new relation hash from updated tree</li>
%%   <li>Updates database merkle tree with new relation hash</li>
%%   <li>Computes new database hash</li>
%% </ol>
%%
%% All operations execute within a Mnesia transaction for ACID guarantees.
%%
%% == Example ==
%% ```
%% DB = operations:create_database(my_db),
%% {DB1, _} = operations:create_relation(DB, users, #{name => string, age => integer}),
%% {DB2, _} = operations:create_tuple(DB1, users, #{name => "Alice", age => 30}).
%% '''
%%
%% @param Database `#database_state{}' record
%% @param RelationName Atom naming the relation
%% @param Tuple Map of attribute names to values (e.g., `#{name => "Alice", age => 30}')
%% @returns `{UpdatedDatabase, UpdatedRelation}' tuple
%%
%% @see create_relation/3
%% @see get_tuples_iterator/2
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
        {_,NewRelationTreeHash,_,_} = NewRelationTree,
	NewRelationHash = hash({RelationRecord#relation.name, RelationRecord#relation.schema, NewRelationTreeHash}),

        %% Update cardinality: increment by 1
        NewCardinality = case RelationRecord#relation.cardinality of
            {finite, N} -> {finite, N + 1};
            Other -> Other  % Shouldn't happen for finite relations
        end,

        UpdatedRelation = #relation{
            hash = NewRelationHash,
            name = RelationName,
            tree = NewRelationTree,
            schema = RelationRecord#relation.schema,
            constraints = RelationRecord#relation.constraints,
            cardinality = NewCardinality,
            generator = RelationRecord#relation.generator,
            provenance = RelationRecord#relation.provenance
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

%% @doc Clear all tuples from a relation (truncate).
%%
%% Wrapper for `clear_relation/3' with transactions enabled.
%%
%% @param Database `#database_state{}' record
%% @param RelationHash Binary hash identifying the relation
%% @returns `{atomic, {UpdatedDatabase, UpdatedRelation}}' or `{aborted, Reason}'
%%
%% @see clear_relation/3
%% @see retract_relation/2
clear_relation(Database, RelationHash) ->
    clear_relation(Database, RelationHash, true).

%% @doc Clear all tuples from a relation with optional transaction control.
%%
%% Removes all tuples from the relation but preserves its schema. The relation's
%% merkle tree is set to `undefined', and a new relation hash is computed.
%%
%% @param Database `#database_state{}' record
%% @param RelationHash Binary hash identifying the relation
%% @param Transact Boolean - if `true', wraps operation in Mnesia transaction
%% @returns When `Transact =:= true': `{atomic, {UpdatedDatabase, UpdatedRelation}}'
%%          When `Transact =:= false': `{UpdatedDatabase, UpdatedRelation}'
%%
%% @see retract_relation/2
clear_relation(Database, RelationHash, Transact) ->
    [Relation] = mnesia:dirty_read(relation, RelationHash),
    NewRelationHash = hash({Relation#relation.name, Relation#relation.schema, undefined}),
    UpdatedRelation =
	Relation#relation{hash = NewRelationHash,
			  tree = undefined},
    DatabaseTreeWithoutTuples = merklet:insert({atom_to_binary(Relation#relation.name, utf8), NewRelationHash}, Database#database_state.tree),
    {_,NewDatabaseHash,_,_} = DatabaseTreeWithoutTuples,
    NewRelations = maps:put(Relation#relation.name, NewRelationHash, Database#database_state.relations),
    UpdatedDatabase =
	Database#database_state{timestamp = erlang:timestamp(),
			        hash = NewDatabaseHash,
			        tree = DatabaseTreeWithoutTuples,
			        relations = NewRelations},
     TX = fun () ->
		 mnesia:write(relation, UpdatedRelation, write),
		 mnesia:write(database_state, UpdatedDatabase, write),
		 {UpdatedDatabase, UpdatedRelation}
	 end,
    case Transact of
	true -> mnesia:transaction(TX);
	false -> {UpdatedDatabase, UpdatedRelation}
    end.

%% @doc Remove a tuple from a relation.
%%
%% Wrapper for `retract_tuple/4' with transactions enabled.
%%
%% @param Database `#database_state{}' record
%% @param RelationHash Binary hash identifying the relation
%% @param TupleHash Binary hash identifying the tuple to remove
%% @returns `{atomic, {UpdatedDatabase, UpdatedRelation}}' or `{aborted, Reason}'
%%
%% @see retract_tuple/4
%% @see create_tuple/3
retract_tuple(Database, RelationHash, TupleHash) ->
    retract_tuple(Database, RelationHash, TupleHash, true).

%% @doc Remove a tuple from a relation with optional transaction control.
%%
%% Deletes a specific tuple from the relation's merkle tree and recomputes
%% the relation and database hashes.
%%
%% @param Database `#database_state{}' record
%% @param RelationHash Binary hash identifying the relation
%% @param TupleHash Binary hash identifying the tuple to remove
%% @param Transact Boolean - if `true', wraps operation in Mnesia transaction
%% @returns When `Transact =:= true': `{atomic, {UpdatedDatabase, UpdatedRelation}}'
%%          When `Transact =:= false': `{UpdatedDatabase, UpdatedRelation}'
%%
%% @see create_tuple/3
retract_tuple(Database, RelationHash, TupleHash, Transact)
  when is_record(Database, database_state) ->
    [Relation] = mnesia:dirty_read(relation, RelationHash),
    RelationTreeWithoutTuple = merklet:delete(TupleHash, Relation#relation.tree),
    {_,NewRelationTreeHash,_,_} = RelationTreeWithoutTuple,
    NewRelationHash = hash({Relation#relation.name, Relation#relation.schema, NewRelationTreeHash}),
    DatabaseTreeWithoutTuple = merklet:insert({atom_to_binary(Relation#relation.name, utf8), NewRelationHash}, Database#database_state.tree),
    {_,NewDatabaseHash,_,_} = DatabaseTreeWithoutTuple,
    NewRelations = maps:put(Relation#relation.name, NewRelationHash, Database#database_state.relations),
    UpdatedDatabase =
	Database#database_state{timestamp = erlang:timestamp(),
				hash = NewDatabaseHash,
			        tree = DatabaseTreeWithoutTuple,
			        relations = NewRelations},
    UpdatedRelation =
	Relation#relation{hash = NewRelationHash,
			  tree = RelationTreeWithoutTuple},
    TX = fun () ->
		 mnesia:write(relation, UpdatedRelation, write),
		 mnesia:write(database_state, UpdatedDatabase, write),
		 {UpdatedDatabase, UpdatedRelation}
	 end,
    case Transact of
	true -> mnesia:transaction(TX);
	false -> {UpdatedDatabase, UpdatedRelation}
    end.

%% @doc Remove a relation from the database.
%%
%% Wrapper for `retract_relation/3' with transactions enabled.
%%
%% @param Database `#database_state{}' record
%% @param Name Atom naming the relation to remove
%% @returns `{atomic, UpdatedDatabase}' or `{aborted, Reason}'
%%
%% @see retract_relation/3
%% @see clear_relation/2
retract_relation(Database, Name) ->
    retract_relation(Database, Name, true).

%% @doc Remove a relation from the database with optional transaction control.
%%
%% First clears all tuples from the relation, then removes the relation entirely
%% from the database's relation map and merkle tree.
%%
%% @param Database `#database_state{}' record
%% @param Name Atom naming the relation to remove
%% @param Transact Boolean - if `true', wraps operation in Mnesia transaction
%% @returns When `Transact =:= true': `{atomic, UpdatedDatabase}'
%%          When `Transact =:= false': `UpdatedDatabase'
%%
%% @see create_relation/3
retract_relation(Database, Name, Transact) ->
    RelationHash = maps:get(Name, Database#database_state.relations, {error, retract_relation_could_not_find}),
    {DatabaseWithClearedRelation, _ClearedRelation} = clear_relation(Database, RelationHash, false),
    NewTree = merklet:delete(atom_to_binary(Name, utf8), DatabaseWithClearedRelation#database_state.tree),
    NewHash = case NewTree of
                  undefined -> <<>>;
                  {_,H,_,_} -> H
              end,
    UpdatedDatabase =
	DatabaseWithClearedRelation#database_state{relations = maps:remove(Name, DatabaseWithClearedRelation#database_state.relations),
						   hash = NewHash,
						   tree = NewTree,
						   timestamp = erlang:timestamp()},
    TX = fun () ->
		 mnesia:write(database_state, UpdatedDatabase, write),
		 UpdatedDatabase
	 end,
    case Transact of
	true -> mnesia:transaction(TX);
	false -> UpdatedDatabase
    end.

%% @doc Create a new relation in the database with a schema definition.
%%
%% Creates an empty relation (no tuples) with the specified schema. The schema
%% is stored but not yet enforced - it serves as documentation and provides a
%% foundation for future domain validation.
%%
%% <b>Important:</b> The relation's identity is tied to its name. Two relations
%% with identical schemas but different names are semantically distinct and can
%% contain different tuples.
%%
%% == Example ==
%% ```
%% DB = operations:create_database(my_db),
%% Schema = #{name => string, age => integer, email => string},
%% {DB1, _Rel} = operations:create_relation(DB, users, Schema).
%% '''
%%
%% @param Database `#database_state{}' record
%% @param Name Atom naming the new relation
%% @param Definition Map specifying schema (e.g., `#{name => string, age => integer}')
%% @returns `{UpdatedDatabase, NewRelation}' tuple
%%
%% @see create_database/1
%% @see create_tuple/3
%% @see retract_relation/2
create_relation(Database, Name, Definition) when is_record(Database, database_state) ->
    F = fun() ->
        %% Create relation with empty tree
	%% The meaning of a relation needs to be attached to its name
        %% It may sound nominalistic but that is a physical concern.
        %% Two relations might share definitions but their name gives another interpretation
        %% and therefore different tuples
        RelationHash = hash({Name, Definition, undefined}),
        NewRelation = #relation{
            hash = RelationHash,
            name = Name,
            tree = undefined,
            schema = Definition,
            constraints = #{},           % No constraints by default
            cardinality = {finite, 0},   % Empty relation
            generator = undefined,        % Finite relation has no generator
            provenance = {base, Name}     % Base relation
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

%% @doc Create a new database state.
%%
%% Initializes an empty database with no relations. The database starts with:
%% <ul>
%%   <li>Empty relation map</li>
%%   <li>Undefined merkle tree (will be created when first relation is added)</li>
%%   <li>Empty hash</li>
%%   <li>Timestamp of creation</li>
%% </ul>
%%
%% All changes are committed to Mnesia within a transaction.
%%
%% == Example ==
%% ```
%% DB = operations:create_database(my_app).
%% '''
%%
%% @param Name Atom identifying the database
%% @returns `#database_state{}' record
%%
%% @see setup/0
%% @see create_relation/3
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

%% @doc Create an infinite relation with a generator.
%%
%% Creates a relation with infinite cardinality (aleph_zero or continuum).
%% The relation is defined by a generator function that produces tuples
%% lazily based on constraints.
%%
%% == Primitive Infinite Relations ==
%%
%% <ul>
%%   <li>`naturals' - Natural numbers: {0, 1, 2, 3, ...}</li>
%%   <li>`integers' - Integers: {0, 1, -1, 2, -2, ...}</li>
%%   <li>`rationals' - Rationals via Stern-Brocot tree</li>
%% </ul>
%%
%% == Example ==
%% ```
%% %% Create infinite relation of natural numbers
%% {DB1, Naturals} = operations:create_infinite_relation(DB, #{
%%     name => naturals,
%%     schema => #{value => natural},
%%     cardinality => aleph_zero,
%%     generator => {primitive, naturals},
%%     constraints => #{value => {gte, 0}}
%% }).
%%
%% %% Query with bounds
%% Iterator = operations:get_tuples_iterator(DB1, naturals,
%%     #{value => {range, 0, 100}}).
%% '''
%%
%% @param Database `#database_state{}' record
%% @param Spec Map with keys: name, schema, cardinality, generator, constraints
%% @returns `{UpdatedDatabase, NewRelation}' tuple
%%
%% @see create_relation/3
%% @see get_tuples_iterator/3
create_infinite_relation(Database, Spec) when is_record(Database, database_state) ->
    Name = maps:get(name, Spec),
    Schema = maps:get(schema, Spec),
    Cardinality = maps:get(cardinality, Spec, aleph_zero),
    GeneratorSpec = maps:get(generator, Spec),
    Constraints = maps:get(constraints, Spec, #{}),

    F = fun() ->
        %% Hash is based on generator specification
        RelationHash = hash({Name, Schema, GeneratorSpec, Cardinality}),

        NewRelation = #relation{
            hash = RelationHash,
            name = Name,
            tree = undefined,  % Infinite relations have no merkle tree
            schema = Schema,
            constraints = Constraints,
            cardinality = Cardinality,
            generator = GeneratorSpec,
            provenance = {base, Name}
        },
        mnesia:write(relation, NewRelation, write),

        %% Update database relations map
        NewRelations = maps:put(Name, RelationHash, Database#database_state.relations),

        %% Update database tree
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

%% @doc Get all relation names in a database.
%%
%% Returns a list of atoms representing all relation names currently
%% defined in the database.
%%
%% @param Database `#database_state{}' record
%% @returns List of atoms (relation names)
%%
%% @see get_relation_hash/2
%% @see create_relation/3
get_relations(Database) ->
    maps:keys(Database#database_state.relations).

%% @doc Get relation hash by name.
%%
%% Looks up a relation's hash in the database's relation map. This hash
%% can be used to retrieve the full relation record from Mnesia.
%%
%% @param Database `#database_state{}' record
%% @param RelationName Atom naming the relation
%% @returns `{ok, Hash}' on success, `{error, not_found}' if relation doesn't exist
%%
%% @see get_relations/1
%% @see hashes_from_tuple/1
get_relation_hash(Database, RelationName) ->
    case maps:find(RelationName, Database#database_state.relations) of
        error -> {error, not_found};
        X -> X
    end.

%% @doc Resolve a tuple hash to actual values.
%%
%% This is an internal function that materializes a tuple by:
%% <ol>
%%   <li>Reading the tuple record from Mnesia (contains attribute_map)</li>
%%   <li>For each attribute, reading the actual value using its hash</li>
%%   <li>Building a map with attribute names and materialized values</li>
%% </ol>
%%
%% Uses dirty reads since all data is immutable.
%%
%% @param TupleHash Binary hash identifying the tuple
%% @returns Map with attribute names and actual values (e.g., `#{name => "Alice", age => 30}')
%%
%% @see get_tuples_iterator/2
%% @see create_tuple/3
resolve_tuple(TupleHash) ->
    [#tuple{attribute_map = AttributeMap}] = mnesia:dirty_read(tuple, TupleHash),
    maps:map(fun(_AttrName, ValueHash) ->
        [#attribute{value = Value}] = mnesia:dirty_read(attribute, ValueHash),
        Value
    end, AttributeMap).

%% @doc Extract all tuple hashes from a relation.
%%
%% Retrieves the list of all tuple hashes stored in the relation's merkle tree.
%% Returns an empty list if the relation has no tuples (tree is `undefined').
%%
%% Uses dirty reads since the relation data is immutable.
%%
%% @param RelationHash Binary hash identifying the relation
%% @returns List of binary tuple hashes, or empty list `[]' if relation is empty
%%
%% @see get_tuples_iterator/2
%% @see create_tuple/3
hashes_from_tuple(RelationHash) ->
    [Relation] = mnesia:dirty_read(relation, RelationHash),
    case Relation#relation.tree of
        undefined -> [];
        Tree -> merklet:keys(Tree)
    end.

%% @doc Take operator (τ): Returns N arbitrary elements from a relation.
%%
%% The take operator is a unary relational operator that produces a finite
%% relation containing N arbitrary elements from the input relation. Since
%% relations are sets (unordered), the specific elements returned are
%% determined by the enumeration order.
%%
%% For finite relations smaller than N, returns all elements.
%% For infinite relations, returns first N elements by generator order.
%%
%% == Example ==
%% ```
%% %% Take 100 naturals (returns {0, 1, 2, ..., 99})
%% {DB1, Naturals100} = operations:take(DB, naturals, 100).
%%
%% %% Can compose with other operators
%% Iterator = operations:get_tuples_iterator(DB1, naturals100).
%% '''
%%
%% @param Database `#database_state{}' record
%% @param RelationName Atom naming the source relation
%% @param N Number of elements to take
%% @returns `{UpdatedDatabase, TakeRelation}' where TakeRelation is finite
%%
%% @see get_tuples_iterator/3
take(Database, RelationName, N) when is_record(Database, database_state), is_integer(N), N > 0 ->
    case maps:get(RelationName, Database#database_state.relations, error) of
        error ->
            erlang:error({relation_not_found, RelationName});
        RelationHash ->
            [SourceRelation] = mnesia:dirty_read(relation, RelationHash),

            %% Compute cardinality of result
            ResultCardinality = case SourceRelation#relation.cardinality of
                {finite, M} when M < N -> {finite, M};
                {finite, _} -> {finite, N};
                _ -> {finite, N}  % Taking from infinite yields finite
            end,

            %% Create take relation
            TakeName = list_to_atom(atom_to_list(RelationName) ++ "_take_" ++ integer_to_list(N)),

            TakeRelation = #relation{
                hash = hash({take, RelationName, N}),
                name = TakeName,
                tree = undefined,  % Generated on-demand
                schema = SourceRelation#relation.schema,
                constraints = SourceRelation#relation.constraints,
                cardinality = ResultCardinality,
                generator = {take, RelationName, N},
                provenance = {take, SourceRelation#relation.provenance, N}
            },

            %% Note: We don't persist take relations to Mnesia for now
            %% They are ephemeral views
            {Database, TakeRelation}
    end.

%% @doc Create an iterator for streaming tuples from a relation with constraints.
%%
%% Extended version of get_tuples_iterator/2 that accepts boundedness constraints.
%% For infinite relations, constraints are mandatory to ensure termination.
%%
%% <b>All tuples include metadata tracking</b> with a nested `meta' field containing:
%% <ul>
%%   <li>`provenance' - Tracks immediate source of each attribute</li>
%%   <li>`lineage' - Tracks complete operational history</li>
%% </ul>
%%
%% == Options Format ==
%%
%% Nested format (preferred):
%% ```
%% #{constraints => #{age => {gt, 30}}}
%% '''
%%
%% Flat format (backward compatible):
%% ```
%% #{age => {gt, 30}}
%% '''
%%
%% == Constraints ==
%%
%% Constraints bound the iteration over infinite relations:
%% <ul>
%%   <li>`{range, Min, Max}' - Range constraint</li>
%%   <li>`{eq, Value}' - Equality constraint</li>
%%   <li>`{lt, Value}', `{lte, Value}' - Upper bounds</li>
%%   <li>`{gt, Value}', `{gte, Value}' - Lower bounds</li>
%%   <li>`{in, List}' - Membership constraint</li>
%% </ul>
%%
%% == Example ==
%% ```
%% %% Iterate employees with age > 30 (metadata always included)
%% Iterator = operations:get_tuples_iterator(DB, employees,
%%     #{constraints => #{age => {gt, 30}}}).
%%
%% %% Results will have:
%% %% #{id => 1, name => "Alice", age => 35,
%% %%   meta => #{
%% %%     provenance => #{id => {employees, id}, ...},
%% %%     lineage => {select, Fun, {base, employees}}
%% %%   }}
%% '''
%%
%% @param Database `#database_state{}' record
%% @param RelationName Atom naming the relation to iterate
%% @param Options Map of constraints (nested or flat format)
%% @returns Pid of iterator process
%%
%% @see get_tuples_iterator/2
%% @see take/3
get_tuples_iterator(Database, RelationName, Options) when is_record(Database, database_state) ->
    case maps:get(RelationName, Database#database_state.relations, error) of
        error ->
            erlang:error({error_tuple_iterator_init, RelationName});
        RelationHash ->
            [Relation] = mnesia:dirty_read(relation, RelationHash),

            %% Extract constraints from options (support both nested and flat format)
            Constraints = case maps:is_key(constraints, Options) of
                true -> maps:get(constraints, Options);
                false -> Options  %% Flat format: entire map is constraints
            end,

            %% Metadata tracking is ALWAYS enabled
            case Relation#relation.cardinality of
                {finite, _} ->
                    %% Finite relation: use tuple hashes
                    TupleHashes = hashes_from_tuple(RelationHash),
                    spawn(fun() -> tuple_iterator_loop(TupleHashes, RelationName, true) end);

                _ ->
                    %% Infinite relation: use generator
                    Generator = instantiate_generator(Relation#relation.generator, Constraints),
                    spawn(fun() -> generator_iterator_loop(Generator, RelationName, true) end)
            end
    end.

%% @doc Create an iterator for streaming tuples from a relation.
%%
%% Implements the Volcano Iterator Model: spawns an independent process that
%% lazily streams tuples from the relation. This enables:
%% <ul>
%%   <li>Memory-bounded queries (tuples materialized on demand)</li>
%%   <li>Composable query operators (future: select, project, join)</li>
%%   <li>Process-based concurrency</li>
%% </ul>
%%
%% The iterator process responds to `{next, Caller}' messages and sends back
%% either `{tuple, Tuple}' or `done' when exhausted.
%%
%% <b>Note:</b> For infinite relations, use `get_tuples_iterator/3' with constraints.
%%
%% == Example ==
%% ```
%% Iterator = operations:get_tuples_iterator(DB, users),
%% {ok, Tuple} = operations:next_tuple(Iterator),
%% operations:close_iterator(Iterator).
%% '''
%%
%% @param Database `#database_state{}' record
%% @param RelationName Atom naming the relation to iterate
%% @returns Pid of iterator process
%%
%% @see get_tuples_iterator/3
%% @see next_tuple/1
%% @see close_iterator/1
%% @see collect_all/1
get_tuples_iterator(Database, RelationName) when is_record(Database, database_state) ->
    get_tuples_iterator(Database, RelationName, #{}).

%% @doc Get next tuple from iterator.
%%
%% Sends a `{next, self()}' message to the iterator process and waits for
%% the response. The iterator will materialize the tuple on demand.
%%
%% Timeout is set to 5 seconds to prevent indefinite blocking.
%%
%% @param IteratorPid Pid returned from `get_tuples_iterator/2'
%% @returns `{ok, Tuple}' with materialized tuple map,
%%          `done' when no more tuples remain, or
%%          `{error, timeout}' if iterator doesn't respond
%%
%% @see get_tuples_iterator/2
%% @see close_iterator/1
next_tuple(IteratorPid) ->
    IteratorPid ! {next, self()},
    receive
        {tuple, Tuple} -> {ok, Tuple};
        done -> done;
        {error, Reason} -> {error, Reason}
    after 5000 ->
        {error, timeout}
    end.

%% @doc Close iterator process.
%%
%% Sends a `stop' message to terminate the iterator process. This should be
%% called when you're done consuming tuples to avoid leaving processes running.
%%
%% @param IteratorPid Pid returned from `get_tuples_iterator/2'
%% @returns `ok'
%%
%% @see get_tuples_iterator/2
%% @see collect_all/1
close_iterator(IteratorPid) ->
    IteratorPid ! stop,
    ok.

%% @private
%% @doc Iterator loop implementation (internal).
%%
%% This is the process loop for the Volcano Iterator Model. It maintains a list
%% of tuple hashes and resolves them lazily when requested.
%%
%% Protocol:
%% <ul>
%%   <li>`{next, Caller}' - Resolve next tuple and send `{tuple, ResolvedTuple}' or `done'</li>
%%   <li>`stop' - Terminate the process</li>
%% </ul>
%%
%% @param TupleHashes List of binary tuple hashes to stream
%% @param RelationName Name of the source relation (for provenance)
%% @param EnableProvenance Boolean flag to enable provenance tracking
tuple_iterator_loop([], _RelationName, _EnableProvenance) ->
    receive
        {next, Caller} ->
            Caller ! done;
        stop ->
            ok
    end;
tuple_iterator_loop([TupleHash | Rest], RelationName, EnableProvenance) ->
    receive
        {next, Caller} ->
            %% Resolve tuple on demand
            ResolvedTuple = resolve_tuple(TupleHash),
            %% Add provenance tracking (always enabled)
            FinalTuple = add_base_provenance(ResolvedTuple, RelationName),
            Caller ! {tuple, FinalTuple},
            tuple_iterator_loop(Rest, RelationName, EnableProvenance);
        stop ->
            ok
    end.

%% @doc Helper to collect all tuples from an iterator into a list.
%%
%% This is a convenience function primarily used for testing. It exhausts the
%% iterator by repeatedly calling `next_tuple/1' until `done' is received,
%% accumulating all tuples into a list.
%%
%% <b>Warning:</b> This materializes all tuples in memory, defeating the purpose
%% of lazy evaluation. Only use for small relations or testing.
%%
%% == Example ==
%% ```
%% Iterator = operations:get_tuples_iterator(DB, users),
%% AllTuples = operations:collect_all(Iterator).
%% %% Iterator is automatically closed
%% '''
%%
%% @param IteratorPid Pid returned from `get_tuples_iterator/2'
%% @returns List of tuple maps, `{error, Reason, PartialResults}' on timeout
%%
%% @see get_tuples_iterator/2
%% @see next_tuple/1
collect_all(IteratorPid) ->
    collect_all(IteratorPid, []).

collect_all(IteratorPid, Acc) ->
    case next_tuple(IteratorPid) of
        {ok, Tuple} -> collect_all(IteratorPid, [Tuple | Acc]);
        done ->
            close_iterator(IteratorPid),
            lists:reverse(Acc);  % Preserve order
        {error, Reason} ->
            close_iterator(IteratorPid),
            {error, Reason, lists:reverse(Acc)}
    end.

%%% ============================================================================
%%% Iterator Operators (Lazy - Volcano Model)
%%% ============================================================================

%% @doc Select iterator: Filters tuples based on a predicate.
%%
%% Lazy operator that wraps a child iterator and only emits tuples that
%% satisfy the predicate function.
%%
%% == Example ==
%% ```
%% BaseIter = get_tuples_iterator(DB, employees, #{}),
%% FilteredIter = select_iterator(BaseIter, fun(E) -> maps:get(age, E) > 30 end),
%% Tuples = collect_all(FilteredIter).
%% '''
%%
%% @param ChildIterator Iterator process to filter
%% @param Predicate Function: tuple() -> boolean()
%% @returns Iterator process (Pid)
select_iterator(ChildIterator, Predicate) when is_function(Predicate, 1) ->
    spawn(fun() -> select_loop(ChildIterator, Predicate) end).

select_loop(ChildPid, Predicate) ->
    receive
        {next, Caller} ->
            case next_tuple(ChildPid) of
                {ok, Tuple} ->
                    case Predicate(Tuple) of
                        true ->
                            %% Wrap lineage with select operation
                            TupleWithLineage = wrap_lineage(Tuple, {select, Predicate}),
                            Caller ! {tuple, TupleWithLineage},
                            select_loop(ChildPid, Predicate);
                        false ->
                            %% Skip this tuple, get next
                            self() ! {next, Caller},
                            select_loop(ChildPid, Predicate)
                    end;
                done ->
                    close_iterator(ChildPid),
                    Caller ! done
            end;
        stop ->
            close_iterator(ChildPid),
            ok
    end.

%% @doc Project iterator: Projects tuples onto specified attributes.
%%
%% Lazy operator that transforms tuples by keeping only the specified
%% attributes. This is the relational projection operator (π).
%%
%% == Example ==
%% ```
%% BaseIter = get_tuples_iterator(DB, employees, #{}),
%% ProjectedIter = project_iterator(BaseIter, [name, age]),
%% Tuples = collect_all(ProjectedIter).
%% '''
%%
%% @param ChildIterator Iterator process to project
%% @param Attributes List of attribute names to keep
%% @returns Iterator process (Pid)
project_iterator(ChildIterator, Attributes) when is_list(Attributes) ->
    spawn(fun() -> project_loop(ChildIterator, Attributes) end).

project_loop(ChildPid, Attributes) ->
    receive
        {next, Caller} ->
            case next_tuple(ChildPid) of
                {ok, Tuple} ->
                    % Project data attributes
                    ProjectedTuple = maps:with(Attributes, Tuple),

                    % Preserve and update metadata
                    case maps:get(meta, Tuple, undefined) of
                        undefined ->
                            % No metadata, just return projected tuple
                            Caller ! {tuple, ProjectedTuple};
                        Meta ->
                            % Filter provenance to only projected attributes
                            OldProv = maps:get(provenance, Meta, #{}),
                            NewProv = maps:with(Attributes, OldProv),

                            % Build new metadata
                            UpdatedMeta = case NewProv of
                                Empty when map_size(Empty) =:= 0 -> maps:remove(provenance, Meta);
                                _ -> maps:put(provenance, NewProv, Meta)
                            end,

                            % Add metadata back and wrap lineage
                            TupleWithMeta = maps:put(meta, UpdatedMeta, ProjectedTuple),
                            TupleWithLineage = wrap_lineage(TupleWithMeta, {project, Attributes}),
                            Caller ! {tuple, TupleWithLineage}
                    end,
                    project_loop(ChildPid, Attributes);
                done ->
                    close_iterator(ChildPid),
                    Caller ! done
            end;
        stop ->
            close_iterator(ChildPid),
            ok
    end.

%% @doc Sort iterator: Sorts all tuples from child iterator.
%%
%% <b>Blocking operator</b> - must consume ALL tuples from child before
%% emitting any output. Materializes and sorts tuples in memory.
%%
%% <b>Warning:</b> Do not use on unbounded streams! Ensure child iterator
%% produces finite tuples (via constraints or take).
%%
%% == Example ==
%% ```
%% BaseIter = get_tuples_iterator(DB, employees, #{}),
%% SortedIter = sort_iterator(BaseIter, fun(A, B) ->
%%     maps:get(age, A) =< maps:get(age, B)
%% end),
%% Tuples = collect_all(SortedIter).
%% '''
%%
%% @param ChildIterator Iterator process to sort
%% @param CompareFun Function: (tuple(), tuple()) -> boolean()
%% @returns Iterator process (Pid)
sort_iterator(ChildIterator, CompareFun) when is_function(CompareFun, 2) ->
    spawn(fun() ->
        %% Phase 1: Consume all tuples (blocking!)
        AllTuples = collect_all(ChildIterator),

        %% Phase 2: Sort in memory
        SortedTuples = lists:sort(CompareFun, AllTuples),

        %% Phase 3: Wrap lineage for all tuples
        TuplesWithLineage = [wrap_lineage(T, {sort, CompareFun}) || T <- SortedTuples],

        %% Phase 4: Emit sorted tuples on demand
        sorted_emit_loop(TuplesWithLineage)
    end).

sorted_emit_loop([]) ->
    receive
        {next, Caller} -> Caller ! done;
        stop -> ok
    end;
sorted_emit_loop([Tuple | Rest]) ->
    receive
        {next, Caller} ->
            Caller ! {tuple, Tuple},
            sorted_emit_loop(Rest);
        stop -> ok
    end.

%% @doc Take iterator: Limits output to N tuples.
%%
%% Lazy operator that emits at most N tuples from the child iterator,
%% then closes the child and signals done.
%%
%% == Example ==
%% ```
%% BaseIter = get_tuples_iterator(DB, naturals, #{value => {range, 0, 1000}}),
%% LimitedIter = take_iterator(BaseIter, 10),
%% Tuples = collect_all(LimitedIter).  % Gets exactly 10 tuples
%% '''
%%
%% @param ChildIterator Iterator process to limit
%% @param N Maximum number of tuples to emit
%% @returns Iterator process (Pid)
take_iterator(ChildIterator, N) when is_integer(N), N > 0 ->
    spawn(fun() -> take_iter_loop(ChildIterator, N, 0, N) end).

take_iter_loop(ChildPid, Limit, Count, OriginalN) when Count < Limit ->
    receive
        {next, Caller} ->
            case next_tuple(ChildPid) of
                {ok, Tuple} ->
                    %% Wrap lineage with take operation
                    TupleWithLineage = wrap_lineage(Tuple, {take, OriginalN}),
                    Caller ! {tuple, TupleWithLineage},
                    take_iter_loop(ChildPid, Limit, Count + 1, OriginalN);
                done ->
                    close_iterator(ChildPid),
                    Caller ! done
            end;
        stop ->
            close_iterator(ChildPid),
            ok
    end;
take_iter_loop(ChildPid, _Limit, _Count, _OriginalN) ->
    %% Reached limit - close child and signal done
    close_iterator(ChildPid),
    receive
        {next, Caller} -> Caller ! done;
        stop -> ok
    end.

%%% ============================================================================
%%% Join Operators
%%% ============================================================================

%% @doc Equijoin iterator: Natural join on matching attribute values.
%%
%% Performs an equijoin between two relations on a common attribute.
%% For each tuple from the left iterator, finds all matching tuples from
%% the right iterator where the specified attributes are equal.
%%
%% The result tuples merge attributes from both sides. If attribute names
%% conflict, right side attributes are prefixed with "right_".
%%
%% Implementation: Nested loop join (simple but works for small-medium datasets).
%% Future: Hash join for better performance.
%%
%% == Example ==
%% ```
%% %% Join employees with departments on dept_id
%% LeftIter = operations:get_tuples_iterator(DB, employees, #{}),
%% RightIter = operations:get_tuples_iterator(DB, departments, #{}),
%% JoinIter = operations:equijoin_iterator(LeftIter, RightIter, dept_id)
%% '''
%%
%% @param LeftIterator Iterator for left relation
%% @param RightIterator Iterator for right relation
%% @param JoinAttribute Attribute name to join on (must exist in both relations)
%% @returns Iterator process that emits joined tuples
equijoin_iterator(LeftIterator, RightIterator, JoinAttribute) ->
    spawn(fun() -> equijoin_loop(LeftIterator, RightIterator, JoinAttribute) end).

%% @doc Theta-join iterator: Join with arbitrary predicate.
%%
%% Performs a theta-join between two relations using a user-provided
%% predicate function. For each pair of tuples (left, right), the predicate
%% is evaluated. If true, the tuples are merged and emitted.
%%
%% == Example ==
%% ```
%% %% Join where employee.age > department.min_age
%% Predicate = fun(EmpTuple, DeptTuple) ->
%%     maps:get(age, EmpTuple) > maps:get(min_age, DeptTuple)
%% end,
%% JoinIter = operations:theta_join_iterator(LeftIter, RightIter, Predicate)
%% '''
%%
%% @param LeftIterator Iterator for left relation
%% @param RightIterator Iterator for right relation
%% @param Predicate Function `fun(LeftTuple, RightTuple) -> boolean()'
%% @returns Iterator process that emits joined tuples
theta_join_iterator(LeftIterator, RightIterator, Predicate) ->
    spawn(fun() -> theta_join_loop(LeftIterator, RightIterator, Predicate) end).

%%% ============================================================================
%%% Materialization (Eager - Relation Creation)
%%% ============================================================================

%% @doc Materialize an iterator pipeline into a new relation.
%%
%% This is the boundary between lazy evaluation (iterator pipeline) and
%% eager materialization (stored relation). Consumes all tuples from the
%% iterator and stores them as a new finite relation in the database.
%%
%% == Example ==
%% ```
%% %% Build lazy pipeline
%% Pipeline = get_tuples_iterator(DB, employees, #{})
%%            |> select_iterator(fun(E) -> maps:get(age, E) > 30 end)
%%            |> sort_iterator(fun(A, B) -> maps:get(age, A) =< maps:get(age, B) end)
%%            |> take_iterator(10)
%%            |> project_iterator([name, age]),
%%
%% %% Materialize into new relation
%% {DB1, SeniorEmployees} = materialize(DB, Pipeline, senior_employees).
%% '''
%%
%% @param Database Current database state
%% @param SourceIterator Iterator process (pipeline endpoint)
%% @param ResultName Atom naming the new relation
%% @returns `{UpdatedDatabase, NewRelation}' tuple
materialize(Database, SourceIterator, ResultName) when is_record(Database, database_state) ->
    %% 1. Pull all tuples from pipeline
    AllTuples = collect_all(SourceIterator),

    case AllTuples of
        [] ->
            %% Empty result - create empty relation
            Schema = #{},  % Empty schema
            create_relation(Database, ResultName, Schema);

        [FirstTuple | _] ->
            %% 2. Infer schema from first tuple
            Schema = infer_schema_from_tuple(FirstTuple),

            %% 3. Create new relation
            {DB1, _Relation} = create_relation(Database, ResultName, Schema),

            %% 4. Insert all tuples
            {FinalDB, FinalRelation} = insert_all_tuples(DB1, ResultName, AllTuples),

            {FinalDB, FinalRelation}
    end.

%% @private
%% @doc Infer schema from a tuple.
infer_schema_from_tuple(Tuple) when is_map(Tuple) ->
    maps:map(fun(_AttrName, Value) -> infer_type(Value) end, Tuple).

%% @private
%% @doc Infer type from value.
infer_type(Value) when is_integer(Value) -> integer;
infer_type(Value) when is_float(Value) -> float;
infer_type(Value) when is_binary(Value) -> binary;
infer_type(Value) when is_list(Value) -> string;
infer_type(Value) when is_atom(Value) -> atom;
infer_type(Value) when is_boolean(Value) -> boolean;
infer_type(_) -> term.

%% @private
%% @doc Insert multiple tuples into a relation.
insert_all_tuples(DB, RelationName, Tuples) ->
    lists:foldl(
        fun(Tuple, {DBacc, _RelAcc}) ->
            create_tuple(DBacc, RelationName, Tuple)
        end,
        {DB, undefined},
        Tuples
    ).

%%% ============================================================================
%%% Generator Support Functions
%%% ============================================================================

%% @private
%% @doc Instantiate a generator from a generator specification.
%%
%% Converts a generator spec (stored in relation record) into an actual
%% generator function that can produce tuples.
%%
%% @param GeneratorSpec Generator specification
%% @param Constraints Boundedness constraints
%% @returns Generator function
instantiate_generator({primitive, naturals}, Constraints) ->
    generators:naturals(Constraints);
instantiate_generator({primitive, integers}, Constraints) ->
    generators:integers(Constraints);
instantiate_generator({primitive, rationals}, Constraints) ->
    generators:rationals(Constraints);
instantiate_generator({primitive, plus}, Constraints) ->
    generators:plus(Constraints);
instantiate_generator({primitive, times}, Constraints) ->
    generators:times(Constraints);
instantiate_generator({primitive, minus}, Constraints) ->
    generators:minus(Constraints);
instantiate_generator({primitive, divide}, Constraints) ->
    generators:divide(Constraints);
instantiate_generator({custom, GeneratorFun}, Constraints) ->
    GeneratorFun(Constraints);
instantiate_generator({take, RelationName, N}, Constraints) ->
    %% Take generator: wrap another generator with limit
    %% This is a simplified version - full implementation would look up the base relation
    make_take_generator(RelationName, N, Constraints);
instantiate_generator(Other, _) ->
    erlang:error({unknown_generator, Other}).

%% @private
%% @doc Generator iterator loop for infinite relations.
%%
%% Similar to tuple_iterator_loop but works with generator functions instead
%% of tuple hashes.
%%
%% @param Generator Generator function
generator_iterator_loop(Generator, RelationName, EnableProvenance) ->
    receive
        {next, Caller} ->
            case Generator(next) of
                done ->
                    Caller ! done;
                {value, Tuple, NextGen} ->
                    %% Add provenance tracking (always enabled)
                    FinalTuple = add_base_provenance(Tuple, RelationName),
                    Caller ! {tuple, FinalTuple},
                    generator_iterator_loop(NextGen, RelationName, EnableProvenance);
                {error, Reason} ->
                    Caller ! {error, Reason}
            end;
        stop ->
            ok
    end.

%% @private
%% @doc Equijoin loop: nested loop join on attribute equality.
%%
%% Strategy:
%% 1. Collect all tuples from right iterator into memory
%% 2. For each left tuple, scan right tuples for matches
%% 3. Emit merged tuple when join attribute values match
%%
%% @param LeftIter Left iterator process
%% @param RightIter Right iterator process
%% @param JoinAttr Attribute to join on
equijoin_loop(LeftIter, RightIter, JoinAttr) ->
    % Materialize right side (for nested loop)
    RightTuples = collect_all(RightIter),
    equijoin_emit_loop(LeftIter, RightTuples, JoinAttr).

equijoin_emit_loop(LeftIter, RightTuples, JoinAttr) ->
    receive
        {next, Caller} ->
            case next_tuple(LeftIter) of
                done ->
                    Caller ! done,
                    ok;  % Stop after sending done
                {ok, LeftTuple} ->
                    % Find all right tuples that match on join attribute
                    case maps:get(JoinAttr, LeftTuple, undefined) of
                        undefined ->
                            % Left tuple doesn't have join attribute - try next
                            self() ! {next, Caller},
                            equijoin_emit_loop(LeftIter, RightTuples, JoinAttr);
                        LeftValue ->
                            % Find matching right tuples
                            Matches = [merge_tuples(LeftTuple, RightTuple, {equijoin, JoinAttr})
                                      || RightTuple <- RightTuples,
                                         maps:get(JoinAttr, RightTuple, undefined) =:= LeftValue],
                            case Matches of
                                [] ->
                                    % No matches - try next left tuple
                                    self() ! {next, Caller},
                                    equijoin_emit_loop(LeftIter, RightTuples, JoinAttr);
                                [FirstMatch | RestMatches] ->
                                    % Emit first match, buffer rest
                                    Caller ! {tuple, FirstMatch},
                                    equijoin_emit_buffered(LeftIter, RightTuples, JoinAttr, RestMatches)
                            end
                    end;
                {error, Reason} ->
                    Caller ! {error, Reason},
                    ok  % Stop after sending error
            end;
        stop ->
            close_iterator(LeftIter),
            ok
    end.

equijoin_emit_buffered(LeftIter, RightTuples, JoinAttr, [Match | Rest]) ->
    receive
        {next, Caller} ->
            Caller ! {tuple, Match},
            equijoin_emit_buffered(LeftIter, RightTuples, JoinAttr, Rest);
        stop ->
            close_iterator(LeftIter),
            ok
    end;
equijoin_emit_buffered(LeftIter, RightTuples, JoinAttr, []) ->
    % Buffer empty - continue with next left tuple
    equijoin_emit_loop(LeftIter, RightTuples, JoinAttr).

%% @private
%% @doc Theta-join loop: nested loop join with predicate.
%%
%% @param LeftIter Left iterator process
%% @param RightIter Right iterator process
%% @param Pred Predicate function fun(LeftTuple, RightTuple) -> boolean()
theta_join_loop(LeftIter, RightIter, Pred) ->
    % Materialize right side
    RightTuples = collect_all(RightIter),
    theta_join_emit_loop(LeftIter, RightTuples, Pred).

theta_join_emit_loop(LeftIter, RightTuples, Pred) ->
    receive
        {next, Caller} ->
            case next_tuple(LeftIter) of
                done ->
                    Caller ! done;
                {ok, LeftTuple} ->
                    % Find all right tuples that satisfy predicate
                    Matches = [merge_tuples(LeftTuple, RightTuple, {theta_join, Pred})
                              || RightTuple <- RightTuples,
                                 Pred(LeftTuple, RightTuple)],
                    case Matches of
                        [] ->
                            % No matches - continue
                            self() ! {next, Caller},
                            theta_join_emit_loop(LeftIter, RightTuples, Pred);
                        [FirstMatch | RestMatches] ->
                            Caller ! {tuple, FirstMatch},
                            theta_join_emit_buffered(LeftIter, RightTuples, Pred, RestMatches)
                    end;
                {error, Reason} ->
                    Caller ! {error, Reason}
            end;
        stop ->
            close_iterator(LeftIter),
            ok
    end.

theta_join_emit_buffered(LeftIter, RightTuples, Pred, [Match | Rest]) ->
    receive
        {next, Caller} ->
            Caller ! {tuple, Match},
            theta_join_emit_buffered(LeftIter, RightTuples, Pred, Rest);
        stop ->
            close_iterator(LeftIter),
            ok
    end;
theta_join_emit_buffered(LeftIter, RightTuples, Pred, []) ->
    theta_join_emit_loop(LeftIter, RightTuples, Pred).

%% @private
%% @doc Wrap existing lineage with a new operation.
%%
%% Takes a tuple with optional lineage and wraps it with a new operation node.
%% If the tuple has no lineage, does nothing.
%%
%% @param Tuple Tuple potentially with meta.lineage
%% @param OpInfo Operation info: {select, Pred} | {project, Attrs} | {sort, Cmp} | {take, N}
wrap_lineage(Tuple, OpInfo) ->
    case maps:get(meta, Tuple, undefined) of
        undefined ->
            % No metadata, return as-is
            Tuple;
        Meta ->
            case maps:get(lineage, Meta, undefined) of
                undefined ->
                    % No lineage, return as-is
                    Tuple;
                ChildLineage ->
                    % Wrap existing lineage with new operation
                    NewLineage = case OpInfo of
                        {select, Pred} -> {select, Pred, ChildLineage};
                        {project, Attrs} -> {project, Attrs, ChildLineage};
                        {sort, CmpFun} -> {sort, CmpFun, ChildLineage};
                        {take, N} -> {take, N, ChildLineage}
                    end,
                    maps:put(meta, maps:put(lineage, NewLineage, Meta), Tuple)
            end
    end.

%% @private
%% @doc Add base provenance and lineage to a tuple from a relation.
%%
%% Creates nested meta fields with both provenance and lineage.
%% Example: add_base_provenance(#{id => 1, name => "Alice"}, employees)
%%   => #{id => 1, name => "Alice",
%%        meta => #{provenance => #{id => {employees, id}, name => {employees, name}},
%%                  lineage => {base, employees}}}
add_base_provenance(Tuple, RelationName) ->
    % Build provenance map for all attributes (excluding existing meta)
    DataAttrs = maps:remove(meta, Tuple),
    Provenance = maps:fold(
        fun(Key, _Value, Acc) ->
            maps:put(Key, {RelationName, Key}, Acc)
        end,
        #{},
        DataAttrs
    ),
    % Add metadata with provenance and lineage
    maps:put(meta, #{
        provenance => Provenance,
        lineage => {base, RelationName}
    }, Tuple).

%% @private
%% @doc Merge two tuples for join result, including metadata.
%%
%% If attribute names conflict, right side attributes are prefixed with "right_".
%% Metadata (including provenance and lineage) is also merged.
%%
%% @param LeftTuple Left side tuple
%% @param RightTuple Right side tuple
%% @param JoinInfo Join information: {equijoin, Attr} | {theta_join, Predicate}
merge_tuples(LeftTuple, RightTuple, JoinInfo) ->
    % Extract metadata from both sides (if present)
    LeftMeta = maps:get(meta, LeftTuple, #{}),
    RightMeta = maps:get(meta, RightTuple, #{}),

    % Extract provenance and lineage from metadata
    LeftProv = maps:get(provenance, LeftMeta, #{}),
    RightProv = maps:get(provenance, RightMeta, #{}),
    LeftLineage = maps:get(lineage, LeftMeta, undefined),
    RightLineage = maps:get(lineage, RightMeta, undefined),

    % Remove metadata from tuples for data merging
    LeftData = maps:remove(meta, LeftTuple),
    RightData = maps:remove(meta, RightTuple),

    % Merge data attributes
    MergedData = maps:fold(
        fun(Key, Value, Acc) ->
            case maps:is_key(Key, Acc) of
                true ->
                    % Conflict - prefix right attribute
                    RightKey = list_to_atom("right_" ++ atom_to_list(Key)),
                    maps:put(RightKey, Value, Acc);
                false ->
                    maps:put(Key, Value, Acc)
            end
        end,
        LeftData,
        RightData
    ),

    % Merge provenance (same conflict resolution)
    MergedProv = maps:fold(
        fun(Key, Source, Acc) ->
            case maps:is_key(Key, Acc) of
                true ->
                    % Conflict - prefix right attribute provenance
                    RightKey = list_to_atom("right_" ++ atom_to_list(Key)),
                    maps:put(RightKey, Source, Acc);
                false ->
                    maps:put(Key, Source, Acc)
            end
        end,
        LeftProv,
        RightProv
    ),

    % Build merged lineage if both sides have lineage
    MergedLineage = case {LeftLineage, RightLineage, JoinInfo} of
        {undefined, undefined, _} -> undefined;
        {L, R, {equijoin, Attr}} when L =/= undefined, R =/= undefined ->
            {join, Attr, L, R};
        {L, R, {theta_join, Pred}} when L =/= undefined, R =/= undefined ->
            {theta_join, Pred, L, R};
        _ -> undefined
    end,

    % Build merged metadata
    MergedMeta = case {maps:size(MergedProv), MergedLineage} of
        {0, undefined} -> #{};
        {0, Lineage} -> #{lineage => Lineage};
        {_, undefined} -> #{provenance => MergedProv};
        {_, Lineage} -> #{provenance => MergedProv, lineage => Lineage}
    end,

    % Add merged metadata to result (only if non-empty)
    case maps:size(MergedMeta) of
        0 -> MergedData;
        _ -> maps:put(meta, MergedMeta, MergedData)
    end.

%% @private
%% @doc Create a take generator that limits another generator.
make_take_generator(_RelationName, N, _Constraints) ->
    %% Simplified version: just count to N
    make_take_gen(0, N).

make_take_gen(Current, Max) when Current < Max ->
    fun(next) ->
        Tuple = #{value => Current},
        NextGen = make_take_gen(Current + 1, Max),
        {value, Tuple, NextGen}
    end;
make_take_gen(_, _) ->
    fun(_) -> done end.

