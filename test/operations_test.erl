-module(operations_test).
-include("operations.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Test fixtures and setup
setup_test_env() ->
    main:setup(),
    ok.

cleanup_test_env() ->
    mnesia:stop(),
    ok.

%% Fixture: Create a sample database with a users relation
sample_database() ->
    DB = operations:create_database(test_db),
    {DB1, _Relation} = operations:create_relation(DB, users, #{
        name => string,
        age => integer
    }),
    DB1.

%% Fixture: Add sample tuples to a database
sample_database_with_tuples() ->
    DB = sample_database(),
    {DB1, _} = operations:create_tuple(DB, users, #{name => "Alice", age => 30}),
    {DB2, _} = operations:create_tuple(DB1, users, #{name => "Bob", age => 25}),
    DB2.

%%====================================================================
%% Database Creation Tests
%%====================================================================

database_creation_test_() ->
    {setup,
     fun setup_test_env/0,
     fun(_) -> cleanup_test_env() end,
     [?_test(begin
                 DB = operations:create_database(my_db),
                 ?assertEqual(my_db, DB#database_state.name),
                 % Database now comes with built-in infinite relations
                 Relations = operations:get_relations(DB),
                 ?assert(lists:member(natural, Relations)),
                 ?assert(lists:member(integer, Relations)),
                 ?assert(lists:member(rational, Relations)),
                 ?assertNotEqual(undefined, DB#database_state.timestamp)
             end)]}.

%%====================================================================
%% Relation Creation Tests
%%====================================================================

relation_creation_test_() ->
    {setup,
     fun setup_test_env/0,
     fun(_) -> cleanup_test_env() end,
     [?_test(begin
                 DB = operations:create_database(test_db),
                 Schema = #{name => string, email => string},
                 {DB1, Relation} = operations:create_relation(DB, users, Schema),

                 % Verify relation was created
                 ?assertEqual(users, Relation#relation.name),
                 ?assertEqual(Schema, Relation#relation.schema),

                 % Verify database was updated (includes built-in relations)
                 Relations = operations:get_relations(DB1),
                 ?assert(lists:member(users, Relations)),
                 ?assert(lists:member(natural, Relations)),
                 ?assert(lists:member(integer, Relations)),
                 ?assert(lists:member(rational, Relations)),
                 {ok, RelationHash} = operations:get_relation_hash(DB1, users),
                 ?assertEqual(Relation#relation.hash, RelationHash)
             end)]}.

%%====================================================================
%% Tuple Operations Tests
%%====================================================================

tuple_creation_test_() ->
    {setup,
     fun setup_test_env/0,
     fun(_) -> cleanup_test_env() end,
     [?_test(begin
                 DB = sample_database(),
                 InitialHash = DB#database_state.hash,

                 % Add a tuple
                 {DB1, _} = operations:create_tuple(DB, users, #{name => "Alice", age => 30}),

                 % Verify database hash changed
                 ?assertNotEqual(InitialHash, DB1#database_state.hash),

                 % Verify tuple can be retrieved
                 Iterator = operations:get_tuples_iterator(DB1, users),
                 Tuples = operations:collect_all(Iterator),
                 ?assertEqual(1, length(Tuples)),
                 [Tuple] = Tuples,
                 ?assertEqual("Alice", maps:get(name, Tuple)),
                 ?assertEqual(30, maps:get(age, Tuple))
             end)]}.

tuple_multiple_insertions_test_() ->
    {setup,
     fun setup_test_env/0,
     fun(_) -> cleanup_test_env() end,
     [?_test(begin
                 DB = sample_database(),

                 % Add multiple tuples
                 {DB1, _} = operations:create_tuple(DB, users, #{name => "Alice", age => 30}),
                 {DB2, _} = operations:create_tuple(DB1, users, #{name => "Bob", age => 25}),
                 {DB3, _} = operations:create_tuple(DB2, users, #{name => "Charlie", age => 35}),

                 % Verify all tuples can be retrieved
                 Iterator = operations:get_tuples_iterator(DB3, users),
                 Tuples = operations:collect_all(Iterator),
                 ?assertEqual(3, length(Tuples)),

                 % Verify hashes changed with each insertion
                 ?assertNotEqual(DB#database_state.hash, DB1#database_state.hash),
                 ?assertNotEqual(DB1#database_state.hash, DB2#database_state.hash),
                 ?assertNotEqual(DB2#database_state.hash, DB3#database_state.hash)
             end)]}.

%%====================================================================
%% Hash Comparison Tests (Time-based)
%%====================================================================

database_state_hash_comparison_test_() ->
    {setup,
     fun setup_test_env/0,
     fun(_) -> cleanup_test_env() end,
     [?_test(begin
                 DB = sample_database(),
                 Hash0 = DB#database_state.hash,

                 % State 1: Add first tuple
                 {DB1, _} = operations:create_tuple(DB, users, #{name => "Alice", age => 30}),
                 Hash1 = DB1#database_state.hash,

                 % State 2: Add second tuple
                 {DB2, _} = operations:create_tuple(DB1, users, #{name => "Bob", age => 25}),
                 Hash2 = DB2#database_state.hash,

                 % State 3: Add third tuple
                 {DB3, _} = operations:create_tuple(DB2, users, #{name => "Charlie", age => 35}),
                 Hash3 = DB3#database_state.hash,

                 % Verify all hashes are unique
                 ?assertNotEqual(Hash0, Hash1),
                 ?assertNotEqual(Hash1, Hash2),
                 ?assertNotEqual(Hash2, Hash3),
                 ?assertNotEqual(Hash0, Hash2),
                 ?assertNotEqual(Hash0, Hash3),
                 ?assertNotEqual(Hash1, Hash3)
             end)]}.

relation_hash_comparison_test_() ->
    {setup,
     fun setup_test_env/0,
     fun(_) -> cleanup_test_env() end,
     [?_test(begin
                 DB = sample_database(),
                 {ok, RelationHash0} = operations:get_relation_hash(DB, users),

                 % Add tuple and get new relation hash
                 {DB1, Relation1} = operations:create_tuple(DB, users, #{name => "Alice", age => 30}),
                 RelationHash1 = Relation1#relation.hash,

                 % Add another tuple
                 {DB2, Relation2} = operations:create_tuple(DB1, users, #{name => "Bob", age => 25}),
                 RelationHash2 = Relation2#relation.hash,

                 % Verify relation hashes changed over time
                 ?assertNotEqual(RelationHash0, RelationHash1),
                 ?assertNotEqual(RelationHash1, RelationHash2),
                 ?assertNotEqual(RelationHash0, RelationHash2),

                 % Verify we can still get relation hash from database
                 {ok, CurrentHash} = operations:get_relation_hash(DB2, users),
                 ?assertEqual(RelationHash2, CurrentHash)
             end)]}.

%%====================================================================
%% Tuple Retraction Tests
%%====================================================================

retract_tuple_test_() ->
    {setup,
     fun setup_test_env/0,
     fun(_) -> cleanup_test_env() end,
     [?_test(begin
                 DB = sample_database_with_tuples(),
                 {ok, RelationHash} = operations:get_relation_hash(DB, users),

                 % Get tuple hashes
                 TupleHashes = operations:hashes_from_tuple(RelationHash),
                 ?assertEqual(2, length(TupleHashes)),

                 % Retract one tuple
                 [TupleHash | _] = TupleHashes,
                 {atomic, {DB1, _}} = operations:retract_tuple(DB, RelationHash, TupleHash),

                 % Verify tuple was removed
                 {ok, NewRelationHash} = operations:get_relation_hash(DB1, users),
                 NewTupleHashes = operations:hashes_from_tuple(NewRelationHash),
                 ?assertEqual(1, length(NewTupleHashes)),

                 % Verify database hash changed
                 ?assertNotEqual(DB#database_state.hash, DB1#database_state.hash)
             end)]}.

%%====================================================================
%% Relation Operations Tests
%%====================================================================

clear_relation_test_() ->
    {setup,
     fun setup_test_env/0,
     fun(_) -> cleanup_test_env() end,
     [?_test(begin
                 DB = sample_database_with_tuples(),
                 {ok, RelationHash} = operations:get_relation_hash(DB, users),

                 % Verify tuples exist
                 TupleHashes = operations:hashes_from_tuple(RelationHash),
                 ?assertEqual(2, length(TupleHashes)),

                 % Clear relation
                 {atomic, {DB1, ClearedRelation}} = operations:clear_relation(DB, RelationHash),

                 % Verify relation is empty
                 ?assertEqual(undefined, ClearedRelation#relation.tree),
                 NewTupleHashes = operations:hashes_from_tuple(ClearedRelation#relation.hash),
                 ?assertEqual([], NewTupleHashes),

                 % Verify database hash changed
                 ?assertNotEqual(DB#database_state.hash, DB1#database_state.hash)
             end)]}.

retract_relation_test_() ->
    {setup,
     fun setup_test_env/0,
     fun(_) -> cleanup_test_env() end,
     [?_test(begin
                 DB = sample_database_with_tuples(),

                 % Verify relation exists (along with built-in relations)
                 Relations = operations:get_relations(DB),
                 ?assert(lists:member(users, Relations)),

                 % Retract relation
                 {atomic, DB1} = operations:retract_relation(DB, users),

                 % Verify users relation was removed (built-in relations remain)
                 Relations1 = operations:get_relations(DB1),
                 ?assertNot(lists:member(users, Relations1)),
                 ?assert(lists:member(natural, Relations1)),
                 ?assert(lists:member(integer, Relations1)),
                 ?assert(lists:member(rational, Relations1)),
                 ?assertEqual({error, not_found}, operations:get_relation_hash(DB1, users)),

                 % Verify database hash changed
                 ?assertNotEqual(DB#database_state.hash, DB1#database_state.hash)
             end)]}.

%%====================================================================
%% Iterator Tests
%%====================================================================

iterator_basic_test_() ->
    {setup,
     fun setup_test_env/0,
     fun(_) -> cleanup_test_env() end,
     [?_test(begin
                 DB = sample_database_with_tuples(),

                 % Get iterator and retrieve tuples one by one
                 Iterator = operations:get_tuples_iterator(DB, users),

                 % Get first tuple
                 {ok, Tuple1} = operations:next_tuple(Iterator),
                 ?assert(is_map(Tuple1)),
                 ?assert(maps:is_key(name, Tuple1)),
                 ?assert(maps:is_key(age, Tuple1)),

                 % Get second tuple
                 {ok, Tuple2} = operations:next_tuple(Iterator),
                 ?assert(is_map(Tuple2)),

                 % No more tuples
                 ?assertEqual(done, operations:next_tuple(Iterator)),

                 operations:close_iterator(Iterator)
             end)]}.

iterator_collect_all_test_() ->
    {setup,
     fun setup_test_env/0,
     fun(_) -> cleanup_test_env() end,
     [?_test(begin
                 DB = sample_database_with_tuples(),

                 Iterator = operations:get_tuples_iterator(DB, users),
                 Tuples = operations:collect_all(Iterator),

                 ?assertEqual(2, length(Tuples)),

                 % Verify both tuples are present
                 Names = [maps:get(name, T) || T <- Tuples],
                 ?assert(lists:member("Alice", Names)),
                 ?assert(lists:member("Bob", Names))
             end)]}.

iterator_empty_relation_test_() ->
    {setup,
     fun setup_test_env/0,
     fun(_) -> cleanup_test_env() end,
     [?_test(begin
                 DB = sample_database(),

                 Iterator = operations:get_tuples_iterator(DB, users),
                 Tuples = operations:collect_all(Iterator),

                 ?assertEqual([], Tuples)
             end)]}.

%%====================================================================
%% Hash Consistency Tests
%%====================================================================

hash_determinism_test_() ->
    {setup,
     fun setup_test_env/0,
     fun(_) -> cleanup_test_env() end,
     [?_test(begin
                 % Same value should produce same hash
                 Hash1 = operations:hash(<<"test">>),
                 Hash2 = operations:hash(<<"test">>),
                 ?assertEqual(Hash1, Hash2),

                 % Different values should produce different hashes
                 Hash3 = operations:hash(<<"different">>),
                 ?assertNotEqual(Hash1, Hash3)
             end)]}.

database_state_consistency_test_() ->
    {setup,
     fun setup_test_env/0,
     fun(_) -> cleanup_test_env() end,
     [?_test(begin
                 % Create two identical databases with same operations
                 DB1 = operations:create_database(db1),
                 {DB1_1, _} = operations:create_relation(DB1, users, #{name => string}),
                 {DB1_2, _} = operations:create_tuple(DB1_1, users, #{name => "Alice"}),

                 DB2 = operations:create_database(db2),
                 {DB2_1, _} = operations:create_relation(DB2, users, #{name => string}),
                 {DB2_2, _} = operations:create_tuple(DB2_1, users, #{name => "Alice"}),

                 % Relations should have the same hash (same schema and tuples)
                 {ok, RelHash1} = operations:get_relation_hash(DB1_2, users),
                 {ok, RelHash2} = operations:get_relation_hash(DB2_2, users),
                 ?assertEqual(RelHash1, RelHash2)
             end)]}.
