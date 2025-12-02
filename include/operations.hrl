%%% Core Records

-record(database_state, {hash, name, tree, relations, timestamp}).

-record(relation, {
    hash,           % Content hash (finite) or generator hash (infinite)
    name,           % Relation name (atom)
    tree,           % Merkle tree (finite only), undefined for infinite
    schema,         % #{attribute_name => type}
    constraints,    % #{attribute_name => constraint_spec()} - integrity constraints
    cardinality,    % {finite, N} | aleph_zero | continuum
    generator,      % Generator function (infinite only), undefined for finite
    provenance      % Provenance tracking for updatable views (future)
}).

-record(tuple, {hash, relation, attribute_map}).
-record(attribute, {hash, value}).

-record(infinite_relation, {name, schema, generator, membership_criteria, cardinality}).

%%% Cardinality Types

-type cardinality() :: {finite, non_neg_integer()}   % Finite set with N elements
                     | aleph_zero                    % Countably infinite (ℵ₀)
                     | continuum.                    % Uncountably infinite (2^ℵ₀)

%%% Constraint Types

-type constraint_spec() ::
    {eq, term()}                          % Equality: attr = value
  | {neq, term()}                         % Inequality: attr ≠ value
  | {lt, number()}                        % Less than: attr < value
  | {lte, number()}                       % Less than or equal: attr ≤ value
  | {gt, number()}                        % Greater than: attr > value
  | {gte, number()}                       % Greater than or equal: attr ≥ value
  | {in, [term()]}                        % Membership: attr ∈ list
  | {range, number(), number()}           % Range: attr ∈ [min, max]
  | {member_of, atom()}                   % Type constraint: attr ∈ Relation
  | {'and', [term()]}
  | is_integer
  | is_float.

-type constraints() :: #{atom() => constraint_spec()}.

-type boundedness_constraints() :: #{atom() => constraints()}.

%%% Generator Types

-type generator_fun() :: fun((constraints()) -> generator_result()).

-type generator_result() ::
    done                                   % No more tuples
  | {value, map(), generator_fun()}        % Tuple and continuation
  | {error, term()}.                       % Error

-type generator_spec() ::
    {primitive, atom()}                    % Built-in: naturals, integers, rationals
  | {custom, generator_fun()}              % User-defined generator
  | {take, atom(), pos_integer()}          % Take N from relation
  | {derived, relational_op()}.            % Derived from relational operation

-type relational_op() ::
    {select, atom(), constraints()}
  | {project, atom(), [atom()]}
  | {join, atom(), atom(), join_spec()}.

-type join_spec() ::
    {equijoin, atom()}                     % Natural join on attribute
  | {theta, atom(), atom(), atom()}.       % Theta join: op(attr1, attr2)

%%% Provenance Types (for updatable views)

%% Relation-level provenance (tracks operation lineage)
-type provenance() :: undefined
                    | {base, atom()}       % Base relation
                    | {join, provenance(), provenance()}
                    | {select, provenance(), constraints()}
                    | {project, provenance(), [atom()]}
                    | {take, provenance(), pos_integer()}.

%% Attribute-level provenance (tracks data lineage for each attribute)
%% Maps attribute name to its source (relation, original_attribute_name)
-type attribute_provenance() :: #{atom() => {atom(), atom()}}.

%% Tuple-level lineage (tracks complete operational history)
%% Operation tree showing how the tuple was derived
-type lineage_op() ::
    {base, atom()}                                      % Base relation
  | {select, fun(), lineage_op()}                       % Filter operation
  | {project, [atom()], lineage_op()}                   % Projection
  | {join, atom(), lineage_op(), lineage_op()}          % Join on attribute
  | {theta_join, fun(), lineage_op(), lineage_op()}     % Join with predicate
  | {sort, fun(), lineage_op()}                         % Sort operation
  | {take, pos_integer(), lineage_op()}                 % Limit operation
  | {aggregate, atom(), atom(), lineage_op()}.          % Aggregation (future)

%% Tuple metadata (contains provenance, lineage, and other system-managed fields)
-type tuple_metadata() :: #{
    provenance => attribute_provenance(),
    lineage => lineage_op()
    % Future: timestamp, version, etc.
}.

%% Tuples with metadata use a nested 'meta' field:
%% #{id => 1, name => "Alice",
%%   meta => #{
%%     provenance => #{id => {employees, id}, name => {employees, name}},
%%     lineage => {select, fun(T) -> maps:get(age, T) > 30 end, {base, employees}}
%%   }}
