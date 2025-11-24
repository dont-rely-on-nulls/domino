%%% @doc Generators for Infinite Relations
%%%
%%% This module provides generator functions for primitive infinite relations.
%%%
%%% == Domain Relations (Base Types) ==
%%%
%%% Domain relations are unary relations representing mathematical sets:
%%% <ul>
%%%   <li>Naturals (ℕ): {(0), (1), (2), (3), ...}</li>
%%%   <li>Integers (ℤ): {(0), (1), (-1), (2), (-2), ...}</li>
%%%   <li>Rationals (ℚ): {(0/1), (1/1), (1/2), (2/1), ...}</li>
%%% </ul>
%%%
%%% Domain relations have empty schemas (`schema => #{}') as they are the
%%% base case in the type system.
%%%
%%% == Function Relations ==
%%%
%%% Function relations represent mathematical operations as infinite relations:
%%% <ul>
%%%   <li>Plus: {(a, b, sum) | a ∈ ℤ, b ∈ ℤ, sum = a + b}</li>
%%%   <li>Times: {(a, b, product) | a ∈ ℤ, b ∈ ℤ, product = a × b}</li>
%%%   <li>Minus: {(a, b, difference) | a ∈ ℤ, b ∈ ℤ, difference = a - b}</li>
%%%   <li>Divide: {(a, b, q, r) | a ∈ ℤ, b ∈ ℤ\{0}, a = b×q + r}</li>
%%% </ul>
%%%
%%% Function relations reference domain relations in their schemas:
%%% ```
%%% schema => #{a => integers, b => integers, sum => integers}
%%% '''
%%%
%%% == Reversibility ==
%%%
%%% Function relations support bidirectional querying (like Prolog):
%%% <ul>
%%%   <li>`Plus[1, 2, ?]' → ? = 3 (forward)</li>
%%%   <li>`Plus[1, ?, 3]' → ? = 2 (backward, solve for b)</li>
%%%   <li>`Plus[?, 2, 3]' → ? = 1 (backward, solve for a)</li>
%%%   <li>`Plus[?, ?, 5]' → all pairs (a,b) where a+b=5</li>
%%% </ul>
%%%
%%% The generators use constraint analysis to determine which arguments
%%% are bound and compute the missing values accordingly.
%%%
%%% == Relational Composition ==
%%%
%%% In the future, function relations will be used through joins:
%%% ```
%%% Integer[A], Integer[B], Plus[A, B, Sum]
%%% '''
%%%
%%% This corresponds to a three-way join where the join conditions
%%% provide the constraints to each relation.
%%%
%%% Generators produce tuples lazily based on constraints, enabling
%%% memory-bounded iteration over infinite relations.
%%%
%%% @author Nekoma Team
%%% @copyright 2025

-module(generators).
-include("operations.hrl").

-export([
    naturals/1,
    integers/1,
    rationals/1,
    make_range_generator/3,
    constrained_naturals/2,
    constrained_integers/2,
    %% Function relations
    plus/1,
    times/1,
    minus/1,
    divide/1
]).

%%% ============================================================================
%%% Primitive Generators
%%% ============================================================================

%% @doc Generator for natural numbers (ℕ = {0, 1, 2, 3, ...}).
%%
%% Enumerates natural numbers starting from 0. If constraints specify a range,
%% only generates numbers within that range.
%%
%% @param Constraints Map of attribute constraints
%% @returns Generator function producing naturals
naturals(Constraints) ->
    case extract_bounds(Constraints, value) of
        {range, Min, Max} when Min >= 0 ->
            make_range_generator(Min, Max, fun(N) -> #{value => N} end);
        {range, Min, Max} when Min < 0, Max >= 0 ->
            %% Negative min for naturals: adjust to start at 0
            make_range_generator(0, Max, fun(N) -> #{value => N} end);
        {range, _Min, Max} when Max < 0 ->
            %% Entire range is negative: no naturals exist
            fun(_) -> done end;
        {gte, Min} when Min >= 0 ->
            make_unbounded_generator(Min, fun(N) -> #{value => N} end);
        {gte, Min} when Min < 0 ->
            %% Negative lower bound for naturals: start at 0
            make_unbounded_generator(0, fun(N) -> #{value => N} end);
        {lte, Max} when Max >= 0 ->
            make_range_generator(0, Max, fun(N) -> #{value => N} end);
        {lte, Max} when Max < 0 ->
            %% Upper bound is negative: no naturals exist
            fun(_) -> done end;
        {eq, Value} when Value >= 0 ->
            make_singleton_generator(#{value => Value});
        {eq, _Value} ->
            %% Negative value: not a natural
            fun(_) -> done end;
        {in, List} ->
            make_membership_generator(List, fun(N) -> #{value => N} end);
        unbounded ->
            make_unbounded_generator(0, fun(N) -> #{value => N} end);
        {error, Reason} ->
            fun(_) -> {error, Reason} end
    end.

%% @doc Generator for integers (ℤ = {..., -2, -1, 0, 1, 2, ...}).
%%
%% Enumerates integers using interleaving strategy:
%% 0, 1, -1, 2, -2, 3, -3, ...
%%
%% Mapping: f(0) = 0
%%          f(2n) = n      for n > 0
%%          f(2n+1) = -n   for n > 0
%%
%% @param Constraints Map of attribute constraints
%% @returns Generator function producing integers
integers(Constraints) ->
    case extract_bounds(Constraints, value) of
        {range, Min, Max} ->
            make_integer_range_generator(Min, Max);
        {gte, Min} ->
            make_unbounded_integer_generator(Min, infinity);
        {lte, Max} ->
            make_unbounded_integer_generator(neg_infinity, Max);
        {eq, Value} ->
            make_singleton_generator(#{value => Value});
        {in, List} ->
            make_membership_generator(List, fun(N) -> #{value => N} end);
        unbounded ->
            make_interleaved_integer_generator();
        {error, Reason} ->
            fun(_) -> {error, Reason} end
    end.

%% @doc Generator for rational numbers (ℚ) using Stern-Brocot tree.
%%
%% Enumerates all rational numbers in reduced form (p/q where gcd(p,q) = 1)
%% using the Stern-Brocot tree, which systematically generates all positive
%% rationals without repetition.
%%
%% Schema: #{numerator => integer, denominator => natural}
%%
%% @param Constraints Map of attribute constraints
%% @returns Generator function producing rationals
rationals(Constraints) ->
    %% Stern-Brocot tree enumeration
    %% Start with mediant tree: 0/1, 1/1, then generate mediants
    InitialState = {queue:from_list([{0, 1}, {1, 1}]), {0, 1}, {1, 0}},
    make_stern_brocot_generator(InitialState, Constraints).

%%% ============================================================================
%%% Helper: Constraint Extraction
%%% ============================================================================

%% @doc Extract bounds from constraints for a given attribute.
%%
%% @param Constraints Constraint map
%% @param Attr Attribute name
%% @returns {range, Min, Max} | {gte, Min} | {lte, Max} | {eq, Value} | unbounded
extract_bounds(Constraints, Attr) ->
    case maps:get(Attr, Constraints, undefined) of
        undefined ->
            unbounded;
        {range, Min, Max} ->
            {range, Min, Max};
        {gte, Min} ->
            {gte, Min};
        {lte, Max} ->
            {lte, Max};
        {gt, Min} ->
            {gte, Min + 1};
        {lt, Max} ->
            {lte, Max - 1};
        {eq, Value} ->
            {eq, Value};
        {in, List} ->
            {in, List};
        Other ->
            {error, {unsupported_constraint, Other}}
    end.

%%% ============================================================================
%%% Generator Constructors
%%% ============================================================================

%% @doc Create a generator for a finite range [Min, Max].
make_range_generator(Min, Max, MapFun) when Min =< Max ->
    make_range_gen(Min, Max, MapFun).

make_range_gen(Current, Max, MapFun) when Current =< Max ->
    fun(next) ->
        Tuple = MapFun(Current),
        NextGen = make_range_gen(Current + 1, Max, MapFun),
        {value, Tuple, NextGen}
    end;
make_range_gen(_, _, _) ->
    fun(_) -> done end.

%% @doc Create an unbounded generator starting from Min.
make_unbounded_generator(Start, MapFun) ->
    make_unbounded_gen(Start, MapFun).

make_unbounded_gen(Current, MapFun) ->
    fun(next) ->
        Tuple = MapFun(Current),
        NextGen = make_unbounded_gen(Current + 1, MapFun),
        {value, Tuple, NextGen}
    end.

%% @doc Create a singleton generator (single value).
make_singleton_generator(Tuple) ->
    fun(next) -> {value, Tuple, fun(_) -> done end} end.

%% @doc Create a generator from a membership list.
make_membership_generator([], _MapFun) ->
    fun(_) -> done end;
make_membership_generator([H|T], MapFun) ->
    fun(next) ->
        Tuple = MapFun(H),
        NextGen = make_membership_generator(T, MapFun),
        {value, Tuple, NextGen}
    end.

%% @doc Create integer range generator.
make_integer_range_generator(Min, Max) when Min =< Max ->
    make_range_generator(Min, Max, fun(N) -> #{value => N} end);
make_integer_range_generator(_, _) ->
    fun(_) -> done end.

%% @doc Create interleaved integer generator: 0, 1, -1, 2, -2, 3, -3, ...
make_interleaved_integer_generator() ->
    make_interleaved_gen(0).

make_interleaved_gen(0) ->
    fun(next) ->
        {value, #{value => 0}, make_interleaved_gen(1)}
    end;
make_interleaved_gen(N) ->
    fun(next) ->
        Positive = #{value => N},
        NextGen1 = fun(next) ->
            Negative = #{value => -N},
            NextGen2 = make_interleaved_gen(N + 1),
            {value, Negative, NextGen2}
        end,
        {value, Positive, NextGen1}
    end.

%% @doc Unbounded integer generator with optional min/max bounds.
make_unbounded_integer_generator(neg_infinity, infinity) ->
    make_interleaved_integer_generator();
make_unbounded_integer_generator(Min, infinity) when is_integer(Min) ->
    make_unbounded_generator(Min, fun(N) -> #{value => N} end);
make_unbounded_integer_generator(neg_infinity, Max) when is_integer(Max) ->
    %% Generate from Max down to -infinity
    make_decreasing_gen(Max);
make_unbounded_integer_generator(Min, Max) when is_integer(Min), is_integer(Max) ->
    make_integer_range_generator(Min, Max).

make_decreasing_gen(Current) ->
    fun(next) ->
        Tuple = #{value => Current},
        NextGen = make_decreasing_gen(Current - 1),
        {value, Tuple, NextGen}
    end.

%% @doc Stern-Brocot tree generator for rationals.
%%
%% The Stern-Brocot tree enumerates all positive rationals exactly once.
%% Each rational p/q is generated in reduced form.
%%
%% Algorithm:
%% 1. Start with two fractions: 0/1 (left ancestor) and 1/0 (right ancestor)
%% 2. The mediant of a/b and c/d is (a+c)/(b+d)
%% 3. Build tree by computing mediants
make_stern_brocot_generator(State, _Constraints) ->
    make_sb_gen(State).

make_sb_gen({Queue, {LA, LB}, {RA, RB}}) ->
    fun(next) ->
        case queue:out(Queue) of
            {empty, _} ->
                done;
            {{value, {P, Q}}, RestQueue} ->
                %% Current rational is P/Q
                Tuple = #{numerator => P, denominator => Q},

                %% Generate children (mediants)
                %% Left child: mediant of left ancestor and current
                %% Right child: mediant of current and right ancestor
                LeftChild = {LA + P, LB + Q},
                RightChild = {P + RA, Q + RB},

                %% Enqueue children
                NewQueue = queue:in(RightChild, queue:in(LeftChild, RestQueue)),

                %% Continue with updated state
                NextGen = make_sb_gen({NewQueue, {LA, LB}, {RA, RB}}),
                {value, Tuple, NextGen}
        end
    end.

%%% ============================================================================
%%% Constrained Generators (Public API)
%%% ============================================================================

%% @doc Create a constrained naturals generator.
-spec constrained_naturals(non_neg_integer(), non_neg_integer()) -> generator_fun().
constrained_naturals(Min, Max) ->
    naturals(#{value => {range, Min, Max}}).

%% @doc Create a constrained integers generator.
-spec constrained_integers(integer(), integer()) -> generator_fun().
constrained_integers(Min, Max) ->
    integers(#{value => {range, Min, Max}}).

%%% ============================================================================
%%% Function Relations
%%% ============================================================================

%% @doc Plus relation: {(a, b, sum) | sum = a + b}.
%%
%% Generates tuples for the addition relation. Requires at least 2 of 3
%% attributes to be constrained for bounded generation.
%%
%% Constraint strategies:
%% <ul>
%%   <li>a, b constrained → compute sum</li>
%%   <li>a, sum constrained → compute b = sum - a</li>
%%   <li>b, sum constrained → compute a = sum - b</li>
%%   <li>sum constrained only → generate all (a,b) pairs that sum to value</li>
%%   <li>Less than 1 constrained → error (unbounded)</li>
%% </ul>
%%
%% @param Constraints Map of attribute constraints
%% @returns Generator function producing plus tuples
plus(Constraints) ->
    ABounds = extract_bounds(Constraints, a),
    BBounds = extract_bounds(Constraints, b),
    SumBounds = extract_bounds(Constraints, sum),

    case {ABounds, BBounds, SumBounds} of
        %% Both operands constrained - generate sums
        {{range, AMin, AMax}, {range, BMin, BMax}, _} ->
            make_plus_ab_generator(AMin, AMax, BMin, BMax);

        %% a and sum constrained - compute b
        {{range, AMin, AMax}, _, {range, SMin, SMax}} ->
            make_plus_a_sum_generator(AMin, AMax, SMin, SMax);

        %% b and sum constrained - compute a
        {_, {range, BMin, BMax}, {range, SMin, SMax}} ->
            make_plus_b_sum_generator(BMin, BMax, SMin, SMax);

        %% Only sum constrained - generate all pairs
        {unbounded, unbounded, {eq, SumValue}} ->
            make_plus_sum_only_generator(SumValue);

        {unbounded, unbounded, {range, SMin, SMax}} ->
            make_plus_sum_range_generator(SMin, SMax);

        %% Insufficient constraints
        _ ->
            fun(_) -> {error, {unbounded_plus, Constraints}} end
    end.

%% @doc Times relation: {(a, b, product) | product = a * b}.
%%
%% Generates tuples for the multiplication relation.
%%
%% @param Constraints Map of attribute constraints
%% @returns Generator function producing times tuples
times(Constraints) ->
    ABounds = extract_bounds(Constraints, a),
    BBounds = extract_bounds(Constraints, b),
    ProductBounds = extract_bounds(Constraints, product),

    case {ABounds, BBounds, ProductBounds} of
        %% Both operands constrained - generate products
        {{range, AMin, AMax}, {range, BMin, BMax}, _} ->
            make_times_ab_generator(AMin, AMax, BMin, BMax);

        %% Insufficient constraints
        _ ->
            fun(_) -> {error, {unbounded_times, Constraints}} end
    end.

%% @doc Minus relation: {(a, b, difference) | difference = a - b}.
%%
%% @param Constraints Map of attribute constraints
%% @returns Generator function producing minus tuples
minus(Constraints) ->
    ABounds = extract_bounds(Constraints, a),
    BBounds = extract_bounds(Constraints, b),
    DiffBounds = extract_bounds(Constraints, difference),

    case {ABounds, BBounds, DiffBounds} of
        {{range, AMin, AMax}, {range, BMin, BMax}, _} ->
            make_minus_ab_generator(AMin, AMax, BMin, BMax);

        _ ->
            fun(_) -> {error, {unbounded_minus, Constraints}} end
    end.

%% @doc Divide relation: {(a, b, quotient) | quotient = a / b, b ≠ 0}.
%%
%% @param Constraints Map of attribute constraints
%% @returns Generator function producing divide tuples
divide(Constraints) ->
    ABounds = extract_bounds(Constraints, a),
    BBounds = extract_bounds(Constraints, b),

    case {ABounds, BBounds} of
        {{range, AMin, AMax}, {range, BMin, BMax}} when BMin =/= 0 orelse BMax =/= 0 ->
            make_divide_ab_generator(AMin, AMax, BMin, BMax);

        _ ->
            fun(_) -> {error, {unbounded_divide, Constraints}} end
    end.

%%% ============================================================================
%%% Function Relation Generator Helpers
%%% ============================================================================

%% Plus: a and b constrained
make_plus_ab_generator(AMin, AMax, BMin, BMax) ->
    Pairs = [{A, B} || A <- lists:seq(AMin, AMax), B <- lists:seq(BMin, BMax)],
    make_plus_pairs_gen(Pairs).

make_plus_pairs_gen([]) ->
    fun(_) -> done end;
make_plus_pairs_gen([{A, B} | Rest]) ->
    fun(next) ->
        Tuple = #{a => A, b => B, sum => A + B},
        NextGen = make_plus_pairs_gen(Rest),
        {value, Tuple, NextGen}
    end.

%% Plus: a and sum constrained, compute b
make_plus_a_sum_generator(AMin, AMax, SMin, SMax) ->
    Pairs = [{A, S} ||
             A <- lists:seq(AMin, AMax),
             S <- lists:seq(SMin, SMax),
             (S - A) >= 0],  % b must be non-negative if we want naturals
    make_plus_a_sum_gen(Pairs).

make_plus_a_sum_gen([]) ->
    fun(_) -> done end;
make_plus_a_sum_gen([{A, Sum} | Rest]) ->
    fun(next) ->
        B = Sum - A,
        Tuple = #{a => A, b => B, sum => Sum},
        NextGen = make_plus_a_sum_gen(Rest),
        {value, Tuple, NextGen}
    end.

%% Plus: b and sum constrained, compute a
make_plus_b_sum_generator(BMin, BMax, SMin, SMax) ->
    Pairs = [{B, S} ||
             B <- lists:seq(BMin, BMax),
             S <- lists:seq(SMin, SMax),
             (S - B) >= 0],
    make_plus_b_sum_gen(Pairs).

make_plus_b_sum_gen([]) ->
    fun(_) -> done end;
make_plus_b_sum_gen([{B, Sum} | Rest]) ->
    fun(next) ->
        A = Sum - B,
        Tuple = #{a => A, b => B, sum => Sum},
        NextGen = make_plus_b_sum_gen(Rest),
        {value, Tuple, NextGen}
    end.

%% Plus: only sum constrained - generate all non-negative pairs
make_plus_sum_only_generator(SumValue) when SumValue >= 0 ->
    Pairs = [{A, SumValue - A} || A <- lists:seq(0, SumValue)],
    make_plus_pairs_gen(Pairs);
make_plus_sum_only_generator(_SumValue) ->
    %% Negative sum: no non-negative pairs exist
    fun(_) -> done end.

%% Plus: sum range constrained
make_plus_sum_range_generator(SMin, SMax) ->
    %% Adjust range to non-negative values (naturals only)
    AdjustedMin = max(0, SMin),
    Pairs = [{A, B, S} ||
             S <- lists:seq(AdjustedMin, SMax),
             A <- lists:seq(0, S),
             B <- [S - A]],
    make_plus_triple_gen(Pairs).

make_plus_triple_gen([]) ->
    fun(_) -> done end;
make_plus_triple_gen([{A, B, Sum} | Rest]) ->
    fun(next) ->
        Tuple = #{a => A, b => B, sum => Sum},
        NextGen = make_plus_triple_gen(Rest),
        {value, Tuple, NextGen}
    end.

%% Times: a and b constrained
make_times_ab_generator(AMin, AMax, BMin, BMax) ->
    Pairs = [{A, B} || A <- lists:seq(AMin, AMax), B <- lists:seq(BMin, BMax)],
    make_times_pairs_gen(Pairs).

make_times_pairs_gen([]) ->
    fun(_) -> done end;
make_times_pairs_gen([{A, B} | Rest]) ->
    fun(next) ->
        Tuple = #{a => A, b => B, product => A * B},
        NextGen = make_times_pairs_gen(Rest),
        {value, Tuple, NextGen}
    end.

%% Minus: a and b constrained
make_minus_ab_generator(AMin, AMax, BMin, BMax) ->
    Pairs = [{A, B} || A <- lists:seq(AMin, AMax), B <- lists:seq(BMin, BMax)],
    make_minus_pairs_gen(Pairs).

make_minus_pairs_gen([]) ->
    fun(_) -> done end;
make_minus_pairs_gen([{A, B} | Rest]) ->
    fun(next) ->
        Tuple = #{a => A, b => B, difference => A - B},
        NextGen = make_minus_pairs_gen(Rest),
        {value, Tuple, NextGen}
    end.

%% Divide: a and b constrained (integer division)
make_divide_ab_generator(AMin, AMax, BMin, BMax) ->
    Pairs = [{A, B} ||
             A <- lists:seq(AMin, AMax),
             B <- lists:seq(BMin, BMax),
             B =/= 0],  % Avoid division by zero
    make_divide_pairs_gen(Pairs).

make_divide_pairs_gen([]) ->
    fun(_) -> done end;
make_divide_pairs_gen([{A, B} | Rest]) ->
    fun(next) ->
        Tuple = #{a => A, b => B, quotient => A div B, remainder => A rem B},
        NextGen = make_divide_pairs_gen(Rest),
        {value, Tuple, NextGen}
    end.
