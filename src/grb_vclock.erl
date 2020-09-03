-module(grb_vclock).

%% API
-export([new/0,
         get_time/2,
         set_time/3,
         set_max_time/3,
         eq/2,
         leq/2,
         min/2,
         max/2,
         min_at/3,
         max_at/3,
         max_at_keys/3,
         to_list/1]).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type vc() :: vc(term()).
-opaque vc(T) :: #{T => grb_time:ts()}.

-export_type([vc/1]).

-spec new() -> vc().
new() -> #{}.

-spec get_time(T, vc(T)) -> grb_time:ts().
get_time(Key, VectorClock) ->
    maps:get(Key, VectorClock, 0).

-spec set_time(T, grb_time:ts(), vc(T)) -> vc(T).
set_time(Key, Value, VectorClock) ->
    maps:put(Key, Value, VectorClock).

-spec set_max_time(T, grb_time:ts(), vc(T)) -> vc(T).
set_max_time(Key, Value, VectorClock) ->
    Old = get_time(Key, VectorClock),
    VectorClock#{Key => erlang:max(Value, Old)}.

-spec eq(vc(T), vc(T)) -> boolean().
eq(VC, VC) -> true;
eq(Left, Right) -> Left =:= Right.

-spec min(vc(T), vc(T)) -> vc(T).
min(Left, _Right) when map_size(Left) == 0 -> Left;
min(_Left, Right) when map_size(Right) == 0 -> Right;
min(Left, Right) ->
    LeftKeys = maps:keys(Left),
    RigthKeys = maps:keys(Right),
    lists:foldl(fun(Key, AccMap) ->
        LeftV = get_time(Key, Left),
        RigthV = get_time(Key, Right),
        AccMap#{Key => erlang:min(LeftV, RigthV)}
    end, #{}, LeftKeys ++ RigthKeys). %% :^(

-spec min_at([T], vc(T), vc(T)) -> vc(T).
min_at(Keys, Left, Right) ->
    lists:foldl(fun(Key, AccMap) ->
        LeftV = get_time(Key, Left),
        RigthV = get_time(Key, Right),
        AccMap#{Key => erlang:min(LeftV, RigthV)}
    end, #{}, Keys).

-spec max(vc(T), vc(T)) -> vc(T).
max(Left, Right) when map_size(Left) == 0 -> Right;
max(Left, Right) when map_size(Right) == 0 -> Left;
max(Left, Right) ->
    maps:merge(Left, maps:map(fun(Key, Value) ->
        case maps:find(Key, Left) of
            {ok, V} -> erlang:max(V, Value);
            error -> Value
        end
    end, Right)).

-spec max_at(T, vc(T), vc(T)) -> vc(T).
max_at(Key, Left, Right) ->
    Left#{Key => erlang:max(maps:get(Key, Left, 0), maps:get(Key, Right, 0))}.

-spec max_at_keys([T], vc(T), vc(T)) -> vc(T).
max_at_keys(Keys, Left, Right) ->
    lists:foldl(fun(Key, AccMap) ->
        AccMap#{Key => erlang:max(get_time(Key, Left), get_time(Key, Right))}
    end, Left, Keys).

-spec leq(vc(T), vc(T)) -> boolean().
leq(Left, Right) ->
    F = fun(Key) ->
        get_time(Key, Left) =< get_time(Key, Right)
    end,
    lists:all(F, maps:keys(maps:merge(Left, Right))).

-spec to_list(vc(T)) -> [{T, grb_time:ts()}].
to_list(VC) -> maps:to_list(VC).

-ifdef(TEST).

grb_vclock_min_test() ->
    A = #{c => 10},
    B = #{a => 5, b => 3, c => 2},
    C = #{a => 3, b => 4, c => 7},
    D = #{a => 0, b => 2, c => 3},

    Result = lists:foldl(fun
        (V, ignore) -> V;
        (V, Acc) -> grb_vclock:min(V, Acc)
    end, ignore, [A, B, C, D]),

    ShouldBe = #{a => 0, b => 0, c => 2},
    ?assertEqual(ShouldBe, Result).

grb_vclock_min_at_test() ->
    A = #{c => 10},
    B = #{a => 5, b => 3, c => 2},
    C = #{a => 3, b => 4, c => 7},
    D = #{a => 0, b => 2, c => 3},

    Result = lists:foldl(fun
        (V, ignore) -> V;
        (V, Acc) -> grb_vclock:min_at([a,b,c], V, Acc)
    end, ignore, [A, B, C, D]),

    ShouldBe = #{a => 0, b => 0, c => 2},
    ?assertEqual(ShouldBe, Result).

grb_vclock_max_test() ->
    A = #{c => 10},
    B = #{a => 5, c => 2},
    C = #{a => 3, b => 4, c => 7},
    D = #{b => 2},

    Result = lists:foldl(fun(V, Acc) ->
        grb_vclock:max(V, Acc)
    end, #{}, [A, B, C, D]),
    ShouldBe = #{a => 5, b => 4, c => 10},
    ?assertEqual(ShouldBe, Result).

grb_vclock_max_at_test() ->
    A = #{a => 0, b => 10},
    B = #{b => 20},

    C = grb_vclock:max_at(b, B, A),
    ?assertEqual(B, C),

    D = grb_vclock:max_at(a, B, A),
    ?assertEqual(#{a => 0, b => 20}, D),

    E = grb_vclock:max_at(b, A, B),
    ?assertEqual(D, E),

    F = grb_vclock:max_at(a, A, B),
    ?assertEqual(A, F).

grb_vclock_max_at_keys_test() ->
    A = #{a => 0, b => 10, c => 30},
    B = #{b => 20},

    % Max at all keys is equivalent to normal max
    C0 = grb_vclock:max_at_keys([a, b, c], B, A),
    C1 = grb_vclock:max(B, A),
    ?assertEqual(C0, C1),

    D = grb_vclock:max_at_keys([c], B, A),
    ?assertEqual(grb_vclock:max_at(c, B, A), D),

    E = grb_vclock:max_at_keys([c], A, B),
    ?assertEqual(A, E),

    F = grb_vclock:max_at_keys([a], A, B),
    ?assertEqual(A, F).

-endif.
