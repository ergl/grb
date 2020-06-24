-module(grb_vclock).

%% API
-export([new/0,
         get_time/2,
         set_time/3,
         set_max_time/3,
         eq/2,
         leq/2,
         max/2,
         max_at/3,
         max_except/3]).

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
    maps:update_with(Key, fun(Old) -> erlang:max(Old, Value) end, Value, VectorClock).

-spec eq(vc(T), vc(T)) -> boolean().
eq(VC, VC) -> true;
eq(Left, Right) -> Left =:= Right.

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

-spec max_except(T, vc(T), vc(T)) -> vc(T).
max_except(Key, Left, Right) ->
    maps:merge(Left, maps:map(fun
        (InnerKey, _) when InnerKey =:= Key -> get_time(Key, Left);
        (InnerKey, Value) ->
            case maps:find(InnerKey, Left) of
                {ok, V} -> erlang:max(V, Value);
                error -> Value
            end
    end, Right)).

-spec max_at(T, vc(T), vc(T)) -> vc(T).
max_at(Key, Left, Right) ->
    Left#{Key => erlang:max(maps:get(Key, Left, 0), maps:get(Key, Right, 0))}.

-spec leq(vc(T), vc(T)) -> boolean().
leq(Left, Right) ->
    F = fun(Key) ->
        get_time(Key, Left) =< get_time(Key, Right)
    end,
    lists:all(F, maps:keys(maps:merge(Left, Right))).

-ifdef(TEST).

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

grb_vclock_max_except_test() ->
    A = #{a => 1,  b => 10},
    B = #{b => 20},

    C = grb_vclock:max_except(b, B, A),
    ?assertEqual(grb_vclock:max(B, A), C),

    D = grb_vclock:max_except(a, B, A),
    ?assertEqual(#{a => 0, b => 20}, D),

    E = grb_vclock:max_except(b, A, B),
    ?assertEqual(A, E),

    F = grb_vclock:max_except(a, A, B),
    ?assertEqual(grb_vclock:max(A, B), F).

-endif.
