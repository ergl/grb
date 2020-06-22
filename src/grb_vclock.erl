-module(grb_vclock).

%% API
-export([new/0,
         get_time/2,
         set_time/3,
         set_max_time/3,
         eq/2,
         leq/2,
         max/2,
         max_at/3]).

%% Debug API
-export([from_list/1]).

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

-spec max_at(T, vc(T), vc(T)) -> vc(T).
max_at(Key, Left, Right) ->
    Left#{Key => erlang:max(maps:get(Key, Left, 0), maps:get(Key, Right, 0))}.

-spec leq(vc(T), vc(T)) -> boolean().
leq(Left, Right) ->
    F = fun(Key) ->
        get_time(Key, Left) =< get_time(Key, Right)
    end,
    lists:all(F, maps:keys(maps:merge(Left, Right))).

from_list(List) ->
    maps:from_list(List).
