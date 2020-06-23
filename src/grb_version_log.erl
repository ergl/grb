-module(grb_version_log).
-include("grb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(DEFAULT, '$vlog_bottom').

-type entry() :: {transaction_type(), val(), vclock()}.
-record(vlog, {
    %% The max capacity of the buffer
    max_size :: non_neg_integer(),
    %% Pointers to the start and end of the buffer
    read_index = 0 :: array:array_indx(),
    write_index = 0 :: array:array_indx(),
    full = false :: boolean(),
    %% The actual data, a circular buffer of versions consisting
    %% of a value  and their commit vector.
    %% Versions are stored in insertion order
    buffer :: array:array(entry())
}).

-type t() :: #vlog{}.
-export_type([entry/0, t/0]).

%% API
-export([new/1,
         append/2,
         get_lower/2,
         to_list/1]).

-ifdef(TEST).
-export([from_list/2]).
-endif.

-spec new(non_neg_integer()) -> t().
new(Size) ->
    #vlog{max_size=Size, buffer=array:new(Size, {default, ?DEFAULT})}.

-spec append(entry(), t()) -> t().
append(Elt, Log=#vlog{buffer=B0, write_index=WIdx}) ->
    forward(Log#vlog{buffer=array:set(WIdx, Elt, B0)}).

forward(L=#vlog{max_size=Size, write_index=WIdx, read_index=RIdx, full=Full}) ->
    NextRIdx = case Full of
        true -> (RIdx + 1) rem Size;
        false -> RIdx
    end,
    NextWIdx = (WIdx + 1) rem Size,
    L#vlog{write_index=NextWIdx, read_index=NextRIdx, full=(NextRIdx =:= NextWIdx)}.

-spec get_lower(vclock(), t()) -> [entry()].
get_lower(VC, #vlog{buffer=B, read_index=Start, write_index=End, max_size=Size}) ->
    case array:get(Start, B) of
        ?DEFAULT ->
            [];
        Elt={_, _, EltVC} ->
            Acc = case grb_vclock:leq(EltVC, VC) of
                true -> [Elt];
                false -> []
            end,
            get_lower(VC, B, (Start + 1) rem Size, End, Size, Acc)
    end.

-spec get_lower(
    VC :: vclock(),
    Buff :: array:array(entry()),
    Start :: array:array_indx(),
    End :: array:array_indx(),
    Mod :: non_neg_integer(),
    Acc :: [entry()]
) -> [entry()].

get_lower(_VC, _Buff, End, End, _Mod, Acc) -> lists:reverse(Acc);
get_lower(VC, Buff, Start, End, Mod, Acc) ->
    {_, _, EltVC}=Elt = array:get(Start, Buff),
    Next = (Start + 1) rem Mod,
    case grb_vclock:leq(EltVC, VC) of
        true -> get_lower(VC, Buff, Next, End, Mod, [Elt | Acc]);
        false -> get_lower(VC, Buff, Next, End, Mod, Acc)
    end.

-spec to_list(t()) -> [entry()].
to_list(#vlog{buffer=B, read_index=Start, write_index=End, max_size=Size}) ->
    case array:get(Start, B) of
        ?DEFAULT -> [];
        Elt -> to_list(B, ((Start + 1) rem Size), End, Size, [Elt])
    end.

-spec to_list(
    array:array(entry()),
    array:array_indx(),
    array:array_indx(),
    non_neg_integer(),
    [entry()]
) -> [entry()].

to_list(_B, End, End, _Size, Acc) -> lists:reverse(Acc);
to_list(Buff, Start, End, Size, Acc) ->
    to_list(Buff, (Start + 1) rem Size, End, Size, [array:get(Start, Buff) | Acc]).

-ifdef(TEST).

from_list(List, Limit) ->
    from_list_inner(List, new(Limit)).

from_list_inner([], Log) -> Log;
from_list_inner([H | Rest], Log) ->
    from_list_inner(Rest, append(H, Log)).

grb_version_log_to_list_test() ->
    Log0 = grb_version_log:new(10),
    ?assertEqual([], grb_version_log:to_list(Log0)),

    List = lists:seq(0, 9),
    Log = grb_version_log:from_list(List, 10),
    ?assertEqual(List, grb_version_log:to_list(Log)),

    List1 = lists:seq(1, 10),
    Log1 = grb_version_log:append(10, Log),
    ?assertEqual(List1, grb_version_log:to_list(Log1)),

    List2 = lists:seq(2, 11),
    Log2 = grb_version_log:append(11, Log1),
    ?assertEqual(List2, grb_version_log:to_list(Log2)).

grb_version_log_get_lower_ordered_test() ->
    MyDCID = '$dc_id',
    Entries = lists:map(fun(V) ->
        VC = grb_vclock:set_time(MyDCID, V, grb_vclock:new()),
        {blue, V, VC}
    end, lists:seq(0, 9)),
    Log = grb_version_log:from_list(Entries, length(Entries)),
    ?assertEqual(Entries, grb_version_log:to_list(Log)),

    MaxVC = grb_vclock:set_time(MyDCID, 10, grb_vclock:new()),
    MaxMatches = grb_version_log:get_lower(MaxVC, Log),
    ?assertEqual(Entries, MaxMatches),

    MinVC = grb_vclock:new(),
    MinMatches0 = grb_version_log:get_lower(MinVC, Log),
    ?assertMatch([{blue, 0, _}], MinMatches0),

    Log1 = grb_version_log:append(
        {blue, 10, grb_vclock:set_time(MyDCID, 10, grb_vclock:new())},
        Log
    ),
    MinMatches1 = grb_version_log:get_lower(MinVC, Log1),
    ?assertMatch([], MinMatches1).

grb_version_log_get_lower_unordered_test() ->
    MyDCID = '$dc_id',
    VClock = fun(N) -> grb_vclock:set_time(MyDCID, N, grb_vclock:new()) end,

    Entries = [
        {blue, 0, VClock(0)},
        {blue, 2, VClock(2)},
        {blue, 1, VClock(1)},
        {blue, 3, VClock(3)},
        {blue, 4, VClock(4)},
        {blue, 7, VClock(7)},
        {blue, 5, VClock(5)},
        {blue, 6, VClock(6)},
        {blue, 9, VClock(9)},
        {blue, 8, VClock(8)}
    ],

    %% Entries are ordered by insertion
    Log = grb_version_log:from_list(Entries, length(Entries)),
    ?assertEqual(Entries, grb_version_log:to_list(Log)),

    MaxVC = grb_vclock:set_time(MyDCID, 10, grb_vclock:new()),
    MaxMatches = grb_version_log:get_lower(MaxVC, Log),
    ?assertEqual(Entries, MaxMatches),

    MinVC = grb_vclock:new(),
    MinMatches0 = grb_version_log:get_lower(MinVC, Log),
    ?assertMatch([{blue, 0, _}], MinMatches0),

    Log1 = grb_version_log:append(
        {blue, 10, grb_vclock:set_time(MyDCID, 10, grb_vclock:new())},
        Log
    ),
    MinMatches1 = grb_version_log:get_lower(MinVC, Log1),
    ?assertMatch([], MinMatches1),

    %% Matches keep the order of insertion
    Matches = grb_version_log:get_lower(VClock(2), Log1),
    ?assertMatch([{blue, 2,  _}, {blue, 1, _}], Matches).

-endif.
