-module(grb_blue_commit_log).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([to_list/1, from_list/2]).
-endif.

-record(state, {
    at :: replica_id(),
    %% entries, indexed by their commit time at this replica (larger commit times first)
    entries :: orddict:dict(neg_integer(), term())
}).

-type t() :: #state{}.
-type entry() :: {writeset(), vclock()}.

-export_type([t/0, entry/0]).

%% API
-export([new/1,
         insert/3,
         get_bigger/2,
         remove_leq/2]).

-ifdef(BASIC_REPLICATION).
-export([remove_bigger/2]).
-endif.

-spec new(replica_id()) -> t().
new(AtId) ->
    #state{at=AtId, entries=orddict:new()}.

-spec insert(writeset(), vclock(), t()) -> t().
insert(WS, CommitVC, S=#state{at=Id, entries=Entries}) ->
    Key = grb_vclock:get_time(Id, CommitVC),
    %% todo(borja, warn): Beware of negating timestamps, if we change the type of grb_time:ts()
    S#state{entries=orddict:store(-Key, {WS, CommitVC}, Entries)}.

%% @doc Get all entries with commit time at the created replica bigger than `Timestamp`
%%
%%      Entries are returned in increasing commit time order
-spec get_bigger(grb_time:ts(), t()) -> [entry()].
get_bigger(Timestamp, #state{entries=Entries}) ->
    get_bigger(Timestamp, Entries, []).

get_bigger(_Cutoff, [], Acc) -> Acc;
get_bigger(Cutoff, [{Key, _} | _], Acc) when abs(Key) =< Cutoff ->
    Acc;
get_bigger(Cutoff, [{Key, Val} | Rest], Acc) when abs(Key) > Cutoff ->
    get_bigger(Cutoff, Rest, [Val | Acc]).

-ifdef(BASIC_REPLICATION).
%% @doc Remove all entries with commit time at the created replica bigger than `Timestamp`
%%
%%      Entries are returned in increasing commit time order
-spec remove_bigger(grb_time:ts(), t()) -> {[entry()], t()}.
remove_bigger(Timestamp, S=#state{entries=Entries}) ->
    {Matches, NewEntries} = remove_bigger(Timestamp, Entries, []),
    {Matches, S#state{entries=NewEntries}}.

remove_bigger(_Cutoff, [], Acc) ->
    {Acc, []};
remove_bigger(Cutoff, All=[{Key, _} | _], Acc) when abs(Key) =< Cutoff ->
    {Acc, All};
remove_bigger(Cutoff, [{Key, Val} | Rest], Acc) when abs(Key) > Cutoff ->
    remove_bigger(Cutoff, Rest, [Val | Acc]).
-endif.

%% @doc Remove all entries with commit time at the created replica lower than `Timestamp`
-spec remove_leq(grb_time:ts(), t()) -> t().
remove_leq(_, S=#state{entries=[]}) -> S;
remove_leq(Timestamp, S=#state{entries=Entries}) ->
    S#state{entries=orddict:filter(fun(Key, _) -> abs(Key) > Timestamp end, Entries)}.

-ifdef(TEST).

to_list(#state{entries=Entries}) ->
    orddict:to_list(Entries).

from_list(At, List) ->
    from_list_inner(List, new(At)).

from_list_inner([], Log) -> Log;
from_list_inner([{WS, CommitVC} | Rest], Log) ->
    from_list_inner(Rest, insert(WS, CommitVC, Log)).

grb_blue_commit_log_get_bigger_ordered_test() ->
    MyReplicaID = '$dc_id',
    Entries = lists:map(fun(V) ->
        {#{}, grb_vclock:set_time(MyReplicaID, V, grb_vclock:new())}
    end, lists:seq(1, 50)),
    Log = grb_blue_commit_log:from_list(MyReplicaID, Entries),

    AllMatches = grb_blue_commit_log:get_bigger(0, Log),
    ?assertEqual(Entries, AllMatches),

    NoMatches = grb_blue_commit_log:get_bigger(50, Log),
    ?assertMatch([], NoMatches),

    SomeMatches = grb_blue_commit_log:get_bigger(25, Log),
    ?assertEqual(lists:sublist(Entries, 26, 50), SomeMatches).

grb_blue_commit_log_get_bigger_unordered_test() ->
    MyReplicaID = '$dc_id',
    VClock = fun(N) -> grb_vclock:set_time(MyReplicaID, N, grb_vclock:new()) end,

    Entries = [
        {#{}, VClock(2)},
        {#{}, VClock(1)},
        {#{}, VClock(3)},
        {#{}, VClock(4)},
        {#{}, VClock(7)},
        {#{}, VClock(5)},
        {#{}, VClock(6)},
        {#{}, VClock(9)},
        {#{}, VClock(8)}
    ],
    SortedList = lists:sort(fun({_, LeftVC}, {_, RightVC}) ->
        grb_vclock:get_time(MyReplicaID, LeftVC) =< grb_vclock:get_time(MyReplicaID, RightVC)
    end, Entries),

    Log = grb_blue_commit_log:from_list(MyReplicaID, Entries),

    AllMatches = grb_blue_commit_log:get_bigger(0, Log),
    ?assertEqual(SortedList, AllMatches),

    NoMatches = grb_blue_commit_log:get_bigger(9, Log),
    ?assertMatch([], NoMatches),

    SomeMatches = grb_blue_commit_log:get_bigger(5, Log),
    ?assertEqual(lists:sublist(SortedList, 6, 9), SomeMatches).

-ifdef(BASIC_REPLICATION).
grb_blue_commit_log_remove_bigger_ordered_test() ->
    MyReplicaID = '$dc_id',
    Entries = lists:map(fun(V) ->
        {#{}, grb_vclock:set_time(MyReplicaID, V, grb_vclock:new())}
    end, lists:seq(1, 50)),
    Log = grb_blue_commit_log:from_list(MyReplicaID, Entries),

    {AllMatches, Log1} = grb_blue_commit_log:remove_bigger(0, Log),
    ?assertEqual(Entries, AllMatches),
    ?assertEqual(grb_blue_commit_log:new(MyReplicaID), Log1),

    {NoMatches, Log2} = grb_blue_commit_log:remove_bigger(50, Log),
    ?assertMatch([], NoMatches),
    ?assertEqual(Log, Log2),

    {SomeMatches, Log3} = grb_blue_commit_log:remove_bigger(25, Log),
    ?assertEqual(lists:sublist(Entries, 26, 50), SomeMatches),
    %% The final log still has the entries 1 to 25
    Resulting = grb_blue_commit_log:from_list(MyReplicaID, lists:sublist(Entries, 1, 25)),
    ?assertEqual(Resulting, Log3).

grb_blue_commit_log_remove_bigger_unordered_test() ->
    MyReplicaID = '$dc_id',
    VClock = fun(N) -> grb_vclock:set_time(MyReplicaID, N, grb_vclock:new()) end,

    Entries = [
        {#{}, VClock(2)},
        {#{}, VClock(1)},
        {#{}, VClock(3)},
        {#{}, VClock(4)},
        {#{}, VClock(7)},
        {#{}, VClock(5)},
        {#{}, VClock(6)},
        {#{}, VClock(9)},
        {#{}, VClock(8)}
    ],
    SortedList = lists:sort(fun({_, LeftVC}, {_, RightVC}) ->
        grb_vclock:get_time(MyReplicaID, LeftVC) =< grb_vclock:get_time(MyReplicaID, RightVC)
    end, Entries),

    Log = grb_blue_commit_log:from_list(MyReplicaID, Entries),

    {AllMatches, Log1} = grb_blue_commit_log:remove_bigger(0, Log),
    ?assertEqual(SortedList, AllMatches),
    ?assertEqual(grb_blue_commit_log:new(MyReplicaID), Log1),

    {NoMatches, Log2} = grb_blue_commit_log:remove_bigger(9, Log),
    ?assertMatch([], NoMatches),
    ?assertEqual(Log, Log2),

    {SomeMatches, Log3} = grb_blue_commit_log:remove_bigger(5, Log),
    ?assertEqual(lists:sublist(SortedList, 6, 9), SomeMatches),
    %% Same as above, in the same order, but removing elements bigger than 5
    Resulting = grb_blue_commit_log:from_list(MyReplicaID, [
        {#{}, VClock(2)},
        {#{}, VClock(1)},
        {#{}, VClock(3)},
        {#{}, VClock(4)},
        {#{}, VClock(5)}
    ]),
    ?assertEqual(Resulting, Log3).
-endif.

grb_blue_commit_log_remove_leq_ordered_test() ->
    MyReplicaID = '$dc_id',
    Entries = lists:map(fun(V) ->
        {#{}, grb_vclock:set_time(MyReplicaID, V, grb_vclock:new())}
    end, lists:seq(1, 50)),
    Log = grb_blue_commit_log:from_list(MyReplicaID, Entries),

    Log1 = grb_blue_commit_log:remove_leq(0, Log),
    ?assertEqual(Log, Log1),

    Log2 = grb_blue_commit_log:remove_leq(50, Log),
    ?assertEqual(grb_blue_commit_log:new(MyReplicaID), Log2),

    Log3 = grb_blue_commit_log:remove_leq(25, Log),
    %% The final log still has the entries 26 to 50
    Resulting = grb_blue_commit_log:from_list(MyReplicaID, lists:sublist(Entries, 26, 50)),
    ?assertEqual(Resulting, Log3).

grb_blue_commit_log_remove_leq_unordered_test() ->
    MyReplicaID = '$dc_id',
    VClock = fun(N) -> grb_vclock:set_time(MyReplicaID, N, grb_vclock:new()) end,

    Entries = [
        {#{}, VClock(2)},
        {#{}, VClock(1)},
        {#{}, VClock(3)},
        {#{}, VClock(4)},
        {#{}, VClock(7)},
        {#{}, VClock(5)},
        {#{}, VClock(6)},
        {#{}, VClock(9)},
        {#{}, VClock(8)}
    ],

    Log = grb_blue_commit_log:from_list(MyReplicaID, Entries),

    Log1 = grb_blue_commit_log:remove_leq(0, Log),
    ?assertEqual(Log, Log1),

    Log2 = grb_blue_commit_log:remove_leq(9, Log),
    ?assertEqual(grb_blue_commit_log:new(MyReplicaID), Log2),

    Log3 = grb_blue_commit_log:remove_leq(5, Log),
    %% Same as above, in the same order, but removing elements lower or equal than 5
    Resulting = grb_blue_commit_log:from_list(MyReplicaID, [
        {#{}, VClock(7)},
        {#{}, VClock(6)},
        {#{}, VClock(9)},
        {#{}, VClock(8)}
    ]),
    ?assertEqual(Resulting, Log3).

-endif.
