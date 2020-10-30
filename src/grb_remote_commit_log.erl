-module(grb_remote_commit_log).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-export([to_list/1, from_list/2]).
-endif.

-define(TABLE_NAME, committed_blue_table).
-define(TABLE_OPTS, [ordered_set, public, {read_concurrency, true}, {write_concurrency, true}]).

-type t() :: cache_id().
-type index() :: ets:continuation().
-export_type([t/0, index/0]).

%% API
-export([new/0,
         delete/1,
         insert/4,
         get_bigger/2,
         get_bigger_iterator/3,
         get_bigger_continue/1,
         remove_leq/2]).

-spec new() -> t().
new() ->
    ets:new(?TABLE_NAME, ?TABLE_OPTS).

-spec insert(grb_time:ts(), writeset(), vclock(), t()) -> ok.
insert(CommitTime, WS, CommitVC, Table) ->
    true = ets:insert(Table, {CommitTime, {WS, CommitVC}}),
    ok.

-spec delete(t()) -> ok.
delete(Table) ->
    true = ets:delete(Table),
    ok.

%% @doc Get all entries with commit time at the created replica bigger than `Timestamp`
%%
%%      Entries are returned in increasing commit time order
-spec get_bigger(grb_time:ts(), t()) -> [tx_entry()].
get_bigger(Timestamp, Table) ->
    ets:select(Table, [{ {'$1', '$2'}, [{'>', '$1', {const, Timestamp}}], ['$2'] }]).

-spec get_bigger_iterator(grb_time:ts(), non_neg_integer(), t()) -> empty | {[tx_entry()], index()}.
get_bigger_iterator(Timestamp, Limit, Table) ->
    case ets:select(Table, [{ {'$1', '$2'}, [{'>', '$1', {const, Timestamp}}], ['$2'] }], Limit) of
        '$end_of_table' -> empty;
        More -> More
    end.

-spec get_bigger_continue(index()) -> empty | {[tx_entry()], index()}.
get_bigger_continue(Cont) ->
    case ets:select(Cont) of
        '$end_of_table' -> empty;
        More -> More
    end.

%% @doc Remove all entries with commit time at the created replica lower than `Timestamp`
-spec remove_leq(grb_time:ts(), t()) -> ok.
remove_leq(Timestamp, Table) ->
    _ = ets:select_delete(Table, [{ {'$1', '_'}, [{'=<', '$1', {const, Timestamp}}], [true] }]),
    ok.

-ifdef(TEST).

-spec to_list(t()) -> [tx_entry()].
to_list(Table) ->
    ets:select(Table, [{ {'$1', '$2'}, [], ['$2'] }]).

from_list(At, List) ->
    Table = new(),
    lists:foreach(fun({WS, CVC}) ->
        insert(grb_vclock:get_time(At, CVC), WS, CVC, Table)
    end, List),
    Table.

grb_remote_commit_log_insert_test() ->
    Replica = '$dc_id',
    L = grb_remote_commit_log:new(),
    ok = grb_remote_commit_log:insert(0, #{a => b}, #{Replica => 0}, L),
    ok = grb_remote_commit_log:insert(0, #{a => c}, #{Replica => 0}, L),
    %% Only the last element is kept, but this should be rare
    ?assertEqual([{#{a => c}, #{Replica => 0}}], grb_remote_commit_log:to_list(L)).

grb_remote_commit_log_get_bigger_test() ->
    Replica = '$dc_id',
    Entries = lists:map(fun(V) ->
        {#{}, grb_vclock:set_time(Replica, V, grb_vclock:new())}
    end, lists:seq(1, 50)),
    Log = grb_remote_commit_log:from_list(Replica, Entries),

    AllMatches = grb_remote_commit_log:get_bigger(0, Log),
    ?assertEqual(Entries, AllMatches),

    NoMatches = grb_remote_commit_log:get_bigger(50, Log),
    ?assertMatch([], NoMatches),

    SomeMatches = grb_remote_commit_log:get_bigger(25, Log),
    ?assertEqual(lists:sublist(Entries, 26, 50), SomeMatches),
    grb_remote_commit_log:delete(Log).

grb_remote_commit_log_get_bigger_iterator_test() ->
    Replica = '$dc_id',
    Entries = lists:map(fun(V) ->
        {#{}, grb_vclock:set_time(Replica, V, grb_vclock:new())}
    end, lists:seq(1, 50)),
    Log = grb_remote_commit_log:from_list(Replica, Entries),

    ChunkSize = 25,

    {M0, Iter0} = grb_remote_commit_log:get_bigger_iterator(0, ChunkSize, Log),
    ?assertEqual(lists:sublist(Entries, ChunkSize), M0),

    {M1, Iter1} = grb_remote_commit_log:get_bigger_continue(Iter0),
    ?assertEqual(lists:sublist(Entries, ChunkSize + 1, ChunkSize), M1),

    ?assertMatch(empty, grb_remote_commit_log:get_bigger_continue(Iter1)).

grb_remote_commit_log_remove_leq_test() ->
    Replica = '$dc_id',
    Entries = lists:map(fun(V) ->
        {#{}, grb_vclock:set_time(Replica, V, grb_vclock:new())}
    end, lists:seq(1, 50)),
    Log = grb_remote_commit_log:from_list(Replica, Entries),

    Before = grb_remote_commit_log:to_list(Log),
    ok = grb_remote_commit_log:remove_leq(0, Log),
    After = grb_remote_commit_log:to_list(Log),
    ?assertEqual(Before, After),

    ok = grb_remote_commit_log:remove_leq(50, Log),
    ?assertMatch([], grb_remote_commit_log:to_list(Log)),
    grb_remote_commit_log:delete(Log),

    AnotherLog = grb_remote_commit_log:from_list(Replica, Entries),
    ok = grb_remote_commit_log:remove_leq(25, AnotherLog),
    %% The final log still has the entries 26 to 50
    Resulting = lists:sublist(Entries, 26, 50),
    ?assertEqual(Resulting, grb_remote_commit_log:to_list(AnotherLog)).

-endif.
