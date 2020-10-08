-module(grb_measurements).
-behavior(gen_server).
-include_lib("kernel/include/logger.hrl").

-define(AGGREGATE_TABLE, aggregate_table).
-define(COUNTER_TABLE, counter_table).

-record(stat_entry, {
    key :: term(),
    max = 0 :: non_neg_integer(),
    ops = 0 :: non_neg_integer(),
    data = 0 :: non_neg_integer()
}).

-record(counter_entry, {
    key :: term(),
    data = 0 :: non_neg_integer()
}).

-record(state, { aggregate_table :: ets:tab(), counter_table :: ets:tab() }).

-define(buffered, buffered).
-define(op_not_ready, op_not_ready).
-define(red_receiver_q_len, red_receiver_queue_length).
-define(leader_q_len, leader_queue_length).
-define(leader_vnode, leader_vnode_time).
-define(follower_q_len, follower_queue_length).
-define(red_read_barrier, red_read_barrier).

-define(MEASUREMENTS, [
    ?red_receiver_q_len,
    ?leader_q_len,
    ?leader_vnode,
    ?follower_q_len
]).

-define(COUNTERS, [
    ?buffered,
    ?op_not_ready,
    ?red_read_barrier
]).

%% Called by rpc
-ignore_xref([report_stats/0,
              start_link/0]).

%% API
-export([report_stats/0]).

-export([start_link/0,
         log_buffered_decision/0,
         log_op_not_ready/0,
         log_red_receiver_qlen/1,
         log_leader_qlen/1,
         log_leader_vnode/1,
         log_follower_qlen/1,
         log_red_read_barrier/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

log_buffered_decision() -> incr_counter(?buffered).
log_op_not_ready() -> incr_counter(?op_not_ready).
log_red_read_barrier() -> incr_counter(?red_read_barrier).

log_red_receiver_qlen(Len) -> add_stat(?red_receiver_q_len, Len).
log_leader_qlen(Len) -> add_stat(?leader_q_len, Len).
log_leader_vnode(Len) -> add_stat(?leader_vnode, Len).
log_follower_qlen(Len) -> add_stat(?follower_q_len, Len).

-spec incr_counter(term()) -> ok.
incr_counter(Key) ->
    ets:update_counter(?COUNTER_TABLE, Key, {#counter_entry.data, 1}),
    ok.

-spec add_stat(term(), non_neg_integer()) -> ok.
add_stat(Key, Measurement) ->
    ets:select_replace(?AGGREGATE_TABLE, [
        { #stat_entry{key=Key, max='$1', ops='$2', data='$3'},
          [{'<', '$1', Measurement}],
          [{ #stat_entry{key=Key, max=Measurement, ops={'+', '$2', 1}, data={'+', '$3', Measurement}} }]},

        { #stat_entry{key=Key, max='$1', ops='$2', data='$3'},
          [],
          [{ #stat_entry{key=Key, max='$1', ops={'+', '$2', 1}, data={'+', '$3', Measurement}}}]}
    ]),
    ok.

-spec report_stats() -> [#{}].
report_stats() ->
    L0 = [ {stat, K, #{max => Max, avg => case N of 0 -> 0; _ -> Total / N end, total => Total}}
        || #stat_entry{key=K, max=Max, ops=N, data=Total} <- ets:tab2list(?AGGREGATE_TABLE)],
    L1 = [ {counter, K, C} || #counter_entry{key=K, data=C} <- ets:tab2list(?COUNTER_TABLE)],
    L0 ++ L1.

init([]) ->
    Aggr = ets:new(?AGGREGATE_TABLE, [set, named_table, public, {write_concurrency, true}, {keypos, #stat_entry.key}]),
    Count = ets:new(?COUNTER_TABLE, [set, named_table, public, {write_concurrency, true}, {keypos, #counter_entry.key}]),
    ets:insert(Aggr, [#stat_entry{key=Key} || Key <- ?MEASUREMENTS]),
    ets:insert(Count, [#counter_entry{key=Key} || Key <- ?COUNTERS]),
    {ok, #state{aggregate_table=Aggr, counter_table=Count}}.

handle_call(E, _From, S) ->
    ?LOG_WARNING("~p unexpected call: ~p~n", [?MODULE, E]),
    {reply, ok, S}.

handle_cast(E, S) ->
    ?LOG_WARNING("~p unexpected cast: ~p~n", [?MODULE, E]),
    {noreply, S}.

handle_info(Info, State) ->
    ?LOG_WARNING("~p Unhandled msg ~p", [?MODULE, Info]),
    {noreply, State}.
