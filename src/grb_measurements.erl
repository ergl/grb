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

-export([start_link/0]).

%% Called by erpc or supervision
-ignore_xref([report_stats/0,
              start_link/0]).

-export([create_stat/1,
         log_counter/1,
         log_stat/2,
         log_queue_length/1,
         report_stats/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2]).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec create_stat(term()) -> ok.
-ifdef(ENABLE_METRICS).
create_stat(Key) ->
    true = ets:insert(persistent_term:get({?MODULE, ?AGGREGATE_TABLE}), #stat_entry{key=Key}),
    ok.
-else.
create_stat(_) ->
    ok.
-endif.

-spec log_counter(term()) -> ok.
-ifdef(ENABLE_METRICS).
log_counter(Key) ->
    ets:update_counter(persistent_term:get({?MODULE, ?COUNTER_TABLE}),
                       Key, {#counter_entry.data, 1}, #counter_entry{key=Key}),
    ok.
-else.
log_counter(_) ->
    ok.
-endif.

-spec log_stat(term(), non_neg_integer()) -> ok.
-ifdef(ENABLE_METRICS).
%% Rolling max and average (non-weighted)
%% If Key does not have a matching stat, it is ignored
log_stat(Key, Amount) ->
    ets:select_replace(persistent_term:get({?MODULE, ?AGGREGATE_TABLE}), [
        { #stat_entry{key=Key, max='$1', ops='$2', data='$3'},
          [{'<', '$1', Amount}],
          max_matchspec(Key, Amount)},

        { #stat_entry{key=Key, max='$1', ops='$2', data='$3'},
          [],
          add_matchspec(Key, Amount)}
    ]),
    ok.

-spec max_matchspec(term(), non_neg_integer()) -> [term()].
max_matchspec(Key, Amount) when is_tuple(Key) ->
    [{ #stat_entry{key={Key}, max=Amount, ops={'+', '$2', 1}, data={'+', '$3', Amount}} }];
max_matchspec(Key, Amount) ->
    [{ #stat_entry{key=Key, max=Amount, ops={'+', '$2', 1}, data={'+', '$3', Amount}} }].

-spec add_matchspec(term(), non_neg_integer()) -> [term()].
add_matchspec(Key, Amount) when is_tuple(Key) ->
    [{ #stat_entry{key={Key}, max='$1', ops={'+', '$2', 1}, data={'+', '$3', Amount}}}];
add_matchspec(Key, Amount) ->
    [{ #stat_entry{key=Key, max='$1', ops={'+', '$2', 1}, data={'+', '$3', Amount}}}].

-else.
log_stat(_, _) ->
    ok.
-endif.

-spec log_queue_length(term()) -> ok.
-ifdef(ENABLE_METRICS).
log_queue_length(Key) ->
    {message_queue_len, N} = process_info(self(), message_queue_len),
    log_stat(Key, N).
-else.
log_queue_length(_) ->
    ok.
-endif.

-spec report_stats() -> [{counter, term(), neg_integer()} | {stat, term(), #{}}].
report_stats() ->
    L0 = [ roll_stat(Stat)
            || Stat <- ets:tab2list(persistent_term:get({?MODULE, ?AGGREGATE_TABLE}))],
    L1 = [ {counter, K, C}
            || #counter_entry{key=K, data=C} <- ets:tab2list(persistent_term:get({?MODULE, ?COUNTER_TABLE}))],
    lists:sort(L0 ++ L1).

-spec roll_stat(#stat_entry{}) -> {stat, term(), #{atom() => term()}}.
roll_stat(#stat_entry{key=K, max=Max, ops=N, data=Total}) ->
    {stat, K, #{max => Max, total => Total, ops => N, avg => case N of
        0 -> 0;
        _ -> Total / N
    end}}.

init([]) ->
    Aggr = ets:new(?AGGREGATE_TABLE, [set, public, {write_concurrency, true}, {keypos, #stat_entry.key}]),
    Count = ets:new(?COUNTER_TABLE, [set, public, {write_concurrency, true}, {keypos, #counter_entry.key}]),
    ok = persistent_term:put({?MODULE, ?AGGREGATE_TABLE}, Aggr),
    ok = persistent_term:put({?MODULE, ?COUNTER_TABLE}, Count),
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
