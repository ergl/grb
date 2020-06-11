%% The read concurrency is the maximum number of concurrent
%% readers per vnode.  This is so shared memory can be used
%% in the case of keys that are read frequently.  There is
%% still only 1 writer per vnode
-define(READ_CONCURRENCY, 20).

%% Wrappers for riak_core
-type partition_id() :: chash:index_as_int().
-type index_node() :: {partition_id(), node()}.

%% Storage
-type cache_id() :: ets:tab().
-type cache(_K, _V) :: ets:tab().

%% todo(borja): temp, change
-type op() :: atom().
-type effect() :: atom().
-type transaction_type() :: red | blue.

%% todo(borja): Swap for real replica value
-type replica_id() :: non_neg_integer().
-type vclock() :: grb_vclock:vc(replica_id()).

%% Opaque types
-type key() :: term().
-type val() :: term().


%% Different ETS tables
-define(OP_LOG_TABLE, op_log_table).
