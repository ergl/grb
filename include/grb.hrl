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
-type replica_id() :: term().
-type vclock() :: grb_vclock:vc(replica_id()).

%% Opaque types
-type key() :: term().
-type val() :: term().


%% Different ETS tables
-define(OP_LOG_TABLE, op_log_table).

%% Describes the current replica, consumed by other replicas (as a whole)
-record(replica_descriptor, {
    replica_id :: replica_id(),
    num_partitions :: non_neg_integer(),
    remote_addresses :: #{partition_id() => {inet:ip_address(), inet:port_number()} }
}).

-type replica_descriptor() :: #replica_descriptor{}.

-define(INTER_DC_SOCK_OPTS, [binary,
                             {active, once},
                             {deliver, term},
                             {packet, 4}]).

-export_type([partition_id/0,
              index_node/0,
              cache_id/0,
              op/0,
              effect/0,
              transaction_type/0,
              replica_id/0,
              vclock/0,
              key/0,
              val/0,
              replica_descriptor/0]).
