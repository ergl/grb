-define(OPLOG_READER_NUM, 20).
%% Defined to match the number of locks for set ETS when write_concurrency is enabled
-define(OPLOG_WRITER_NUM, 64).
%% Number of write coordinator per partition, used to write the keys
-define(OPLOG_COORDS_NUM, 8).

%% Wrappers for riak_core
-type partition_id() :: chash:index_as_int().
-type index_node() :: {partition_id(), node()}.

%% Storage
-type cache_id() :: ets:tab().
-type cache(_K, _V) :: ets:tab().

%% todo(borja, crdt): change operation type when adding crdt lib
-type op() :: atom().
-type effect() :: atom().
-type transaction_type() :: red | blue.

-type replica_id() :: {atom(), erlang:timestamp()}.
-type vclock() :: grb_vclock:vc(replica_id() | atom()).
%% the entry for red transactions in the clock
-define(RED_REPLICA, red).

%% Opaque types
-type key() :: binary().
-type val() :: binary().

%% todo(borja, crdt): change operation type when adding crdt lib
-type readset() :: [key()].
-type writeset() :: #{key() => val()}.
-type tx_entry() :: grb_blue_commit_log:entry().

%% Describes the current replica, consumed by other replicas (as a whole)
-record(replica_descriptor, {
    replica_id :: replica_id(),
    num_partitions :: non_neg_integer(),
    remote_addresses :: #{partition_id() => {inet:ip_address(), inet:port_number()}}
}).

-type replica_descriptor() :: #replica_descriptor{}.

-define(INTER_DC_SOCK_OPTS, [binary,
                             {active, once},
                             {deliver, term},
                             {packet, 4},
                             {nodelay, true}]).

-type inter_dc_conn() :: atom().

-type red_coordinator() :: pid().
-type red_vote() :: ok | {abort, atom()}.

-type ballot() :: non_neg_integer().

-type red_coord_location() :: {coord, replica_id(), node()}.

%% The location of a red leader
-type leader_location() :: {local, index_node()}
                         | {remote, replica_id()}
                         | {proxy, node(), replica_id()}.

-export_type([partition_id/0,
              index_node/0,
              cache_id/0,
              cache/2,
              op/0,
              effect/0,
              readset/0,
              writeset/0,
              tx_entry/0,
              transaction_type/0,
              replica_id/0,
              vclock/0,
              key/0,
              val/0,
              replica_descriptor/0,
              inter_dc_conn/0,
              red_coordinator/0,
              red_vote/0,
              ballot/0,
              leader_location/0,
              red_coord_location/0]).
