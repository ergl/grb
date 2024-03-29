-define(OPLOG_READER_NUM, 20).

%% For most purposes, we don't care if the cancelled timer is in the past, or ensure
%% that the timer is really cancelled before proceeding.
-define(CANCEL_TIMER_FAST(__TRef), erlang:cancel_timer(__TRef, [{async, true}, {info, false}])).

%% Wrappers for riak_core
-type partition_id() :: chash:index_as_int().
-type index_node() :: {partition_id(), node()}.

%% Storage
-type cache_id() :: ets:tab().
-type cache(_K, _V) :: ets:tab().

-type transaction_type() :: red | blue.

-type replica_id() :: {term(), erlang:timestamp()}.
-type all_replica_id() :: red | replica_id().
-type vclock() :: grb_vclock:vc(all_replica_id()).
%% the entry for red transactions in the clock
-define(RED_REPLICA, red).

%% Opaque types
-type key() :: term().
-type snapshot() :: term().
-type crdt() :: grb_crdt:crdt().
-type operation() :: grb_crdt:operation().

-type tx_label() :: binary().
-type conflict_relations() :: #{tx_label() := tx_label()}.

-type readset() :: [key()].
-type writeset() :: #{key() => operation()}.
-type tx_entry() :: grb_blue_commit_log:entry().

%% Describes the current replica, consumed by other replicas (as a whole)
-record(replica_descriptor, {
    replica_id :: replica_id(),
    num_partitions :: non_neg_integer(),
    remote_addresses :: #{partition_id() => {inet:ip_address(), inet:port_number()}}
}).

-type replica_descriptor() :: #replica_descriptor{}.

-define(INTER_DC_SOCK_PACKET_OPT, 4).
-define(INTER_DC_SOCK_OPTS, [binary,
                             {active, once},
                             {deliver, term},
                             {packet, ?INTER_DC_SOCK_PACKET_OPT},
                             {nodelay, true}]).

-type red_coordinator() :: pid().
-type red_vote() :: ok | {abort, atom()}.

-type ballot() :: non_neg_integer().

-type red_coord_location() :: {coord, replica_id(), node()}.

%% The location of a red leader
-type leader_location() :: {local, index_node()}
                         | {remote, replica_id()}
                         | {proxy, node(), replica_id()}.

-define(red_heartbeat_marker, heartbeat).
-type red_heartbeat_id() :: {?red_heartbeat_marker, non_neg_integer()}.

-ifdef(SENDER_SOCKET_BACKEND).
-define(SENDER_MODULE, grb_dc_connection_sender_socket).
-else.
-define(SENDER_MODULE, grb_dc_connection_sender_driver).
-endif.

-export_type([partition_id/0,
              index_node/0,
              cache_id/0,
              cache/2,
              readset/0,
              writeset/0,
              tx_entry/0,
              transaction_type/0,
              replica_id/0,
              all_replica_id/0,
              vclock/0,
              key/0,
              crdt/0,
              snapshot/0,
              operation/0,
              tx_label/0,
              conflict_relations/0,
              replica_descriptor/0,
              red_coordinator/0,
              red_vote/0,
              ballot/0,
              leader_location/0,
              red_coord_location/0,
              red_heartbeat_id/0]).
