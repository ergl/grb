-define(VERSION, 0).
-define(VERSION_BYTES, 1).
-define(VERSION_BITS, (?VERSION_BYTES * 8)).

%% Riak partitions are 160-bit ints
-define(PARTITION_BYTES, 20).
-define(PARTITION_BITS, (?PARTITION_BYTES * 8)).

%% Serialize messages as ints instead of records
-define(MSG_KIND_BITS, 8).
-define(REPL_TX_KIND, 0).
-define(BLUE_HB_KIND, 1).
-define(UPDATE_CLOCK_KIND, 2).
-define(UPDATE_CLOCK_HEARTBEAT_KIND, 3).

%% Red transactions
-define(RED_PREPARE_KIND, 4).
-define(RED_ACCEPT_KIND, 5).
-define(RED_ACCEPT_ACK_KIND, 6).
-define(RED_DECIDE_KIND, 7).
-define(RED_ALREADY_DECIDED_KIND, 8).

%% Red heartbeats
-define(RED_HB_KIND, 9).
-define(RED_HB_ACK_KIND, 10).
-define(RED_HB_DECIDE_KIND, 11).

-record(replicate_tx, {
    tx_id :: term(),
    writeset :: #{},
    commit_vc :: vclock()
}).

-record(blue_heartbeat, {
    timestamp :: grb_time:ts()
}).

-record(update_clocks, {
    known_vc :: vclock(),
    stable_vc :: vclock()
}).

-record(update_clocks_heartbeat, {
    known_vc :: vclock(),
    stable_vc :: vclock()
}).

-record(red_prepare, {
    tx_id :: term(),
    readset :: #{},
    writeset :: #{},
    snapshot_vc :: vclock()
}).

-record(red_accept, {
    ballot :: ballot(),
    tx_id :: term(),
    readset :: #{},
    writeset :: #{},
    decision :: term(),
    prepare_vc :: vclock()
}).

-record(red_accept_ack, {
    ballot :: ballot(),
    tx_id :: term(),
    decision :: term(),
    prepare_vc :: vclock()
}).

-record(red_decision, {
    ballot :: ballot(),
    tx_id :: term(),
    decision :: term(),
    commit_vc :: vclock()
}).

-record(red_already_decided, {
    tx_id :: term(),
    decision :: term(),
    commit_vc :: vclock()
}).

-record(red_heartbeat, {
    ballot :: ballot(),
    heartbeat_id :: term(),
    timestamp :: grb_time:ts()
}).

-record(red_heartbeat_ack, {
    ballot :: ballot(),
    heartbeat_id :: term(),
    timestamp :: grb_time:ts()
}).

-record(red_heartbeat_decide, {
    ballot :: ballot(),
    heartbeat_id :: term(),
    timestamp :: grb_time:ts()
}).

-type replica_message() :: #replicate_tx{}
                         | #blue_heartbeat{}
                         | #update_clocks{}
                         | #update_clocks_heartbeat{}
                         | #red_prepare{}
                         | #red_accept{}
                         | #red_accept_ack{}
                         | #red_decision{}
                         | #red_already_decided{}
                         | #red_heartbeat{}
                         | #red_heartbeat_ack{}
                         | #red_heartbeat_decide{}.

-export_type([replica_message/0]).
