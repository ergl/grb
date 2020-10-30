-define(VERSION, 0).
-define(VERSION_BYTES, 1).
-define(VERSION_BITS, (?VERSION_BYTES * 8)).

%% Riak partitions are 160-bit ints
-define(PARTITION_BYTES, 20).
-define(PARTITION_BITS, (?PARTITION_BYTES * 8)).

%% Serialize messages as ints instead of records
-define(MSG_KIND_BITS, 8).
-define(BLUE_HB_KIND, 0).
-define(REPL_TX_KIND, 1).
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

%% Forward messages
-define(FWD_BLUE_HB_KIND, 12).
-define(FWD_BLUE_TX_KIND, 13).

%% Ping
-define(DC_PING, 14).

-define(REPL_TX_4_KIND, 15).
-define(REPL_TX_8_KIND, 16).

-record(blue_heartbeat, {
    timestamp :: grb_time:ts()
}).

-record(replicate_tx, {
    writeset :: #{},
    commit_vc :: vclock()
}).

-record(replicate_tx_4, {
    tx_1 :: {#{}, vclock()},
    tx_2 :: {#{}, vclock()},
    tx_3 :: {#{}, vclock()},
    tx_4 :: {#{}, vclock()}
}).

-record(replicate_tx_8, {
    tx_1 :: {#{}, vclock()},
    tx_2 :: {#{}, vclock()},
    tx_3 :: {#{}, vclock()},
    tx_4 :: {#{}, vclock()},
    tx_5 :: {#{}, vclock()},
    tx_6 :: {#{}, vclock()},
    tx_7 :: {#{}, vclock()},
    tx_8 :: {#{}, vclock()}
}).

-record(update_clocks, {
    known_vc :: vclock(),
    stable_vc :: vclock()
}).

-record(update_clocks_heartbeat, {
    known_vc :: vclock(),
    stable_vc :: vclock()
}).

-record(forward_heartbeat, {
    replica :: replica_id(),
    timestamp :: grb_time:ts()
}).

-record(forward_transaction, {
    replica :: replica_id(),
    writeset :: #{},
    commit_vc :: vclock()
}).

-record(red_prepare, {
    coord_location :: term(),
    tx_id :: term(),
    readset :: [term()],
    writeset :: #{},
    snapshot_vc :: vclock()
}).

-record(red_accept, {
    coord_location :: term(),
    ballot :: ballot(),
    tx_id :: term(),
    readset :: [term()],
    writeset :: #{},
    decision :: term(),
    prepare_vc :: vclock()
}).

-record(red_accept_ack, {
    target_node :: node(),
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
    target_node :: node(),
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

-type replica_message() :: #blue_heartbeat{}
                         | #replicate_tx{}
                         | #replicate_tx_4{}
                         | #replicate_tx_8{}
                         | #update_clocks{}
                         | #update_clocks_heartbeat{}
                         | #forward_heartbeat{}
                         | #forward_transaction{}
                         | #red_prepare{}
                         | #red_accept{}
                         | #red_accept_ack{}
                         | #red_decision{}
                         | #red_already_decided{}
                         | #red_heartbeat{}
                         | #red_heartbeat_ack{}
                         | #red_heartbeat_decide{}.

-export_type([replica_message/0]).
