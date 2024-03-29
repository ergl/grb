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

%% Red delivery / abort
-define(RED_DELIVER_KIND, 11).
-define(RED_LEARN_ABORT_KIND, 24).

%% Forward messages
-define(FWD_BLUE_HB_KIND, 12).
-define(FWD_BLUE_TX_KIND, 13).

%% Vector Replication
-define(REPL_TX_4_KIND, 14).
-define(REPL_TX_8_KIND, 15).

%% Control Plane Messages
-define(DC_PING, 16).
-define(DC_CREATE, 17).
-define(DC_GET_DESCRIPTOR, 18).
-define(DC_CONNECT_TO_DESCR, 19).
-define(DC_START_BLUE_PROCESSES, 20).
-define(DC_START_RED_FOLLOWER, 21).

%% Cure-FT Messages
-define(UPDATE_CLOCK_CURE_KIND, 22).
-define(UPDATE_CLOCK_CURE_HEARTBEAT_KIND, 23).

-record(blue_heartbeat, {
    sequence_number :: non_neg_integer(),
    timestamp :: grb_time:ts()
}).

-record(replicate_tx, {
    sequence_number :: non_neg_integer(),
    writeset :: #{},
    commit_vc :: vclock()
}).

-record(replicate_tx_4, {
    sequence_number :: non_neg_integer(),
    tx_1 :: {#{}, vclock()},
    tx_2 :: {#{}, vclock()},
    tx_3 :: {#{}, vclock()},
    tx_4 :: {#{}, vclock()}
}).

-record(replicate_tx_8, {
    sequence_number :: non_neg_integer(),
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
    sequence_number :: non_neg_integer(),
    known_vc :: vclock(),
    stable_vc :: vclock()
}).

-record(update_clocks_heartbeat, {
    sequence_number :: non_neg_integer(),
    known_vc :: vclock(),
    stable_vc :: vclock()
}).

-record(update_clocks_cure, {
    sequence_number :: non_neg_integer(),
    known_vc :: vclock()
}).

-record(update_clocks_cure_heartbeat, {
    sequence_number :: non_neg_integer(),
    known_vc :: vclock()
}).

-record(forward_heartbeat, {
    sequence_number :: non_neg_integer(),
    replica :: replica_id(),
    timestamp :: grb_time:ts()
}).

-record(forward_transaction, {
    sequence_number :: non_neg_integer(),
    replica :: replica_id(),
    writeset :: #{},
    commit_vc :: vclock()
}).

-record(red_prepare, {
    coord_location :: term(),
    tx_id :: term(),
    tx_label :: binary(),
    readset :: [term()],
    writeset :: #{},
    snapshot_vc :: vclock()
}).

-record(red_accept, {
    coord_location :: term(),
    ballot :: ballot(),
    tx_id :: term(),
    tx_label :: binary(),
    readset :: [term()],
    writeset :: #{},
    decision :: term(),
    prepare_vc :: vclock(),
    sequence_number :: non_neg_integer()
}).

-record(red_accept_ack, {
    target_node :: node(),
    ballot :: ballot(),
    tx_id :: term(),
    decision :: term(),
    prepare_ts :: non_neg_integer()
}).

-record(red_decision, {
    ballot :: ballot(),
    tx_id :: term(),
    decision :: term(),
    commit_ts :: non_neg_integer()
}).

-record(red_learn_abort, {
    ballot :: ballot(),
    tx_id :: term(),
    reason :: term(),
    commit_ts :: non_neg_integer()
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
    timestamp :: grb_time:ts(),
    sequence_number :: non_neg_integer()
}).

-record(red_heartbeat_ack, {
    ballot :: ballot(),
    heartbeat_id :: term(),
    timestamp :: grb_time:ts()
}).

-record(red_deliver, {
    ballot :: ballot(),
    sequence_number :: non_neg_integer(),
    timestamp :: grb_time:ts(),
    transactions :: [ { TxId :: term(), Label :: term() }
                    | { HB :: term(), HBId :: non_neg_integer() } ]
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
                         | #red_deliver{}
                         | #red_learn_abort{}
                         | #update_clocks_cure{}
                         | #update_clocks_cure_heartbeat{}.

-export_type([replica_message/0]).
