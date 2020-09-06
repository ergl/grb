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
-define(RED_PREPARE_KIND, 4).

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

-record(prepare_red, {
    tx_id :: term(),
    readset :: #{},
    writeset :: #{},
    snapshot_vc :: vclock()
}).

-type replica_message() :: #replicate_tx{}
                         | #blue_heartbeat{}
                         | #update_clocks{}
                         | #update_clocks_heartbeat{}
                         | #prepare_red{}.

-export_type([replica_message/0]).
