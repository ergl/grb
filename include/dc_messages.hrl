-define(VERSION, 0).
-define(VERSION_BYTES, 1).
-define(VERSION_BITS, (?VERSION_BYTES * 8)).
%% Riak partitions are 160-bit ints
-define(PARTITION_BYTES, 20).
-define(PARTITION_BITS, (?PARTITION_BYTES * 8)).

-record(update_clocks, {
    known_vc :: vclock(),
    stable_vc :: vclock()
}).

-record(blue_heartbeat, {
    timestamp :: grb_time:ts()
}).

-record(replicate_tx, {
    tx_id :: term(),
    writeset :: #{},
    commit_vc :: vclock()
}).

-type replica_message() :: #update_clocks{} | #blue_heartbeat{} | #replicate_tx{}.

-record(inter_dc_message, {
    source_id :: replica_id(),
    payload :: replica_message()
}).

-export_type([replica_message/0]).
