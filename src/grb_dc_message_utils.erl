-module(grb_dc_message_utils).
-include("grb.hrl").
-include("dc_messages.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% API
-export([encode_msg/3,
         decode_payload/1]).

-spec encode_msg(replica_id(), partition_id(), replica_message()) -> binary().
encode_msg(FromReplica, ToPartition, Payload) ->
    PBin = pad(?PARTITION_BYTES, binary:encode_unsigned(ToPartition)),
    {Kind, PayloadBin} = encode_payload(FromReplica, Payload),
    <<?VERSION:?VERSION_BITS, PBin/binary, Kind:?MSG_KIND_BITS, PayloadBin/binary>>.

encode_payload(Replica, #replicate_tx{tx_id=TxId, writeset=WS, commit_vc=CommitVC}) ->
    {?REPL_TX_KIND, term_to_binary({Replica, TxId, WS, CommitVC})};

encode_payload(Replica, #blue_heartbeat{timestamp=Ts}) ->
    {?BLUE_HB_KIND, term_to_binary({Replica, Ts})};

encode_payload(Replica, #update_clocks{known_vc=KnownVC, stable_vc=StableVC}) ->
    {?UPDATE_CLOCK_KIND, term_to_binary({Replica, KnownVC, StableVC})};

encode_payload(Replica, #update_clocks_heartbeat{known_vc=KnownVC, stable_vc=StableVC}) ->
    {?UPDATE_CLOCK_HEARTBEAT_KIND, term_to_binary({Replica, KnownVC, StableVC})};

encode_payload(Replica, #prepare_red{tx_id=TxId, readset=RS, writeset=WS, snapshot_vc=VC}) ->
    {?RED_PREPARE_KIND, term_to_binary({Replica, TxId, RS, WS, VC})}.

-spec decode_payload(binary()) -> {replica_id(), replica_message()}.
decode_payload(<<?REPL_TX_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {FromReplica, Tx, WS, CommitVC} = binary_to_term(Payload),
    {FromReplica, #replicate_tx{tx_id=Tx, writeset=WS, commit_vc=CommitVC}};

decode_payload(<<?BLUE_HB_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {FromReplica, Ts} = binary_to_term(Payload),
    {FromReplica, #blue_heartbeat{timestamp=Ts}};

decode_payload(<<?UPDATE_CLOCK_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {FromReplica, KnownVC, StableVC} = binary_to_term(Payload),
    {FromReplica, #update_clocks{known_vc=KnownVC, stable_vc=StableVC}};

decode_payload(<<?UPDATE_CLOCK_HEARTBEAT_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {FromReplica, KnownVC, StableVC} = binary_to_term(Payload),
    {FromReplica, #update_clocks_heartbeat{known_vc=KnownVC, stable_vc=StableVC}};

decode_payload(<<?RED_PREPARE_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {FromReplica, Tx, RS, WS, VC} = binary_to_term(Payload),
    {FromReplica, #prepare_red{tx_id=Tx, readset=RS, writeset=WS, snapshot_vc=VC}}.

%% Util functions

-spec pad(non_neg_integer(), binary()) -> binary().
pad(Width, Binary) ->
    case Width - byte_size(Binary) of
        0 -> Binary;
        N when N > 0-> <<0:(N*8), Binary/binary>>
    end.

-ifdef(TEST).
grb_dc_message_utils_test() ->
    ReplicaId = dc_id1,
    VC = #{ReplicaId => 10},
    Partitions = lists:seq(0, 10),

    Payloads = [
        #blue_heartbeat{timestamp=10},
        #update_clocks{known_vc=VC, stable_vc=VC},
        #update_clocks_heartbeat{known_vc=VC, stable_vc=VC},
        #replicate_tx{tx_id=ignore, writeset=#{foo => bar}, commit_vc=VC},
        #prepare_red{tx_id=ignore, readset=#{foo => 0}, writeset=#{foo => bar}, snapshot_vc=VC}
    ],

    lists:foreach(fun(Partition) ->
        lists:foreach(fun(Payload) ->
            Bin = encode_msg(ReplicaId, Partition, Payload),
            << ?VERSION:?VERSION_BITS,
               RcvPartition:?PARTITION_BITS/big-unsigned-integer,
               BinPayload/binary >> = Bin,

            {RcvFromReplica, RcvPayload} = decode_payload(BinPayload),

            ?assertEqual(Partition, RcvPartition),
            ?assertEqual(ReplicaId, RcvFromReplica),
            ?assertEqual(Payload, RcvPayload)
        end, Payloads)
    end, Partitions).

-endif.
