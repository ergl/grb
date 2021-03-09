-module(grb_dc_messages).
-include("grb.hrl").
-include("dc_messages.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% For CURE-FT
-export([clocks/1,
         clocks_heartbeat/1]).

-ifndef(STABLE_SNAPSHOT).
-ignore_xref([clocks/1, clocks_heartbeat/1]).
-endif.

-define(header(Packet, Size), (Size):(Packet)/unit:8-integer-big-unsigned).

%% Msg API
-export([ping/2,
         blue_heartbeat/1,
         clocks/2,
         clocks_heartbeat/2,
         transaction/2,
         transaction_array/4,
         transaction_array/8]).

-export([forward_heartbeat/2,
         forward_transaction/3]).

-export([red_heartbeat/3,
         red_heartbeat_ack/3]).

-export([red_prepare/6,
         red_accept/8,
         red_accept_ack/5,
         red_decision/4,
         red_already_decided/4,
         red_learn_abort/4,
         red_deliver/4]).

-export([decode_payload/1]).

-export([decode_payload/3]).

-export([frame/1]).

-spec frame(iodata()) -> iodata().
-ifndef(ENABLE_METRICS).
frame(Data) ->
    Size = erlang:iolist_size(Data),
    [<<?header(?INTER_DC_SOCK_PACKET_OPT, Size)>>, Data].
-else.
frame(<<?VERSION:?VERSION_BITS, ?DC_PING:?MSG_KIND_BITS, _Rest/binary>>=All) ->
    frame_1(All);

frame(<<?VERSION:?VERSION_BITS, Rest/binary>>) ->
    Msg = <<?VERSION:?VERSION_BITS,
            (grb_time:timestamp()):8/unit:8-integer-big-unsigned,
            Rest/binary>>,
    Size = erlang:iolist_size(Msg),
    [<<?header(?INTER_DC_SOCK_PACKET_OPT, Size)>>, Msg];

frame(All) ->
    frame_1(All).

frame_1(Data) ->
    Size = erlang:iolist_size(Data),
    [<<?header(?INTER_DC_SOCK_PACKET_OPT, Size)>>, Data].
-endif.

-spec ping(replica_id(), partition_id()) -> binary().
ping(ReplicaId, Partition) ->
    PBin = pad(?PARTITION_BYTES, binary:encode_unsigned(Partition)),
    Payload = term_to_binary(ReplicaId),
    <<?VERSION:?VERSION_BITS, ?DC_PING:?MSG_KIND_BITS, PBin/binary, Payload/binary>>.

-spec blue_heartbeat(grb_time:ts()) -> binary().
blue_heartbeat(Time) ->
    encode_msg(#blue_heartbeat{timestamp=Time}).

-spec transaction(writeset(), vclock()) -> binary().
transaction(Writeset, CommitVC) ->
    encode_msg(#replicate_tx{writeset=Writeset, commit_vc=CommitVC}).

-spec transaction_array(tx_entry(), tx_entry(),
                        tx_entry(), tx_entry()) -> binary().
transaction_array(Tx1, Tx2, Tx3, Tx4) ->
    encode_msg(#replicate_tx_4{tx_1=Tx1, tx_2=Tx2, tx_3=Tx3, tx_4=Tx4}).

-spec transaction_array(tx_entry(), tx_entry(), tx_entry(), tx_entry(),
                        tx_entry(), tx_entry(), tx_entry(), tx_entry())-> binary().
transaction_array(Tx1, Tx2, Tx3, Tx4, Tx5, Tx6, Tx7, Tx8) ->
    encode_msg(#replicate_tx_8{tx_1=Tx1, tx_2=Tx2, tx_3=Tx3, tx_4=Tx4,
                               tx_5=Tx5, tx_6=Tx6, tx_7=Tx7, tx_8=Tx8}).

-spec clocks(vclock()) -> binary().
clocks(KnownVC) ->
    encode_msg(#update_clocks_cure{known_vc=KnownVC}).

-spec clocks(vclock(), vclock()) -> binary().
clocks(KnownVC, StableVC) ->
    encode_msg(#update_clocks{known_vc=KnownVC, stable_vc=StableVC}).

-spec clocks_heartbeat(vclock()) -> binary().
clocks_heartbeat(KnownVC) ->
    encode_msg(#update_clocks_cure_heartbeat{known_vc=KnownVC}).

-spec clocks_heartbeat(vclock(), vclock()) -> binary().
clocks_heartbeat(KnownVC, StableVC) ->
    encode_msg(#update_clocks_heartbeat{known_vc=KnownVC, stable_vc=StableVC}).

-spec forward_heartbeat(replica_id(), grb_time:ts()) -> binary().
forward_heartbeat(ReplicaId, Time) ->
    encode_msg(#forward_heartbeat{replica=ReplicaId, timestamp=Time}).

-spec forward_transaction(replica_id(), writeset(), vclock()) -> binary().
forward_transaction(ReplicaId, Writeset, CommitVC) ->
    encode_msg(#forward_transaction{replica=ReplicaId, writeset=Writeset, commit_vc=CommitVC}).

-spec red_heartbeat(ballot(), term(), grb_time:ts()) -> binary().
red_heartbeat(Ballot, Id, Time) ->
    encode_msg(#red_heartbeat{ballot=Ballot, heartbeat_id=Id, timestamp=Time}).

-spec red_heartbeat_ack(ballot(), term(), grb_time:ts()) -> binary().
red_heartbeat_ack(Ballot, Id, Time) ->
    encode_msg(#red_heartbeat_ack{ballot=Ballot, heartbeat_id=Id, timestamp=Time}).

-spec red_learn_abort(ballot(), term(), term(), vclock()) -> binary().
red_learn_abort(Ballot, TxId, Reason, CommitVC) ->
    encode_msg(#red_learn_abort{ballot=Ballot, tx_id=TxId, reason=Reason, commit_vc=CommitVC}).

-spec red_deliver(_, Ballot :: ballot(),
                  Timestamp :: grb_time:ts(),
                  TransactionIds :: [ {term(), tx_label(), vclock()} | red_heartbeat_id()]) -> binary().

-ifndef(ENABLE_METRICS).
red_deliver(_, Ballot, Timestamp, TransactionIds) ->
    encode_msg(#red_deliver{ballot=Ballot, timestamp=Timestamp, transactions=TransactionIds}).
-else.
red_deliver(Partition, Ballot, Timestamp, TransactionIds) ->
    Bytes = encode_msg(#red_deliver{ballot=Ballot, timestamp=Timestamp, transactions=TransactionIds}),
    N = erlang:byte_size(Bytes),
    grb_measurements:log_stat({?MODULE, Partition, red_deliver_bin_size}, N),
    Bytes.
-endif.

-spec red_prepare(red_coord_location(), term(), tx_label(), readset(), writeset(), vclock()) -> binary().
red_prepare(Coordinator, TxId, Label, RS, WS, SnapshotVC) ->
    encode_msg(#red_prepare{coord_location=Coordinator,
                            tx_id=TxId,
                            tx_label=Label,
                            readset=RS,
                            writeset=WS,
                            snapshot_vc=SnapshotVC}).

-spec red_accept(red_coord_location(), ballot(), red_vote(), term(), tx_label(), readset(), writeset(), vclock()) -> binary().
red_accept(Coordinator, Ballot, Vote, TxId, Label, RS, WS, PrepareVC) ->
    encode_msg(#red_accept{coord_location=Coordinator, ballot=Ballot, decision=Vote,
                           tx_id=TxId, tx_label=Label, readset=RS, writeset=WS, prepare_vc=PrepareVC}).

-spec red_accept_ack(node(), ballot(), red_vote(), term(), grb_time:ts()) -> binary().
red_accept_ack(DstNode, Ballot, Vote, TxId, PrepareTS) ->
    encode_msg(#red_accept_ack{target_node=DstNode, ballot=Ballot, decision=Vote, tx_id=TxId, prepare_ts=PrepareTS}).

-spec red_decision(ballot(), red_vote(), term(), vclock()) -> binary().
red_decision(Ballot, Decision, TxId, CommitVC) ->
    encode_msg(#red_decision{ballot=Ballot, decision=Decision, tx_id=TxId, commit_vc=CommitVC}).

-spec red_already_decided(node(), red_vote(), term(), vclock()) -> binary().
red_already_decided(DstNode, Decision, TxId, CommitVC) ->
    encode_msg(#red_already_decided{target_node=DstNode, decision=Decision, tx_id=TxId, commit_vc=CommitVC}).

-spec encode_msg(replica_message()) -> binary().
encode_msg(Payload) ->
    {MsgKind, Msg} = encode_payload(Payload),
    <<?VERSION:?VERSION_BITS, MsgKind:?MSG_KIND_BITS, Msg/binary>>.

%% blue payloads
encode_payload(#blue_heartbeat{timestamp=Ts}) ->
    {?BLUE_HB_KIND, term_to_binary(Ts)};

encode_payload(#replicate_tx{writeset=WS, commit_vc=CommitVC}) ->
    {?REPL_TX_KIND, term_to_binary({WS, CommitVC})};

encode_payload(#replicate_tx_4{tx_1=Tx1, tx_2=Tx2, tx_3=Tx3, tx_4=Tx4}) ->
    {?REPL_TX_4_KIND, term_to_binary({Tx1, Tx2, Tx3, Tx4})};

encode_payload(#replicate_tx_8{tx_1=Tx1, tx_2=Tx2, tx_3=Tx3, tx_4=Tx4,
                               tx_5=Tx5, tx_6=Tx6, tx_7=Tx7, tx_8=Tx8}) ->
    {?REPL_TX_8_KIND, term_to_binary({Tx1, Tx2, Tx3, Tx4, Tx5, Tx6, Tx7, Tx8})};

encode_payload(#update_clocks{known_vc=KnownVC, stable_vc=StableVC}) ->
    {?UPDATE_CLOCK_KIND, term_to_binary({KnownVC, StableVC})};

encode_payload(#update_clocks_heartbeat{known_vc=KnownVC, stable_vc=StableVC}) ->
    {?UPDATE_CLOCK_HEARTBEAT_KIND, term_to_binary({KnownVC, StableVC})};

%% forward payloads
encode_payload(#forward_heartbeat{replica=ReplicaId, timestamp=Ts}) ->
    {?FWD_BLUE_HB_KIND, term_to_binary({ReplicaId, Ts})};

encode_payload(#forward_transaction{replica=ReplicaId, writeset=WS, commit_vc=CommitVC}) ->
    {?FWD_BLUE_TX_KIND, term_to_binary({ReplicaId, WS, CommitVC})};

%% red payloads

encode_payload(#red_prepare{coord_location=Coordinator, tx_id=TxId, tx_label=Label, readset=RS, writeset=WS, snapshot_vc=VC}) ->
    {?RED_PREPARE_KIND, term_to_binary({Coordinator, TxId, Label, RS, WS, VC})};

encode_payload(#red_accept{coord_location=Coordinator, ballot=Ballot, tx_id=TxId,
                           tx_label=Label, readset=RS, writeset=WS, decision=Vote, prepare_vc=VC}) ->
    {?RED_ACCEPT_KIND, term_to_binary({Coordinator, Ballot, TxId, Label, RS, WS, Vote, VC})};

encode_payload(#red_accept_ack{target_node=TargetNode, ballot=Ballot,
                               tx_id=TxId, decision=Vote, prepare_ts=PrepareTS}) ->
    {?RED_ACCEPT_ACK_KIND, term_to_binary({TargetNode, Ballot, TxId, Vote, PrepareTS})};

encode_payload(#red_decision{ballot=Ballot, tx_id=TxId, decision=Decision, commit_vc=CommitVC}) ->
    {?RED_DECIDE_KIND, term_to_binary({Ballot, TxId, Decision, CommitVC})};

encode_payload(#red_already_decided{target_node=TargetNode, tx_id=TxId, decision=Vote, commit_vc=CommitVC}) ->
    {?RED_ALREADY_DECIDED_KIND, term_to_binary({TargetNode, TxId, Vote, CommitVC})};

encode_payload(#red_learn_abort{ballot=B, tx_id=TxId, reason=Reason, commit_vc=CommitVC}) ->
    {?RED_LEARN_ABORT_KIND, term_to_binary({B, TxId, Reason, CommitVC})};

encode_payload(#red_deliver{ballot=Ballot, timestamp=Timestamp, transactions=TransactionIds}) ->
    {?RED_DELIVER_KIND, term_to_binary({Ballot, Timestamp, TransactionIds})};

%% red heartbeat payloads

encode_payload(#red_heartbeat{ballot=B, heartbeat_id=Id, timestamp=Ts}) ->
    {?RED_HB_KIND, term_to_binary({B, Id, Ts})};

encode_payload(#red_heartbeat_ack{ballot=B, heartbeat_id=Id, timestamp=Ts}) ->
    {?RED_HB_ACK_KIND, term_to_binary({B, Id, Ts})};

%% FT-CURE Payloads
encode_payload(#update_clocks_cure{known_vc=KnownVC}) ->
    {?UPDATE_CLOCK_CURE_KIND, term_to_binary(KnownVC)};

encode_payload(#update_clocks_cure_heartbeat{known_vc=KnownVC}) ->
    {?UPDATE_CLOCK_CURE_HEARTBEAT_KIND, term_to_binary(KnownVC)}.

-spec decode_payload(replica_id(), partition_id(), binary()) -> replica_message().
-ifndef(ENABLE_METRICS).
decode_payload(_FromReplica, _Partition, Payload) ->
    decode_payload(Payload).
-else.
decode_payload(FromReplica, Partition,
               <<SentTimestamp:8/unit:8-integer-big-unsigned,
                 MsgKind:?MSG_KIND_BITS,
                 Payload/binary>>) ->
    case kind_to_type(MsgKind) of
        ignore ->
            ok;
        Other ->
            Now = grb_time:timestamp(),
            grb_measurements:log_stat({?MODULE, Partition, FromReplica, Other}, grb_time:diff_native(Now, SentTimestamp))
    end,
    decode_payload(<<MsgKind:?MSG_KIND_BITS, Payload/binary>>);

decode_payload(_, _, Payload) ->
    decode_payload(Payload).
-endif.

decode_payload(<<?BLUE_HB_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    #blue_heartbeat{timestamp=binary_to_term(Payload)};

decode_payload(<<?REPL_TX_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {WS, CommitVC} = binary_to_term(Payload),
    #replicate_tx{writeset=WS, commit_vc=CommitVC};

decode_payload(<<?REPL_TX_4_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {Tx1, Tx2, Tx3, Tx4} = binary_to_term(Payload),
    #replicate_tx_4{tx_1=Tx1, tx_2=Tx2, tx_3=Tx3, tx_4=Tx4};

decode_payload(<<?REPL_TX_8_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {Tx1, Tx2, Tx3, Tx4, Tx5, Tx6, Tx7, Tx8} = binary_to_term(Payload),
    #replicate_tx_8{tx_1=Tx1, tx_2=Tx2, tx_3=Tx3, tx_4=Tx4,
                    tx_5=Tx5, tx_6=Tx6, tx_7=Tx7, tx_8=Tx8};

decode_payload(<<?UPDATE_CLOCK_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {KnownVC, StableVC} = binary_to_term(Payload),
    #update_clocks{known_vc=KnownVC, stable_vc=StableVC};

decode_payload(<<?UPDATE_CLOCK_HEARTBEAT_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {KnownVC, StableVC} = binary_to_term(Payload),
    #update_clocks_heartbeat{known_vc=KnownVC, stable_vc=StableVC};

decode_payload(<<?FWD_BLUE_HB_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {FromReplica, Ts} = binary_to_term(Payload),
    #forward_heartbeat{replica=FromReplica, timestamp=Ts};

decode_payload(<<?FWD_BLUE_TX_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {FromReplica, WS, CommitVC} = binary_to_term(Payload),
    #forward_transaction{replica=FromReplica, writeset=WS, commit_vc=CommitVC};

decode_payload(<<?RED_PREPARE_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {Coordinator, Tx, Label, RS, WS, VC} = binary_to_term(Payload),
    #red_prepare{coord_location=Coordinator, tx_id=Tx, tx_label=Label, readset=RS, writeset=WS, snapshot_vc=VC};

decode_payload(<<?RED_ACCEPT_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {Coordinator, Ballot, TxId, Label, RS, WS, Vote, VC} = binary_to_term(Payload),
    #red_accept{coord_location=Coordinator, ballot=Ballot, tx_id=TxId,
                tx_label=Label, readset=RS, writeset=WS, decision=Vote, prepare_vc=VC};

decode_payload(<<?RED_ACCEPT_ACK_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {TargetNode, Ballot, TxId, Vote, PrepareTS} = binary_to_term(Payload),
    #red_accept_ack{target_node=TargetNode, ballot=Ballot, tx_id=TxId, decision=Vote, prepare_ts=PrepareTS};

decode_payload(<<?RED_DECIDE_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {Ballot, TxId, Decision, CommitVC} = binary_to_term(Payload),
    #red_decision{ballot=Ballot, tx_id=TxId, decision=Decision, commit_vc=CommitVC};

decode_payload(<<?RED_ALREADY_DECIDED_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {TargetNode, TxId, Vote, CommitVC} = binary_to_term(Payload),
    #red_already_decided{target_node=TargetNode, tx_id=TxId, decision=Vote, commit_vc=CommitVC};

decode_payload(<<?RED_LEARN_ABORT_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {B, TxId, Reason, CommitVC} = binary_to_term(Payload),
    #red_learn_abort{ballot=B, tx_id=TxId, reason=Reason, commit_vc=CommitVC};

decode_payload(<<?RED_DELIVER_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {Ballot, Timestamp, TransactionIds} = binary_to_term(Payload),
    #red_deliver{ballot=Ballot, timestamp=Timestamp, transactions=TransactionIds};

decode_payload(<<?RED_HB_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {B, Id, Ts} = binary_to_term(Payload),
    #red_heartbeat{ballot=B, heartbeat_id=Id, timestamp=Ts};

decode_payload(<<?RED_HB_ACK_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    {B, Id, Ts} = binary_to_term(Payload),
    #red_heartbeat_ack{ballot=B, heartbeat_id=Id, timestamp=Ts};

decode_payload(<<?UPDATE_CLOCK_CURE_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    #update_clocks_cure{known_vc=binary_to_term(Payload)};

decode_payload(<<?UPDATE_CLOCK_CURE_HEARTBEAT_KIND:?MSG_KIND_BITS, Payload/binary>>) ->
    #update_clocks_cure_heartbeat{known_vc=binary_to_term(Payload)}.

-ifdef(ENABLE_METRICS).
-spec kind_to_type(non_neg_integer()) -> atom().
kind_to_type(?RED_ACCEPT_KIND) -> red_accept;
kind_to_type(?RED_ACCEPT_ACK_KIND) -> red_accept_ack;
kind_to_type(?RED_DECIDE_KIND) -> red_decide;
kind_to_type(?RED_DELIVER_KIND) -> red_deliver;
kind_to_type(_) -> ignore.
-endif.

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
    TargetNode = node(),
    Coordinator = {coord, ReplicaId, node()},
    VC = #{ReplicaId => 10},

    Payloads = [
        #blue_heartbeat{timestamp=10},
        #replicate_tx{writeset=#{foo => bar}, commit_vc=VC},
        #replicate_tx_4{
            tx_1 = {#{foo => bar}, VC},
            tx_2 = {#{foo => bar}, VC},
            tx_3 = {#{foo => bar}, VC},
            tx_4 = {#{foo => bar}, VC}
        },
        #replicate_tx_8{
            tx_1 = {#{foo => bar}, VC},
            tx_2 = {#{foo => bar}, VC},
            tx_3 = {#{foo => bar}, VC},
            tx_4 = {#{foo => bar}, VC},
            tx_5 = {#{foo => bar}, VC},
            tx_6 = {#{foo => bar}, VC},
            tx_7 = {#{foo => bar}, VC},
            tx_8 = {#{foo => bar}, VC}
        },

        #update_clocks{known_vc=VC, stable_vc=VC},
        #update_clocks_heartbeat{known_vc=VC, stable_vc=VC},

        #update_clocks_cure{known_vc=VC},
        #update_clocks_cure_heartbeat{known_vc=VC},

        #forward_heartbeat{replica=ReplicaId, timestamp=10},
        #forward_transaction{replica=ReplicaId, writeset=#{foo => bar}, commit_vc=VC},

        #red_prepare{
            coord_location = Coordinator,
            tx_id = ignore,
            tx_label = <<>>,
            readset = [foo],
            writeset = #{foo => bar},
            snapshot_vc = VC
        },
        #red_accept{
            coord_location = Coordinator,
            tx_id = ignore,
            tx_label = <<>>,
            readset = [foo],
            writeset = #{foo => bar},
            decision = ok,
            prepare_vc = VC
        },
        #red_accept_ack{target_node=TargetNode, ballot=0, tx_id=ignore, decision=ok, prepare_ts=0},
        #red_decision{ballot=10, tx_id=ignore, decision=ok, commit_vc=VC},
        #red_already_decided{target_node=TargetNode, tx_id=ignore, decision=ok, commit_vc=VC},
        #red_learn_abort{ballot=10, tx_id=ignore, commit_vc=VC},
        #red_deliver{ballot=10, timestamp=10, transactions=[ {?red_heartbeat_marker, 0}, {tx_0, <<"foo">>, #{}}]},

        #red_heartbeat{ballot=4, heartbeat_id={?red_heartbeat_marker, 0}, timestamp=10},
        #red_heartbeat_ack{ballot=4, heartbeat_id={?red_heartbeat_marker, 0}, timestamp=10}
    ],

    lists:foreach(fun(Msg) ->
        Bin = encode_msg(Msg),
        << ?VERSION:?VERSION_BITS, BinPayload/binary >> = Bin,
        ?assertEqual(Msg, decode_payload(BinPayload))
    end, Payloads).

-endif.
