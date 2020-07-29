-module(grb_tcp_handler).

%% API
-export([process/3]).

-spec process(grb_promise:t(), atom(), #{}) -> ok.
process(Promise, 'Load', #{bin_size := Size}) ->
    grb_promise:resolve(grb:load(Size), Promise);

process(Promise, 'UniformBarrier', #{client_vc := CVC, partition := Partition}) ->
    grb:uniform_barrier(Promise, Partition, CVC);

process(Promise, 'ConnectRequest', _) ->
    grb_promise:resolve(grb:connect(), Promise);

process(Promise, 'StartReq', #{client_vc := CVC, partition := Partition}) ->
    grb_promise:resolve(grb:start_transaction(Partition, CVC), Promise);

process(Promise, 'OpRequest', Args) ->
    #{partition := P, key := K, value := V, snapshot_vc := VC} = Args,
    grb:perform_op(Promise, P, K, VC, V);

process(Promise, 'PrepareBlueNode', Args) ->
    #{transaction_id := TxId, snapshot_vc := VC, prepares := Prepares} = Args,
    Votes = [ {ok, P, grb:prepare_blue(P, TxId, WS, VC)} || #{partition := P, writeset := WS} <- Prepares],
    grb_promise:resolve(Votes, Promise);

process(_Promise, 'DecideBlueNode', Args) ->
    #{transaction_id := TxId, partitions := Ps, commit_vc := CVC} = Args,
    _ = [grb:decide_blue(P, TxId, CVC) || P <- Ps],
    ok.
