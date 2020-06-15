-module(grb_tcp_handler).

%% API
-export([process/3]).

-spec process(grb_promise:t(), atom(), #{}) -> ok.
process(Promise, 'UniformBarrier', _) ->
    %% todo(borja): implement this when replication is done
    grb_promise:resolve(ok, Promise);

process(Promise, 'ConnectRequest', _) ->
    grb_promise:resolve(grb:connect(), Promise);

process(Promise, 'StartReq', #{client_vc := CVC}) ->
    grb_promise:resolve(grb:start_transaction(CVC), Promise);

process(Promise, 'OpRequest', Args) ->
    #{partition := P, key := K, value := V, snapshot_vc := VC} = Args,
    grb:perform_op(Promise, P, K, VC, V);

%% TODO(borja): Make prepare node parallel
%% See https://medium.com/@jlouis666/testing-a-parallel-map-implementation-2d9eab47094e
process(Promise, 'PrepareBlueNode', Args) ->
    #{transaction_id := TxId, snapshot_vc := VC, prepares := Prepares} = Args,
    Votes = [{ok, P, grb:prepare_blue(P, TxId, WS, VC)}
             || #{partition := P, writeset := WS} <- Prepares],
    grb_promise:resolve(Votes, Promise);

%% TODO(borja): Make decide node parallel
%% See https://medium.com/@jlouis666/testing-a-parallel-map-implementation-2d9eab47094e
process(_Promise, 'DecideBlueNode', Args) ->
    #{transaction_id := TxId, partitions := Ps, commit_vc := CVC} = Args,
    _ = [grb:decide_blue(P, TxId, CVC) || P <- Ps],
    ok.
