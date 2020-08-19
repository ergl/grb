#!/usr/bin/env escript
%%! -smp enable -hidden -name test_clock_advance@127.0.0.1 -setcookie grb_cookie

-define(RETRIES, 100).
-define(SLEEP_TIME, 100).

-mode(compile).
-export([main/1]).

main([RemoteNodeStr]) ->
  Node = list_to_atom(RemoteNodeStr),
  Partitions = erpc:call(Node, grb_dc_utils, my_partitions, []),
  Replicas = erpc:call(Node, grb_dc_manager, all_replicas, []),
  ok = all_replica_test(Node, Partitions, Replicas),
  ok = clock_advance_test(Node, Partitions, Replicas);

main(_) -> halt(1).

all_replica_test(Node, Partitions, Replicas) ->
  ok = all_replicas(Node, Partitions, Replicas, known_vc),
  ok = all_replicas(Node, Partitions, Replicas, stable_vc),
  ok = all_replicas(Node, Partitions, Replicas, uniform_vc).

all_replicas(Node, Partitions, Replicas, ClockName) ->
  io:format("~p knows all replicas: ", [ClockName]),
  ok = lists:foreach(fun(P) ->
    true = test_knows_replicas(Node, P, Replicas, ClockName)
  end, Partitions),
  io:format("ok~n").

clock_advance_test(Node, Partitions, Replicas) ->
  ok = clock_advance(Node, Partitions, Replicas, known_vc),
  ok = clock_advance(Node, Partitions, Replicas, stable_vc),
  ok = clock_advance(Node, Partitions, Replicas, uniform_vc).

clock_advance(Node, Partitions, Replicas, ClockName) ->
  io:format("~p advances at all replicas: ", [ClockName]),
  ok = lists:foreach(fun(P) ->
    true = test_advances(Node, P, Replicas, ClockName)
  end, Partitions),
  io:format("ok~n").

test_knows_replicas(Node, P, Replicas, ClockName) ->
  Clock = erpc:call(Node, grb_propagation_vnode, ClockName, [P]),
  lists:all(fun(R) ->
    case maps:is_key(R, Clock) of
      true -> true;
      false ->
        io:fwrite(standard_error, "Partiton ~p does not know all replicas~n", [P]),
        false
    end
  end, Replicas).

test_advances(Node, P, Replicas, Clock) ->
  test_advances(Node, P, Replicas, Clock, ?RETRIES, erpc:call(Node, grb_propagation_vnode, Clock, [P])).

test_advances(_Node, P, _Replicas, _Clock, 0, _Original) ->
  io:format("\t~p gave up from too many retries", [P]),
  false;

test_advances(Node, P, Replicas, Clock, Retries, Original) ->
  timer:sleep(?SLEEP_TIME),
  New = erpc:call(Node, grb_propagation_vnode, Clock, [P]),
  true = lists:all(fun(R) ->
    Before = maps:get(R, Original),
    After = maps:get(R, New),
    case Before < After of
      true -> true;
      false ->
        io:format("\t~p undetermined, retrying (~b/~b)...~n", [P, Retries, ?RETRIES]),
        test_advances(Node, P, Replicas, Clock, Retries - 1, Original)
    end
  end, Replicas).
