-module(grb_version_log).
-include("grb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {
    base :: grb_crdt:t(),
    actors :: [replica_id()],
    snapshot :: {vclock(), grb_crdt:t()},
    size :: non_neg_integer(),
    max_size :: non_neg_integer(),
    operations :: [{vclock(), operation()}]
}).

-type t() :: #state{}.
-export_type([t/0]).

%% API
-export([new/3,
         insert/3,
         snapshot_lower/2]).

-ifdef(TEST).
-export([from_list/4]).
-endif.

%% fixme(borja, dialyzer): Opaqueness breaks in new/3 and more

-spec new(grb_crdt:t(), [replica_id()], non_neg_integer()) -> t().
new(Base, Actors, Size) ->
    #state{base=Base,
           actors=Actors,
           snapshot={grb_vclock:new(), Base},
           size=0,
           max_size=Size,
           operations=[]}.

-spec insert(operation(), vclock(), t()) -> t().
insert(Op, VC, S=#state{size=N, actors=Actors, operations=Operations}) ->
    maybe_gc(S#state{size=N+1, operations=insert_operation(Op, Actors, VC, Operations)}).

-spec insert_operation(operation(), [replica_id()], vclock(), [{vclock(), operation()}]) -> [{vclock(), operation()}].
insert_operation(Op, _, VC, []) -> [{VC, Op}];
insert_operation(Op, Actors, VC, L) ->
    insert_operation(Op, Actors, VC, L, []).

insert_operation(Op, _, VC, [], Acc) ->
    lists:reverse(Acc) ++ [{VC, Op}];
insert_operation(Op, Actors, InVC, [{FirstVC, _}=Entry | Rest], Acc) ->
    case grb_vclock:leq_at_keys(Actors, InVC, FirstVC) of
        true ->
            lists:reverse(Acc) ++ [{InVC, Op}, Entry | Rest];
        false ->
            insert_operation(Op, Actors, InVC, Rest, [Entry | Acc])
    end.

-spec maybe_gc(t()) -> t().
maybe_gc(S=#state{size=N, max_size=M})
    when N =< M ->
        S;

maybe_gc(S=#state{snapshot=BaseSnapshot, actors=Actors, operations=Ops}) ->
    S#state{size=0, snapshot=compress(Ops, Actors, BaseSnapshot), operations=[]}.

-spec compress([{vclock(), operation()}], [replica_id()], {vclock(), grb_crdt:t()}) -> {vclock(), grb_crdt:t()}.
compress([], _, Acc) ->
    Acc;
compress([{VC, Op} | Rest], Actors, {SnapshotVC, Acc}) ->
    compress(Rest, Actors,
             {grb_vclock:max_at_keys(Actors, SnapshotVC, VC), grb_crdt:apply_op(Op, Actors, VC, Acc)}).

-spec snapshot_lower(vclock(), t()) -> grb_crdt:t() | {not_found, grb_crdt:t()}.
snapshot_lower(VC, S=#state{actors=Actors, snapshot={SnapshotVC, Snapshot}, operations=Ops}) ->
    case grb_vclock:lt_at_keys(Actors, VC, SnapshotVC) of
        true ->
            %% everything in the log is too fresh for this VC, return the base version
            {not_found, S#state.base};
        false ->
            snapshot_lower(VC, Actors, Snapshot, Ops)
    end.

-spec snapshot_lower(vclock(), [replica_id()], grb_crdt:t(), [{vclock(), grb_crdt:op()}]) -> grb_crdt:t().
snapshot_lower(_, _, Acc, []) ->
    Acc;

snapshot_lower(VC, Actors, Acc, [{OpVC, Op} | Rest]) ->
    case grb_vclock:leq_at_keys(Actors, OpVC, VC) of
        true ->
            snapshot_lower(VC, Actors, grb_crdt:apply_op(Op, Actors, OpVC, Acc), Rest);
        false ->
            %% ops are sorted from small to big, so if we are above, we can simply return now
            Acc
    end.

-ifdef(TEST).

-spec from_list(grb_crdt:t(), [replica_id()], non_neg_integer(), [{vclock(), operation()}]) -> t().
from_list(Base, Actors, Size, Ops) ->
    from_list_(Ops, new(Base, Actors, Size)).

from_list_([], S) -> S;
from_list_([{VC, Op} | Rest], S) -> from_list_(Rest, insert(Op, VC, S)).

grb_version_log_snapshot_lower_lww_test() ->
    DC1 = replica_1, DC2 = replica_2, DC3 = replica_3,
    Actors = [DC1, DC2, DC3],
    VC = fun maps:from_list/1,

    Base = grb_crdt:new(grb_lww),
    BaseVal = grb_crdt:value(Base),
    Ops = [ { VC([{DC1, X}]), grb_crdt:make_op(grb_lww, X) }
          || X <- lists:seq(1, 9) ],

    Log = from_list(Base, Actors, 10, Ops),

    %% If there's no suitable snapshot, return the base value
    ?assertEqual(BaseVal, grb_crdt:value(grb_version_log:snapshot_lower(VC([{DC1, 0}]), Log))),

    %% Max entry
    ?assertEqual(9, grb_crdt:value(grb_version_log:snapshot_lower(VC([{DC1, 10}]), Log))),

    %% Each snapshot equals each vector
    lists:foreach(fun(X) ->
        ?assertEqual(X, grb_crdt:value(grb_version_log:snapshot_lower(VC([{DC1, X}]), Log)))
    end, lists:seq(1, 9)).

grb_version_log_snapshot_lower_gset_test() ->
    DC1 = replica_1, DC2 = replica_2, DC3 = replica_3,
    Actors = [DC1, DC2, DC3],
    VC = fun maps:from_list/1,

    Base = grb_crdt:new(grb_gset),
    BaseVal = grb_crdt:value(Base),
    Ops = [ { VC([{DC1, X}]), grb_crdt:make_op(grb_gset, X) }
          || X <- lists:seq(1, 9) ],

    Log = from_list(Base, Actors, 10, Ops),

    %% If there's no suitable snapshot, return the base value
    ?assertEqual(BaseVal, grb_crdt:value(grb_version_log:snapshot_lower(VC([{DC1, 0}]), Log))),

    %% Max entry is union of all
    ?assertEqual(lists:seq(1,9),
                 lists:sort(maps:keys(grb_crdt:value(grb_version_log:snapshot_lower(VC([{DC1, 10}]), Log))))),

    %% Each snapshot contains one extra element each vector
    lists:foreach(fun(X) ->
        ?assertEqual(lists:seq(1, X),
                     lists:sort(maps:keys(grb_crdt:value(grb_version_log:snapshot_lower(VC([{DC1, X}]), Log)))))
    end, lists:seq(1, 9)).

grb_version_log_insert_gc_test() ->
    DC1 = replica_1, DC2 = replica_2, DC3 = replica_3,
    Actors = [DC1, DC2, DC3],
    VC = fun maps:from_list/1,

    Base = grb_crdt:new(grb_gset),
    BaseVal = grb_crdt:value(Base),
    Ops = [ { VC([{DC1, X}]), grb_crdt:make_op(grb_gset, X) }
        || X <- lists:seq(1, 10) ],

    Log = from_list(Base, Actors, 10, Ops),
    ?assertEqual(BaseVal, grb_crdt:value(grb_version_log:snapshot_lower(VC([{DC1, 0}]), Log))),
    ?assertEqual(lists:seq(1, 5),
                 lists:sort(maps:keys(grb_crdt:value(grb_version_log:snapshot_lower(VC([{DC1, 5}]), Log))))),

    Ops2 = [ { VC([{DC1, X}]), grb_crdt:make_op(grb_gset, X) }
            || X <- lists:seq(11, 20) ],
    Log2 = lists:foldl(fun({T, Op}, R) -> grb_version_log:insert(Op, T, R) end, Log, Ops2),

    %% Log has been GC'd, if we ask anything below the snapshot, it should return the base version
    ?assertMatch({not_found, _}, grb_version_log:snapshot_lower(VC([{DC1, 0}]), Log2)),
    ?assertMatch({not_found, _}, grb_version_log:snapshot_lower(VC([{DC1, 5}]), Log2)),

    %% Anything above the snapshot works as usual, and takes into account older operations
    ?assertEqual(lists:seq(1,20),
                 lists:sort(maps:keys(grb_crdt:value(grb_version_log:snapshot_lower(VC([{DC1, 20}]), Log2))))).

-endif.
