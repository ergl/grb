-module(grb_version_log).
-include("grb.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(lww, grb_lww).
-define(gset, grb_gset).

-record(state, {
    type :: crdt(),
    base :: grb_crdt:t(),
    actors :: [all_replica_id()],
    snapshot :: {vclock(), grb_crdt:t()},
    size :: non_neg_integer(),
    max_size :: non_neg_integer(),
    operations :: [{vclock(), operation()}]
}).

-type t() :: #state{}.
-export_type([t/0]).

%% API
-export([new/4,
         insert/3,
         snapshot_lower/2]).

-ifdef(TEST).
-export([from_list/5]).
-endif.

-spec new(crdt(), grb_crdt:t(), [all_replica_id()], non_neg_integer()) -> t().
%% For some reason, dialyzer things that matching snapshot={_, _} breaks the opaqueness of the clock
-dialyzer({nowarn_function, new/4}).
new(Type, Base, Actors, Size) ->
    #state{type=Type,
           base=Base,
           actors=Actors,
           snapshot={grb_vclock:new(), Base},
           size=0,
           max_size=Size,
           operations=[]}.

-spec insert(operation(), vclock(), t()) -> t().
insert(Op, VC, S=#state{size=N, type=?lww, actors=Actors, operations=Operations}) ->
    maybe_gc_new_first(S#state{size=N+1, operations=insert_new_first(Op, Actors, VC, Operations)});

insert(Op, VC, S=#state{size=N, type=?gset, actors=Actors, operations=Operations}) ->
    maybe_gc_old_first(S#state{size=N+1, operations=insert_old_first(Op, Actors, VC, Operations)}).

-spec insert_old_first(Op :: operation(),
                       Actors :: [all_replica_id()],
                       CommitVC :: vclock(),
                       Operations0 :: [{vclock(), operation()}]) -> Operations :: [{vclock(), operation()}].

insert_old_first(Op, _, VC, []) -> [{VC, Op}];
insert_old_first(Op, Actors, VC, L) ->
    insert_old_first(Op, Actors, VC, L, []).

-spec insert_old_first(Op :: operation(),
                       Actors :: [all_replica_id()],
                       CommitVC :: vclock(),
                       Operations0 :: [{vclock(), operation()}],
                       Acc :: [{vclock(), operation()}]) -> Operations :: [{vclock(), operation()}].

insert_old_first(Op, _, VC, [], Acc) ->
    lists:reverse(Acc) ++ [{VC, Op}];
insert_old_first(Op, Actors, InVC, [{FirstVC, _}=Entry | Rest], Acc) ->
    case grb_vclock:leq_at_keys(Actors, InVC, FirstVC) of
        true ->
            lists:reverse(Acc) ++ [{InVC, Op}, Entry | Rest];
        false ->
            insert_old_first(Op, Actors, InVC, Rest, [Entry | Acc])
    end.

-spec insert_new_first(Op :: operation(),
                       Actors :: [all_replica_id()],
                       CommitVC :: vclock(),
                       Operations0 :: [{vclock(), operation()}]) -> Operations :: [{vclock(), operation()}].

insert_new_first(Op, _, VC, []) -> [{VC, Op}];
insert_new_first(Op, Actors, VC, L) ->
    insert_new_first(Op, Actors, VC, L, []).

-spec insert_new_first(Op :: operation(),
                       Actors :: [all_replica_id()],
                       CommitVC :: vclock(),
                       Operations0 :: [{vclock(), operation()}],
                       Acc :: [{vclock(), operation()}]) -> Operations :: [{vclock(), operation()}].

insert_new_first(Op, _, VC, [], Acc) ->
    lists:reverse(Acc) ++ [{VC, Op}];
insert_new_first(Op, Actors,InVC, [{LastVC, _}=Entry | Rest], Acc) ->
    case grb_vclock:leq_at_keys(Actors, LastVC, InVC) of
        true ->
            lists:reverse(Acc) ++ [{InVC, Op}, Entry | Rest];
        false ->
            insert_new_first(Op, Actors, InVC, Rest, [Entry | Acc])
    end.

-spec maybe_gc_old_first(t()) -> t().
maybe_gc_old_first(S=#state{size=N, max_size=M})
    when N =< M ->
        S;

maybe_gc_old_first(S=#state{snapshot=BaseSnapshot, actors=Actors, operations=Ops}) ->
    S#state{size=0, snapshot=compress(Ops, Actors, BaseSnapshot), operations=[]}.

-spec compress([{vclock(), operation()}], [all_replica_id()], {vclock(), grb_crdt:t()}) -> {vclock(), grb_crdt:t()}.
-dialyzer({[no_opaque, no_return, no_match], [compress/3, maybe_gc_new_first/1]}).
compress([], _, Acc) ->
    Acc;
compress([{VC, Op} | Rest], Actors, {SnapshotVC, Acc}) ->
    compress(Rest, Actors,
             {grb_vclock:max_at_keys(Actors, SnapshotVC, VC), grb_crdt:apply_op(Op, Actors, VC, Acc)}).

-spec maybe_gc_new_first(t()) -> t().
maybe_gc_new_first(S=#state{size=N, max_size=M})
    when N =< M ->
        S;

maybe_gc_new_first(S=#state{snapshot={_, Snapshot}, actors=Actors, operations=[{VC, Op} | _]}) ->
    S#state{size=0, snapshot={VC, grb_crdt:apply_op(Op, Actors, VC, Snapshot)}, operations=[]}.

-spec snapshot_lower(vclock(), t()) -> {ok, grb_crdt:t()} | {not_found, grb_crdt:t()}.
snapshot_lower(VC, S=#state{type=Type, actors=Actors, snapshot={SnapshotVC, Snapshot}, operations=Ops}) ->
    case grb_vclock:lt_at_keys(Actors, VC, SnapshotVC) of
        true ->
            %% everything in the log is too fresh for this VC, return the base version
            {not_found, S#state.base};
        false ->
            snapshot_lower(Type, VC, Actors, Snapshot, Ops)
    end.

-spec snapshot_lower(Type :: crdt(),
                     VC :: vclock(),
                     Actors :: [all_replica_id()],
                     Snapshot :: grb_crdt:t(),
                     Ops :: [{vclock(), operation()}]) -> {ok, grb_crdt:t()} | {not_found, grb_crdt:t()}.

-dialyzer({no_opaque, snapshot_lower/5}).
snapshot_lower(?lww, VC, Actors, Snapshot, Ops) ->
     case find_first_lower(Ops, Actors, VC) of
         not_found ->
             {not_found, Snapshot};
         {OpVC, Op} ->
             {ok, grb_crdt:apply_op(Op, Actors, OpVC, Snapshot)}
     end;

snapshot_lower(?gset, VC, Actors, Snapshot, Ops) ->
    {ok, snapshot_lower_old_first(VC, Actors, Snapshot, Ops)}.

-spec find_first_lower(Ops :: [{vclock(), operation()}],
                       Actors :: [replica_id()],
                       VC :: vclock()) -> {vclock(), operation()} | not_found.
find_first_lower([], _, _) ->
    not_found;

find_first_lower([{OpVC, _}=Entry | Rest], Actors, VC) ->
    case grb_vclock:leq_at_keys(Actors, OpVC, VC) of
        true ->
            %% ops are sorted from big to small, so if we are below, we can simply return now
            Entry;
        false ->
            find_first_lower(Rest, Actors, VC)
    end.

-spec snapshot_lower_old_first(VC :: vclock(),
                               Actors :: [all_replica_id()],
                               Acc :: grb_crdt:t(),
                               Ops :: [{vclock(), operation()}]) -> grb_crdt:t().

-dialyzer({no_opaque, snapshot_lower_old_first/4}).
snapshot_lower_old_first(_, _, Acc, []) ->
    Acc;

snapshot_lower_old_first(VC, Actors, Acc, [{OpVC, Op} | Rest]) ->
    case grb_vclock:leq_at_keys(Actors, OpVC, VC) of
        true ->
            snapshot_lower_old_first(VC, Actors, grb_crdt:apply_op(Op, Actors, OpVC, Acc), Rest);
        false ->
            %% ops are sorted from small to big, so if we are above, we can simply return now
            Acc
    end.

-ifdef(TEST).

-spec from_list(crdt(), grb_crdt:t(), [all_replica_id()], non_neg_integer(), [{vclock(), operation()}]) -> t().
from_list(Type, Base, Actors, Size, Ops) ->
    from_list_(Ops, new(Type, Base, Actors, Size)).

from_list_([], S) -> S;
from_list_([{VC, Op} | Rest], S) -> from_list_(Rest, insert(Op, VC, S)).

grb_version_log_snapshot_lower_lww_test() ->
    DC1 = replica_1, DC2 = replica_2, DC3 = replica_3,
    Actors = [DC1, DC2, DC3],
    VC = fun maps:from_list/1,

    Type = grb_lww,
    Base = grb_crdt:new(Type),
    BaseVal = grb_crdt:value(Base),
    Ops = [ { VC([{DC1, X}]), grb_crdt:make_op(Type, X) }
          || X <- lists:seq(1, 9) ],

    Log = from_list(Type, Base, Actors, 10, Ops),

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

    Type = grb_gset,
    Base = grb_crdt:new(Type),
    BaseVal = grb_crdt:value(Base),
    Ops = [ { VC([{DC1, X}]), grb_crdt:make_op(Type, X) }
          || X <- lists:seq(1, 9) ],

    Log = from_list(Type, Base, Actors, 10, Ops),

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

grb_version_log_insert_gc_lww_test() ->
    DC1 = replica_1, DC2 = replica_2, DC3 = replica_3,
    Actors = [DC1, DC2, DC3],
    VC = fun maps:from_list/1,

    Type = grb_lww,
    Base = grb_crdt:new(Type),
    BaseVal = grb_crdt:value(Base),
    Ops = [ { VC([{DC1, X}]), grb_crdt:make_op(Type, X) }
        || X <- lists:seq(1, 10) ],

    Log = from_list(Type, Base, Actors, 10, Ops),
    ?assertEqual(BaseVal, grb_crdt:value(grb_version_log:snapshot_lower(VC([{DC1, 0}]), Log))),
    ?assertEqual(5,
                 grb_crdt:value(grb_version_log:snapshot_lower(VC([{DC1, 5}]), Log))),

    Ops2 = [ { VC([{DC1, X}]), grb_crdt:make_op(Type, X) }
            || X <- lists:seq(11, 20) ],
    Log2 = lists:foldl(fun({T, Op}, R) -> grb_version_log:insert(Op, T, R) end, Log, Ops2),

    %% Log has been GC'd, if we ask anything below the snapshot, it should return the base version
    ?assertMatch({not_found, _}, grb_version_log:snapshot_lower(VC([{DC1, 0}]), Log2)),
    ?assertMatch({not_found, _}, grb_version_log:snapshot_lower(VC([{DC1, 5}]), Log2)),

    %% Anything above the snapshot works as usual, and takes into account older operations
    ?assertEqual(20,
                 grb_crdt:value(grb_version_log:snapshot_lower(VC([{DC1, 20}]), Log2))).

grb_version_log_insert_gc_gset_test() ->
    DC1 = replica_1, DC2 = replica_2, DC3 = replica_3,
    Actors = [DC1, DC2, DC3],
    VC = fun maps:from_list/1,

    Type = grb_gset,
    Base = grb_crdt:new(Type),
    BaseVal = grb_crdt:value(Base),
    Ops = [ { VC([{DC1, X}]), grb_crdt:make_op(Type, X) }
        || X <- lists:seq(1, 10) ],

    Log = from_list(Type, Base, Actors, 10, Ops),
    ?assertEqual(BaseVal, grb_crdt:value(grb_version_log:snapshot_lower(VC([{DC1, 0}]), Log))),
    ?assertEqual(lists:seq(1, 5),
                 lists:sort(maps:keys(grb_crdt:value(grb_version_log:snapshot_lower(VC([{DC1, 5}]), Log))))),

    Ops2 = [ { VC([{DC1, X}]), grb_crdt:make_op(Type, X) }
            || X <- lists:seq(11, 20) ],
    Log2 = lists:foldl(fun({T, Op}, R) -> grb_version_log:insert(Op, T, R) end, Log, Ops2),

    %% Log has been GC'd, if we ask anything below the snapshot, it should return the base version
    ?assertMatch({not_found, _}, grb_version_log:snapshot_lower(VC([{DC1, 0}]), Log2)),
    ?assertMatch({not_found, _}, grb_version_log:snapshot_lower(VC([{DC1, 5}]), Log2)),

    %% Anything above the snapshot works as usual, and takes into account older operations
    ?assertEqual(lists:seq(1,20),
                 lists:sort(maps:keys(grb_crdt:value(grb_version_log:snapshot_lower(VC([{DC1, 20}]), Log2))))).

-endif.
