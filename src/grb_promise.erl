-module(grb_promise).

-record(promise, {
    reply_to :: pid(),
    context :: term()
}).

-type t() :: #promise{}.

-export([new/2,
         resolve/2]).

-export_type([t/0]).

-spec new(pid(), term()) -> t().
new(From, Context) ->
    #promise{reply_to=From, context=Context}.

-spec resolve(term(), t()) -> ok.
resolve(Reply, #promise{reply_to=To, context=Context}) ->
    To ! {'$grb_promise_resolve', Reply, Context},
    ok.
