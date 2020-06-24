-module(grb_time).

%% API
-type ts() :: non_neg_integer().
-export_type([ts/0]).
-export([timestamp/0]).

%% todo(borja, warn): Change type of timestamp if we enable time warp mode
%%
%% one possible option is to use the following:
%%
%% # Determine Order of Events with Time of the Event:
%% ```
%% Time = erlang:monotonic_time(),
%% UMI = erlang:unique_integer([monotonic]),
%% {Time, UMI}
%% ```
-spec timestamp() -> ts().
timestamp() ->
    erlang:system_time(micro_seconds).
