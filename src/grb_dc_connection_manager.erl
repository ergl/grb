-module(grb_dc_connection_manager).
-include("grb.hrl").
-include_lib("kernel/include/logger.hrl").

%% API
-export([connect_to/1]).

-spec connect_to(replica_descriptor()) -> ok | {error, term()}.
connect_to(#replica_descriptor{replica_id=ReplicaID, remote_addresses=RemoteNodes}) ->
    ?LOG_DEBUG("Node ~p connected succesfully to DC ~p", [ReplicaID]),
    %% RemoteNodes is a map, with Partitions as keys
    %% The value is a tuple {IP, Port}, to allow addressing
    %% that specific partition at the replica
    %%
    %% Get our local partitions (to our nodes),
    %% and get those from the remote addresses
    try
        lists:foldl(fun(Partition, AccSet) ->
            Entry={RemoteIP, RemotePort} = maps:get(Partition, RemoteNodes),
            case sets:is_element(Entry, AccSet) of
                true ->
                    AccSet;
                false ->
                    case grb_dc_connection_sender_sup:start_connection(ReplicaID, RemoteIP, RemotePort) of
                        {ok, _} ->
                            sets:add_element(Entry, AccSet);
                        Err ->
                            throw({error, {sender_connection, ReplicaID, RemoteIP, Err}})
                    end
            end
        end, sets:new(), grb_dc_utils:my_partitions()),
        ok
    catch Exn -> Exn
    end.
