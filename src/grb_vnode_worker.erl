-module(grb_vnode_worker).
-include("grb.hrl").

-callback persist_worker_num(partition_id(), non_neg_integer()) -> ok.
-callback start_worker(partition_id(), non_neg_integer()) -> {ok, pid()} | ignore | {error, term()}.
-callback is_ready(partition_id(), non_neg_integer()) -> boolean().
-callback terminate_worker(partition_id(), non_neg_integer()) -> ok.
