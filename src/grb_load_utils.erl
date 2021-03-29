-module(grb_load_utils).
-include_lib("kernel/include/logger.hrl").

%% Microbenchmark API
-export([preload_micro/2,
         preload_micro_sync/1]).

%% Rubis API
-export([preload_rubis/2,
         preload_rubis_sync/1]).

-ignore_xref([preload_micro_sync/1,
              preload_rubis_sync/1]).

-define(global_indices, [
    <<"global_index_0">>,
    <<"global_index_1">>,
    <<"global_index_2">>,
    <<"global_index_3">>,
    <<"global_index_4">>,
    <<"global_index_5">>,
    <<"global_index_6">>,
    <<"global_index_7">>,
    <<"global_index_8">>,
    <<"global_index_9">>
]).

%% Negative so it always wins. Empty binary so we can filter out later
-define(base_maxtuple, {-1, <<>>}).

%%%===================================================================
%%% API
%%%===================================================================

-spec preload_micro(grb_promise:t(), map()) -> ok.
preload_micro(Promise, Properties) ->
     _ = spawn(fun() ->
        try
            ok = preload_micro_sync(Properties),
            ?LOG_INFO("GRB preload finished")
        catch Error:Kind:Stck ->
            ?LOG_ERROR("GRB preload failed!: ~p", [{Error, Kind, Stck}])
        end,
        grb_promise:resolve(ok, Promise)
    end),
    ok.

-spec preload_rubis(grb_promise:t(), map()) -> ok.
preload_rubis(Promise, Properties) ->
    _ = spawn(fun() ->
        try
            ok = preload_rubis_sync(Properties),
            ?LOG_INFO("Preload finished")
        catch Error:Kind:Stck ->
            ?LOG_ERROR("Preload failed!: ~p", [{Error, Kind, Stck}])
        end,
        grb_promise:resolve(ok, Promise)
    end),
    ok.

%%%===================================================================
%%% Micro Internal
%%%===================================================================

preload_micro_sync(#{key_limit := KeyLimit, val_size := ValueSize}) ->
    AllIdx = grb_dc_utils:get_index_nodes(),
    {_, Preprocess} = lists:foldl(
        fun(Idx, {Nth, Acc}) ->
            {Nth + 1, [ {Idx, Nth} | Acc ]}
        end,
        {0, []},
        AllIdx
    ),

    Size = length(AllIdx),
    Value = crypto:strong_rand_bytes(ValueSize),
    ok = pfor(
        fun({IndexNode, StartKey}) ->
            BuildKeys = fun S(Key, N, Acc) ->
                if
                    Key > KeyLimit ->
                        Acc;
                    true ->
                        NextKey = StartKey + (N * Size),
                        S(NextKey, N + 1, Acc#{Key => {grb_lww, Value}})
                end
            end,
            PartitionKeys = BuildKeys(StartKey, 1, #{}),
            ok = grb_oplog_vnode:put_direct_vnode(
                sync,
                IndexNode,
                PartitionKeys
            )
        end,
        Preprocess
    ),
    ok.

%%%===================================================================
%%% Rubis Internal
%%%===================================================================

-spec preload_rubis_sync(map()) -> ok.
preload_rubis_sync(Properties) ->
    #{ regions := Regions,
       categories := CategoriesAndItems,
       user_total := TotalUsers } = Properties,

    Categories = [ C || {C, _} <- CategoriesAndItems ],

    ok = load_regions(Regions),
    ?LOG_INFO("Finished loading regions"),
    ok = load_categories(Categories),
    ?LOG_INFO("Finished loading categories"),

    UsersPerRegion = (TotalUsers div length(Regions)),
    ok = load_users(Regions, UsersPerRegion),
    ?LOG_INFO(
        "Finished loading users (total ~b, per region ~b)",
        [(UsersPerRegion * length(Regions)), UsersPerRegion]
    ),
    ok = load_items(Regions, TotalUsers, CategoriesAndItems, Properties),
    ?LOG_INFO("Finished loading items"),
    ok.

-spec load_regions(Regions :: [binary()]) -> ok.
load_regions(Regions) ->
    ok = pfor(
        fun(Index) ->
            [ store_region(Index, R) || R <- Regions ],
            ok
        end,
        ?global_indices
    ),
    ok.

-spec load_categories(Categories :: [binary()]) -> ok.
load_categories(Categories) ->
    ok = pfor(
        fun(Index) ->
            [ store_category(Index, C) || C <- Categories ],
            ok
        end,
        ?global_indices
    ),
    ok.

-spec load_users(Regions :: [binary()], PerRegion :: non_neg_integer()) -> ok.
load_users(Regions, PerRegion) ->
    ok = pfor(
        fun(Region) ->
            foreach_limit(fun(Id) -> store_user(Region, Id) end, PerRegion)
        end,
        Regions
    ).

-spec load_items(Regions :: [binary()],
                 PerRegion :: non_neg_integer(),
                 CategoriesAndItems :: [{binary(), non_neg_integer()}],
                 Properties :: map()) -> ok.

load_items(Regions, UsersPerRegion, CategoriesAndItems, Properties) ->
    NRegions = length(Regions),
    PreprocessedItems = lists:foldl(
        fun({Category, NumberOfItems}, Acc) ->
            fold_limit(
                fun(Id, InAcc) ->
                    Region = lists:nth(((Id rem NRegions) + 1), Regions),
                    PrevIds = maps:get(Region, InAcc, []),
                    InAcc#{Region => [ {Category, Id} | PrevIds ]}
                end,
                Acc,
                NumberOfItems
            )
        end,
        #{}, CategoriesAndItems
    ),
    ok = pfor(
        fun({Region, ItemTuples}) ->
            lists:foreach(
                fun({Category, Id}) ->
                    UserId = rand:uniform(UsersPerRegion),
                    UserKey = {Region, users, list_to_binary(io_lib:format("~s/user/preload_~b", [Region, UserId]))},
                    Ret = store_item(Region, UserKey, Category, Id, Properties),
                    {ItemKey, NBids, NComments, BidProps} = Ret,
                    MaxBid = load_bids(Region, ItemKey, NBids, Regions, UsersPerRegion, BidProps),
                    %% Update the max_bid for the item id
                    {_, _, ItemId} = ItemKey,
                    ok = grb_oplog_vnode:append_direct_vnode(
                        sync,
                        grb_dc_utils:key_location(Region),
                        #{
                            {Region, items, ItemId, max_bid} => grb_crdt:make_op(grb_maxtuple, MaxBid)
                        }
                    ),
                    ok = load_comments(Region, UserKey, ItemKey, NComments, Regions, UsersPerRegion, Properties),
                    ok
                end,
                ItemTuples
            )
        end,
        maps:to_list(PreprocessedItems)
    ),
    ok.

-spec store_region(binary(), binary()) -> ok.
store_region(Index, Region) ->
    ok = grb_oplog_vnode:append_direct_vnode(
        sync,
        grb_dc_utils:key_location(Index),
        #{
            {Index, all_regions} => grb_crdt:make_op(grb_gset, Region)
        }
    ).

-spec store_category(binary(), binary()) -> ok.
store_category(Index, Category) ->
    ok = grb_oplog_vnode:append_direct_vnode(
        sync,
        grb_dc_utils:key_location(Index),
        #{
            {Index, all_categories} => grb_crdt:make_op(grb_gset, Category)
        }
    ).

-spec store_user(Region :: binary(), Id :: non_neg_integer()) -> ok.
store_user(Region, Id) ->
    Table = users,
    %% Ensure nickname partitioning during load so we can do it in parallel
    NickName =  list_to_binary(io_lib:format("~s/user/preload_~b", [Region, Id])),
    %% Set it to the nickname so we can guess it at the client to authenticate
    Password = NickName,

    FirstName = list_to_binary(io_lib:format("User_~b", [Id])),
    LastName = list_to_binary(io_lib:format("McUser_~b", [Id])),
    Email = random_email(20),

    UserKey = {Region, Table, NickName},
    %% Create user object
    ok = grb_oplog_vnode:put_direct_vnode(
        sync,
        grb_dc_utils:key_location(Region),
        #{
            UserKey => {grb_lww, NickName},
            {Region, Table, NickName, name} => {grb_lww, FirstName},
            {Region, Table, NickName, lastname} => {grb_lww, LastName},
            {Region, Table, NickName, password} => {grb_lww, Password},
            {Region, Table, NickName, email} => {grb_lww, Email},
            {Region, Table, NickName, rating} => {grb_gcounter, 0}
        }
    ),

    %% Update username index
    ok = grb_oplog_vnode:put_direct_vnode(
        sync,
        grb_dc_utils:key_location(NickName),
        #{
            NickName => {grb_lww, UserKey}
        }
    ).

-spec store_item(Region :: binary(),
                 Seller :: term(),
                 Category :: binary(),
                 Id :: non_neg_integer(),
                 ItemProperties :: #{}
                 ) -> { ItemKey :: term(), BidsN :: non_neg_integer(), CommentsN :: non_neg_integer(), BidProperties :: #{}}.
store_item(Region, Seller, Category, Id, ItemProperties) ->
    Table = items,
    %% Ensure id partitioning during load so we can do it in parallel
    ItemId = list_to_binary(io_lib:format("~s/items/preload_~b", [Category, Id])),

    Name = ItemId,

    #{
        item_max_initial_price := MaxInitialPrice,
        item_description_max_len := MaxDescriptionLen,
        item_max_quantity := MaxQuantity,
        item_reserve_percentage := ReservedPercentage,
        item_buy_now_percentage := BuyNowPercentage,
        item_closed_percentage := ClosedPercentage,
        item_max_bids := MaxBids,
        item_max_comments := MaxComments,
        bid_max_quantity := MaxBidQuantity
    } = ItemProperties,

    InitialPrice = safe_uniform(MaxInitialPrice),
    Quantity = safe_uniform(MaxQuantity),
    Closed = (rand:uniform(100) =< ClosedPercentage),
    BidsN = safe_uniform(MaxBids),
    CommentsN = safe_uniform(MaxComments),

    ReservePrice = InitialPrice + begin
        case (rand:uniform(100) =< ReservedPercentage) of
            true -> rand:uniform(10);
            false -> 0
        end
    end,

    BuyNow = ReservePrice + begin
        case (rand:uniform(100) =< BuyNowPercentage) of
            true -> rand:uniform(10);
            false -> 0
        end
    end,

    Description = case safe_uniform(safe_uniform(MaxDescriptionLen)) of
        N when N >= 1 -> random_binary(N);
        _ -> <<>>
    end,

    ItemKey = {Region, Table, ItemId},
    ok = grb_oplog_vnode:put_direct_vnode(
        sync,
        grb_dc_utils:key_location(Region),
        #{
            %% primary key and foreign keys
            ItemKey => {grb_lww, ItemKey},
            {Region, Table, ItemId, seller} => {grb_lww, Seller},
            {Region, Table, ItemId, category} => {grb_lww, Category},
            {Region, Table, ItemId, max_bid} => {grb_maxtuple, ?base_maxtuple},

            %% Meat. Quantity can be LWW since it is only modified in a red transaction
            {Region, Table, ItemId, initial_price} => {grb_lww, InitialPrice},
            {Region, Table, ItemId, quantity} => {grb_lww, Quantity},
            {Region, Table, ItemId, reserve_price} => {grb_lww, ReservePrice},
            {Region, Table, ItemId, buy_now} => {grb_lww, BuyNow},
            {Region, Table, ItemId, closed} => {grb_lww, Closed},

            %% Can do this already, we know how many we'll generate
            {Region, Table, ItemId, bids_number} => {grb_gcounter, BidsN},

            %% Junk info
            {Region, Table, ItemId, name} => {grb_lww, Name},
            {Region, Table, ItemId, description} => {grb_lww, Description}
        }
    ),

    %% Append to user_id -> item_id index and to (region_id, category_id) -> item_id index
    ok = grb_oplog_vnode:append_direct_vnode(
        sync,
        grb_dc_utils:key_location(Region),
        #{
            {Region, items_seller, Seller} => grb_crdt:make_op(grb_gset, ItemKey),
            {Region, items_region_category, Category} => grb_crdt:make_op(grb_gset, ItemKey)
        }
    ),

    BidQuantity = erlang:min(MaxBidQuantity, Quantity),
    BidProperties = #{item_initial_price => InitialPrice, item_max_quantity => BidQuantity},
    {ItemKey, BidsN, CommentsN, BidProperties}.

load_bids(ItemRegion, ItemKey, NBids, Regions, UsersPerRegion, BidProps) ->
    load_bids_(NBids, ItemRegion, ItemKey, length(Regions), Regions, UsersPerRegion, BidProps, ?base_maxtuple).

load_bids_(0, _, _, _, _, _, _, Acc) ->
    Acc;
load_bids_(Id, ItemRegion, ItemKey, NRegions, Regions, UsersPerRegion, BidProps, Max0) ->
    BidderRegion = lists:nth(rand:uniform(NRegions), Regions),
    BidderId = rand:uniform(UsersPerRegion),
    BidderKey = {BidderRegion, users, list_to_binary(io_lib:format("~s/user/preload_~b", [BidderRegion, BidderId]))},
    {BidKey, Amount} = store_bid(ItemRegion, ItemKey, BidderKey, Id, BidProps),
    NewMax = case Max0 of
        {OldAmount, _} when Amount > OldAmount -> {Amount, {BidKey, BidderKey}};
        _ -> Max0
    end,
    load_bids_(Id - 1, ItemRegion, ItemKey, NRegions, Regions, UsersPerRegion, BidProps, NewMax).

%% Have to index by item and by user
%% Store the nickname for the user in the BIDS_item index.
%% Since the user key is {Region, users, Nickname}, simply extract it
%% Store amount in BIDS_user, so we don't have to do cross-partiton
%% Update number of bids? (or do it outside)
%% Update max bid for item
-spec store_bid(Region :: binary(),
                ItemKey :: {binary(), atom(), binary()},
                UserKey :: {binary(), atom(), binary()},
                Id :: non_neg_integer(),
                BidProperties :: map()
                ) -> {BidKey :: {binary(), atom(), binary()}, BidAmount :: non_neg_integer()}.

store_bid(Region, ItemKey={Region, _, ItemId}, UserKey={UserRegion, _, _}, Id, BidProperties) ->
    %% Fields:
    %% nickname of bidder (user id)
    %% item key
    %% bid amount
    %% quantity of item
    Table = bids,

    #{ item_initial_price := ItemInitalPrice } = BidProperties,
    BidAmount = ItemInitalPrice + safe_uniform((ItemInitalPrice div 2)),

    #{ item_max_quantity := ItemMaxQty } = BidProperties,
    BidQty = safe_uniform(ItemMaxQty),

    %% Ensure id partitioning during load so we can do it in parallel
    BidId = list_to_binary(io_lib:format("~s/bid/preload_~b", [ItemId, Id])),

    %% Main bid object
    BidKey = {Region, Table, BidId},
    ok = grb_oplog_vnode:put_direct_vnode(
        sync,
        grb_dc_utils:key_location(Region),
        #{
            BidKey => {grb_lww, BidKey},
            {Region, Table, BidId, bidder} => {grb_lww, UserKey},
            {Region, Table, BidId, item} => {grb_lww, ItemKey},
            {Region, Table, BidId, amount} => {grb_lww, BidAmount},
            {Region, Table, BidId, quantity} => {grb_lww, BidQty}
        }
    ),

    %% Append to user_id -> {bid_id, bid_amount} index, colocated with the bidder
    ok = grb_oplog_vnode:append_direct_vnode(
        sync,
        grb_dc_utils:key_location(UserRegion),
        #{
            {UserRegion, bids_user, UserKey} => grb_crdt:make_op(grb_gset, {BidKey, BidAmount})
        }
    ),

    %% Append to item_id -> {bid_id, user_id} index, colocated with the item
    ok = grb_oplog_vnode:append_direct_vnode(
        sync,
        grb_dc_utils:key_location(Region),
        #{
            {Region, bids_item, ItemKey} => grb_crdt:make_op(grb_gset, {BidKey, UserKey})
        }
    ),

    {BidKey, BidAmount}.

load_comments(Region, SellerKey, ItemKey, NComments, Regions, UsersPerRegion, CommentProps) ->
    load_comments_(NComments, Region, SellerKey, ItemKey, length(Regions), Regions, UsersPerRegion, CommentProps).

load_comments_(0, _, _, _, _, _, _, _) ->
    ok;
load_comments_(Id, RecipientRegion, RecipientKey, ItemKey, NRegions, Regions, UsersPerRegion, CommentProps) ->
    SenderKey = rand_user_key_different(RecipientKey, NRegions, Regions, UsersPerRegion),
    ok = store_comment(RecipientRegion, RecipientKey, ItemKey, SenderKey, Id, CommentProps),
    load_comments_(Id - 1, RecipientRegion, RecipientKey, ItemKey, NRegions, Regions, UsersPerRegion, CommentProps).

%% comments objects are colocated with the recipient key
%% and there's the COMMENTS_to_user index also colocated with the recipient
%%
store_comment(RecipientRegion, RecipientKey={_, _, RecipientId}, ItemKey={_, _, ItemId}, SenderKey, Id, CommentProps) ->
    %% fields:
    %% sender
    %% recipient
    %% item_id
    %% rating
    %% text
    %% remember to update rating of recipient
    %% can store the nickname in the comments_to_user index, but it's not necessary, nickname is built into the key
    Table = comments,

    #{ comment_max_len := MaxCommentLength} = CommentProps,
    CommentText = random_binary(safe_uniform(MaxCommentLength)),
    CommentRating = random_rating(),

    %% Ensure id partitioning during load so we can do it in parallel
    CommentId = list_to_binary(io_lib:format("~s/comment/preload_~b", [ItemId, Id])),
    CommentKey = {RecipientRegion, Table, CommentId},
    ok = grb_oplog_vnode:put_direct_vnode(
        sync,
        grb_dc_utils:key_location(RecipientRegion),
        #{
            CommentKey => {grb_lww, CommentKey},
            {RecipientRegion, Table, CommentId, from} => {grb_lww, SenderKey},
            {RecipientRegion, Table, CommentId, to} => {grb_lww, RecipientKey},
            {RecipientRegion, Table, CommentId, on_item} => {grb_lww, ItemKey},
            {RecipientRegion, Table, CommentId, rating} => {grb_lww, CommentRating},
            {RecipientRegion, Table, CommentId, text} => {grb_lww, CommentText}
        }
    ),

    %% Append to users_id -> comment_id index, and modify recipient rating
    ok = grb_oplog_vnode:append_direct_vnode(
        sync,
        grb_dc_utils:key_location(RecipientRegion),
        #{
            {RecipientRegion, comments_to_user, RecipientKey} => grb_crdt:make_op(grb_gset, CommentKey),
            {RecipientRegion, users, RecipientId, rating} => grb_crdt:make_op(grb_gcounter, CommentRating)
        }
    ),

    ok.

%%%===================================================================
%%% Random Utils
%%%===================================================================

-spec safe_uniform(non_neg_integer()) -> non_neg_integer().
safe_uniform(0) -> 0;
safe_uniform(X) when X >= 1 -> rand:uniform(X).

-spec random_binary(Size :: non_neg_integer()) -> binary().
random_binary(0) ->
    <<>>;
random_binary(N) ->
    list_to_binary(random_string_(N)).

-spec random_email(Size :: non_neg_integer()) -> binary().
random_email(N) ->
    list_to_binary(random_string_(N) ++ "@example.com").

-spec random_rating() -> non_neg_integer().
random_rating() ->
    rand:uniform(11) - 6.

-spec random_string_(Size :: non_neg_integer()) -> list().
random_string_(N) ->
    Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890",
    lists:foldl(fun(_, Acc) ->
        [lists:nth(rand:uniform(length(Chars)), Chars)] ++ Acc
    end, [], lists:seq(1, N)).

-spec rand_user_key_different(UserKey :: {binary(), atom(), binary()},
                              NRegions :: non_neg_integer(),
                              Regions:: [binary()],
                              UsersPerRegion :: non_neg_integer()) -> OtherUserKey :: {binary(), atom(), binary()}.

rand_user_key_different({UserRegion, _, UserId}=UserKey, NRegions, Regions, UsersPerRegion) ->
    SenderRegion = lists:nth(rand:uniform(NRegions), Regions),
    Id = rand:uniform(UsersPerRegion),
    if
        SenderRegion =/= UserRegion ->
            {SenderRegion, users, list_to_binary(io_lib:format("~s/user/preload_~b", [SenderRegion, Id]))};
        true ->
            AttemptUserId = list_to_binary(io_lib:format("~s/user/preload_~b", [UserRegion, Id])),
            if
                AttemptUserId =/= UserId ->
                    {UserRegion, users, AttemptUserId};
                true ->
                    rand_user_key_different(UserKey, NRegions, Regions, UsersPerRegion)
            end
    end.

%%%===================================================================
%%% Util
%%%===================================================================

-spec foreach_limit(fun((non_neg_integer()) -> ok), non_neg_integer()) -> ok.
foreach_limit(_, 0) ->
    ok;
foreach_limit(Fun, Total) ->
    _ = Fun(Total),
    foreach_limit(Fun, Total - 1).

-spec fold_limit(fun((non_neg_integer(), term()) -> term()), term(), non_neg_integer()) -> term().
fold_limit(_, Acc, 0) ->
    Acc;
fold_limit(Fun, Acc, Total) ->
    fold_limit(Fun, Fun(Total, Acc), Total - 1).

-spec pfor(fun(), list()) -> ok | {error, term()}.
pfor(F, L) ->
    Parent = self(),
    Reference = make_ref(),
    lists:foldl(
        fun(X, N) ->
            spawn_link(
                fun() ->
                    Res = try
                        {ok, F(X)}
                    catch Error:Kind:Stck ->
                        {error, {Error, Kind, Stck}}
                    end,
                    Parent ! {pfor, Reference, Res}
                end),
            N+1
        end, 0, L),
    L2 = [receive {pfor, Reference, R} -> R end || _ <- L],
    case lists:keyfind(error, 1, L2) of
        false ->
            ok;
        {error, Reason} ->
            {error, Reason}
    end.
