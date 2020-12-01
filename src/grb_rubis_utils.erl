-module(grb_rubis_utils).

%% API
-export([preload/2]).
-export([preload_sync/1]).

-define(global_indices, <<"global_index">>).

-spec preload(grb_promise:t(), map()) -> ok.
preload(Promise, Properties) ->
    _ = spawn_link(fun() ->
        grb_promise:resolve(preload_sync(Properties), Promise)
    end),
    ok.

-spec preload_sync(map()) -> ok.
preload_sync(Properties) ->
    #{ regions := Regions,
       categories := CategoriesAndItems,
       user_total := TotalUsers } = Properties,

    Categories = [ C || {C, _} <- CategoriesAndItems ],

    ok = load_regions(Regions),
    ok = load_categories(Categories),

    UsersPerRegion = (TotalUsers div length(Regions)),
    ok = load_users(Regions, UsersPerRegion),
    ok = load_items(Regions, TotalUsers, CategoriesAndItems, Properties),

    ok.

-spec load_regions(Regions :: [binary()]) -> ok.
load_regions(Regions) ->
    pmap(fun store_region/1, Regions),
    ok.

-spec load_categories(Categories :: [binary()]) -> ok.
load_categories(Categories) ->
    pmap(fun store_category/1, Categories),
    ok.

-spec load_users(Regions :: [binary()], PerRegion :: non_neg_integer()) -> ok.
load_users(Regions, PerRegion) ->
    _ = pmap(fun(Region) ->
        pmap_limit(fun(Id) ->
            store_user(Region, Id)
        end, PerRegion)
    end, Regions),
    ok.

-spec load_items(Regions :: [binary()],
                 PerRegion :: non_neg_integer(),
                 CategoriesAndItems :: [{binary(), non_neg_integer()}],
                 Properties :: map()) -> ok.

load_items(Regions, UsersPerRegion, CategoriesAndItems, Properties) ->
    NRegions = length(Regions),
    _ = pmap(fun({Category, NumberOfItems}) ->
        pmap_limit(fun(Id) ->
            Region = lists:nth(rand:uniform(NRegions), Regions),
            UserId = rand:uniform(UsersPerRegion),
            UserKey = {Region, users, list_to_binary(io_lib:format("~s/user/preload_~b", [Region, UserId]))},
            Ret = store_item(Region, UserKey, Category, Id, #{
                item_max_quantity => maps:get(item_max_quantity, Properties),
                item_reserve_percentage => maps:get(item_reserve_percentage, Properties, 0),
                item_buy_now_percentage => maps:get(item_buy_now_percentage, Properties, 0),
                item_closed_percentage => maps:get(item_closed_percentage, Properties, 0),
                item_max_bids => maps:get(item_max_bids, Properties, 0),
                item_max_comments => maps:get(item_max_comments, Properties, 0)
            }),

            {ToWait, ItemKey, NBids, NComments, BidProps} = Ret,
            {ToWaitBids, MaxBid} = load_bids(Region, ItemKey, NBids, Regions, UsersPerRegion, BidProps),

            %% Update the max_bid for the item id
            {_, _, ItemId} = ItemKey,
            ok = grb_oplog_vnode:append_direct_vnode(
                async,
                grb_dc_utils:key_location(Region),
                #{
                    {Region, items, ItemId, max_bid} => grb_crdt:make_op(grb_maxtuple, MaxBid)
                }
            ),

            ToWaitComments = load_comments(Region, UserKey, ItemKey, NComments, Regions, UsersPerRegion, #{
                comment_max_len => maps:get(comment_max_len, Properties, 10)
            }),
            ToWait + ToWaitBids + 1 + ToWaitComments
        end, NumberOfItems)
    end, CategoriesAndItems),
    ok.

-spec store_region(binary()) -> ok.
store_region(Region) ->
    ok = grb_oplog_vnode:append_direct_vnode(
        sync,
        grb_dc_utils:key_location(?global_indices),
        #{
            {?global_indices, all_regions} => grb_crdt:make_op(grb_gset, Region)
        }
    ).

-spec store_category(binary()) -> ok.
store_category(Category) ->
    ok = grb_oplog_vnode:append_direct_vnode(
        sync,
        grb_dc_utils:key_location(?global_indices),
        #{
            {?global_indices, all_categories} => grb_crdt:make_op(grb_gset, Category)
        }
    ).

-spec store_user(Region :: binary(), Id :: non_neg_integer()) -> ToAck :: non_neg_integer().
store_user(Region, Id) ->
    Table = users,
    %% Ensure nickname partitioning during load so we can do it in parallel
    NickName =  list_to_binary(io_lib:format("~s/user/preload_~b", [Region, Id])),
    FirstName = random_binary(20),
    LastName = random_binary(20),
    Password = NickName,
    Email = random_email(20),
    Rating = random_rating(),

    UserKey = {Region, Table, NickName},
    %% Create user object
    ok = grb_oplog_vnode:put_direct_vnode(
        async,
        grb_dc_utils:key_location(Region),
        #{
            UserKey => {grb_lww, NickName},
            {Region, Table, NickName, name} => {grb_lww, FirstName},
            {Region, Table, NickName, lastname} => {grb_lww, LastName},
            {Region, Table, NickName, password} => {grb_lww, Password},
            {Region, Table, NickName, email} => {grb_lww, Email},
            {Region, Table, NickName, rating} => {grb_gcounter, Rating}
        }
    ),

    %% Update username index
    ok = grb_oplog_vnode:put_direct_vnode(
        async,
        grb_dc_utils:key_location(NickName),
        #{
            NickName => {grb_lww, UserKey}
        }
    ),

    2.

store_item(Region, Seller, Category, Id, ItemProperties) ->
    Table = items,
    %% Ensure id partitioning during load so we can do it in parallel
    ItemId = list_to_binary(io_lib:format("~s/items/preload_~b", [Category, Id])),

    Name = random_binary(50),
    Description = random_binary(100),

    InitialPrice = rand:uniform(100),

    MaxQuantity = maps:get(item_max_quantity, ItemProperties),
    Quantity = safe_uniform(MaxQuantity),

    ReservedPercentage = maps:get(item_reserve_percentage, ItemProperties, 0),
    ReservePrice = case (rand:uniform(100) =< ReservedPercentage) of
        true -> InitialPrice + rand:uniform(10);
        false -> 0
    end,

    BuyNowPercentage = maps:get(item_buy_now_percentage, ItemProperties, 0),
    BuyNow = case (rand:uniform(100) =< BuyNowPercentage) of
        true -> InitialPrice + ReservePrice + rand:uniform(10);
        false -> 0
    end,

    ClosedPercentage = maps:get(item_closed_percentage, ItemProperties, 0),
    Closed = (rand:uniform(100) =< ClosedPercentage),

    MaxBids = maps:get(item_max_bids, ItemProperties),
    BidsN = safe_uniform(MaxBids),

    MaxComments = maps:get(item_max_comments, ItemProperties),
    CommentsN = safe_uniform(MaxComments),

    ItemKey = {Region, Table, ItemId},
    ok = grb_oplog_vnode:put_direct_vnode(
        async,
        grb_dc_utils:key_location(Region),
        #{
            %% primary key and foreign keys
            ItemKey => {grb_lww, ItemKey},
            {Region, Table, ItemId, seller} => {grb_lww, Seller},
            {Region, Table, ItemId, category} => {grb_lww, Category},
            {Region, Table, ItemId, max_bid} => {grb_maxtuple, {0, <<>>}},

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

    %% Append to user_id -> item_id index
    ok = grb_oplog_vnode:append_direct_vnode(
        async,
        grb_dc_utils:key_location(Region),
        #{
            {Region, items_seller, Seller} => grb_crdt:make_op(grb_gset, ItemKey)
        }
    ),

    %% Append to (region_id, category_id) -> item_id index
    ok = grb_oplog_vnode:append_direct_vnode(
        async,
        grb_dc_utils:key_location(Region),
        #{
            {Region, items_region_category, Category} => grb_crdt:make_op(grb_gset, ItemKey)
        }
    ),

    BidProperties = #{item_initial_price => InitialPrice, item_max_quantity => Quantity},
    {3, ItemKey, BidsN, CommentsN, BidProperties}.

load_bids(ItemRegion, ItemKey, NBids, Regions, UsersPerRegion, BidProps) ->
    load_bids_(NBids, ItemRegion, ItemKey, length(Regions), Regions, UsersPerRegion, BidProps, {0, {ignore, -1}}).

load_bids_(0, _, _, _, _, _, _, Acc) ->
    Acc;
load_bids_(Id, ItemRegion, ItemKey, NRegions, Regions, UsersPerRegion, BidProps, {ToWait0, Max0}) ->
    BidderRegion = lists:nth(rand:uniform(NRegions), Regions),
    BidderId = rand:uniform(UsersPerRegion),
    BidderKey = {BidderRegion, users, list_to_binary(io_lib:format("~s/user/preload_~b", [BidderRegion, BidderId]))},
    {MsgsToWait, BidKey, Amount} = store_bid(ItemRegion, ItemKey, BidderKey, Id, BidProps),
    NewMax = case Max0 of
        {_, OldAmount} when Amount > OldAmount -> {BidKey, Amount};
        _ -> Max0
    end,
    load_bids_(Id - 1, ItemRegion, ItemKey, NRegions, Regions, UsersPerRegion, BidProps, {ToWait0 + MsgsToWait, NewMax}).

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
                BidProperties :: map()) -> { ToWait :: non_neg_integer(), BidAmount :: non_neg_integer()}.
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
        async,
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
        async,
        grb_dc_utils:key_location(UserRegion),
        #{
            {UserRegion, bids_user, UserKey} => grb_crdt:make_op(grb_gset, {BidKey, BidAmount})
        }
    ),

    %% Append to item_id -> {bid_id, user_id} index, colocated with the item
    ok = grb_oplog_vnode:append_direct_vnode(
        async,
        grb_dc_utils:key_location(Region),
        #{
            {Region, bids_item, ItemKey} => grb_crdt:make_op(grb_gset, {BidKey, UserKey})
        }
    ),

    {3, BidKey, BidAmount}.

load_comments(Region, SellerKey, ItemKey, NComments, Regions, UsersPerRegion, CommentProps) ->
    load_comments_(NComments, Region, SellerKey, ItemKey, length(Regions), Regions, UsersPerRegion, CommentProps, 0).

load_comments_(0, _, _, _, _, _, _, _, Acc) ->
    Acc;
load_comments_(Id, RecipientRegion, RecipientKey, ItemKey, NRegions, Regions, UsersPerRegion, CommentProps, Acc) ->
    SenderKey = rand_user_key_different(RecipientKey, NRegions, Regions, UsersPerRegion),
    MsgsToWait = store_comment(RecipientRegion, RecipientKey, ItemKey, SenderKey, Id, CommentProps),
    load_comments_(Id - 1, RecipientRegion, RecipientKey, ItemKey, NRegions, Regions, UsersPerRegion, CommentProps, Acc + MsgsToWait).

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
        async,
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
        async,
        grb_dc_utils:key_location(RecipientRegion),
        #{
            {RecipientRegion, comments_to_user, RecipientKey} => grb_crdt:make_op(grb_gset, CommentKey),
            {RecipientRegion, users, RecipientId, rating} => grb_crdt:make_op(grb_gcounter, CommentRating)
        }
    ),

    2.

%%%===================================================================
%%% Random Utils
%%%===================================================================

-spec safe_uniform(pos_integer()) -> pos_integer().
safe_uniform(0) -> 0;
safe_uniform(X) when X >= 1 -> rand:uniform(X).

-spec random_binary(Size :: non_neg_integer()) -> binary().
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

-spec pmap_limit(fun((non_neg_integer()) -> non_neg_integer()), non_neg_integer()) -> non_neg_integer().
pmap_limit(Fun, Total) ->
    Send = fun
       S(0, Acc) -> Acc;
       S(Id, Acc) ->
           S(Id - 1, Acc + Fun(Id))
    end,

    ToWait = Send(Total, 0),

    Receive = fun
        R(0, ok) -> ok;
        R(Id, ok) -> receive ok -> R(Id - 1, ok) end
    end,

    ok = Receive(ToWait, ok).

-spec pmap(fun(), list()) -> list().
pmap(F, L) ->
    Parent = self(),
    Reference = make_ref(),
    lists:foldl(
        fun(X, N) ->
            spawn_link(fun() ->
                           Parent ! {pmap, Reference, N, F(X)}
                       end),
            N+1
        end, 0, L),
    L2 = [receive {pmap, Reference, N, R} -> {N, R} end || _ <- L],
    {_, L3} = lists:unzip(lists:keysort(1, L2)),
    L3.
