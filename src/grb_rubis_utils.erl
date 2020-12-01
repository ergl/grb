-module(grb_rubis_utils).

%% API
-export([persist_configs/0,
         preload/2]).

-spec persist_configs() -> ok.
persist_configs() ->
    {ok, File} = application:get_env(grb, rubis_defaults_path),
    {ok, Terms} = file:consult(File),
    lists:foreach(fun({TermKey, TermValue}) ->
        persistent_term:put({?MODULE, TermKey}, TermValue)
    end, Terms).

-spec get_config(term()) -> term().
get_config(Key) ->
    persistent_term:get({?MODULE, Key}).

-spec preload(grb_promise:t(), map()) -> ok.
preload(Promise, Properties) ->
    _ = spawn_link(fun() ->
        grb_promise:resolve(preload_sync(Properties), Promise)
    end),
    ok.

%% todo(borja, rubis): Make client supply entire config (list of regions, etc)
%% so we don't have to worry about mismatched configs in two separate places
-spec preload_sync(map()) -> ok.
preload_sync(Properties) ->
    Regions = get_config(regions),
    ok = load_regions(Regions),

    CategoriesAndItems = case maps:get(category_profile, Properties, small) of
         small -> get_config(categories_small);
         _ -> get_config(categories)
    end,
    Categories = [ C || {C, _} <- CategoriesAndItems ],
    ok = load_categories(Categories),

    TotalUsers = maps:get(user_number, Properties),
    UsersPerRegion = (TotalUsers div length(Regions)),
    ok = load_users(get_config(regions), UsersPerRegion),

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
            store_item(Region, UserKey, Category, Id, #{
                item_max_quantity => maps:get(item_max_quantity, Properties),
                item_reserve_percentage => maps:get(item_reserve_percentage, Properties, 0),
                item_buy_now_percentage => maps:get(item_buy_now_percentage, Properties, 0),
                item_closed_percentage => maps:get(item_closed_percentage, Properties, 0),
                item_max_bids => maps:get(item_max_bids, Properties, 0),
                item_max_comments => maps:get(item_max_comments, Properties, 0),
                comment_max_len => maps:get(comment_max_len, Properties, 0)
            })
        end, NumberOfItems)
    end, CategoriesAndItems),
    ok.

-spec store_region(binary()) -> ok.
store_region(Region) ->
    ok = grb_oplog_vnode:put_direct_vnode(
        sync,
        grb_dc_utils:key_location(Region),
        #{
            {Region, regions, Region} => {grb_lww, Region}
        }
    ).

-spec store_category(binary()) -> ok.
store_category(Category) ->
    ok = grb_oplog_vnode:put_direct_vnode(
        sync,
        grb_dc_utils:key_location(Category),
        #{
            {Category, categories, Category} => {grb_lww, Category}
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
    Quantity = rand:uniform(MaxQuantity),

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

    %% fixme(borja): generate this
    %% MaxBids = maps:get(item_max_bids, ItemProperties),
    %% MaxComments = maps:get(item_max_comments, ItemProperties),
    %% CommentLength = maps:get(comment_max_len, ItemProperties),
    %% BidsN = rand:uniform(10),
    %% MaxBid = rand:uniform(10),

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

            %% Will be overwirten later, should be kept the same as the index size
            {Region, Table, ItemId, bids_number} => {grb_gcounter, 0},

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

    3.

%%%===================================================================
%%% Random Utils
%%%===================================================================

-spec random_binary(Size :: non_neg_integer()) -> binary().
random_binary(N) ->
    list_to_binary(random_string_(N)).

-spec random_email(Size :: non_neg_integer()) -> binary().
random_email(N) ->
    list_to_binary(random_string_(N) ++ "@example.com").

-spec random_rating() -> non_neg_integer().
random_rating() ->
    rand:uniform(11) - 6.

%%-spec random_boolean() -> boolean().
%%random_boolean() ->
%%    (rand:uniform(16) > 8).

-spec random_string_(Size :: non_neg_integer()) -> list().
random_string_(N) ->
    Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890",
    lists:foldl(fun(_, Acc) ->
        [lists:nth(rand:uniform(length(Chars)), Chars)] ++ Acc
    end, [], lists:seq(1, N)).

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
