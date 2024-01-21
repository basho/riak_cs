%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved.
%% @doc Support multi Riak clusters in single Riak CS system

-module(riak_cs_multibag).

-export([process_specs/0, choose_bag_id/2]).
-export([bags/0, cluster_id/2]).
-export([tab_name/0, tab_info/0]).
-export([bag_id_from_bucket/1]).

-export_type([weight_info/0]).

-define(ETS_TAB, ?MODULE).

-record(bag, {bag_id :: bag_id(),
              pool_name :: atom(),
              address :: string(),
              port :: non_neg_integer(),
              cluster_id :: binary() | undefined}).

-include("riak_cs_multibag.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").

-type weight_info() :: #weight_info{}.

process_specs() ->
    maybe_init(),
    BagServer = #{id => riak_cs_multibag_server,
                  start => {riak_cs_multibag_server, start_link, []}},
    %% Pass connection open/close information not to "explicitly" depends on riak_cs
    %% and to make unit test easier.
    %% TODO: Pass these MF's by argument process_specs
    WeightUpdaterArgs = [{conn_open_mf, {riak_cs_utils, riak_connection}},
                         {conn_close_mf, {riak_cs_utils, close_riak_connection}}],
    WeightUpdater = #{id => riak_cs_multibag_weight_updater,
                      start => {riak_cs_multibag_weight_updater, start_link, [WeightUpdaterArgs]}},
    [BagServer, WeightUpdater].

%% Choose bag ID for new bucket or new manifest
-spec choose_bag_id(manifet | block, term()) -> bag_id().
choose_bag_id(AllocType, Seed) ->
    {ok, BagId} = riak_cs_multibag_server:choose_bag(AllocType, Seed),
    BagId.

-spec bags() -> [{bag_id(), Address::string(), Port::non_neg_integer()}].
bags() ->
    maybe_init(),
    [{BagId, Address, Port} ||
        #bag{bag_id=BagId, pool_name=_Name, address=Address, port=Port} <-
            ets:tab2list(?ETS_TAB)].

-spec cluster_id(fun(), bag_id()) -> undefined | binary().
cluster_id(GetClusterIdFun, undefined) ->
    cluster_id(GetClusterIdFun, <<"master">>);
cluster_id(GetClusterIdFun, BagId) ->
    case ets:lookup(?ETS_TAB, BagId) of
        [#bag{cluster_id=ClusterId}] when is_binary(ClusterId) ->
            ClusterId;
        [Bag] ->
            ClusterId = GetClusterIdFun(BagId),
            true = ets:insert(?ETS_TAB, Bag#bag{cluster_id=ClusterId}),
            ClusterId
    end.

%% Extract bag ID from `riakc_obj' in moss.buckets
-spec bag_id_from_bucket(riakc_obj:riakc_obj()) -> bag_id().
bag_id_from_bucket(BucketObj) ->
    Contents = riakc_obj:get_contents(BucketObj),
    bag_id_from_contents(Contents).

maybe_init() ->
    case catch ets:info(?ETS_TAB) of
        undefined -> init();
        _ -> ok
    end.

init() ->
    _Tid = init_ets(),
    {ok, Bags} = application:get_env(riak_cs_multibag, bags),
    {MasterAddress, MasterPort} = riak_cs_config:riak_host_port(),
    ok = store_pool_record({"master", MasterAddress, MasterPort}),
    ok = store_pool_records(Bags),
    ok.

init_ets() ->
    ets:new(?ETS_TAB, [{keypos, #bag.bag_id},
                       named_table, public, {read_concurrency, true}]).

store_pool_records([]) ->
    ok;
store_pool_records([Bag | RestBags]) ->
    ok = store_pool_record(Bag),
    store_pool_records(RestBags).

store_pool_record({BagIdStr, Address, Port}) ->
    BagId = list_to_binary(BagIdStr),
    Name = riak_cs_riak_client:pbc_pool_name(BagId),
    true = ets:insert_new(?ETS_TAB, #bag{bag_id = BagId,
                                         pool_name = Name,
                                         address = Address,
                                         port = Port}),
    ok.

bag_id_from_contents([]) ->
    undefined;
bag_id_from_contents([{MD, _} | Contents]) ->
    case bag_id_from_meta(dict:fetch(?MD_USERMETA, MD)) of
        undefined ->
            bag_id_from_contents(Contents);
        BagId ->
            BagId
    end.

bag_id_from_meta([]) ->
    undefined;
bag_id_from_meta([{?MD_BAG, Value} | _]) ->
    binary_to_term(Value);
bag_id_from_meta([_MD | MDs]) ->
    bag_id_from_meta(MDs).

%% For Debugging

tab_name() ->
    ?ETS_TAB.

tab_info() ->
    ets:tab2list(?ETS_TAB).
