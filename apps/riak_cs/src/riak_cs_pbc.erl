%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% ---------------------------------------------------------------------

%% @doc Thin wrapper of `riakc_pb_socket'

-module(riak_cs_pbc).

-export([ping/3,
         get/6,
         repl_get/7,
         put/4,
         put/5,
         delete_obj/5,
         mapred/5,
         list_keys/4,
         get_index_eq/5,
         get_index_eq/6,
         get_index_range/7,
         get_cluster_id/1,
         check_connection_status/2,
         pause_to_reconnect/3
        ]).

%% Lower level APIs, which don't update stats.
-export([
         get_sans_stats/5,
         put_object/5,
         put_sans_stats/3,
         put_sans_stats/4,
         list_keys_sans_stats/3
         ]).

-include_lib("riakc/include/riakc.hrl").

-define(WITH_STATS(StatsKey, Statement),
        begin
            _ = riak_cs_stats:inflow(StatsKey),
            StartTime__with_stats = os:timestamp(),
            Result__with_stats = Statement,
            _ = riak_cs_stats:update_with_start(StatsKey, StartTime__with_stats,
                                                Result__with_stats),
            Result__with_stats
        end).

-spec ping(pid(), timeout(), riak_cs_stats:key()) -> pong.
ping(PbcPid, Timeout, StatsKey) ->
    _ = riak_cs_stats:inflow(StatsKey),
    StartTime = os:timestamp(),
    Result = riakc_pb_socket:ping(PbcPid, Timeout),
    case Result of
        pong ->
            _ = riak_cs_stats:update_with_start(StatsKey, StartTime);
        _ ->
            _ = riak_cs_stats:update_error_with_start(StatsKey, StartTime)
    end,
    Result.

%% @doc Get an object from Riak
-spec get_sans_stats(pid(), binary(), binary(), proplists:proplist(), timeout()) ->
                        {ok, riakc_obj:riakc_obj()} | {error, term()}.
get_sans_stats(PbcPid, BucketName, Key, Opts, Timeout)  ->
    riakc_pb_socket:get(PbcPid, BucketName, Key, Opts, Timeout).

-spec get(pid(), binary(), binary(), proplists:proplist(), timeout(),
          riak_cs_stats:key()) ->
                            {ok, riakc_obj:riakc_obj()} | {error, term()}.
get(PbcPid, BucketName, Key, Opts, Timeout, StatsKey) ->
    ?WITH_STATS(StatsKey, get_sans_stats(PbcPid, BucketName, Key, Opts, Timeout)).

-spec repl_get(pid(), binary(), binary(), binary(),
                          proplists:proplist(), timeout(), riak_cs_stats:key()) ->
                                 {ok, riakc_obj:riakc_obj()} | {error, term()}.
repl_get(PbcPid, BucketName, Key, ClusterID, Opts, Timeout, StatsKey) ->
    ?WITH_STATS(StatsKey,
                riak_repl_pb_api:get(PbcPid, BucketName, Key, ClusterID, Opts, Timeout)).

%% @doc Store an object in Riak
%% TODO: two `put_object' are without stats yet.
-spec put_object(pid(), binary(), undefined | binary(), binary(), [term()]) -> ok | {error, term()}.
put_object(_PbcPid, BucketName, undefined, Value, Metadata) ->
    error_logger:warning_msg("Attempt to put object into ~p with undefined key "
                             "and value ~P and dict ~p\n",
                             [BucketName, Value, 30, Metadata]),
    {error, bad_key};
put_object(PbcPid, BucketName, Key, Value, Metadata) ->
    RiakObject = riakc_obj:new(BucketName, Key, Value),
    NewObj = riakc_obj:update_metadata(RiakObject, Metadata),
    riakc_pb_socket:put(PbcPid, NewObj).

put_sans_stats(PbcPid, RiakcObj, Timeout) ->
    put_sans_stats(PbcPid, RiakcObj, [], Timeout).

put_sans_stats(PbcPid, RiakcObj, Options, Timeout) ->
    riakc_pb_socket:put(PbcPid, RiakcObj, Options, Timeout).

put(PbcPid, RiakcObj, Timeout, StatsKey) ->
    ?WITH_STATS(StatsKey, put_sans_stats(PbcPid, RiakcObj, [], Timeout)).

put(PbcPid, RiakcObj, Options, Timeout, StatsKey) ->
    ?WITH_STATS(StatsKey, put_sans_stats(PbcPid, RiakcObj, Options, Timeout)).

-spec delete_obj(pid(), riakc_obj:riakc_obj(), delete_options(),
                            non_neg_integer(),riak_cs_stats:key()) ->
                                   ok | {error, term()}.
delete_obj(PbcPid, RiakcObj, Options, Timeout, StatsKey) ->
    ?WITH_STATS(StatsKey,
                riakc_pb_socket:delete_obj(PbcPid, RiakcObj, Options, Timeout)).

-spec mapred(pid(), mapred_inputs(), [mapred_queryterm()], timeout(),
             riak_cs_stats:key()) ->
                    {ok, mapred_result()} |
                    {error, {badqterm, mapred_queryterm()}} |
                    {error, timeout} |
                    {error, term()}.
mapred(Pid, Inputs, Query, Timeout, StatsKey) ->
    ?WITH_STATS(StatsKey,
                riakc_pb_socket:mapred(Pid, Inputs, Query, Timeout)).

%% @doc List the keys from a bucket
-spec list_keys(pid(), binary(), timeout(), riak_cs_stats:key()) ->
                       {ok, [binary()]} | {error, term()}.
list_keys(PbcPid, BucketName, Timeout, StatsKey) ->
    ?WITH_STATS(StatsKey, list_keys_sans_stats(PbcPid, BucketName, Timeout)).

-spec list_keys_sans_stats(pid(), binary(), timeout()) -> {ok, [binary()]} | {error, term()}.
list_keys_sans_stats(PbcPid, BucketName, Timeout) ->
    case riakc_pb_socket:list_keys(PbcPid, BucketName, Timeout) of
        {ok, Keys} ->
            %% TODO:
            %% This is a naive implementation,
            %% the longer-term solution is likely
            %% going to involve 2i and merging the
            %% results from each of the vnodes.
            {ok, lists:sort(Keys)};
        {error, _}=Error ->
            Error
    end.

-spec get_index_eq(pid(), bucket(), binary() | secondary_index_id(), key() | integer(),
                   riak_cs_stats:key()) ->
                          {ok, index_results()} | {error, term()}.
get_index_eq(PbcPid, Bucket, Index, Key, StatsKey) ->
    ?WITH_STATS(StatsKey,
                riakc_pb_socket:get_index_eq(PbcPid, Bucket, Index, Key)).

-spec get_index_eq(pid(), bucket(), binary() | secondary_index_id(), key() | integer(),
                   list(), riak_cs_stats:key()) ->
                       {ok, index_results()} | {error, term()}.
get_index_eq(PbcPid, Bucket, Index, Key, Opts, StatsKey) ->
    ?WITH_STATS(StatsKey,
                riakc_pb_socket:get_index_eq(PbcPid, Bucket, Index, Key, Opts)).

-spec get_index_range(pid(), bucket(), binary() | secondary_index_id(),
                      key() | integer() | list(),
                      key() | integer() | list(),
                      list(),
                      riak_cs_stats:key()) ->
                             {ok, index_results()} | {error, term()}.
get_index_range(PbcPid, Bucket, Index, StartKey, EndKey, Opts, StatsKey) ->
    ?WITH_STATS(StatsKey,
                riakc_pb_socket:get_index_range(PbcPid, Bucket, Index,
                                                StartKey, EndKey, Opts)).

%% @doc Attempt to determine the cluster id
-spec get_cluster_id(pid()) -> undefined | binary().
get_cluster_id(Pbc) ->
    StatsKey = [riakc, get_clusterid],
    Res = ?WITH_STATS(StatsKey, get_cluster_id_sans_stats(Pbc)),
    case Res of
        {ok, ClusterID} -> ClusterID;
        {error, _} -> undefined
    end.

get_cluster_id_sans_stats(Pbc) ->
    Timeout = riak_cs_config:cluster_id_timeout(),
    try
        riak_repl_pb_api:get_clusterid(Pbc, Timeout)
    catch C:R ->
            %% Disable `proxy_get' so we do not repeatedly have to
            %% handle this same exception. This would happen if an OSS
            %% install has `proxy_get' enabled.
            application:set_env(riak_cs, proxy_get, disabled),
            {error, {C, R}}
    end.

%% @doc don't reuse return value
-spec check_connection_status(pid(), term()) -> any().
check_connection_status(Pbc, Where) ->
    try
        case riakc_pb_socket:is_connected(Pbc) of
            true -> ok;
            Other ->
                _ = lager:warning("Connection status of ~p at ~p: ~p",
                                  [Pbc, Where, Other])
        end
    catch
        Type:Error ->
            _ = lager:warning("Connection status of ~p at ~p: ~p",
                              [Pbc, Where, {Type, Error}])
    end.

%% @doc Pause for a while so that underlying `riaic_pb_socket' can have
%%      room for reconnection.
%%
%% If `Reason' (second argument) is `timeout' or `disconnected', loop
%% until the connection will be reconnected. Otherwise, do nothing.
%% This function return always `ok' even in the case of timeout.
-spec pause_to_reconnect(pid(), term(), non_neg_integer()) -> ok.
pause_to_reconnect(Pbc, Reason, Timeout)
  when Reason =:= timeout orelse Reason =:= disconnected ->
    pause_to_reconnect0(Pbc, Timeout, os:timestamp());
pause_to_reconnect(_Pbc, _Other, _Timeout) ->
    ok.

pause_to_reconnect0(Pbc, Timeout, Start) ->
    lager:debug("riak_cs_pbc:pause_to_reconnect0"),
    case riakc_pb_socket:is_connected(Pbc, ?FIRST_RECONNECT_INTERVAL) of
        true -> ok;
        {false, _} ->
            Remaining = Timeout - timer:now_diff(os:timestamp(), Start) div 1000,
            case Remaining of
                Positive when 0 < Positive ->
                    %% sleep to avoid busy-loop calls of `is_connected'
                    _ = timer:sleep(?FIRST_RECONNECT_INTERVAL),
                    pause_to_reconnect0(Pbc, Timeout, Start);
                _ ->
                    ok
            end
    end.
