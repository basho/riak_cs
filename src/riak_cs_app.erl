%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Callbacks for the riak_cs application.

-module(riak_cs_app).

-behaviour(application).

%% application API
-export([start/2,
         stop/1,
         sanity_check/2,
         check_bucket_props/2]).

-include("riak_cs.hrl").

-type start_type() :: normal | {takeover, node()} | {failover, node()}.
-type start_args() :: term().

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc application start callback for riak_cs.
-spec start(start_type(), start_args()) -> {ok, pid()} |
                                           {error, term()}.
start(_Type, _StartArgs) ->
    sanity_check(is_config_valid(),
                 check_bucket_props()).

%% @doc application stop callback for riak_cs.
-spec stop(term()) -> ok.
stop(_State) ->
    ok.

-spec sanity_check(boolean(), boolean()) -> {ok, pid()} | {error, term()}.
sanity_check(true, true) ->
    riak_cs_sup:start_link();
sanity_check(false, _) ->
    _ = lager:error("You must update your Riak CS app.config. Please see the"
                    "release notes for more information on updating you"
                    "configuration."),
    {error, bad_config};
sanity_check(true, false) ->
        _ = lager:error("Invalid Riak bucket properties detected. Please "
                        "verify that allow_mult is set to true for all "
                        "buckets."),
    {error, invalid_bucket_props}.

-spec is_config_valid() -> boolean().
is_config_valid() ->
    get_env_response_to_bool(application:get_env(riak_cs, connection_pools)).

-spec get_env_response_to_bool(term())  -> boolean().
get_env_response_to_bool({ok, _}) ->
    true;
get_env_response_to_bool(_) ->
    false.

-spec check_bucket_props() -> boolean().
check_bucket_props() ->
    Buckets = [?USER_BUCKET,
               ?ACCESS_BUCKET,
               ?STORAGE_BUCKET,
               ?BUCKETS_BUCKET],
    {ok, Riak} = riak_connection(),
    try
        lists:all(fun(X) -> X =:= ok end,
                  [check_bucket_props(Bucket, Riak) ||
                      Bucket <- Buckets])
    after
        riakc_pb_socket:stop(Riak)
    end.

-spec check_bucket_props(binary(), pid()) -> ok | {error, failed}.
check_bucket_props(Bucket, Riak) ->
    case catch riakc_pb_socket:get_bucket(Riak, Bucket) of
        {ok, Props} ->
            case lists:keyfind(allow_mult, 1, Props) of
                {allow_mult, true} ->
                    _ = lager:debug("~s bucket was"
                                    " already configured correctly.",
                                    [Bucket]),
                    ok;
                _ ->
                    _ = lager:warning("~p is misconfigured", [Bucket]),
                    {error, failed}
            end;
        {_error, Reason} ->
            _ = lager:warning(
                  "Unable to verify ~s bucket settings (~p).",
                  [Bucket, Reason]),
            {error, failed}
    end.

riak_connection() ->
    {Host, Port} = riak_host_port(),
    Timeout = case application:get_env(riak_cs, riakc_connect_timeout) of
                  {ok, ConfigValue} ->
                      ConfigValue;
                  undefined ->
                      10000
              end,
    StartOptions = [{connect_timeout, Timeout},
                    {auto_reconnect, true}],
    riakc_pb_socket:start_link(Host, Port, StartOptions).

%% @TODO This is duplicated code from
%% `riak_cs_riakc_pool_worker'. Move this to `riak_cs_config' once
%% that has been merged.
-spec riak_host_port() -> {string(), pos_integer()}.
riak_host_port() ->
    case application:get_env(riak_cs, riak_ip) of
        {ok, Host} ->
            ok;
        undefined ->
            Host = "127.0.0.1"
    end,
    case application:get_env(riak_cs, riak_pb_port) of
        {ok, Port} ->
            ok;
        undefined ->
            Port = 8087
    end,
    {Host, Port}.
