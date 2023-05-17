%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.  All Rights Reserved,
%%               2021, 2022 TI Tokyo    All Rights Reserved.
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
         stop/1]).

-include("riak_cs.hrl").
-include_lib("kernel/include/logger.hrl").

-type start_type() :: normal | {takeover, node()} | {failover, node()}.
-type start_args() :: term().

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc application start callback for riak_cs.
-spec start(start_type(), start_args()) -> {ok, pid()} |
                                           {error, term()}.
start(_Type, _StartArgs) ->
    riak_cs_config:warnings(),
    sanity_check(is_config_valid(),
                 check_admin_creds(),
                 ensure_bucket_props()).

%% @doc application stop callback for riak_cs.
-spec stop(term()) -> ok.
stop(_State) ->
    ok.

-spec check_admin_creds() -> ok | {error, term()}.
check_admin_creds() ->
    case riak_cs_config:admin_creds() of
        {ok, {"admin-key", _}} ->
            %% The default key
            logger:warning("admin.key is defined as default. Please create"
                           " admin user and configure it.", []),
            application:set_env(riak_cs, admin_secret, "admin-secret"),
            ok;
        {ok, {undefined, _}} ->
            logger:warning("The admin user's key id has not been specified."),
            {error, admin_key_undefined};
        {ok, {[], _}} ->
            logger:warning("The admin user's key id has not been specified."),
            {error, admin_key_undefined};
        {ok, {Key, undefined}} ->
            fetch_and_cache_admin_creds(Key);
        {ok, {Key, []}} ->
            fetch_and_cache_admin_creds(Key);
        {ok, {Key, _}} ->
            logger:warning("The admin user's secret is specified. Ignoring."),
            fetch_and_cache_admin_creds(Key)
    end.

fetch_and_cache_admin_creds(Key) ->
    fetch_and_cache_admin_creds(Key, _MaxRetries = 5, no_error).
fetch_and_cache_admin_creds(_Key, 0, Error) ->
    Error;
fetch_and_cache_admin_creds(Key, Attempt, _Error) ->
    %% Not using as the master pool might not be initialized
    {ok, MasterPbc} = riak_connection(),
    ?LOG_DEBUG("setting admin as ~s", [Key]),
    try
        %% Do we count this into stats?; This is a startup query and
        %% system latency is expected to be low. So get timeout can be
        %% low like 10% of configuration value.
        case riak_cs_pbc:get_sans_stats(MasterPbc, ?USER_BUCKET, iolist_to_binary(Key),
                                        [{notfound_ok, false}],
                                        riak_cs_config:get_user_timeout() div 10) of
            {ok, Obj} ->
                User = riak_cs_user:from_riakc_obj(Obj, false),
                Secret = User?RCS_USER.key_secret,
                application:set_env(riak_cs, admin_secret, Secret);
            Error ->
                logger:error("Couldn't get admin user (~s) record: ~p. Will retry ~b more times", [Key, Error, Attempt]),
                timer:sleep(3000),
                fetch_and_cache_admin_creds(Key, Attempt - 1, Error)
        end
    after
        riakc_pb_socket:stop(MasterPbc)
    end.

sanity_check(true, ok, ok) ->
    riak_cs_sup:start_link();
sanity_check(false, _, _) ->
    logger:error("You must update your Riak CS app.config. Please see the"
                 "release notes for more information on updating you configuration."),
    {error, bad_config};
sanity_check(_, {error, Reason}, _) ->
    logger:error("Admin credentials are not properly set: ~p.", [Reason]),
    {error, Reason}.

is_config_valid() ->
    get_env_response_to_bool(application:get_env(riak_cs, connection_pools)).

get_env_response_to_bool({ok, _}) ->
    true;
get_env_response_to_bool(_) ->
    false.

ensure_bucket_props() ->
    BucketsWithMultiTrue = [?USER_BUCKET,
                            ?ACCESS_BUCKET,
                            ?STORAGE_BUCKET,
                            ?BUCKETS_BUCKET],
    BucketsWithMultiFalse = [?SERVICE_BUCKET,
                             ?IAM_ROLE_BUCKET,
                             ?IAM_SAMLPROVIDER_BUCKET],
    %% %% Put atoms into atom table to suppress warning logs in `check_bucket_props'
    %% _PreciousAtoms = [riak_core_util, chash_std_keyfun,
    %%                   riak_kv_wm_link_walker, mapreduce_linkfun],
    {ok, Pbc} = riak_connection(),
    [riakc_pb_socket:set_bucket(Pbc, B, [{allow_mult, true}]) || B <- BucketsWithMultiTrue],
    [riakc_pb_socket:set_bucket(Pbc, B, [{allow_mult, false}]) || B <- BucketsWithMultiFalse],
    riakc_pb_socket:stop(Pbc).



riak_connection() ->
    {Host, Port} = riak_cs_config:riak_host_port(),
    Timeout = case application:get_env(riak_cs, riakc_connect_timeout) of
                  {ok, ConfigValue} ->
                      ConfigValue;
                  undefined ->
                      10000
              end,
    StartOptions = [{connect_timeout, Timeout},
                    {auto_reconnect, true}],
    riakc_pb_socket:start_link(Host, Port, StartOptions).
