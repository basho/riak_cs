%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.  All Rights Reserved,
%%               2021-2023 TI Tokyo    All Rights Reserved.
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
-include_lib("riakc/include/riakc.hrl").
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
    {ok, Pbc} = riak_connection(),
    try
        sanity_check(is_config_valid(),
                     check_admin_creds(Pbc),
                     ensure_bucket_props(Pbc))
    after
        ok = riakc_pb_socket:stop(Pbc)
    end.

%% @doc application stop callback for riak_cs.
-spec stop(term()) -> ok.
stop(_State) ->
    ok.

check_admin_creds(Pbc) ->
    case riak_cs_config:admin_creds() of
        {ok, {<<"admin-key">>, _}} ->
            %% The default key
            logger:warning("admin.key is defined as default. Please create"
                           " admin user and configure it.", []),
            application:set_env(riak_cs, admin_secret, <<"admin-secret">>),
            ok;
        {ok, {undefined, _}} ->
            logger:warning("The admin user's key id has not been specified."),
            {error, admin_key_undefined};
        {ok, {<<>>, _}} ->
            logger:warning("The admin user's key id has not been specified."),
            {error, admin_key_undefined};
        {ok, {Key, undefined}} ->
            fetch_and_cache_admin_creds(Key, Pbc);
        {ok, {Key, <<>>}} ->
            fetch_and_cache_admin_creds(Key, Pbc);
        {ok, {Key, _}} ->
            logger:warning("The admin user's secret is specified. Ignoring."),
            fetch_and_cache_admin_creds(Key, Pbc)
    end.

fetch_and_cache_admin_creds(Key, Pbc) ->
    fetch_and_cache_admin_creds(Key, Pbc, _MaxRetries = 5, no_error).
fetch_and_cache_admin_creds(_Key, _, 0, Error) ->
    Error;
fetch_and_cache_admin_creds(Key, Pbc, Attempt, _Error) ->
    case find_user(Key, Pbc) of
        {ok, ?IAM_USER{key_secret = SAK}} ->
            application:set_env(riak_cs, admin_secret, SAK);
        Error ->
            logger:warning("Couldn't get admin user (~s) record: ~p. Will retry ~b more times",
                           [Key, Error, Attempt]),
            timer:sleep(3000),
            fetch_and_cache_admin_creds(Key, Pbc, Attempt - 1, Error)
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

ensure_bucket_props(Pbc) ->
    BucketsWithMultiTrue = [?USER_BUCKET,
                            ?ACCESS_BUCKET,
                            ?STORAGE_BUCKET,
                            ?BUCKETS_BUCKET],
    BucketsWithMultiFalse = [?SERVICE_BUCKET,
                             ?IAM_ROLE_BUCKET,
                             ?IAM_POLICY_BUCKET,
                             ?IAM_SAMLPROVIDER_BUCKET,
                             ?TEMP_SESSIONS_BUCKET],
    %% %% Put atoms into atom table to suppress warning logs in `check_bucket_props'
    %% _PreciousAtoms = [riak_core_util, chash_std_keyfun,
    %%                   riak_kv_wm_link_walker, mapreduce_linkfun],
    [riakc_pb_socket:set_bucket(Pbc, B, [{allow_mult, true}]) || B <- BucketsWithMultiTrue],
    [riakc_pb_socket:set_bucket(Pbc, B, [{allow_mult, false}]) || B <- BucketsWithMultiFalse],
    ok.


riak_connection() ->
    {Host, Port} = riak_cs_config:riak_host_port(),
    Timeout = application:get_env(riak_cs, riakc_connect_timeout, 10_000),
    StartOptions = [{connect_timeout, Timeout},
                    {auto_reconnect, true}],
    riakc_pb_socket:start_link(Host, Port, StartOptions).

find_user(A, Pbc) ->
    Res = riakc_pb_socket:get_index_eq(Pbc, ?USER_BUCKET, ?USER_KEYID_INDEX, A),
    case Res of
        {ok, ?INDEX_RESULTS{keys = []}} ->
            %% try_get_user_pre_3_2
            maybe_get_3_1_user_with_policy(A, Pbc);
        {ok, ?INDEX_RESULTS{keys = [Arn|_]}} ->
            get_user(Arn, Pbc);
        {error, Reason} ->
            logger:warning("Riak client connection error while finding user ~s: ~p", [A, Reason]),
            {error, Reason}
    end.

maybe_get_3_1_user_with_policy(KeyForArn, Pbc) ->
    case get_user(KeyForArn, Pbc) of
        {ok, OldAdminUser = ?IAM_USER{name = AdminUserName}} ->
            %% without updatng the user first, attach_user_policy won't find it
            {ok, NewAdminUser} = riak_cs_iam:update_user(OldAdminUser),
            %% now it is saved properly, by arn, and will be found as such

            logger:notice("Found pre-3.2 admin user; "
                          "converting it to rcs_user_v3, attaching an admin policy and saving", []),

            AdminPolicyDocument =
                #{<<"Version">> => <<"2012-10-17">>,
                  <<"Statement">> => [#{<<"Principal">> => <<"*">>,
                                        <<"Effect">> => <<"Allow">>,
                                        <<"Action">> => [<<"sts:*">>, <<"iam:*">>, <<"s3:*">>],
                                        <<"Resource">> => <<"*">>
                                       }
                                     ]
                 },
            {ok, ?IAM_POLICY{arn = PolicyArn}} =
                riak_cs_iam:create_policy(#{policy_name => <<"AdminPolicy">>,
                                            policy_document => jsx:encode(AdminPolicyDocument)}),
            ok = riak_cs_iam:attach_user_policy(PolicyArn, AdminUserName),
            {ok, NewAdminUser};
        ER ->
            ER
    end.

get_user(Arn, Pbc) ->
    case riak_cs_pbc:get_sans_stats(Pbc, ?USER_BUCKET, Arn,
                                    [{notfound_ok, false}],
                                    riak_cs_config:get_user_timeout()) of
        {ok, Obj} ->
            {ok, riak_cs_user:from_riakc_obj(Obj, _KeepDeletedBuckets = false)};
        ER ->
            ER
    end.

