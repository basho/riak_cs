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
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").
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
    ok = is_config_valid(),
    ok = ensure_bucket_props(Pbc),
    {ok, PostFun} = check_admin_creds(Pbc),
    Ret = {ok, _Pid} = riak_cs_sup:start_link(),
    ok = PostFun(),
    ok = riakc_pb_socket:stop(Pbc),
    Ret.



%% @doc application stop callback for riak_cs.
-spec stop(term()) -> ok.
stop(_State) ->
    ok.


is_config_valid() ->
    case application:get_env(riak_cs, connection_pools) of
        {ok, _} ->
            ok;
        _ ->
            logger:error("connection_pools is not set"),
            not_ok
    end.


ensure_bucket_props(Pbc) ->
    BucketsWithMultiTrue = [?ACCESS_BUCKET,
                            ?STORAGE_BUCKET],
    BucketsWithMultiFalse = [?USER_BUCKET,
                             ?BUCKETS_BUCKET,
                             ?SERVICE_BUCKET,
                             ?IAM_ROLE_BUCKET,
                             ?IAM_POLICY_BUCKET,
                             ?IAM_SAMLPROVIDER_BUCKET,
                             ?TEMP_SESSIONS_BUCKET],
    %% %% Put atoms into atom table to suppress warning logs in `check_bucket_props'
    %% _PreciousAtoms = [riak_core_util, chash_std_keyfun,
    %%                   riak_kv_wm_link_walker, mapreduce_linkfun],
    [riakc_pb_socket:set_bucket(Pbc, B, [{allow_mult, true}]) || B <- BucketsWithMultiTrue],
    [riakc_pb_socket:set_bucket(Pbc, B, [{allow_mult, false}]) || B <- BucketsWithMultiFalse],
    ?LOG_DEBUG("ensure_bucket_props done"),
    ok.

check_admin_creds(Pbc) ->
    case riak_cs_config:admin_creds() of
        {ok, {<<"admin-key">>, _}} ->
            %% The default key
            logger:warning("admin.key is defined as default. Please create"
                           " admin user and configure it.", []),
            application:set_env(riak_cs, admin_secret, <<"admin-secret">>),
            {ok, nop()};
        {ok, {undefined, _}} ->
            logger:error("The admin user's key id has not been specified."),
            {error, admin_key_undefined};
        {ok, {<<>>, _}} ->
            logger:error("The admin user's key id has not been specified."),
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
        {ok, ?IAM_USER{key_secret = SAK}, PostFun} ->
            application:set_env(riak_cs, admin_secret, SAK),
            {ok, PostFun};
        Error ->
            logger:warning("Couldn't get admin user (~s) record: ~p. Will retry ~b more times",
                           [Key, Error, Attempt]),
            timer:sleep(3000),
            fetch_and_cache_admin_creds(Key, Pbc, Attempt - 1, Error)
    end.


find_user(A, Pbc) ->
    case riakc_pb_socket:get_index_eq(Pbc, ?USER_BUCKET, ?USER_KEYID_INDEX, A) of
        {ok, ?INDEX_RESULTS{keys = []}} ->
            %% try_get_user_pre_3_2
            maybe_get_3_1_user_with_policy(A, Pbc);
        {ok, ?INDEX_RESULTS{keys = [Arn|_]}} ->
            case get_user(Arn, Pbc) of
                {ok, User} ->
                    {ok, User, nop()};
                ER ->
                    ER
            end;
        {error, Reason} ->
            logger:warning("Riak client connection error while finding user ~s: ~p", [A, Reason]),
            {error, Reason}
    end.

maybe_get_3_1_user_with_policy(KeyForArn, Pbc) ->
    case get_user(KeyForArn, Pbc) of
        {ok, OldAdminUser = ?IAM_USER{arn = AdminArn,
                                      name = AdminUserName,
                                      key_id = KeyId}} ->
            logger:notice("Found pre-3.2 admin user; "
                          "converting it to rcs_user_v3, attaching an admin policy and saving", []),
            ?LOG_DEBUG("Converted admin: ~p", [OldAdminUser]),

            ?LOG_DEBUG("deleting old admin user", []),
            ok = riakc_pb_socket:delete(Pbc, ?USER_BUCKET, KeyId, ?CONSISTENT_DELETE_OPTIONS),
            ?LOG_DEBUG("recreating it as rcs_user_v3"),
            Meta = dict:store(?MD_INDEX, riak_cs_utils:object_indices(OldAdminUser), dict:new()),
            Obj = riakc_obj:update_metadata(
                    riakc_obj:new(?USER_BUCKET, AdminArn, term_to_binary(OldAdminUser)),
                    Meta),
            ok = riakc_pb_socket:put(Pbc, Obj, ?CONSISTENT_WRITE_OPTIONS),

            CompleteAdminConversion =
                fun() ->
                        ?LOG_DEBUG("Attaching admin policy to the recreated admin"),
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
                                                        policy_document => D = jsx:encode(AdminPolicyDocument)}),
                        ?LOG_DEBUG("Attached policy: ~p", [D]),
                        ok = riak_cs_iam:attach_user_policy(PolicyArn, AdminUserName, Pbc),
                        ok
                end,
            {ok, OldAdminUser, CompleteAdminConversion};
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


riak_connection() ->
    {Host, Port} = riak_cs_config:riak_host_port(),
    Timeout = application:get_env(riak_cs, riakc_connect_timeout, 10_000),
    StartOptions = [{connect_timeout, Timeout},
                    {auto_reconnect, true}],
    riakc_pb_socket:start_link(Host, Port, StartOptions).

nop() ->
    fun() -> ok end.
