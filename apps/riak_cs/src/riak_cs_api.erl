%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
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

-module(riak_cs_api).

-export([list_buckets/1,
         list_objects/6,
         list_users/2,
         list_roles/2,
         list_policies/2,
         list_saml_providers/2
        ]).

-include("riak_cs.hrl").
-include("riak_cs_web.hrl").
-include_lib("kernel/include/logger.hrl").

%% @doc Return a user's buckets.
-spec list_buckets(rcs_user()) -> ?LBRESP{}.
list_buckets(User = ?RCS_USER{buckets=Buckets}) ->
    ?LBRESP{user = User,
            buckets = [Bucket || Bucket <- Buckets,
                                 Bucket?RCS_BUCKET.last_action /= deleted]}.

-spec list_objects(list_objects_req_type(), [string()], binary(), non_neg_integer(), proplists:proplist(), riak_client()) ->
          {ok, list_objects_response() | list_object_versions_response()} | {error, term()}.
list_objects(_, [], _, _, _, _) ->
    {error, no_such_bucket};
list_objects(_, _UserBuckets, _Bucket, {error, _} = Error, _Options, _RcPid) ->
    Error;
list_objects(ReqType, _UserBuckets, Bucket, MaxKeys, Options, RcPid) ->
    Request = riak_cs_list_objects:new_request(
                ReqType, Bucket, MaxKeys, Options),
    case riak_cs_list_objects_fsm_v2:start_link(RcPid, Request) of
        {ok, ListFSMPid} ->
            riak_cs_list_objects_utils:get_object_list(ListFSMPid);
        {error, _} = Error ->
            Error
    end.

-spec list_users(riak_client(), #list_users_request{}) ->
          {ok, maps:map()} | {error, term()}.
list_users(RcPid, #list_users_request{path_prefix = PathPrefix,
                                      max_items = MaxItems,
                                      marker = Marker}) ->
    Arg = #{path_prefix => PathPrefix,
            max_items => MaxItems,
            marker => Marker},
    {ok, MasterPbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riakc_pb_socket:mapred_bucket(
           MasterPbc, ?IAM_USER_BUCKET, mapred_query(users, Arg)) of
        {ok, Batches} ->
            {ok, #{users => extract_objects(Batches, []),
                   marker => undefined,
                   is_truncated => false}};
        {error, _} = ER ->
            ER
    end.


-spec list_roles(riak_client(), #list_roles_request{}) ->
          {ok, maps:map()} | {error, term()}.
list_roles(RcPid, #list_roles_request{path_prefix = PathPrefix,
                                      max_items = MaxItems,
                                      marker = Marker}) ->
    Arg = #{path_prefix => PathPrefix,
            max_items => MaxItems,
            marker => Marker},
    {ok, MasterPbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riakc_pb_socket:mapred_bucket(
           MasterPbc, ?IAM_ROLE_BUCKET, mapred_query(roles, Arg)) of
        {ok, Batches} ->
            {ok, #{roles => extract_objects(Batches, []),
                   marker => undefined,
                   is_truncated => false}};
        {error, _} = ER ->
            ER
    end.


-spec list_policies(riak_client(), #list_policies_request{}) ->
          {ok, maps:map()} | {error, term()}.
list_policies(RcPid, #list_policies_request{path_prefix = PathPrefix,
                                            only_attached = OnlyAttached,
                                            policy_usage_filter = PolicyUsageFilter,
                                            scope = Scope,
                                            max_items = MaxItems,
                                            marker = Marker}) ->
    Arg = #{path_prefix => PathPrefix,
            only_attached => OnlyAttached,
            policy_usage_filter => PolicyUsageFilter,
            scope => Scope,
            max_items => MaxItems,
            marker => Marker},
    {ok, MasterPbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riakc_pb_socket:mapred_bucket(
           MasterPbc, ?IAM_POLICY_BUCKET, mapred_query(policies, Arg)) of
        {ok, Batches} ->
            {ok, #{policies => extract_objects(Batches, []),
                   marker => undefined,
                   is_truncated => false}};
        {error, _} = ER ->
            ER
    end.


-spec list_saml_providers(riak_client(), #list_saml_providers_request{}) ->
          {ok, maps:map()} | {error, term()}.
list_saml_providers(RcPid, #list_saml_providers_request{}) ->
    Arg = #{},
    {ok, MasterPbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riakc_pb_socket:mapred_bucket(
           MasterPbc, ?IAM_SAMLPROVIDER_BUCKET, mapred_query(saml_providers, Arg)) of
        {ok, Batches} ->
            {ok, #{saml_providers => extract_objects(Batches, [])}};
        {error, _} = ER ->
            ER
    end.


extract_objects([], Q) ->
    Q;
extract_objects([{_N, RR}|Rest], Q) ->
    extract_objects(Rest, Q ++ RR).

mapred_query(users, Arg) ->
    [{map, {modfun, riak_cs_utils, map_users},
      Arg, false},
     {reduce, {modfun, riak_cs_utils, reduce_users},
      Arg, true}];
mapred_query(roles, Arg) ->
    [{map, {modfun, riak_cs_utils, map_roles},
      Arg, false},
     {reduce, {modfun, riak_cs_utils, reduce_roles},
      Arg, true}];
mapred_query(policies, Arg) ->
    [{map, {modfun, riak_cs_utils, map_policies},
      Arg, false},
     {reduce, {modfun, riak_cs_utils, reduce_policies},
      Arg, true}];
mapred_query(saml_providers, Arg) ->
    [{map, {modfun, riak_cs_utils, map_saml_providers},
      Arg, false},
     {reduce, {modfun, riak_cs_utils, reduce_saml_providers},
      Arg, true}].
