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
         list_objects/5
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

-spec list_objects(list_objects_req_type(), binary(), non_neg_integer(), proplists:proplist(), riak_client()) ->
          {ok, list_objects_response() | list_object_versions_response()} | {error, term()}.
list_objects(_, _Bucket, {error, _} = Error, _Options, _RcPid) ->
    Error;
list_objects(ReqType, Bucket, MaxKeys, Options, RcPid) ->
    Request = riak_cs_list_objects:new_request(
                ReqType, Bucket, MaxKeys, Options),
    case riak_cs_list_objects_fsm_v2:start_link(RcPid, Request) of
        {ok, ListFSMPid} ->
            riak_cs_list_objects_utils:get_object_list(ListFSMPid);
        {error, _} = Error ->
            Error
    end.
