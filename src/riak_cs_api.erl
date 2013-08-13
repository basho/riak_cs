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

-module(riak_cs_api).

-export([list_buckets/1,
         list_objects/5]).

-include("riak_cs.hrl").
-include("riak_cs_api.hrl").
-include("list_objects.hrl").

%% @doc Return a user's buckets.
-spec list_buckets(rcs_user()) -> ?LBRESP{}.
list_buckets(User=?RCS_USER{buckets=Buckets}) ->
    ?LBRESP{user=User,
            buckets=[Bucket || Bucket <- Buckets,
                               Bucket?RCS_BUCKET.last_action /= deleted]}.

-type options() :: [{atom(), 'undefined' | binary()}].
-spec list_objects([string()], binary(), non_neg_integer(), options(), pid()) ->
                          {ok, ?LORESP{}} | {error, term()}.
list_objects([], _, _, _, _) ->
    {error, no_such_bucket};
list_objects(_UserBuckets, _Bucket, {error, _}=Error, _Options, _RiakPid) ->
    Error;
list_objects(_UserBuckets, Bucket, MaxKeys, Options, RiakPid) ->
    ListKeysRequest = riak_cs_list_objects:new_request(Bucket,
                                                       MaxKeys,
                                                       Options),
    BinPid = riak_cs_utils:pid_to_binary(self()),
    CacheKey = << BinPid/binary, <<":">>/binary, Bucket/binary >>,
    UseCache = riak_cs_list_objects_ets_cache:cache_enabled(),
    case riak_cs_list_objects_utils:start_link(RiakPid,
                                               self(),
                                               ListKeysRequest,
                                               CacheKey,
                                               UseCache) of
        {ok, ListFSMPid} ->
            riak_cs_list_objects_utils:get_object_list(ListFSMPid);
        {error, _}=Error ->
            Error
    end.
