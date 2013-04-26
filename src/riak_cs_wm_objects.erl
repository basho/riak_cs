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

-module(riak_cs_wm_objects).

-export([init/1,
         allowed_methods/0,
         content_types_provided/2,
         to_xml/2]).

-export([authorize/2]).

-include("riak_cs.hrl").
-include("riak_cs_api.hrl").
-include("list_objects.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-define(RIAKCPOOL, bucket_list_pool).

-spec init(#context{}) -> {ok, #context{}}.
init(Ctx) ->
    {ok, Ctx#context{riakc_pool=?RIAKCPOOL}}.

-spec allowed_methods() -> [atom()].
allowed_methods() ->
    %% TODO: POST (multi-delete)
    ['GET'].

-spec content_types_provided(#wm_reqdata{}, #context{}) -> {[{string(), atom()}], #wm_reqdata{}, #context{}}.
content_types_provided(RD,Ctx) ->
    {[{"application/xml", to_xml}], RD, Ctx}.


%% TODO: change to authorize/spec/cleanup unneeded cases
%% TODO: requires update for multi-delete
-spec authorize(#wm_reqdata{}, #context{}) -> {boolean(), #wm_reqdata{}, #context{}}.
authorize(RD, Ctx) ->
    riak_cs_wm_utils:bucket_access_authorize_helper(bucket, false, RD, Ctx).

-spec to_xml(#wm_reqdata{}, #context{}) ->
                    {binary() | {'halt', non_neg_integer()}, #wm_reqdata{}, #context{}}.
to_xml(RD, Ctx=#context{start_time=StartTime,
                        user=User,
                        bucket=Bucket,
                        requested_perm='READ',
                        riakc_pid=RiakPid}) ->
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"list_keys">>, [], [riak_cs_wm_utils:extract_name(User), Bucket]),
    StrBucket = binary_to_list(Bucket),
    case [B || B <- riak_cs_utils:get_buckets(User),
               B?RCS_BUCKET.name =:= StrBucket] of
        [] ->
            CodeName = no_such_bucket,
            Res = riak_cs_s3_response:api_error(CodeName, RD, Ctx),
            Code = riak_cs_s3_response:status_code(CodeName),
            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"list_keys">>, [Code], [riak_cs_wm_utils:extract_name(User), Bucket]),
            Res;
        [_BucketRecord] ->
            MaxKeys = get_max_keys(RD),
            case MaxKeys of
                _ when is_integer(MaxKeys) ->
                    _ = StartTime,
                    Options = get_options(RD),
                    ListKeysRequest = riak_cs_list_objects:new_request(Bucket,
                                                                       MaxKeys,
                                                                       Options),
                    BinPid = riak_cs_utils:pid_to_binary(self()),
                    CacheKey = << BinPid/binary, <<":">>/binary, Bucket/binary >>,
                    UseCache = riak_cs_list_objects_ets_cache:cache_enabled(),
                    %% Get the pid with the abstraction layer, that lets
                    %% us start either the old or the new version.
                    %% If we start the new version, some of the arguments
                    %% like `UseCache', `BinPid',  and `CacheKey' will be
                    %% ignored.
                    case riak_cs_list_objects_utils:start_link(RiakPid, self(),
                                                             ListKeysRequest, CacheKey,
                                                             UseCache) of
                        {ok, ListFSMPid} ->
                            {ok, ListObjectsResponse} = riak_cs_list_objects_fsm:get_object_list(ListFSMPid),
                            Response = riak_cs_xml:to_xml(ListObjectsResponse),
                            ok = riak_cs_stats:update_with_start(bucket_list_keys,
                                                                 StartTime),
                            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"list_keys">>, [200], [riak_cs_wm_utils:extract_name(User), Bucket]),
                            riak_cs_s3_response:respond(200, Response, RD, Ctx);
                        {error, Reason} ->
                            Code = riak_cs_s3_response:status_code(Reason),
                            Response = riak_cs_s3_response:api_error(Reason, RD, Ctx),
                            riak_cs_dtrace:dt_bucket_return(?MODULE, <<"list_keys">>, [Code], [riak_cs_wm_utils:extract_name(User), Bucket]),
                            Response
                    end;
                Reason ->
                    Code = riak_cs_s3_response:status_code(Reason),
                    Response = riak_cs_s3_response:api_error(Reason, RD, Ctx),
                    riak_cs_dtrace:dt_bucket_return(?MODULE, <<"list_keys">>, [Code], [riak_cs_wm_utils:extract_name(User), Bucket]),
                    Response
            end
    end.

-spec get_options(#wm_reqdata{}) -> [{atom(), 'undefined' | binary()}].
get_options(RD) ->
    [get_option(list_to_atom(Opt), wrq:get_qs_value(Opt, RD)) ||
        Opt <- ["delimiter", "marker", "prefix"]].

-spec get_option(atom(), 'undefined' | string()) -> {atom(), 'undefined' | binary()}.
get_option(Option, undefined) ->
    {Option, undefined};
get_option(Option, Value) ->
    {Option, list_to_binary(Value)}.

-spec get_max_keys(#wm_reqdata{}) -> non_neg_integer() | {error, 'invalid_argument'}.
get_max_keys(RD) ->
    case wrq:get_qs_value("max-keys", RD) of
        undefined ->
            ?DEFAULT_LIST_OBJECTS_MAX_KEYS;
        StringKeys ->
            try
                erlang:min(list_to_integer(StringKeys),
                           ?DEFAULT_LIST_OBJECTS_MAX_KEYS)
            catch _:_ ->
                    {error, invalid_argument}
            end
    end.
