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
         api_request/2
        ]).

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

%% TODO: change to authorize/spec/cleanup unneeded cases
%% TODO: requires update for multi-delete
-spec authorize(#wm_reqdata{}, #context{}) -> {boolean(), #wm_reqdata{}, #context{}}.
authorize(RD, Ctx) ->
    riak_cs_wm_utils:bucket_access_authorize_helper(bucket, false, RD, Ctx).

-spec api_request(#wm_reqdata{}, #context{}) -> {ok, ?LORESP{}} | {error, term()}.
api_request(RD, Ctx=#context{bucket=Bucket,
                             user=User,
                             start_time=StartTime}) ->
    UserName = riak_cs_wm_utils:extract_name(User),
    riak_cs_dtrace:dt_bucket_entry(?MODULE, <<"list_keys">>, [], [UserName, Bucket]),
    Res = riak_cs_api:list_objects(
            [B || B <- riak_cs_utils:get_buckets(User),
                  B?RCS_BUCKET.name =:= binary_to_list(Bucket)],
            Ctx#context.bucket,
            get_max_keys(RD),
            get_options(RD),
            Ctx#context.riakc_pid),
    ok = riak_cs_stats:update_with_start(bucket_list_keys, StartTime),
    riak_cs_dtrace:dt_bucket_return(?MODULE, <<"list_keys">>, [200], [UserName, Bucket]),
    Res.

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
