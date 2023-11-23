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

%% @doc WM resource for object version listing

-module(riak_cs_wm_object_versions).

-export([init/1,
         authorize/2,
         stats_prefix/0,
         allowed_methods/0,
         api_request/2
        ]).

-ignore_xref([init/1,
              authorize/2,
              stats_prefix/0,
              allowed_methods/0,
              api_request/2
             ]).


-include("riak_cs.hrl").
-include("riak_cs_web.hrl").

-define(RIAKCPOOL, bucket_list_pool).

-spec init(#rcs_web_context{}) -> {ok, #rcs_web_context{}}.
init(Ctx) ->
    {ok, Ctx#rcs_web_context{rc_pool = ?RIAKCPOOL}}.

-spec stats_prefix() -> list_object_versions.
stats_prefix() -> list_object_versions.

-spec allowed_methods() -> [atom()].
allowed_methods() ->
    ['GET'].

-spec authorize(#wm_reqdata{}, #rcs_web_context{}) -> {boolean(), #wm_reqdata{}, #rcs_web_context{}}.
authorize(RD, Ctx) ->
    riak_cs_wm_utils:bucket_access_authorize_helper(bucket, false, RD, Ctx).

-spec api_request(#wm_reqdata{}, #rcs_web_context{}) -> {ok, ?LOVRESP{}} | {error, term()}.
api_request(RD, Ctx = #rcs_web_context{riak_client = RcPid}) ->
    riak_cs_api:list_objects(
      versions,
      Ctx#rcs_web_context.bucket,
      get_max_keys(RD),
      get_options(RD),
      RcPid).

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
