%% -------------------------------------------------------------------
%%
%% stats_http_resource: publishing RiakCS runtime stats via HTTP
%%
%% Copyright (c) 2007-2010 Basho Technologies, Inc.  All Rights Reserved.
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
%% -------------------------------------------------------------------

-module(riak_cs_wm_stats).

%% webmachine resource exports
-export([
         init/1,
         ping/2,
         encodings_provided/2,
         content_types_provided/2,
         service_available/2,
         produce_body/2,
         pretty_print/2
        ]).

-include_lib("webmachine/include/wm_reqdata.hrl").

-record(ctx, {
          props,                                 % Static properties
          path_tokens :: [string()]
         }).

init(Props) ->
    {ok, #ctx{props = Props}}.

%% @spec encodings_provided(webmachine:wrq(), context()) ->
%%         {[encoding()], webmachine:wrq(), context()}
%% @doc Get the list of encodings this resource provides.
%%      "identity" is provided for all methods, and "gzip" is
%%      provided for GET as well
encodings_provided(RD, Context) ->
    case wrq:method(RD) of
        'GET' ->
            {[{"identity", fun(X) -> X end},
              {"gzip", fun(X) -> zlib:gzip(X) end}], RD, Context};
        _ ->
            {[{"identity", fun(X) -> X end}], RD, Context}
    end.

%% @spec content_types_provided(webmachine:wrq(), context()) ->
%%          {[ctype()], webmachine:wrq(), context()}
%% @doc Get the list of content types this resource provides.
%%      "application/json" and "text/plain" are both provided
%%      for all requests.  "text/plain" is a "pretty-printed"
%%      version of the "application/json" content.
content_types_provided(RD, Context) ->
    {[{"application/json", produce_body},
      {"text/plain", pretty_print}],
     RD, Context}.

ping(RD, Ctx) ->
    {pong, RD, Ctx#ctx{path_tokens = path_tokens(RD)}}.

service_available(RD, #ctx{path_tokens = ["stats"]} = Ctx) ->
    case riak_moss_utils:get_env(riak_moss, riak_cs_stat, false) of
        false ->
            {false, wrq:append_to_response_body("riak_cs_stat is disabled on this node.\n", RD),
             Ctx};
        true ->
            {true, RD, Ctx}
    end;
service_available(RD, Ctx) ->
    {false, wrq:append_to_response_body("Unrecognized path.\n", RD), Ctx}.

produce_body(RD, Ctx) ->
    Body = mochijson2:encode({struct, get_stats()}),
    {Body, RD, Ctx}.

%% @spec pretty_print(webmachine:wrq(), context()) ->
%%          {string(), webmachine:wrq(), context()}
%% @doc Format the respons JSON object is a "pretty-printed" style.
pretty_print(RD1, C1=#ctx{}) ->
    {Json, RD2, C2} = produce_body(RD1, C1),
    {riak_moss_utils:json_pp_print(lists:flatten(Json)), RD2, C2}.

get_stats() ->
    riak_cs_stats:get_stats().

path_tokens(RD) ->
    wrq:path_tokens(RD).
