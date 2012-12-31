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

-module(riak_cs_wm_stats).

%% webmachine resource exports
-export([
         init/1,
         ping/2,
         encodings_provided/2,
         content_types_provided/2,
         service_available/2,
         forbidden/2,
         finish_request/2]).
-export([
         produce_body/2,
         pretty_print/2
        ]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/wm_reqdata.hrl").

-record(ctx, {
          auth_bypass :: boolean(),
          riakc_pid :: pid(),
          path_tokens :: [string()]
         }).

init(Props) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"init">>),
    AuthBypass = not proplists:get_value(admin_auth_enabled, Props),
    {ok, #ctx{auth_bypass = AuthBypass}}.

%% @spec encodings_provided(webmachine:wrq(), context()) ->
%%         {[encoding()], webmachine:wrq(), context()}
%% @doc Get the list of encodings this resource provides.
%%      "identity" is provided for all methods, and "gzip" is
%%      provided for GET as well
encodings_provided(RD, Context) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"encodings_provided">>),
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
%%
%%      NOTE: s3cmd doesn't appear to honor --add-header when using
%%            "get --add-header=Accept:text/plain s3://RiakCS/stats",
%%            so s3cmd will only be able to get the JSON flavor.

content_types_provided(RD, Context) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"content_types_provided">>),
    {[{"application/json", produce_body},
      {"text/plain", pretty_print}],
     RD, Context}.

ping(RD, Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"pong">>, [], []),
    {pong, RD, Ctx#ctx{path_tokens = path_tokens(RD)}}.

service_available(RD, #ctx{path_tokens = []} = Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"service_available">>),
    case riak_cs_config:riak_cs_stat() of
        false ->
            {false, RD, Ctx};
        true ->
            case riak_cs_utils:riak_connection() of
                {ok, Pid} ->
                    {true, RD, Ctx#ctx{riakc_pid = Pid}};
                _ ->
                    {false, RD, Ctx}
            end
    end;
service_available(RD, Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"service_available">>),
    {false, RD, Ctx}.

produce_body(RD, Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"produce_body">>),
    Body = mochijson2:encode(get_stats()),
    ETag = riak_cs_utils:etag_from_binary(crypto:md5(Body)),
    RD2 = wrq:set_resp_header("ETag", ETag, RD),
    riak_cs_dtrace:dt_wm_return(?MODULE, <<"produce_body">>),
    {Body, RD2, Ctx}.

forbidden(RD, #ctx{auth_bypass = AuthBypass, riakc_pid = RiakPid} = Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"forbidden">>),
    case riak_cs_wm_utils:validate_auth_header(RD, AuthBypass, RiakPid, undefined) of
        {ok, User, _UserObj} ->
            UserKeyId = User?RCS_USER.key_id,
            case riak_cs_config:admin_creds() of
                {ok, {Admin, _}} when Admin == UserKeyId ->
                    %% admin account is allowed
                    riak_cs_dtrace:dt_wm_return(?MODULE, <<"forbidden">>,
                                                [], [<<"false">>, Admin]),
                    {false, RD, Ctx};
                _ ->
                    %% non-admin account is not allowed -> 403
                    Res = riak_cs_wm_utils:deny_access(RD, Ctx),
                    riak_cs_dtrace:dt_wm_return(?MODULE, <<"forbidden">>, [], [<<"true">>]),
                    Res
            end;
        _ ->
            Res = riak_cs_wm_utils:deny_access(RD, Ctx),
            riak_cs_dtrace:dt_wm_return(?MODULE, <<"forbidden">>, [], [<<"true">>]),
            Res
    end.

finish_request(RD, #ctx{riakc_pid=undefined}=Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"finish_request">>, [0], []),
    {true, RD, Ctx};
finish_request(RD, #ctx{riakc_pid=RiakPid}=Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"finish_request">>, [1], []),
    riak_cs_utils:close_riak_connection(RiakPid),
    riak_cs_dtrace:dt_wm_return(?MODULE, <<"finish_request">>, [1], []),
    {true, RD, Ctx#ctx{riakc_pid=undefined}}.

%% @spec pretty_print(webmachine:wrq(), context()) ->
%%          {string(), webmachine:wrq(), context()}
%% @doc Format the respons JSON object is a "pretty-printed" style.
pretty_print(RD1, C1=#ctx{}) ->
    {Json, RD2, C2} = produce_body(RD1, C1),
    Body = riak_cs_utils:json_pp_print(lists:flatten(Json)),
    ETag = riak_cs_utils:etag_from_binary(crypto:md5(term_to_binary(Body))),
    RD3 = wrq:set_resp_header("ETag", ETag, RD2),
    {Body, RD3, C2}.

get_stats() ->
    riak_cs_stats:get_stats().

path_tokens(RD) ->
    wrq:path_tokens(RD).
