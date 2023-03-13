%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
%%               2021-2022 TI Tokyo    All Rights Reserved.
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
-export([init/1,
         encodings_provided/2,
         content_types_provided/2,
         service_available/2,
         forbidden/2,
         finish_request/2,
         produce_body/2
        ]).
-ignore_xref([init/1,
              encodings_provided/2,
              content_types_provided/2,
              service_available/2,
              forbidden/2,
              finish_request/2,
              produce_body/2
             ]).

-export([pretty_print/2
        ]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").

init(Props) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"init">>),
    AuthBypass = not proplists:get_value(admin_auth_enabled, Props),
    {ok, #rcs_s3_context{auth_bypass = AuthBypass}}.

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

service_available(RD, Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"service_available">>),
    case riak_cs_config:riak_cs_stat() of
        false ->
            {false, RD, Ctx};
        true ->
            case riak_cs_riak_client:checkout() of
                {ok, Pid} ->
                    {true, RD, Ctx#rcs_s3_context{riak_client = Pid}};
                _ ->
                    {false, RD, Ctx}
            end
    end.

produce_body(RD, Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"produce_body">>),
    Body = mochijson2:encode(get_stats()),
    ETag = riak_cs_utils:etag_from_binary(riak_cs_utils:md5(Body)),
    RD2 = wrq:set_resp_header("ETag", ETag, RD),
    riak_cs_dtrace:dt_wm_return(?MODULE, <<"produce_body">>),
    {Body, RD2, Ctx}.

forbidden(RD, Ctx=#rcs_s3_context{auth_bypass=AuthBypass}) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"forbidden">>),
    riak_cs_wm_utils:find_and_auth_admin(RD, Ctx, AuthBypass).

finish_request(RD, #rcs_s3_context{riak_client=undefined}=Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"finish_request">>, [0], []),
    {true, RD, Ctx};
finish_request(RD, #rcs_s3_context{riak_client=RcPid}=Ctx) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"finish_request">>, [1], []),
    riak_cs_riak_client:checkin(RcPid),
    riak_cs_dtrace:dt_wm_return(?MODULE, <<"finish_request">>, [1], []),
    {true, RD, Ctx#rcs_s3_context{riak_client=undefined}}.

%% @spec pretty_print(webmachine:wrq(), context()) ->
%%          {string(), webmachine:wrq(), context()}
%% @doc Format the respons JSON object is a "pretty-printed" style.
pretty_print(RD1, C1=#rcs_s3_context{}) ->
    {Json, RD2, C2} = produce_body(RD1, C1),
    Body = riak_cs_utils:json_pp_print(lists:flatten(Json)),
    ETag = riak_cs_utils:etag_from_binary(riak_cs_utils:md5(term_to_binary(Body))),
    RD3 = wrq:set_resp_header("ETag", ETag, RD2),
    {Body, RD3, C2}.

get_stats() ->
    riak_cs_stats:get_stats().
