%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2023 TI Tokyo    All Rights Reserved.
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

-module(riak_cs_aws_iam_rewrite).
-behaviour(riak_cs_rewrite).

-export([rewrite/5,
         original_resource/1,
         raw_url/1]).

-include("riak_cs.hrl").
-include("aws_api.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("kernel/include/logger.hrl").

-spec rewrite(atom(), atom(), {integer(), integer()}, mochiweb_headers(), string()) ->
          {mochiweb_headers(), string()}.
rewrite(Method, _Scheme, _Vsn, Headers, Url) ->
    {Path, QueryString, _} = mochiweb_util:urlsplit_path(Url),
    rewrite_path_and_headers(Method, Headers, Url, Path, QueryString).


-spec original_resource(#wm_reqdata{}) -> undefined | {string(), [{term(),term()}]}.
original_resource(RD) ->
    riak_cs_rewrite:original_resource(RD).

-spec raw_url(#wm_reqdata{}) -> undefined | {string(), [{term(), term()}]}.
raw_url(RD) ->
    riak_cs_rewrite:raw_url(RD).

rewrite_path_and_headers(Method, Headers, Url, Path, QueryString) ->
    RewrittenPath =
        rewrite_path(Method, Path, QueryString),
    RewrittenHeaders =
        mochiweb_headers:default(
          ?RCS_RAW_URL_HEADER, Url,
          mochiweb_headers:default(
            ?RCS_REWRITE_HEADER, Url,
            Headers)),
    {RewrittenHeaders, RewrittenPath}.


%% All IAM requests appear to be POSTs without a resource
%% path. Instead of attempting to rewrite path and map those POSTs to
%% something we may deem more logical, let's just skip rewriting
%% altogether. In riak_cs_wm_iam.erl, we read the www-form and handle
%% the request based on presented Action.
rewrite_path(_Method, "/", _QS) ->
    "/iam".
