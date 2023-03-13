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

-module(riak_cs_rewrite).

-callback rewrite(Method::atom(), Scheme::atom(), Vsn::{integer(), integer()},
                  mochiweb_headers(), Url::string()) ->
    {mochiweb_headers(), string()}.

-export([rewrite/5,
         original_resource/1,
         raw_url/1
        ]).

-include("riak_cs.hrl").
-include("riak_cs_web.hrl").
-include_lib("webmachine/include/webmachine.hrl").


%% no-rewrite, for calls directly to riak_cs
-spec rewrite(Method::atom(), Scheme::atom(), Vsn::{integer(), integer()},
              mochiweb_headers(), Url::string()) ->
    {mochiweb_headers(), string()}.
rewrite(_Method, _Scheme, _Vsn, Headers, Url) ->
    {Path, _QS, _} = mochiweb_util:urlsplit_path(Url),
    RewrittenHeaders = mochiweb_headers:default(
                         ?RCS_RAW_URL_HEADER, Url, Headers),
    {RewrittenHeaders, Path}.


-spec original_resource(#wm_reqdata{}) -> undefined | {string(), [{term(), term()}]}.
original_resource(RD) ->
    case wrq:get_req_header(?RCS_REWRITE_HEADER, RD) of
        undefined -> undefined;
        RawPath ->
            {Path, QS, _} = mochiweb_util:urlsplit_path(RawPath),
            {Path, mochiweb_util:parse_qs(QS)}
    end.

-spec raw_url(#wm_reqdata{}) -> undefined | {string(), [{term(), term()}]}.
raw_url(RD) ->
    case wrq:get_req_header(?RCS_RAW_URL_HEADER, RD) of
        undefined -> undefined;
        RawUrl ->
            {Path, QS, _} = mochiweb_util:urlsplit_path(RawUrl),
            {Path, mochiweb_util:parse_qs(QS)}
    end.
