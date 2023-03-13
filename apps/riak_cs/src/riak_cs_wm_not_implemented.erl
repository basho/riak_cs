%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved,
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

%% @doc This resource allows all HTTP 1.1 methods, however, responds
%%      with error by `malformed_request/2'.
-module(riak_cs_wm_not_implemented).

-export([allowed_methods/0,
         malformed_request/2
        ]).
-ignore_xref([allowed_methods/0,
              malformed_request/2
             ]).

-include("riak_cs.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-spec allowed_methods() -> [atom()].
allowed_methods() ->
    ['GET', 'HEAD', 'POST', 'PUT', 'DELETE', 'TRACE', 'CONNECT', 'OPTIONS'].

-spec malformed_request(#wm_reqdata{}, #rcs_s3_context{}) ->
          {false | {halt, _}, #wm_reqdata{}, #rcs_s3_context{}}.
malformed_request(RD, Ctx) ->
    riak_cs_s3_response:api_error(not_implemented, RD, Ctx).
