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

-module(riak_cs_wm_buckets).

-export([stats_prefix/0,
         allowed_methods/0,
         api_request/2,
         anon_ok/0
        ]).

-ignore_xref([stats_prefix/0,
              allowed_methods/0,
              api_request/2,
              anon_ok/0
             ]).

-include("riak_cs.hrl").
-include("riak_cs_web.hrl").
-include_lib("kernel/include/logger.hrl").

-spec stats_prefix() -> service.
stats_prefix() -> service.

%% @doc Get the list of methods this resource supports.
-spec allowed_methods() -> [atom()].
allowed_methods() ->
    ['GET'].

-spec api_request(#wm_reqdata{}, #rcs_web_context{}) -> ?LBRESP{}.
api_request(_RD, #rcs_web_context{user = User}) ->
    riak_cs_api:list_buckets(User).

-spec anon_ok() -> boolean().
anon_ok() ->
    false.
