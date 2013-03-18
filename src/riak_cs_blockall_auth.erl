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

-module(riak_cs_blockall_auth).

-behavior(riak_cs_auth).

-include("riak_cs.hrl").

-export([identify/2,authenticate/4]).

-spec identify(term(), term()) -> {undefined, block_all}.
identify(_RD,_Ctx) ->
    {undefined, block_all}.

-spec authenticate(rcs_user(), term(), term(), term()) -> ok | {error, term()}.
authenticate(_User, AuthData, _RD, _Ctx) ->
    {error, AuthData}.
