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

-module(riak_cs_policy).

-include("riak_cs.hrl").

-callback eval(access(), policy() | undefined | binary() ) -> boolean() | undefined.
-callback check_policy(access(), policy()) -> ok | {error, atom()}.
-callback reqdata_to_access(RD :: term(), Target::atom(), ID::binary()|undefined) -> access().
-callback policy_from_json(JSON::binary()) -> {ok, policy()} | {error, term()}.
-callback policy_to_json_term(policy()) -> JSON::binary().

%% -export([behaviour_info/1]).

%% -compile(export_all).

%% -spec behaviour_info(atom()) -> 'undefined' | [{atom(), arity()}].
%% behaviour_info(callbacks) ->
%%     [{eval, 2}, {check_policy, 2}, {reqdata_to_access, 3},
%%      {policy_from_json, 1}, {policy_to_json_term, 1}];
%% behaviour_info(_Other) ->
%%     undefined.
