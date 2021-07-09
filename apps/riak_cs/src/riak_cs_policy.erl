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

-callback fetch_bucket_policy(binary(), pid()) -> {ok, policy()} | {error, term()}.
-callback bucket_policy(riakc_obj:riakc_obj()) -> {ok, policy()} | {error, term()}.
-callback eval(access(), policy() | undefined | binary() ) -> boolean() | undefined.
-callback check_policy(access(), policy()) -> ok | {error, atom()}.
-callback reqdata_to_access(RD :: term(), Target::atom(), ID::string()|undefined) -> access().
-callback policy_from_json(JSON::binary()) -> {ok, policy()} | {error, term()}.
-callback policy_to_json_term(policy()) -> JSON::binary().
