%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved,
%%               2021 TI Tokyo    All Rights Reserved.
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

%% @doc dummy riak_object for testing functions executed in Riak KV context

-module(riak_object).
-export([new/3, bucket/1, key/1, get_contents/1]).
-export([new/1]).

new(B, K, V) when is_binary(B), is_binary(K) ->
    {B, K, V}.

bucket({B, _K, _V}) -> B.
key({_B, K, _V}) -> K.

get_contents({_B, _K, V}) ->
    [{dict:new(), V}].

new(V) ->
    {b, k, term_to_binary(V)}.
