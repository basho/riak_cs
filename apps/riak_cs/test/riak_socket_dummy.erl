%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
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

%% @doc Module to mirror the API of `riakc_pb_socket' so
%% to facilitate testing of the `riak_cs_writer' and
%% `riak_cs_deleter' modules.

-module(riak_socket_dummy).

%% API
-export([get/3,
         put/2]).

%% @doc Dummy get function
-spec get(pid(), binary(), binary()) -> {ok, term()}.
get(_Pid, Bucket, Key) ->
    {ok, riakc_obj:new_obj(Bucket, Key, <<"fakevclock">>, [{dict:new(), <<"val">>}])}.

%% @doc Dummy put function
-spec put(pid(), term()) -> ok.
put(_Pid, _Obj) ->
    ok.
