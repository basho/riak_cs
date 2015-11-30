%% -------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.
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
%%-------------------------------------------------------------------

-module(riakc_pb_socket_intercepts).
-compile(export_all).
%-include_lib("intercept.hrl").
-define(M, riakc_pb_socket_orig).

get_timeout(_Pid, _Bucket, _Key, _Options, _Timeout) ->
    {error, timeout}.

get_orig(Pid, Bucket, Key, Options, Timeout) ->
    ?M:get_orig(Pid, Bucket, Key, Options, Timeout).

put_timeout(_Pid, _Obj, _Options, _Timeout) ->
    {error, timeout}.

put_orig(Pid, Obj, Options, Timeout) ->
    ?M:put_orig(Pid, Obj, Options, Timeout).

get_overload(_Pid, _Bucket, _Key, _Options, _Timeout) ->
    {error, <<"overload">>}.
