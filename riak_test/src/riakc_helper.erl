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
-module(riakc_helper).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

to_bucket_name(CSBucket) ->
    <<"0o:", (riak_cs_utils:md5(CSBucket))/binary>>.

get_riakc_obj(Node, B, K) ->
    Riakc = rt:pbc(Node),
    Result = riakc_pb_socket:get(Riakc, B, K),
    riakc_pb_socket:stop(Riakc),
    Result.

update_riakc_obj(Node, B, K, NewObj) ->
    NewMD = riakc_obj:get_metadata(NewObj),
    NewValue = riakc_obj:get_value(NewObj),
    Riakc = rt:pbc(Node),
    Result = case riakc_pb_socket:get(Riakc, B, K, [deletedvclock]) of
                 {ok, OldObj} ->
                     %% io:format(user, "========================== OldObj: ~p~n", [OldObj]),
                     Updated = riakc_obj:update_value(riakc_obj:update_metadata(OldObj, NewMD), NewValue),
                     riakc_pb_socket:put(Riakc, Updated);
                 {error, notfound} ->
                     %% io:format(user, "========================== OldObj: ~p~n", [notfound]),
                     Obj = riakc_obj:new(B, K, NewValue),
                     Updated = riakc_obj:update_metadata(Obj, NewMD),
                     riakc_pb_socket:put(Riakc, Updated)
             end,
    riakc_pb_socket:stop(Riakc),
    Result.

list_keys(Node, B) ->
    Riakc = rt:pbc(Node),
    {ok, Keys} = riakc_pb_socket:list_keys(Riakc, B),
    io:format(user, "========================== Keys in ~p: ~p~n", [B, Keys]),
    riakc_pb_socket:stop(Riakc),
    {ok, Keys}.
