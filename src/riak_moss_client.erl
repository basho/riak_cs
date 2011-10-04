%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
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
%% -------------------------------------------------------------------

-module(riak_moss_client, [Pid, User]).

-compile(export_all).

-include("riak_moss.hrl").

make_bucket(Bucket) ->
    iolist_to_binary([User#rs3_user.key_id, ".", Bucket]).

stop() ->
    riakc_pb_socket:stop(Pid).

get_object(Bucket, Key) ->
    riakc_pb_socket:get(Pid, make_bucket(Bucket), Key).

put_object(Bucket, Key, Value) ->
    Obj = riakc_obj:new(make_bucket(Bucket), Key, Value),
    riakc_pb_socket:put(Pid, Obj).

delete_object(Bucket, Key) ->
    riakc_pb_socket:delete(Pid, make_bucket(Bucket), Key).

list_bucket(Bucket) ->
    {ok, KL} = riakc_pb_socket:list_keys(Pid, make_bucket(Bucket)),
    add_sizes(KL, Bucket, []).

add_sizes([], _Bucket, Acc) ->
    lists:reverse(Acc);
add_sizes([H|T], Bucket, Acc) ->
    case get_object(Bucket, H) of
        {ok, O} ->
            add_sizes(T, Bucket, [{H, size(riakc_obj:get_value(O))}|Acc]);
        _ ->
            add_sizes(T, Bucket, [{H, 0}|Acc])
    end.

