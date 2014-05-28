%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(rc_helper).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

to_riak_bucket(manifest, CSBucket) ->
    %%  or make version switch here.
    <<"0o:", (stanchion_utils:md5(CSBucket))/binary>>;
to_riak_bucket(block, CSBucket) ->
    %%  or make version switch here.
    <<"0b:", (stanchion_utils:md5(CSBucket))/binary>>;
to_riak_bucket(_, CSBucket) ->
    CSBucket.

to_riak_key(manifest, CsKey) ->
    CsKey;
to_riak_key(_,_) ->
    throw(not_yet_implemented).

get_riakc_obj(RiakNodes, ObjectKind, CsBucket, CsKey) ->
    Pbc = rtcs:pbc(RiakNodes, ObjectKind, CsBucket, CsKey),
    RiakBucket = to_riak_bucket(ObjectKind, CsBucket),
    RiakKey = to_riak_key(ObjectKind, CsKey),
    Result = riakc_pb_socket:get(Pbc, RiakBucket, RiakKey),
    riakc_pb_socket:stop(Pbc),
    Result.

update_riakc_obj(RiakNodes, ObjectKind, CsBucket, CsKey, NewObj) ->
    NewMD = riakc_obj:get_metadata(NewObj),
    NewValue = riakc_obj:get_value(NewObj),
    Pbc = rtcs:pbc(RiakNodes, ObjectKind, CsBucket, CsKey),
    RiakBucket = to_riak_bucket(ObjectKind, CsBucket),
    RiakKey = to_riak_key(ObjectKind, CsKey),
    Result = case riakc_pb_socket:get(Pbc, RiakBucket, RiakKey, [deletedvclock]) of
                 {ok, OldObj} ->
                     Updated = riakc_obj:update_value(
                                 riakc_obj:update_metadata(OldObj, NewMD), NewValue),
                     riakc_pb_socket:put(Pbc, Updated);
                 {error, notfound} ->
                     Obj = riakc_obj:new(RiakBucket, RiakKey, NewValue),
                     Updated = riakc_obj:update_metadata(Obj, NewMD),
                     riakc_pb_socket:put(Pbc, Updated)
             end,
    riakc_pb_socket:stop(Pbc),
    Result.
