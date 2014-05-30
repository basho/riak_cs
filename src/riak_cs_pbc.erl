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

%% @doc Thin wrapper of `riakc_pb_socket'

-module(riak_cs_pbc).

-export([get_object/3,
         put_object/5,
         put/2,
         put/3,
         put_with_no_meta/2,
         put_with_no_meta/3,
         list_keys/2]).

%% @doc Get an object from Riak
-spec get_object(pid(), binary(), binary()) ->
                        {ok, riakc_obj:riakc_obj()} | {error, term()}.
get_object(PbcPid, BucketName, Key) ->
    riakc_pb_socket:get(PbcPid, BucketName, Key).


%% @doc Store an object in Riak
-spec put_object(pid(), binary(), undefined | binary(), binary(), [term()]) -> ok | {error, term()}.
put_object(_PbcPid, BucketName, undefined, Value, Metadata) ->
    error_logger:warning_msg("Attempt to put object into ~p with undefined key "
                             "and value ~P and dict ~p\n",
                             [BucketName, Value, 30, Metadata]),
    {error, bad_key};
put_object(PbcPid, BucketName, Key, Value, Metadata) ->
    RiakObject = riakc_obj:new(BucketName, Key, Value),
    NewObj = riakc_obj:update_metadata(RiakObject, Metadata),
    riakc_pb_socket:put(PbcPid, NewObj).

put(PbcPid, RiakcObj) ->
    put(PbcPid, RiakcObj, []).

put(PbcPid, RiakcObj, Options) ->
    riakc_pb_socket:put(PbcPid, RiakcObj, Options).

put_with_no_meta(PbcPid, RiakcObj) ->
    put_with_no_meta(PbcPid, RiakcObj, []).

%% @doc Put an object in Riak with empty
%% metadata. This is likely used when because
%% you want to avoid manually setting the metadata
%% to an empty dict. You'd want to do this because
%% if the previous object had metadata siblings,
%% not explicitly setting the metadata will
%% cause a siblings exception to be raised.
-spec put_with_no_meta(pid(), riakc_obj:riakc_obj(), term()) ->
    ok | {ok, riakc_obj:riakc_obj()} | {ok, binary()} | {error, term()}.
put_with_no_meta(PbcPid, RiakcObject, Options) ->
    WithMeta = riakc_obj:update_metadata(RiakcObject, dict:new()),
    riakc_pb_socket:put(PbcPid, WithMeta, Options).

%% @doc List the keys from a bucket
-spec list_keys(pid(), binary()) -> {ok, [binary()]} | {error, term()}.
list_keys(PbcPid, BucketName) ->
    case riakc_pb_socket:list_keys(PbcPid, BucketName) of
        {ok, Keys} ->
            %% TODO:
            %% This is a naive implementation,
            %% the longer-term solution is likely
            %% going to involve 2i and merging the
            %% results from each of the vnodes.
            {ok, lists:sort(Keys)};
        {error, _}=Error ->
            Error
    end.
