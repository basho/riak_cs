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

-module(riak_cs_manifest).

-export([fetch/3,
         etag/1]).

-include("riak_cs.hrl").

-spec fetch(pid(), binary(), binary()) -> {ok, lfs_manifest()} | {error, term()}.
fetch(RcPid, Bucket, Key) ->
    case riak_cs_utils:get_manifests(RcPid, Bucket, Key) of
        {ok, _, Manifests} ->
            riak_cs_manifest_utils:active_manifest(orddict:from_list(Manifests));
        Error ->
            Error
    end.

-spec etag(lfs_manifest()) -> string().
etag(?MANIFEST{content_md5={MD5, Suffix}}) ->
    riak_cs_utils:etag_from_binary(MD5, Suffix);
etag(?MANIFEST{content_md5=MD5}) ->
    riak_cs_utils:etag_from_binary(MD5).
