%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved,
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

-module(rcs_common_manifest).

-export([make_versioned_key/2,
         decompose_versioned_key/1]).

-include("rcs_common_defs.hrl").
-include("rcs_common_manifest.hrl").


-spec make_versioned_key(binary(), binary()) -> binary().
make_versioned_key(Key, ?LFS_DEFAULT_OBJECT_VERSION) ->
%% old keys written without a version should continue to be accessible
%% for reads with the default version
    Key;
make_versioned_key(Key, Vsn) ->
    <<Key/binary, ?VERSIONED_KEY_SEPARATOR/binary, Vsn/binary>>.


-spec decompose_versioned_key(binary()) -> {binary(), binary()}.
decompose_versioned_key(VK) ->
    case binary:split(VK, ?VERSIONED_KEY_SEPARATOR) of
        [K, V] ->
            {K, V};
        [K] ->
            {K, ?LFS_DEFAULT_OBJECT_VERSION}
    end.

