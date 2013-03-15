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

-module(riak_cs_dummy_gets).

%% API
-export([get_object/2,
         get_object/3]).

%% @doc Dummy get function
get_object(Bucket, Key) ->
    Manifest = riak_cs_lfs_utils:new_manifest(<<"dummy">>,
                                                <<"dummy">>,
                                                <<"uuid">>,
                                                10000, % content length
                                                <<"md5">>,
                                                dict:new(),
                                                riak_cs_lfs_utils:block_size()), % metadata
    RiakObj = riakc_obj:new_obj(Bucket, Key, [], [{dict:new(), term_to_binary(Manifest)}]),
    {ok, RiakObj}.

%% @doc Dummy get function
get_object(Bucket, Key, _RiakPid) ->
    get_object(Bucket, Key).
