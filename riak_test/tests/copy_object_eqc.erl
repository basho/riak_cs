
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

-module(copy_object_eqc).

%% @doc `riak_test' module that uses eqc for testing copy object behavior.

-export([confirm/0]).


-ifndef(EQC).
confirm() ->
    pass.

-else.

-include_lib("eunit/include/eunit.hrl").

%% keys for non-multipart objects
-define(SRC_BUCKET, "copy-object-test-bucket-src").
-define(DST_BUCKET, "copy-object-test-bucket-dst").

confirm() ->
    {UserConfig, {_RiakNodes, CSNodes, _Stanchion}} = rtcs:setup(4),

    lager:info("User is valid on the cluster, and has no buckets"),
    ?assertEqual([{buckets, []}], erlcloud_s3:list_buckets(UserConfig)),

    lager:info("creating bucket ~p", [?SRC_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?SRC_BUCKET, UserConfig)),

    lager:info("creating bucket ~p", [?DST_BUCKET]),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?DST_BUCKET, UserConfig)),
    CSNode1 = hd(CSNodes),
    {module, _} = rpc:call(CSNode1, code, load_file, [copy_object_eqc]),
    Result = rpc:call(CSNode1, copy_object_eqc, run, []),
    ?assertEqual(Result, pass).

-endif.
