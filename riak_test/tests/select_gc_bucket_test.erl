%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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

-module(select_gc_bucket_test).

%% @doc `riak_test' module for testing select_gc_bucket script

-export([confirm/0]).

-include_lib("eunit/include/eunit.hrl").

-define(BUCKET, "rt-bucket").

-define(KEY_ALIVE,      "alive").
-define(KEY_DELETED_S,  "deleted-S").
-define(KEY_ALIVE_MP,   "alive-mp").
-define(KEY_DELETED_L1, "deleted-L1").
-define(KEY_DELETED_L2, "deleted-L2").

confirm() ->
    case rt_config:get(flavor, basic) of
        {multibag, _} ->
            lager:info("select_gc_bucket script does not supprt multibag env."),
            lager:info("Skip the test."),
            rtcs:pass();
        _ -> confirm1()
    end.

confirm1() ->
    {UserConfig, {RiakNodes, CSNodes, Stanchion}} = rtcs:setup(1),

    BlockKeysFile = "/tmp/select_gc.txt",
    os:cmd("rm -f " ++ BlockKeysFile),
    rtcs_exec:gc(1, "set-interval infinity"),
    rtcs_exec:gc(1, "cancel"),

    ?assertEqual(ok, erlcloud_s3:create_bucket(?BUCKET, UserConfig)),
    [upload_object(UserConfig, ?BUCKET, normal, K) ||
        K <- [?KEY_ALIVE, ?KEY_DELETED_S]],
    [upload_object(UserConfig, ?BUCKET, mp, K) ||
        K <- [?KEY_ALIVE_MP, ?KEY_DELETED_L1, ?KEY_DELETED_L2]],
    [delete_object(UserConfig, ?BUCKET, K) ||
        K <- [?KEY_DELETED_S, ?KEY_DELETED_L1, ?KEY_DELETED_L2]],

    timer:sleep(1000),
    Res1 = rtcs_exec:exec_priv_escript(1, "internal/select_gc_bucket.erl",
                                  "-h 127.0.0.1 -p 10017 -e today "
                                  "-o " ++ BlockKeysFile),
    lager:debug("select_gc_bucket.erl log:\n~s", [Res1]),
    lager:debug("select_gc_bucket.erl log:============= END"),

    tools_helper:offline_delete({RiakNodes, CSNodes, Stanchion}, [BlockKeysFile]),
    rtcs:pass().

upload_object(UserConfig, Bucket, normal, Key) ->
    SingleBlock = crypto:rand_bytes(400),
    erlcloud_s3:put_object(Bucket, Key, SingleBlock, UserConfig);
upload_object(UserConfig, Bucket, mp, Key) ->
    rtcs_multipart:multipart_upload(Bucket, Key,
                                    [mb(5), mb(1)], UserConfig).

delete_object(UserConfig, Bucket, Key) ->
    ?assertEqual([{delete_marker, false}, {version_id, "null"}],
                 erlcloud_s3:delete_object(Bucket, Key, UserConfig)).

mb(MegaBytes) ->
    MegaBytes * 1024 * 1024.

