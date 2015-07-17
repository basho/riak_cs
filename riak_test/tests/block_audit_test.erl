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

-module(block_audit_test).

%% @doc `riak_test' module for testing block audit scripts

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(BUCKET1,               "rt-bucket1").
-define(BUCKET2,               "rt-bucket2").

-define(KEY_ALIVE,             "alive").
-define(KEY_ORPHANED,          "orphaned").
-define(KEY_FALSE_ORPHANED,    "false-orphaned").
-define(KEY_ALIVE_MP,          "alive-mp").
-define(KEY_ORPHANED_MP,       "orphaned-mp").
-define(KEY_FALSE_ORPHANED_MP, "false-orphaned-mp").

confirm() ->
    case rt_config:get(flavor, basic) of
        {multibag, _} ->
            lager:info("Block audit script does not supprt multibag env."),
            lager:info("Skip the test."),
            rtcs:pass();
        _ -> confirm1()
    end.

confirm1() ->
    {UserConfig, {RiakNodes, CSNodes, Stanchion}} = rtcs:setup(1),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?BUCKET1, UserConfig)),
    ?assertEqual(ok, erlcloud_s3:create_bucket(?BUCKET2, UserConfig)),
    FalseOrphans1 =
        [setup_objects(RiakNodes, UserConfig, Bucket, normal,
                       ?KEY_ALIVE, ?KEY_ORPHANED, ?KEY_FALSE_ORPHANED) ||
            Bucket <- [?BUCKET1, ?BUCKET2]],
    FalseOrphans2 =
        [setup_objects(RiakNodes, UserConfig, Bucket, mp,
                       ?KEY_ALIVE_MP, ?KEY_ORPHANED_MP, ?KEY_FALSE_ORPHANED_MP) ||
            Bucket <- [?BUCKET1, ?BUCKET2]],
    Home = rtcs:riakcs_home(rtcs:get_rt_config(cs, current), 1),
    os:cmd("rm -rf " ++ filename:join([Home, "maybe-orphaned-blocks"])),
    os:cmd("rm -rf " ++ filename:join([Home, "actual-orphaned-blocks"])),
    Res1 = rtcs:exec_priv_escript(1, "internal/block_audit.erl",
                                  "-h 127.0.0.1 -p 10017 -dd"),
    lager:debug("block_audit.erl log:\n~s", [Res1]),
    lager:debug("block_audit.erl log:============= END"),
    fake_false_orphans(RiakNodes, FalseOrphans1 ++ FalseOrphans2),
    Res2 = rtcs:exec_priv_escript(1, "internal/ensure_orphan_blocks.erl",
                                  "-h 127.0.0.1 -p 10017 -dd"),
    lager:debug("ensure_orphan_blocks.erl log:\n~s", [Res2]),
    lager:debug("ensure_orphan_blocks.erl log:============= END"),
    assert_result(?BUCKET1),
    assert_result(?BUCKET2),

    BlockKeysFileList = [filename:join([Home, "actual-orphaned-blocks", B]) ||
                        B <- [?BUCKET1, ?BUCKET2]],
    tools_helper:offline_delete({RiakNodes, CSNodes, Stanchion}, BlockKeysFileList),
    rtcs:pass().

setup_objects(RiakNodes, UserConfig, Bucket, Type,
              KeyAlive, KeyOrphaned, KeyFalseOrphaned) ->
    case Type of
        normal ->
            SingleBlock = crypto:rand_bytes(400),
            [erlcloud_s3:put_object(Bucket, Key, SingleBlock, UserConfig) ||
             Key <- [KeyAlive, KeyOrphaned, KeyFalseOrphaned]];
        mp ->
            ok = rc_helper:delete_riakc_obj(RiakNodes, objects, Bucket, KeyOrphaned),
            [rtcs_multipart:multipart_upload(Bucket, Key,
                                             [mb(10), mb(5)], UserConfig) ||
               Key <- [KeyAlive, KeyOrphaned, KeyFalseOrphaned]]
    end,
    ok = rc_helper:delete_riakc_obj(RiakNodes, objects, Bucket, KeyOrphaned),
    lager:info("To fake deficit replicas for ~p, delete objects and restore it "
               "between block_audit.er and block_audit2.erl runs",
               [{Bucket, KeyFalseOrphaned}]),
    {ok, FalseOrphanedRObj} = rc_helper:get_riakc_obj(RiakNodes, objects,
                                                      Bucket, KeyFalseOrphaned),
    ok = rc_helper:delete_riakc_obj(RiakNodes, objects, Bucket, KeyFalseOrphaned),
    {Bucket, KeyFalseOrphaned, FalseOrphanedRObj}.

fake_false_orphans(RiakNodes, FalseOrphans) ->
    [begin
         R = rc_helper:update_riakc_obj(RiakNodes, objects, B, K, O),
         lager:debug("fake_false_orphans ~p: ~p", [{B, K}, R])
     end ||
        {B, K, O} <- FalseOrphans].

assert_result(Bucket) ->
    Home = rtcs:riakcs_home(rtcs:get_rt_config(cs, current), 1),
    OutFile1 = filename:join([Home, "actual-orphaned-blocks", Bucket]),
    {ok, Bin} = file:read_file(OutFile1),
    KeySeqs = [begin
                   [_RiakBucketHex, _RiakKeyHex,
                    _CSBucket, CSKeyHex, _UUIDHex, SeqStr] =
                       binary:split(Line, [<<$\t>>], [global, trim]),
                   {binary_to_list(mochihex:to_bin(binary_to_list(CSKeyHex))),
                    list_to_integer(binary_to_list(SeqStr))}
               end || Line <- binary:split(Bin, [<<$\n>>], [global, trim])],
    ?assertEqual([?KEY_ORPHANED, ?KEY_ORPHANED_MP],
                 lists:sort(proplists:get_keys(KeySeqs))),
    ?assertEqual([0], proplists:get_all_values(?KEY_ORPHANED, KeySeqs)),
    ?assertEqual([0,0,1,1,2,2,3,3,4,4,5,6,7,8,9],
                 lists:sort(proplists:get_all_values(?KEY_ORPHANED_MP, KeySeqs))),
    ok.

mb(MegaBytes) ->
    MegaBytes * 1024 * 1024.

