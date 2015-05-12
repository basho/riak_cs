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

-module(tools_helper).
-export([offline_delete/2]).
-include_lib("eunit/include/eunit.hrl").

%% @doc execute `offline_delete.erl` scripts with assertion at
%% before / after of it.
%% - Assert all blocks in `BlockKeysFileList` exist before execution
%% - Stop all nodes
%% - Execute `offline_delete.erl`
%% - Start all nodes
%% - Assert no blocks in `BlockKeysFileList` exist after execution
offline_delete({RiakNodes, CSNodes, Stanchion}, BlockKeysFileList) ->
    lager:info("Assert all blocks exist before deletion"),
    [assert_all_blocks_exists(RiakNodes, BlockKeysFile) ||
        BlockKeysFile <- BlockKeysFileList],

    lager:info("Stop nodes and execute offline_delete script..."),
    NL0 = lists:zip(CSNodes, RiakNodes),
    {CS1, R1} = hd(NL0),
    NodeList = [{CS1, R1, Stanchion} | tl(NL0)],
    rtcs:stop_all_nodes(NodeList, current),

    [begin
         Res = rtcs:exec_priv_escript(
                 1, "internal/offline_delete.erl",
                 "-r 8 --yes " ++
                     rtcs:riak_bitcaskroot(rtcs:get_rt_config(riak, current), 1) ++
                     " " ++ BlockKeysFile,
                 riak),
         lager:debug("offline_delete.erl log:\n~s", [Res]),
         lager:debug("offline_delete.erl log:============= END")
     end || BlockKeysFile <- BlockKeysFileList],

    lager:info("Assert all blocks are non-existent now"),
    rtcs:start_all_nodes(NodeList, current),
    [assert_any_blocks_not_exists(RiakNodes, BlockKeysFile) ||
        BlockKeysFile <- BlockKeysFileList],
    lager:info("All cleaned up!"),
    ok.

assert_all_blocks_exists(RiakNodes, BlocksListFile) ->
    BlockKeys = block_keys(BlocksListFile),
    lager:info("Assert all blocks still exist."),
    [assert_block_exists(RiakNodes, BlockKey) ||
        BlockKey <- BlockKeys],
    ok.

assert_any_blocks_not_exists(RiakNodes, BlocksListFile) ->
    BlockKeys = block_keys(BlocksListFile),
    lager:info("Assert all blocks still exist."),
    [assert_block_not_exists(RiakNodes, BlockKey) ||
        BlockKey <- BlockKeys],
    ok.

block_keys(FileName) ->
    {ok, Bin} = file:read_file(FileName),
    Lines = binary:split(Bin, <<"\n">>, [global]),
    [begin
         [_BHex, _KHex, CsBucket, CsKey, UUIDHex, SeqStr] =
             binary:split(L, [<<"\t">>, <<" ">>], [global]),
         {CsBucket,
          mochihex:to_bin(binary_to_list(CsKey)),
          mochihex:to_bin(binary_to_list(UUIDHex)),
          list_to_integer(binary_to_list(SeqStr))}
     end || L <- Lines, L =/= <<>>].

assert_block_exists(RiakNodes, {CsBucket, CsKey, UUID, Seq}) ->
    ok = case rc_helper:get_riakc_obj(RiakNodes, blocks, CsBucket, {CsKey, UUID, Seq}) of
             {ok, _Obj} -> ok;
             Other ->
                 lager:error("block not found: ~p for ~p~n",
                             [Other, {CsBucket, CsKey, UUID, Seq}]),
                 {error, block_notfound}
         end.

assert_block_not_exists(RiakNodes, {CsBucket, CsKey, UUID, Seq}) ->
    ok = case rc_helper:get_riakc_obj(RiakNodes, blocks,
                                      CsBucket, {CsKey, UUID, Seq}) of
             {error, notfound} -> ok;
             {ok, _Obj} ->
                 lager:error("block found: ~p", [{CsBucket, CsKey, UUID, Seq}]),
                 {error, block_found}
         end.
