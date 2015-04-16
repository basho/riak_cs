#!/usr/bin/env escript

%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2015 Basho Technologies, Inc.  All Rights Reserved.
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

-module(offline_delete).

-compile(export_all).

%% @doc This is an offline deletion script that'll directly opens
%% bitcask files and reads some file where keys and partitions which
%% should be deleted are written, and then delete them, without
%% bothering KV.
%%
%% Note: make sure you remove AAE tree after this script was run, and
%% turn off AAE on other nodes that's running on the cluster.

main(["--dry-run", BitcaskDir, BlocksListFile]) ->
    offline_delete(BitcaskDir, BlocksListFile, true);
main([BitcaskDir, BlocksListFile]) ->
    offline_delete(BitcaskDir, BlocksListFile, false);
main(_) ->
    io:format(standard_error,
              "options: [--dry-run] <BitcaskDir> <BlocksListFile>~n", []).

-spec open_all_bitcask(filename:filename()) ->
                              orddict:orddict(non_neg_integer(), reference()).
open_all_bitcask(BitcaskDir) ->
    {ok, List} = file:list_dir(BitcaskDir),
    Result = lists:map(fun(File) ->
                               Filename = filename:join(BitcaskDir, File),
                               case bitcask:open(Filename, [read_write]) of
                                   Ref when is_reference(Ref) ->
                                       {list_to_integer(File), Ref};
                                   Other ->
                                       error({File, Other})
                               end
                       end, List),
    orddict:from_list(Result).

-spec close_all_bitcask(orddict:orddict(non_neg_integer(), reference())) -> ok.
close_all_bitcask(Bitcasks) ->
    orddict:map(fun(_, Ref) ->
                        bitcask:close(Ref)
                end, Bitcasks).

offline_delete(BitcaskDir, BlocksListFile, DryRun) ->
    {ok, Fd} = file:open(BlocksListFile, [read]),
    BC = open_all_bitcask(BitcaskDir),
    io:format(standard_error, "~p bitcask directories at ~s opened.~n",
              [length(BC), BitcaskDir]),
    {ok, Deleted} = for_each_line(Fd, BC, DryRun, 0),
    %% io:format(standard_error, "~p~n", [BC]),
    io:format(standard_error, "~p blocks at ~s was deleted"
              " (dry run: ~p).~n",
              [Deleted, BitcaskDir, DryRun]),
    close_all_bitcask(BC),
    ok = file:close(Fd).

for_each_line(Fd, BC, DryRun, Count) ->
    case Count rem 1000 of
        500 ->
            io:format(standard_error,
                      "~p blocks has been deleted.~n",
                      [Count]);
        _ ->
            noop
    end,
    case file:read_line(Fd) of
        {ok, Line} ->
            Tokens = string:tokens(Line, "\t \n"),
            [V1, V2, V3, B, K, _UUIDStr, _SeqNo] = Tokens,
            Bucket = mochihex:to_bin(B),
            Key = mochihex:to_bin(K),
            %% io:format("trying ~p~n", [{list_to_integer(V1),
            %%                            list_to_integer(V2),
            %%                            list_to_integer(V3),
            %%                            UUIDStr,
            %%                            list_to_integer(SeqNo)}]),
            delete(BC, list_to_integer(V1), Bucket, Key, DryRun),
            delete(BC, list_to_integer(V2), Bucket, Key, DryRun),
            delete(BC, list_to_integer(V3), Bucket, Key, DryRun),
            for_each_line(Fd, BC, DryRun, Count+3);
        eof ->
            {ok, Count};
        {error, Reason} ->
            io:format(standard_error, "Error: ~p~n", Reason)
    end.

-define(VERSION_1, 1).
-define(VERSION_BYTE, ?VERSION_1).

delete(BC, Idx, Bucket, Key, DryRun) ->
    case orddict:find(Idx, BC) of
        {ok, Bitcask} ->
            BitcaskKey = make_bk(?VERSION_1, Bucket, Key),
            case (case DryRun of
                      true ->
                          bitcask:get(Bitcask, BitcaskKey);
                      false ->
                          bitcask:delete(Bitcask, BitcaskKey)
                  end) of
                {ok, _Value} ->
                    %% io:format("found.~n");
                    ok;
                ok ->
                    ok;
                Error ->
                    io:format(standard_error, "error: ~p~n", [Error])
            end;
        error ->
            %% Key does not exist here. Ignore.
            ok
    end.

make_bk(0, Bucket, Key) ->
    term_to_binary({Bucket, Key});
make_bk(1, {Type, Bucket}, Key) ->
    TypeSz = size(Type),
    BucketSz = size(Bucket),
    <<?VERSION_BYTE:7, 1:1, TypeSz:16/integer, Type/binary,
      BucketSz:16/integer, Bucket/binary, Key/binary>>;
make_bk(1, Bucket, Key) ->
    BucketSz = size(Bucket),
    <<?VERSION_BYTE:7, 0:1, BucketSz:16/integer,
     Bucket/binary, Key/binary>>.
