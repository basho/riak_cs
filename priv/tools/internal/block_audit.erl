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

-module(block_audit).

-export([main/1]).

-define(DEFAULT_RIAK_HOST, "127.0.0.1").
-define(DEFAULT_RIAK_PORT, 8087).
-define(BUCKET_TOMBSTONE, <<"0">>).
-define(SLK_TIMEOUT, 360000000). %% 100 hours

-include_lib("riak_cs/include/riak_cs.hrl").

riak_host_and_port([]) ->
    {?DEFAULT_RIAK_HOST, ?DEFAULT_RIAK_PORT};
riak_host_and_port([Host, PortStr | _]) ->
    try
        Port = list_to_integer(PortStr),
        {Host, Port}
    catch _:_ ->
            usage()
    end.

usage() ->
    io:format("Usage: ./block_audit.escript <RIAK_HOST> <RIAK_PORT>~n").

main(Args) when length(Args) < 2 ->
    usage();
main(Args) ->
    {RiakHost, RiakPort} = riak_host_and_port(Args),
    {ok, Pid} = riakc_pb_socket:start_link(RiakHost, RiakPort),
    {ok, Buckets} = riakc_pb_socket:list_keys(Pid, ?BUCKETS_BUCKET),
    riakc_pb_socket:stop(Pid),
    io:format("Retrieved bucket list. There are ~p buckets, including tombstones.~n",
        [length(Buckets)]),
    io:format("Searching for orphaned blocks. This may take a while..."),
    log_orphaned_blocks(find_orphaned_blocks(Buckets, RiakHost, RiakPort)).

log_orphaned_blocks(OrphanedBlocks) ->
    Filename = "orphanedBlocks.log",
    {ok, File} = file:open(Filename, [write]),
    TotalCount = lists:foldl(fun({Bucket, UuidCountTuples}, TotalCount) ->
                    BlockCount = lists:foldl(fun({Uuid, Count}, Total) ->
                                                 ok = io:format(File, "~p.~n", [{Bucket, Uuid, Count}]),
                                                 Total + Count
                                             end, 0, UuidCountTuples),
                    TotalCount + BlockCount
                end, 0, OrphanedBlocks),
    ok = file:close(File),
    io:format("~nTotal # Missing Manifests: ~p~n", [length(OrphanedBlocks)]),
    io:format("Total # Orphaned Blocks: ~p (~p MB) ~n", [TotalCount, TotalCount]),
    io:format("Orphaned Blocks written to ~p~n", [Filename]).

find_orphaned_blocks(Buckets, RiakHost, RiakPort) ->
    RegOrphanedBlocks = lists:foldl(
        fun(Bucket, Blocks) ->
            io:format("~nFinding Orphaned blocks for Bucket ~p~n", [Bucket]),
            {ok, Pid} = riakc_pb_socket:start_link(RiakHost, RiakPort),
            {ok, _BlocksTable} = cache_block_keys(Pid, Bucket),
            case log_object_keys(Pid, Bucket) of
                {ok, ObjectKeysFilename} ->
                    ok = delete_cached_manifest_uuids(Pid, Bucket, ObjectKeysFilename),
                    riakc_pb_socket:stop(Pid),
                    MissingManifests = find_missing_uuids(Bucket),
                    {Bucket, Missing} = MissingManifests,
                    io:format("Found ~p missing manifests for bucket ~p~n", [length(Missing), Bucket]),
                    NewBlocks = [MissingManifests | Blocks],
                    ok = file:delete(ObjectKeysFilename),
                    NewBlocks;
                {error, Error, ObjectKeysFilename} ->
                    io:format("Skipping bucket ~p due to error: ~p~n", [Bucket, Error]),
                    Table = list_to_atom(binary_to_list(Bucket)),
                    ets:delete(Table),
                    riakc_pb_socket:stop(Pid),
                    ok = file:delete(ObjectKeysFilename),
                    Blocks
            end
        end, [], Buckets),
    remove_gc_blocks_from_orphans(RegOrphanedBlocks, RiakHost, RiakPort).

remove_gc_blocks_from_orphans(OrphanedBlocks, RiakHost, RiakPort) ->
    {ok, Pid} = riakc_pb_socket:start_link(RiakHost, RiakPort),
    io:format("~nSearching GC bucket...~n"),
    {ok, Timestamps} = riakc_pb_socket:list_keys(Pid, ?GC_BUCKET),
    io:format("~p timestamps in gc bucket~n", [length(Timestamps)]),
    GcUuids = lists:foldl(fun(Timestamp, Acc) ->
                case riakc_pb_socket:get(Pid, ?GC_BUCKET, Timestamp) of
                    {ok, ManifestsObj} ->
                        io:format("*"),
                        P0 = riak_cs_gc:decode_and_merge_siblings(ManifestsObj, twop_set:new()),
                        lists:foldl(fun({Uuid, _}, Uuids) ->
                                        sets:add_element(Uuid, Uuids)
                                    end, Acc, twop_set:to_list(P0));
                        _ ->
                           Acc
                end
            end, sets:new(), Timestamps),
    io:format("~p block uuids retrieved from gc bucket~n", [sets:size(GcUuids)]),
    Result = subtract_gc_uuids(GcUuids, OrphanedBlocks),
    riakc_pb_socket:stop(Pid),
    Result.

-spec subtract_gc_uuids(set(), list()) -> list().
subtract_gc_uuids(GcUuids, OrphanedBlocks) ->
    io:format("~n"),
    lists:foldl(fun({Bucket, UuidCountTuples}, Acc) ->
                    io:format("X"),
                    Uuids = sets:from_list([Uuid || {Uuid, _} <- UuidCountTuples]),
                    RemovedUuids = sets:intersection(Uuids, GcUuids),
                    RemainingUuidCountTuples = lists:foldl(fun(RemovedUuid, Tuples) ->
                                                               lists:keydelete(RemovedUuid, 1, Tuples)
                                                           end, UuidCountTuples, sets:to_list(RemovedUuids)),
                    [{Bucket, RemainingUuidCountTuples} | Acc]
                end, [], OrphanedBlocks).

cache_block_keys(Pid, Bucket) ->
    BlocksBucket = riak_cs_utils:to_bucket_name(blocks, Bucket),
    {ok, ReqId} = riakc_pb_socket:stream_list_keys(Pid, BlocksBucket, ?SLK_TIMEOUT),
    BlocksTable = list_to_atom(binary_to_list(Bucket)),
    {ok, NumKeys} = receive_and_cache_blocks(ReqId, BlocksTable),
    io:format("Logged ~p block keys to ~p~n", [NumKeys, BlocksTable]),
    {ok, BlocksTable}.

log_object_keys(Pid, Bucket) ->
    ManifestsBucket = riak_cs_utils:to_bucket_name(objects, Bucket),
    {ok, ReqId} = riakc_pb_socket:stream_list_keys(Pid, ManifestsBucket, ?SLK_TIMEOUT),
    ObjectKeysFilename = "./"++binary_to_list(Bucket)++"_object_keys.log",
    case receive_and_log(ReqId, ObjectKeysFilename, objects) of
        {ok, NumKeys} ->
            io:format("Logged ~p object keys to ~p~n", [NumKeys, ObjectKeysFilename]),
            {ok, ObjectKeysFilename};
        {error, Reason} ->
            {error, Reason, ObjectKeysFilename}
    end.

delete_cached_manifest_uuids(Pid, Bucket, ObjectKeysFilename) ->
    Table = list_to_atom(binary_to_list(Bucket)),
    {ok, ObjectKeysFile} = file:open(ObjectKeysFilename, [read]),
    ok = delete_cached_manifest_uuids(Pid, Bucket, Table, ObjectKeysFile),
    ok = file:close(ObjectKeysFile).

delete_cached_manifest_uuids(Pid, Bucket, Table, ObjectKeysFile) ->
    ManifestsBucket = riak_cs_utils:to_bucket_name(objects, Bucket),
    case io:fread(ObjectKeysFile, meh, "~s") of
        {ok, [Key]} ->
            Uuids = get_uuids(riakc_pb_socket:get(Pid, ManifestsBucket, Key)),
            [ets:delete(Table, Uuid) || Uuid <- Uuids],
            delete_cached_manifest_uuids(Pid, Bucket, Table, ObjectKeysFile);
        eof ->
            ok;
        Error ->
            io:format("Error in delete_cached_manifest_uuids/5: ~p for Bucket ~p", [Error, Bucket]),
            ok
    end.

receive_and_cache_blocks(ReqId, TableName) ->
    ets:new(TableName, [ordered_set, named_table, public]),
    io:format("Saving keys to ~p~n", [TableName]),
    receive_and_cache_blocks(ReqId, TableName, 0).

receive_and_cache_blocks(ReqId, Table, Count) ->
    receive
        {ReqId, done} ->
            {ok, Count};
        {ReqId, {error, Reason}} ->
            io:format("receive_and_cache_blocks/3 got error: ~p for table ~p, count: ~p. Returning current count.~n",
                [Reason, Table, Count]),
            {ok, Count};
        {ReqId, {_, Keys}} ->
            lists:map(fun(Key) ->
                        {Uuid, _} = riak_cs_lfs_utils:block_name_to_term(Key),
                        case ets:lookup(Table, Uuid) of
                            [{Uuid, BlockCount}] ->
                                ets:insert(Table, {Uuid, BlockCount+1});
                            [] ->
                                ets:insert(Table, {Uuid, 1})
                        end
                      end, Keys),
            receive_and_cache_blocks(ReqId, Table, Count+length(Keys))
    end.

receive_and_log(ReqId, Filename, Type) ->
    file:delete(Filename),
    io:format("Opening ~p~n", [Filename]),
    {ok, File} = file:open(Filename, [append]),
    Res = receive_and_log(ReqId, File, Type, 0),
    ok = file:close(File),
    Res.

receive_and_log(ReqId, File, Type, Count) ->
    receive
        {ReqId, done} ->
            {ok, Count};
        {ReqId, {error, Reason}} ->
            io:format("receive_and_log/3 got error: ~p for file ~p, count: ~p.",
                [Reason, File, Count]),
            {error, Reason};
        {ReqId, {_, Keys}} ->
            case Type of
                objects ->
                    [io:format(File, "~s~n", [Key]) || Key <- Keys];
                blocks ->
                    lists:map(fun(Key) ->
                        {UuidBin, _} = riak_cs_lfs_utils:block_name_to_term(Key),
                        Uuid = mochihex:to_hex(UuidBin),
                        io:format(File, "~s~n", [Uuid])
                    end, Keys)
            end,
            file:datasync(File),
            receive_and_log(ReqId, File, Type, Count+length(Keys))
    end.

find_missing_uuids(Bucket) ->
    Table = list_to_atom(binary_to_list(Bucket)),
    MissingBlocks = ets:tab2list(Table),
    ets:delete(Table),
    {Bucket, MissingBlocks}.

get_uuids({ok, Obj}) ->
    UuidList = lists:foldl(fun(V, Uuids) ->
                    case V of
                        <<>> ->
                            Uuids;
                        _ ->
                            Uuids2 = [Uuid || {Uuid, _} <- binary_to_term(V)],
                            Uuids ++ Uuids2
                    end
                end, [], riakc_obj:get_values(Obj)),
    sets:to_list(sets:from_list(UuidList));
get_uuids(_) ->
    [].
