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

-mode(compile).

-export([main/1]).
-export([info/2, verbose/3]).

-define(SLK_TIMEOUT, 360000000). %% 100 hours

-include_lib("riak_cs/include/riak_cs.hrl").

-record(buuid, {uuid  :: binary(),
                seqs  :: [non_neg_integer()] % sequence numbers
               }).

main(Args) ->
    _ = application:load(lager),
    ok = application:set_env(lager, handlers, [{lager_console_backend, info}]),
    ok = lager:start(),
    {ok, {Options, _PlainArgs}} = getopt:parse(option_spec(), Args),
    LogLevel = case proplists:get_value(debug, Options) of
                   0 ->
                       info;
                   _ ->
                       ok = lager:set_loglevel(lager_console_backend, debug),
                       debug
               end,
    debug("Log level is set to ~p", [LogLevel]),
    debug("Options: ~p", [Options]),
    case proplists:get_value(host, Options) of
        undefined ->
            getopt:usage(option_spec(), "riak-cs escript /path/to/block_audit.erl"),
            halt(1);
        Host ->
            Port = proplists:get_value(port, Options),
            debug("Connecting to Riak ~s:~B...", [Host, Port]),
            case riakc_pb_socket:start_link(Host, Port) of
                {ok, Pid} ->
                    pong = riakc_pb_socket:ping(Pid),
                    audit(Pid, Options),
                    timer:sleep(100);
                {error, Reason} ->
                    err("Connection to Riak failed ~p", [Reason]),
                    halt(2)
            end
    end.

option_spec() ->
    [
     {host, $h, "host", string, "Host of Riak PB"},
     {port, $p, "port", {integer, 8087}, "Port number of Riak PB"},
     {bucket, $b, "bucket", string, "CS Bucket to audit, repetitions possible"},
     {output, $o, "output", {string, "maybe-orphaned-blocks"}, "Directory to output resutls"},
     %% {page_size, $s, "page-size", {integer, 1000}, "Specify page size for 2i listing"},
     {debug, $d, "debug", {integer, 0}, "Enable debug (-dd for more verbose)"}
    ].

err(Format, Args) ->
    log(error, Format, Args).

debug(Format, Args) ->
    log(debug, Format, Args).

verbose(Options, Format, Args) ->
    {debug, DebugLevel} = lists:keyfind(debug, 1, Options),
    case DebugLevel of
        Level when 2 =< Level ->
            debug(Format, Args);
        _ ->
            ok
    end.

info(Format, Args) ->
    log(info, Format, Args).

log(Level, Format, Args) ->
    lager:log(Level, self(), Format, Args).

audit(Pid, Opts) ->
    Buckets = case proplists:get_all_values(bucket, Opts) of
                  [] ->
                      {ok, AllBuckets} = riakc_pb_socket:list_keys(Pid, ?BUCKETS_BUCKET),
                      AllBuckets;
                  Values ->
                      Values
              end,
    info("Retrieved bucket list. There are ~p buckets, including tombstones.",
         [length(Buckets)]),
    info("Searching for orphaned blocks. This may take a while...", []),
    log_all_maybe_orphaned_blocks(Pid, Opts, Buckets),
    ok.

log_all_maybe_orphaned_blocks(_Pid, _Opts, []) ->
    ok;
log_all_maybe_orphaned_blocks(Pid, Opts, [Bucket | Buckets]) ->
    _ = log_maybe_orphaned_blocks(Pid, Opts, Bucket),
    log_all_maybe_orphaned_blocks(Pid, Opts, Buckets).

log_maybe_orphaned_blocks(Pid, Opts, Bucket) when is_binary(Bucket) ->
    log_maybe_orphaned_blocks(Pid, Opts, binary_to_list(Bucket));
log_maybe_orphaned_blocks(Pid, Opts, Bucket) ->
    info("Finding Orphaned blocks for Bucket ~p", [Bucket]),
    BlocksTable = list_to_atom(Bucket),
    ets:new(BlocksTable, [set, named_table, public, {keypos, #buuid.uuid}]),
    try
        {ok, NumKeys} = cache_block_keys(Pid, Bucket, BlocksTable),
        case NumKeys of
            0 -> ok;
            _ -> case delete_manifest_uuids(Pid, Bucket, BlocksTable) of
                     ok ->
                         write_uuids(Opts, Bucket,
                                     BlocksTable, ets:info(BlocksTable, size));
                     _ ->
                         nop
                 end
        end
    after
        catch ets:delete(BlocksTable)
    end.

write_uuids(_Opts, _Bucket, _BlocksTable, 0) ->
    ok;
write_uuids(Opts, Bucket, BlocksTable, _) ->
    OutDir = proplists:get_value(output, Opts),
    Filename = filename:join(OutDir, Bucket),
    ok = filelib:ensure_dir(Filename),
    {ok, File} = file:open(Filename, [write, raw, delayed_write]),
    {UUIDs, Blocks} = ets:foldl(
                        fun(#buuid{uuid=UUID, seqs=Seqs},
                            {TotalUUIDs, TotalBlocks}) ->
                                verbose(Opts, "~s ~s ~B ~p",
                                        [Bucket, mochihex:to_hex(UUID),
                                         length(Seqs), Seqs]),
                                [ok = file:write(File,
                                                 [Bucket, $ ,
                                                  mochihex:to_hex(UUID), $ ,
                                                  integer_to_list(Seq), $\n]) ||
                                    Seq <- Seqs],
                                {TotalUUIDs + 1, TotalBlocks + length(Seqs)}
                        end, {0, 0}, BlocksTable),
    ok = file:close(File),
    info("Total # Missing Block UUIDs: ~p [count]", [UUIDs]),
    info("Total # Orphaned Blocks: ~p [count]", [Blocks]),
    info("Orphaned Blocks written to ~p", [Filename]).

cache_block_keys(Pid, Bucket, BlocksTable) ->
    BlocksBucket = riak_cs_utils:to_bucket_name(blocks, Bucket),
    {ok, ReqId} = riakc_pb_socket:stream_list_keys(Pid, BlocksBucket, ?SLK_TIMEOUT),
    {ok, NumKeys} = receive_and_cache_blocks(ReqId, BlocksTable),
    info("Logged ~p block keys to ~p~n", [NumKeys, BlocksTable]),
    {ok, NumKeys}.

delete_manifest_uuids(Pid, Bucket, BlocksTable) ->
    ManifestsBucket = riak_cs_utils:to_bucket_name(objects, Bucket),
    Opts = [%% {max_results, 1000},
            {start_key, <<>>},
            {end_key, big_end_key(128)},
            {timeout, ?SLK_TIMEOUT}],
    {ok, ReqID} = riakc_pb_socket:cs_bucket_fold(Pid, ManifestsBucket, Opts),
    handle_manifest_fold(Pid, Bucket, BlocksTable, ReqID).

handle_manifest_fold(Pid, Bucket, BlocksTable, ReqID) ->
    receive
        {ReqID, {ok, Objs}} ->
            [ets:delete(BlocksTable, UUID) ||
                Obj <- Objs,
                UUID <- get_uuids(Obj)],
            handle_manifest_fold(Pid, Bucket, BlocksTable, ReqID);
        {ReqID, {done, _}} ->
            info("handle_manifest_fold done for bucket: ~p", [Bucket]),
            ok;
        Other ->
            err("handle_manifest_fold error; ~p", [Other]),
            error
 end.

receive_and_cache_blocks(ReqId, TableName) ->
    receive_and_cache_blocks(ReqId, TableName, 0).

receive_and_cache_blocks(ReqId, Table, Count) ->
    receive
        {ReqId, done} ->
            {ok, Count};
        {ReqId, {error, Reason}} ->
            err("receive_and_cache_blocks/3 got error: ~p for table ~p, count: ~p."
                " Returning current count.",
                [Reason, Table, Count]),
            {ok, Count};
        {ReqId, {_, Keys}} ->
            NewCount = handle_keys(Table, Count, Keys),
            receive_and_cache_blocks(ReqId, Table, NewCount)
    end.

handle_keys(Table, Count, Keys) ->
    lists:foldl(
      fun(Key, Acc) ->
              {UUID, Seq} = riak_cs_lfs_utils:block_name_to_term(Key),
              Rec = case ets:lookup(Table, UUID) of
                        [#buuid{seqs=Seqs} = B] -> B#buuid{seqs=[Seq|Seqs]};
                        _ -> #buuid{uuid=UUID, seqs=[Seq]}
                    end,
              ets:insert(Table, Rec),
              Acc + 1
      end, Count, Keys).

get_uuids(Obj) ->
    Manifests = riak_cs_manifest:manifests_from_riak_object(Obj),
    BlockUUIDs = [UUID ||
                     {_ManiUUID, M} <- Manifests,
                     %% TODO: more efficient way
                     {UUID, _} <- riak_cs_lfs_utils:block_sequences_for_manifest(M)],
    lists:usort(BlockUUIDs).

big_end_key(NumBytes) ->
    MaxByte = <<255:8/integer>>,
    iolist_to_binary([MaxByte || _ <- lists:seq(1, NumBytes)]).
