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

-module(ensure_orphan_blocks).

-mode(compile).

-export([main/1]).
-export([info/2, verbose/3]).

-define(SLK_TIMEOUT, 360000000). %% 100 hours
-define(MD_USERMETA, <<"X-Riak-Meta">>).

-include_lib("riak_cs/include/riak_cs.hrl").

main(Args) ->
    logger:update_primary_config(#{level => info}),
    {ok, {Options, _PlainArgs}} = getopt:parse(option_spec(), Args),
    LogLevel = case proplists:get_value(debug, Options) of
                   0 ->
                       info;
                   _ ->
                       logger:update_primary_config(#{level => debug}),
                       debug
               end,
    debug("Log level is set to ~p", [LogLevel]),
    debug("Options: ~p", [Options]),
    case proplists:get_value(host, Options) of
        undefined ->
            getopt:usage(option_spec(), "riak-cs escript /path/to/ensure_orphan_blocks.erl"),
            halt(1);
        Host ->
            Port = proplists:get_value(port, Options),
            debug("Connecting to Riak ~s:~B...", [Host, Port]),
            case riakc_pb_socket:start_link(Host, Port) of
                {ok, Pid} ->
                    pong = riakc_pb_socket:ping(Pid),
                    audit2(Pid, Options),
                    riakc_pb_socket:stop(Pid),
                    timer:sleep(100); % OTP-9985
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
     {input, $i, "input", {string, "maybe-orphaned-blocks"}, "Directory for input data"},
     {output, $o, "output", {string, "actual-orphaned-blocks"}, "Directory to output resutls"},
     %% {page_size, $s, "page-size", {integer, 1000}, "Specify page size for 2i listing"},
     %% {dry_run, undefined, "dry-run", {boolean, false}, "if set, actual update does not happen"},
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
    logger:log(Level, Format, Args).

audit2(Pid, Opts) ->
    info("Filter actual orphaned blocks from maybe ones. This may take a while...", []),
    InDir = proplists:get_value(input, Opts),
    info("Input directory: ~p", [InDir]),
    {ok, Filenames} = file:list_dir(InDir),
    OutDir = proplists:get_value(output, Opts),
    filelib:ensure_dir(filename:join(OutDir, "dummy")),
    filter_all_actually_orphaned_blocks(Pid, Opts, InDir, OutDir, Filenames),
    ok.

filter_all_actually_orphaned_blocks(_Pid, _Opts, _InDir, _OutDir, []) ->
    ok;
filter_all_actually_orphaned_blocks(Pid, Opts, InDir, OutDir, [Filename | Filenames]) ->
    _ = filter_actually_orphaned_blocks(Pid, Opts, InDir, OutDir, Filename),
    filter_all_actually_orphaned_blocks(Pid, Opts, InDir, OutDir, Filenames).

filter_actually_orphaned_blocks(Pid, Opts, InDir, OutDir, Filename) ->
    InFile = filename:join(InDir, Filename),
    info("Filter actually orphaned blocks from ~p", [filename:absname(InFile)]),
    {ok, Input} = file:open(InFile, [read, raw, binary]),
    OutFile = filename:join(OutDir, Filename),
    info("Output goes to ~p", [filename:absname(OutFile)]),
    {ok, Output} = file:open(OutFile, [write, raw, delayed_write]),
    try
        handle_lines(Pid, Opts, Filename, Input, Output, undefined, undefined)
    after
        catch file:close(Input),
        catch file:close(Output)
    end.

handle_lines(Pid, Opts, Filename, Input, Output, PrevUUID, PrevState) ->
    case file:read_line(Input) of
        eof -> ok;
        {error, Reason} ->
            err("error in processing ~p : ~p", [Filename, Reason]);
        {ok, LineData} ->
            [Bucket, UUIDHex, Seq] = binary:split(LineData,
                                                  [<<$\n>>, <<$\t>>, <<$ >>], [global, trim]),
            UUID = mochihex:to_bin(binary_to_list(UUIDHex)),
            {ok, NewPrevState}  = handle_line(Pid, Opts, Output, PrevUUID, PrevState,
                                              Bucket, UUID,
                                              list_to_integer(binary_to_list(Seq))),
            handle_lines(Pid, Opts, Filename, Input, Output, UUID, NewPrevState)
    end.

handle_line(_Pid, Opts, Output, PrevUUID, {actual_orphan, CSKey}, Bucket, PrevUUID, Seq) ->
    write_uuid(Opts, Output, Bucket, PrevUUID, Seq, CSKey),
    {ok, {actual_orphan, CSKey}};
handle_line(_Pid, _Opts, _Output, PrevUUID, false_orphan, _Bucket, PrevUUID, _Seq) ->
    {ok, false_orphan};
handle_line(Pid, Opts, Output, _PrevUUID, _PrevState, Bucket, UUID, Seq) ->
    case manifest_state(Pid, Opts, Output, Bucket, UUID, Seq) of
        {ok, block_notfound} ->
            {ok, undefined};
        {ok, {actual_orphan, CSKey}} ->
            write_uuid(Opts, Output, Bucket, UUID, Seq, CSKey),
            {ok, {actual_orphan, CSKey}};
        {error, existing} ->
            {ok, false_orphan};
        {error, {please_ignore_this_uuid, Reason}} ->
            info("Ignore UUID ~p: ~p", [Reason]),
            {ok, false_orphan};
        {error, _Reason} ->
            %% Error occured, skip subsequent sequence numbers for the same UUID
            {ok, false_orphan}
    end.

-spec manifest_state(pid(), list(), IO::term(), binary(), binary(), non_neg_integer()) ->
                            {ok, {actual_orphan, CSKey::binary()}} |
                            {ok, block_notfound} |
                            {error, existing} |
                            {error, term()}.
manifest_state(Pid, Opts, _Output, CSBucket, UUID, Seq) ->
    BlocksBucket = riak_cs_utils:to_bucket_name(blocks, CSBucket),
    BlockKey = riak_cs_lfs_utils:block_name(dummy_key, UUID, Seq),
    case riakc_pb_socket:get(Pid, BlocksBucket, BlockKey) of
        {error, notfound} ->
            verbose(Opts, "Block notfound for ~p", [{CSBucket, UUID, Seq}]),
            {ok, block_notfound};
        {error, Reason} ->
            err("block get error for ~p: ~p",
                [{CSBucket, UUID, Seq}, Reason]),
            {error, Reason};
        {ok, BObj} ->
            MDs = [MD ||
                      {MD, V} <- riakc_obj:get_contents(BObj),
                      not riak_cs_utils:has_tombstone({MD, V})],
            case MDs of
                [] ->
                    %% this block has been deleted, blocks with the same UUIDs
                    %% should be removed
                    {ok, block_notfound};
                _ ->
                    MD = hd(MDs),
                    %% verbose(Opts, "block metadata (to_list'ed): ~p", [dict:to_list(MD)]),
                    {ok, UserMeta} = dict:find(?MD_USERMETA, MD),
                    CSBucket = proplists:get_value(<<?USERMETA_BUCKET>>, UserMeta),
                    CSKey = proplists:get_value(<<?USERMETA_KEY>>, UserMeta),
                    ManifestsBucket = riak_cs_utils:to_bucket_name(objects, CSBucket),
                    %% TODO: What is appropriate options here? Possible corner cases:
                    %% - Only single replica existing.
                    %% - Two small and one big replicas.
                    %%   R=quorum can not fetch big one, almost by latency difference.
                    %%   But R=all may make latencies horrible.
                    MGetOpts = [{notfound_ok, false}, {basic_quorum, false}],
                    case riakc_pb_socket:get(Pid, ManifestsBucket, CSKey, MGetOpts) of
                        {error, notfound} ->
                            verbose(Opts, "Manifests notfound for ~p", [{CSBucket, CSKey}]),
                            {ok, {actual_orphan, CSKey}};
                        {error, Reason2} ->
                            err("manifest_state error for ~p: ~p",
                                [{CSBucket, CSKey}, Reason2]),
                            {error, Reason2};
                        {ok, MObj} ->
                            UUIDs = get_block_uuids(MObj),
                            case lists:member(UUID, UUIDs) of
                                false ->
                                    verbose(Opts, "Block UUID ~p not found in manifest for ~p",
                                            [UUID, {CSBucket, CSKey}]),
                                    {ok, {actual_orphan, CSKey}};
                                true ->
                                    verbose(Opts, "Block UUID ~p found in manifest for ~p",
                                            [UUID, {CSBucket, CSKey}]),
                                    {error, existing}
                            end
                    end
            end
    end.

get_block_uuids(Obj) ->
    Manifests = riak_cs_manifest:manifests_from_riak_object(Obj),
    BlockUUIDs = [UUID ||
                     {_ManiUUID, M} <- Manifests,
                     %% TODO: more efficient way
                     {UUID, _} <- riak_cs_lfs_utils:block_sequences_for_manifest(M)],
    lists:usort(BlockUUIDs).

write_uuid(_Opts, Output, CSBucket, UUID, Seq, CSKey) ->
    RiakBucket = riak_cs_utils:to_bucket_name(blocks, CSBucket),
    RiakKey = riak_cs_lfs_utils:block_name(dummy_key, UUID, Seq),
    ok = file:write(Output,
                    [mochihex:to_hex(RiakBucket), $\t,
                     mochihex:to_hex(RiakKey), $\t,
                     CSBucket, $\t,
                     mochihex:to_hex(CSKey), $\t,
                     mochihex:to_hex(UUID), $\t,
                     integer_to_list(Seq), $\n]).

