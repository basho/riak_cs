%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% TODO:
%% We're evenutally going to have to start storing more
%% than just a single lfs_manifest record at a key, and store
%% a list of them. This will be used so that we can still have
%% pointers to the chunks to do GC when an update has been made.
%% We also want to be able to continue to serve the "current"
%% version of an object as a new one is being streamed in. While
%% the new one is being streamed in, we're going to want to check point
%% the chunks so that we can do GC is the PUT fails.

-module(riak_moss_lfs_utils).

-include("riak_moss.hrl").

-export([new_manifest/5,
         is_manifest/1,
         remove_block/2,
         still_waiting/1,
         block_count/1,
         block_name/3,
         block_name_to_term/1,
         initial_blocks/1,
         sorted_blocks_remaining/1,
         block_keynames/1,
         block_keynames/3,
         metadata_from_manifest/1,
         riak_connection/0,
         riak_connection/2]).

%% Opaque record for a large-file manifest.
-record(lfs_manifest, {
    version :: atom(),
    uuid :: binary(),
    metadata :: dict(),
    block_size :: integer(),
    bkey :: {term(), term()},
    content_length :: integer(),
    content_md5 :: term(),
    created :: term(),
    finished :: term(),
    active :: boolean(),
    blocks_remaining = sets:new()}).

new_manifest(BKey, UUID, Metadata, ContentLength, ContentMd5) ->
    #lfs_manifest{version=v1,
                  uuid=UUID,
                  metadata=Metadata,
                  bkey=BKey,
                  block_size=?LFS_BLOCK_SIZE,
                  content_length=ContentLength,
                  content_md5=ContentMd5,
                  blocks_remaining=initial_blocks(ContentLength)}.

%% @doc Returns true if Value is
%%      a manifest record
is_manifest(Value) ->
    is_record(Value, lfs_manifest).

%% @doc Remove a chunk from the
%%      blocks_remaining field of Manifest
remove_block(Manifest, Chunk) ->
    Remaining = Manifest#lfs_manifest.blocks_remaining,
    Updated = sets:del_element(Chunk, Remaining),
    Manifest#lfs_manifest{blocks_remaining=Updated}.

%% @doc Return true or false
%%      depending on whether
%%      we're still waiting
%%      to accumulate more chunks
still_waiting(#lfs_manifest{blocks_remaining=Remaining}) ->
    sets:size(Remaining) =/= 0.

%% @doc A set of all of the blocks that
%%      make up the file.
initial_blocks(ContentLength) ->
    UpperBound = block_count(ContentLength),
    Seq = lists:seq(0, (UpperBound - 1)),
    sets:from_list(Seq).

block_name(Key, UUID, Number) ->
    term_to_binary({Key, UUID, Number}).

block_name_to_term(Name) ->
    binary_to_term(Name).

%% @doc The number of blocks that this
%%      size will be broken up into
block_count(ContentLength) ->
    block_count(ContentLength, ?LFS_BLOCK_SIZE).

block_count(ContentLength, BlockSize) ->
    Quotient = ContentLength div BlockSize,
    case ContentLength rem BlockSize of
        0 ->
            Quotient;
        _ ->
            Quotient + 1
    end.

set_to_sorted_list(Set) ->
    lists:sort(sets:to_list(Set)).

sorted_blocks_remaining(#lfs_manifest{blocks_remaining=Remaining}) ->
    set_to_sorted_list(Remaining).

block_keynames(#lfs_manifest{bkey={_, KeyName},
                             uuid=UUID}=Manifest) ->
    BlockList = sorted_blocks_remaining(Manifest),
    block_keynames(KeyName, UUID, BlockList).

block_keynames(KeyName, UUID, BlockList) ->
    MapFun = fun(BlockSeq) ->
        {BlockSeq, block_name(KeyName, UUID, BlockSeq)} end,
    lists:map(MapFun, BlockList).

%% @doc Return the metadata for the object
%%      represented in the manifest
metadata_from_manifest(#lfs_manifest{metadata=Metadata}) ->
    Metadata.

%% @doc Get a protobufs connection to the riak cluster
%% using information from the application environment.
-spec riak_connection() -> {ok, pid()} | {error, term()}.
riak_connection() ->
    case application:get_env(riak_moss, riak_ip) of
        {ok, Host} ->
            ok;
        undefined ->
            Host = "127.0.0.1"
    end,
    case application:get_env(riak_moss, riak_pb_port) of
        {ok, Port} ->
            ok;
        undefined ->
            Port = 8087
    end,
    riak_connection(Host, Port).

%% @doc Get a protobufs connection to the riak cluster.
-spec riak_connection(string(), pos_integer()) -> {ok, pid()} | {error, term()}.
riak_connection(Host, Port) ->
    riakc_pb_socket:start_link(Host, Port).
