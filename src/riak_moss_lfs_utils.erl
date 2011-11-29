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

-export([block_count/1,
         block_count/2,
         block_keynames/1,
         block_keynames/3,
         block_name/3,
         block_name_to_term/1,
         block_size/0,
         finalize_manifest/1,
         initial_blocks/1,
         initial_blocks/2,
         is_manifest/1,
         metadata_from_manifest/1,
         new_manifest/5,
         new_manifest/6,
         remove_block/2,
         riak_connection/0,
         riak_connection/2,
         sorted_blocks_remaining/1,
         still_waiting/1]).

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

-type lfs_manifest() :: #lfs_manifest{}.

%% -------------------------------------------------------------------
%% Public API
%% -------------------------------------------------------------------

%% @doc The number of blocks that this
%%      size will be broken up into
block_count(ContentLength) ->
    block_count(ContentLength, ?DEFAULT_LFS_BLOCK_SIZE).

%% @doc The number of blocks that this
%%      size will be broken up into
-spec block_count(pos_integer(), pos_integer()) -> non_neg_integer().
block_count(ContentLength, BlockSize) ->
    Quotient = ContentLength div BlockSize,
    case ContentLength rem BlockSize of
        0 ->
            Quotient;
        _ ->
            Quotient + 1
    end.

block_keynames(#lfs_manifest{bkey={_, KeyName},
                             uuid=UUID}=Manifest) ->
    BlockList = sorted_blocks_remaining(Manifest),
    block_keynames(KeyName, UUID, BlockList).

block_keynames(KeyName, UUID, BlockList) ->
    MapFun = fun(BlockSeq) ->
        {BlockSeq, block_name(KeyName, UUID, BlockSeq)} end,
    lists:map(MapFun, BlockList).

block_name(Key, UUID, Number) ->
    term_to_binary({Key, UUID, Number}).

block_name_to_term(Name) ->
    binary_to_term(Name).

%% @doc Return the configured block size
-spec block_size() -> pos_integer().
block_size() ->
    case application:get_env(riak_moss, lfs_block_size) of
        undefined ->
            ?DEFAULT_LFS_BLOCK_SIZE;
        BlockSize ->
            case BlockSize > ?DEFAULT_LFS_BLOCK_SIZE of
                true ->
                    ?DEFAULT_LFS_BLOCK_SIZE;
                false ->
                    BlockSize
            end
    end.

%% @doc Finalize the manifest of a file by
%% marking it as active, setting a finished time,
%% and setting blocks_remaining as an empty list.
-spec finalize_manifest(lfs_manifest()) -> lfs_manifest().
finalize_manifest(Manifest) ->
    Manifest#lfs_manifest{active=true,
                          finished=httpd_util:rfc1123_date(),
                          blocks_remaining=sets:new()}.

%% @doc A set of all of the blocks that
%%      make up the file.
-spec initial_blocks(pos_integer()) -> set().
initial_blocks(ContentLength) ->
    UpperBound = block_count(ContentLength),
    Seq = lists:seq(0, (UpperBound - 1)),
    sets:from_list(Seq).

%% @doc A set of all of the blocks that
%%      make up the file.
-spec initial_blocks(pos_integer(), pos_integer()) -> set().
initial_blocks(ContentLength, BlockSize) ->
    UpperBound = block_count(ContentLength, BlockSize),
    Seq = lists:seq(0, (UpperBound - 1)),
    sets:from_list(Seq).

%% @doc Returns true if Value is
%%      a manifest record
is_manifest(Value) ->
    is_record(Value, lfs_manifest).

%% @doc Return the metadata for the object
%%      represented in the manifest
metadata_from_manifest(#lfs_manifest{metadata=Metadata}) ->
    Metadata.

%% @doc Initialize a new file manifest
-spec new_manifest(binary(),
                   binary(),
                   binary(),
                   pos_integer(),
                   term()) -> lfs_manifest().
new_manifest(Bucket, FileName, UUID, ContentLength, ContentMd5) ->
    new_manifest(Bucket,
                 FileName,
                 UUID,
                 ContentLength,
                 ContentMd5,
                 block_size()).

%% @doc Initialize a new file manifest
-spec new_manifest(binary(),
                   binary(),
                   binary(),
                   pos_integer(),
                   term(),
                   pos_integer()) -> lfs_manifest().
new_manifest(Bucket, FileName, UUID, ContentLength, ContentMd5, BlockSize) ->
    Blocks = initial_blocks(ContentLength, BlockSize),
    #lfs_manifest{bkey={Bucket, FileName},
                  uuid=UUID,
                  content_length=ContentLength,
                  content_md5=ContentMd5,
                  block_size=BlockSize,
                  blocks_remaining=Blocks}.

%% @doc Remove a chunk from the
%%      blocks_remaining field of Manifest
remove_block(Manifest, Chunk) ->
    Remaining = Manifest#lfs_manifest.blocks_remaining,
    Updated = sets:del_element(Chunk, Remaining),
    Manifest#lfs_manifest{blocks_remaining=Updated}.

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

set_to_sorted_list(Set) ->
    lists:sort(sets:to_list(Set)).

sorted_blocks_remaining(#lfs_manifest{blocks_remaining=Remaining}) ->
    set_to_sorted_list(Remaining).

%% @doc Return true or false
%%      depending on whether
%%      we're still waiting
%%      to accumulate more chunks
still_waiting(#lfs_manifest{blocks_remaining=Remaining}) ->
    sets:size(Remaining) =/= 0.
