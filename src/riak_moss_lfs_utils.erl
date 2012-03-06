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
         initial_block_keynames/1,
         remaining_block_keynames/1,
         block_keynames/3,
         block_name/3,
         block_name_to_term/1,
         block_size/0,
         max_content_len/0,
         safe_block_size_from_manifest/1,
         file_uuid/1,
         finalize_manifest/2,
         initial_blocks/1,
         initial_blocks/2,
         block_sequences_for_manifest/1,
         is_manifest/1,
         metadata_from_manifest/1,
         new_manifest/6,
         remove_block/2,
         set_inactive/1,
         sorted_blocks_remaining/1,
         still_waiting/1,
         content_md5/1,
         content_length/1,
         created/1,
         is_active/1]).

-export_type([lfs_manifest/0]).

%% Opaque record for a large-file manifest.
-record(lfs_manifest, {
    version=1 :: integer(),
    uuid :: binary(),
    block_size :: integer(),
    bkey :: {binary(), binary()},
    content_length :: integer(),
    content_md5 :: term(),
    metadata :: dict(),
    created=riak_moss_wm_utils:iso_8601_datetime(),
    finished :: term(),
    active=false :: boolean(),
    blocks_remaining = sets:new()}).

-type lfs_manifest() :: #lfs_manifest{}.

%% -------------------------------------------------------------------
%% Public API
%% -------------------------------------------------------------------

%% @doc The number of blocks that this
%%      size will be broken up into
-spec block_count(lfs_manifest() | pos_integer()) -> non_neg_integer().
block_count(ContentLength) when is_integer(ContentLength) ->
    block_count(ContentLength, block_size());
block_count(Manifest) ->
    block_count(Manifest#lfs_manifest.content_length,
                Manifest#lfs_manifest.block_size).

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

initial_block_keynames(#lfs_manifest{bkey={_, KeyName},
                             uuid=UUID,
                             content_length=ContentLength}) ->
    BlockList = initial_blocks(ContentLength),
    block_keynames(KeyName, UUID, BlockList).

remaining_block_keynames(#lfs_manifest{bkey={_, KeyName},
                             uuid=UUID}=Manifest) ->
    BlockList = sorted_blocks_remaining(Manifest),
    block_keynames(KeyName, UUID, BlockList).

block_keynames(KeyName, UUID, BlockList) ->
    MapFun = fun(BlockSeq) ->
        {BlockSeq, block_name(KeyName, UUID, BlockSeq)} end,
    lists:map(MapFun, BlockList).

block_name(Key, UUID, Number) ->
    sext:encode({Key, Number, UUID}).

block_name_to_term(Name) ->
    {Key, Number, UUID} = sext:decode(Name),
    {Key, UUID, Number}.

%% @doc Return the configured block size
-spec block_size() -> pos_integer().
block_size() ->
    case application:get_env(riak_moss, lfs_block_size) of
        undefined ->
            ?DEFAULT_LFS_BLOCK_SIZE;
        {ok, BlockSize} ->
            BlockSize
    end.

%% @doc Return the configured block size
-spec max_content_len() -> pos_integer().
max_content_len() ->
    case application:get_env(riak_moss, max_content_length) of
        undefined ->
            ?DEFAULT_MAX_CONTENT_LENGTH;
        {ok, MaxContentLen} ->
            MaxContentLen
    end.

safe_block_size_from_manifest(#lfs_manifest{block_size=BlockSize}) ->
    case BlockSize of
        undefined ->
            block_size();
        _ -> BlockSize
    end.

%% @doc Get the file UUID from the manifest.
-spec file_uuid(lfs_manifest()) -> binary().
file_uuid(#lfs_manifest{uuid=UUID}) ->
    UUID.

%% @doc Finalize the manifest of a file by
%% marking it as active, setting a finished time,
%% and setting blocks_remaining as an empty list.
-spec finalize_manifest(lfs_manifest(), binary()) -> lfs_manifest().
finalize_manifest(Manifest, MD5) ->
    Manifest#lfs_manifest{active=true,
                          content_md5=MD5,
                          finished=httpd_util:rfc1123_date(),
                          blocks_remaining=sets:new()}.

%% @doc A set of all of the blocks that
%%      make up the file.
-spec initial_blocks(pos_integer()) -> set().
initial_blocks(ContentLength) ->
    initial_blocks(ContentLength, block_size()).

%% @doc A set of all of the blocks that
%%      make up the file.
-spec initial_blocks(pos_integer(), pos_integer()) -> set().
initial_blocks(ContentLength, BlockSize) ->
    UpperBound = block_count(ContentLength, BlockSize),
    lists:seq(0, (UpperBound - 1)).

block_sequences_for_manifest(#lfs_manifest{content_length=ContentLength}=Manifest) ->
    SafeBlockSize = safe_block_size_from_manifest(Manifest),
    initial_blocks(ContentLength, SafeBlockSize).

%% @doc Returns true if Value is
%%      a manifest record
is_manifest(BinaryValue) ->
    try binary_to_term(BinaryValue) of
        Term ->
            is_record(Term, lfs_manifest)
    catch
        error:badarg ->
            false
    end.

%% @doc Return the metadata for the object
%%      represented in the manifest
metadata_from_manifest(#lfs_manifest{metadata=Metadata}) ->
    Metadata.

%% @doc Initialize a new file manifest
-spec new_manifest(binary(),
                   binary(),
                   binary(),
                   pos_integer(),
                   term(),
                   dict()) -> lfs_manifest().
new_manifest(Bucket, FileName, UUID, ContentLength, ContentMd5, MetaData) ->
    BlockSize = block_size(),
    Blocks = sets:from_list(initial_blocks(ContentLength, BlockSize)),
    #lfs_manifest{bkey={Bucket, FileName},
                  uuid=UUID,
                  content_length=ContentLength,
                  content_md5=ContentMd5,
                  block_size=BlockSize,
                  blocks_remaining=Blocks,
                  metadata=MetaData}.

%% @doc Remove a chunk from the
%%      blocks_remaining field of Manifest
remove_block(Manifest, Chunk) ->
    Remaining = Manifest#lfs_manifest.blocks_remaining,
    Updated = sets:del_element(Chunk, Remaining),
    Manifest#lfs_manifest{blocks_remaining=Updated}.

%% @doc Mark a file manifest as inactive by setting
%% the `active' field of the manifest to `false'.
-spec set_inactive(lfs_manifest()) -> lfs_manifest().
set_inactive(Manifest) ->
    Manifest#lfs_manifest{active=false}.

sorted_blocks_remaining(#lfs_manifest{blocks_remaining=Remaining}) ->
    lists:sort(sets:to_list(Remaining)).

%% @doc Return true or false
%%      depending on whether
%%      we're still waiting
%%      to accumulate more chunks
still_waiting(#lfs_manifest{blocks_remaining=Remaining}) ->
    sets:size(Remaining) =/= 0.

-spec content_md5(lfs_manifest()) -> binary().
content_md5(#lfs_manifest{content_md5=ContentMD5}) ->
    ContentMD5.

-spec content_length(lfs_manifest()) -> integer().
content_length(#lfs_manifest{content_length=CL}) ->
    CL.

-spec created(lfs_manifest()) -> string().
created(#lfs_manifest{created=Created}) ->
    Created.

-spec is_active(lfs_manifest()) -> boolean().
is_active(#lfs_manifest{active=A}) ->
    A.
