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

-export([block_count/2,
         block_keynames/3,
         block_name/3,
         block_name_to_term/1,
         block_size/0,
         max_content_len/0,
         fetch_concurrency/0,
         put_concurrency/0,
         put_fsm_buffer_size_factor/0,
         safe_block_size_from_manifest/1,
         initial_blocks/2,
         block_sequences_for_manifest/1,
         is_manifest/1,
         new_manifest/9,
         new_manifest/11,
         remove_write_block/2,
         sorted_blocks_remaining/1]).

%% -------------------------------------------------------------------
%% Public API
%% -------------------------------------------------------------------

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

block_keynames(KeyName, UUID, BlockList) ->
    MapFun = fun(BlockSeq) ->
                     {BlockSeq, block_name(KeyName, UUID, BlockSeq)} end,
    lists:map(MapFun, BlockList).

block_name(_Key, UUID, Number) ->
    %% 16 bits & 1MB chunk size = 64GB max object size
    %% 24 bits & 1MB chunk size = 16TB max object size
    %% 32 bits & 1MB chunk size = 4PB max object size
    <<UUID/binary, Number:32>>.

block_name_to_term(<<UUID:16/binary, Number:32>>) ->
    {UUID, Number}.

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

safe_block_size_from_manifest(#lfs_manifest_v2{block_size=BlockSize}) ->
    case BlockSize of
        undefined ->
            block_size();
        _ -> BlockSize
    end.

%% @doc A list of all of the blocks that
%%      make up the file.
-spec initial_blocks(pos_integer(), pos_integer()) -> list().
initial_blocks(ContentLength, BlockSize) ->
    UpperBound = block_count(ContentLength, BlockSize),
    lists:seq(0, (UpperBound - 1)).

block_sequences_for_manifest(#lfs_manifest_v2{content_length=ContentLength}=Manifest) ->
    SafeBlockSize = safe_block_size_from_manifest(Manifest),
    initial_blocks(ContentLength, SafeBlockSize).

%% @doc Return the configured file block fetch concurrency .
-spec fetch_concurrency() -> pos_integer().
fetch_concurrency() ->
    case application:get_env(riak_moss, fetch_concurrency) of
        undefined ->
            ?DEFAULT_FETCH_CONCURRENCY;
        {ok, Concurrency} ->
            Concurrency
    end.

%% @doc Return the configured file block put concurrency .
-spec put_concurrency() -> pos_integer().
put_concurrency() ->
    case application:get_env(riak_moss, put_concurrency) of
        undefined ->
            ?DEFAULT_PUT_CONCURRENCY;
        {ok, Concurrency} ->
            Concurrency
    end.

%% @doc Return the configured put fsm buffer
%% size factor
-spec put_fsm_buffer_size_factor() -> pos_integer().
put_fsm_buffer_size_factor() ->
    case application:get_env(riak_moss, put_buffer_factor) of
        undefined ->
            ?DEFAULT_PUT_BUFFER_FACTOR;
        {ok, Factor} ->
            Factor
    end.

%% @doc Returns true if Value is
%%      a manifest record
is_manifest(BinaryValue) ->
    try binary_to_term(BinaryValue) of
        Term ->
            is_record(Term, lfs_manifest_v2)
    catch
        error:badarg ->
            false
    end.

%% @doc Initialize a new file manifest
-spec new_manifest(binary(),
                   binary(),
                   binary(),
                   binary(),
                   pos_integer(),
                   term(),
                   term(),
                   pos_integer(),
                   acl()) -> lfs_manifest().
new_manifest(Bucket, FileName, UUID, ContentLength, ContentType, ContentMd5, MetaData, BlockSize, Acl) ->
    new_manifest(Bucket, FileName, UUID, ContentLength, ContentType, ContentMd5, MetaData, BlockSize, Acl, [], undefined).

-spec new_manifest(binary(),
                   binary(),
                   binary(),
                   binary(),
                   pos_integer(),
                   term(),
                   term(),
                   pos_integer(),
                   acl(),
                   proplists:proplist(),
                   cluster_id()) -> lfs_manifest().
new_manifest(Bucket, FileName, UUID, ContentLength, ContentType, ContentMd5, MetaData, BlockSize, Acl, Props, ClusterID) ->
    Blocks = ordsets:from_list(initial_blocks(ContentLength, BlockSize)),
    #lfs_manifest_v2{bkey={Bucket, FileName},
                     uuid=UUID,
                     state=writing,
                     content_length=ContentLength,
                     content_type=ContentType,
                     content_md5=ContentMd5,
                     block_size=BlockSize,
                     write_blocks_remaining=Blocks,
                     metadata=MetaData,
                     acl=Acl,
                     props=Props,
                     cluster_id=ClusterID}.

%% @doc Remove a chunk from the
%%      write_blocks_remaining field of Manifest
remove_write_block(Manifest, Chunk) ->
    Remaining = Manifest#lfs_manifest_v2.write_blocks_remaining,
    Updated = ordsets:del_element(Chunk, Remaining),
    ManiState = case Updated of
                    [] ->
                        active;
                    _ ->
                        writing
                end,
    Manifest#lfs_manifest_v2{write_blocks_remaining=Updated,
                             state=ManiState,
                             last_block_written_time=erlang:now()}.

sorted_blocks_remaining(#lfs_manifest_v2{write_blocks_remaining=Remaining}) ->
    lists:sort(sets:to_list(Remaining)).
