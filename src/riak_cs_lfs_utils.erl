%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_cs_lfs_utils).

-include("riak_cs.hrl").
-include("riak_cs_lfs.hrl").

-export([block_count/2,
         block_keynames/3,
         block_name/3,
         block_name_to_term/1,
         block_size/0,
         get_bclass/1,
         max_content_len/0,
         fetch_concurrency/0,
         put_concurrency/0,
         delete_concurrency/0,
         put_fsm_buffer_size_factor/0,
         get_fsm_buffer_size_factor/0,
         safe_block_size_from_manifest/1,
         initial_blocks/2,
         block_sequences_for_manifest/1,
         block_sequences_for_manifest/2,
         new_manifest/9,
         new_manifest/11,
         remove_write_block/2,
         remove_delete_block/2,
         calculate_bucket_class/1]).
-export([chash_cs_keyfun/1]).

%% -------------------------------------------------------------------
%% Public API
%% -------------------------------------------------------------------

%% @doc The number of blocks that this
%%      size will be broken up into
-spec block_count(non_neg_integer(), pos_integer()) -> non_neg_integer().
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
    case application:get_env(riak_cs, lfs_block_size) of
        undefined ->
            ?DEFAULT_LFS_BLOCK_SIZE;
        {ok, BlockSize} ->
            BlockSize
    end.

%% @doc Extract bucket bclass from manifest
-spec get_bclass(lfs_manifest()) -> riak_cs_utils:bclass().
get_bclass(?MANIFEST{props=Props}) ->
    proplists:get_value(bclass, Props, v0).

%% @doc Return the configured block size
-spec max_content_len() -> pos_integer().
max_content_len() ->

    case application:get_env(riak_cs, max_content_length) of
        undefined ->

            ?DEFAULT_MAX_CONTENT_LENGTH;
                {ok, MaxContentLen} ->
            MaxContentLen
    end.

safe_block_size_from_manifest(?MANIFEST{block_size=BlockSize}) ->
    case BlockSize of
        undefined ->
            block_size();
        _ -> BlockSize
    end.

%% @doc A list of all of the blocks that
%%      make up the file.
-spec initial_blocks(non_neg_integer(), pos_integer()) -> list().
initial_blocks(ContentLength, BlockSize) ->
    UpperBound = block_count(ContentLength, BlockSize),
    lists:seq(0, (UpperBound - 1)).

initial_blocks(ContentLength, SafeBlockSize, UUID) ->
    Bs = initial_blocks(ContentLength, SafeBlockSize),
    [{UUID, B} || B <- Bs].

-spec range_blocks(integer(), integer(), integer(), binary(), riak_cs_utils:bclass())->
      {[riak_cs_utils:next_block()], integer(), integer()}.
range_blocks(Start, End, SafeBlockSize, UUID, BClass) ->
    SkipInitial = Start rem SafeBlockSize,
    KeepFinal = (End rem SafeBlockSize) + 1,
    _ = lager:debug("InitialBlock: ~p, FinalBlock: ~p~n",
                [Start div SafeBlockSize, End div SafeBlockSize]),
    _ = lager:debug("SkipInitial: ~p, KeepFinal: ~p~n", [SkipInitial, KeepFinal]),
    {[{UUID, B, BClass} || B <- lists:seq(Start div SafeBlockSize, End div SafeBlockSize)],
     SkipInitial, KeepFinal}.

block_sequences_for_manifest(?MANIFEST{props=undefined}=Manifest) ->
    block_sequences_for_manifest(Manifest?MANIFEST{props=[]});
block_sequences_for_manifest(?MANIFEST{content_length=ContentLength}=Manifest)->
    block_sequences_for_manifest(Manifest, {0, ContentLength-1}).

block_sequences_for_manifest(?MANIFEST{props=undefined}=Manifest, {Start, End}) ->
    block_sequences_for_manifest(Manifest?MANIFEST{props=[]}, {Start, End});
block_sequences_for_manifest(?MANIFEST{uuid=UUID}=Manifest,
                             {Start, End})->
    SafeBlockSize = safe_block_size_from_manifest(Manifest),
    case riak_cs_mp_utils:get_mp_manifest(Manifest) of
        undefined ->
            BClass = riak_cs_lfs_utils:get_bclass(Manifest),
            range_blocks(Start, End, SafeBlockSize, UUID, BClass);
        MpM ->
            PartManifests = MpM?MULTIPART_MANIFEST.parts,
            block_sequences_for_part_manifests_skip(SafeBlockSize, PartManifests,
                                                    Start, End)
    end.

block_sequences_for_part_manifests_skip(SafeBlockSize, [PM | Rest],
                                        StartOffset, EndOffset) ->
    _ = lager:debug("StartOffset: ~p, EndOffset: ~p, PartLength: ~p~n",
                [StartOffset, EndOffset, PM?PART_MANIFEST.content_length]),
    BClass = PM?PART_MANIFEST.bclass,
    case PM?PART_MANIFEST.content_length of
        %% Skipped
        PartLength when PartLength =< StartOffset ->
            block_sequences_for_part_manifests_skip(
              SafeBlockSize, Rest,
              StartOffset - PartLength, EndOffset - PartLength);
        %% The first block, more blocks needed
        PartLength when PartLength =< EndOffset ->
            {Blocks, SkipInitial, _KeepFinal} =
                range_blocks(StartOffset, PartLength - 1,
                               SafeBlockSize, PM?PART_MANIFEST.part_id, BClass),
            block_sequences_for_part_manifests_keep(
              SafeBlockSize, SkipInitial, Rest,
              EndOffset - PartLength, [Blocks]);
        %% The first block, also the last
        _PartLength ->
            range_blocks(StartOffset, EndOffset,
                         SafeBlockSize, PM?PART_MANIFEST.part_id, BClass)
    end.

block_sequences_for_part_manifests_keep(SafeBlockSize, SkipInitial, [PM | Rest],
                                        EndOffset, ListOfBlocks) ->
    _ = lager:debug("EndOffset: EndOffset: ~p, PartLength: ~p~n",
                [EndOffset, PM?PART_MANIFEST.content_length]),
    case PM?PART_MANIFEST.content_length of
        %% More blocks needed
        PartLength when PartLength =< EndOffset ->
            IBlocks = initial_blocks(PM?PART_MANIFEST.content_length,
                                     SafeBlockSize, PM?PART_MANIFEST.part_id),
            BClass = PM?PART_MANIFEST.bclass,
            IBlocksBClass = [{X, Y, BClass} || {X, Y} <- IBlocks],
            block_sequences_for_part_manifests_keep(
              SafeBlockSize, SkipInitial, Rest,
              EndOffset - PartLength,
              [IBlocksBClass | ListOfBlocks]);
        %% Reaches to the last block
        _PartLength ->
            BClass = PM?PART_MANIFEST.bclass,
            {Blocks, _SkipInitial, KeepFinal}
                = range_blocks(0, EndOffset,
                               SafeBlockSize, PM?PART_MANIFEST.part_id, BClass),
            {lists:append(lists:reverse([Blocks | ListOfBlocks])),
             SkipInitial, KeepFinal}
    end.

%% @doc Return the configured file block fetch concurrency .
-spec fetch_concurrency() -> pos_integer().
fetch_concurrency() ->
    case application:get_env(riak_cs, fetch_concurrency) of
        undefined ->
            ?DEFAULT_FETCH_CONCURRENCY;
        {ok, Concurrency} ->
            Concurrency
    end.

%% @doc Return the configured file block put concurrency .
-spec put_concurrency() -> pos_integer().
put_concurrency() ->
    case application:get_env(riak_cs, put_concurrency) of
        undefined ->
            ?DEFAULT_PUT_CONCURRENCY;
        {ok, Concurrency} ->
            Concurrency
    end.

%% @doc Return the configured file block delete concurrency .
-spec delete_concurrency() -> pos_integer().
delete_concurrency() ->
    case application:get_env(riak_cs, delete_concurrency) of
        undefined ->
            ?DEFAULT_DELETE_CONCURRENCY;
        {ok, Concurrency} ->
            Concurrency
    end.

%% @doc Return the configured put fsm buffer
%% size factor
-spec put_fsm_buffer_size_factor() -> pos_integer().
put_fsm_buffer_size_factor() ->
    case application:get_env(riak_cs, put_buffer_factor) of
        undefined ->
            ?DEFAULT_PUT_BUFFER_FACTOR;
        {ok, Factor} ->
            Factor
    end.

%% @doc Return the configured get fsm buffer
%% size factor
-spec get_fsm_buffer_size_factor() -> pos_integer().
get_fsm_buffer_size_factor() ->
    case application:get_env(riak_cs, fetch_buffer_factor) of
        undefined ->
            ?DEFAULT_FETCH_BUFFER_FACTOR;
        {ok, Factor} ->
            Factor
    end.

%% @doc Initialize a new file manifest
-spec new_manifest(binary(),
                   binary(),
                   binary(),
                   non_neg_integer(),
                   binary(),
                   term(),
                   term(),
                   pos_integer(),
                   acl() | no_acl_yet) -> lfs_manifest().
new_manifest(Bucket, FileName, UUID, ContentLength, ContentType, ContentMd5, MetaData, BlockSize, Acl) ->
    new_manifest(Bucket, FileName, UUID, ContentLength, ContentType, ContentMd5, MetaData, BlockSize, Acl, [], undefined).

-spec new_manifest(binary(),
                   binary(),
                   binary(),
                   non_neg_integer(),
                   binary(),
                   term(),
                   term(),
                   pos_integer(),
                   acl() | no_acl_yet,
                   proplists:proplist(),
                   cluster_id()) -> lfs_manifest().
new_manifest(Bucket, FileName, UUID, ContentLength, ContentType, ContentMd5, MetaData, BlockSize, Acl, Props, ClusterID) ->
    Blocks = ordsets:from_list(initial_blocks(ContentLength, BlockSize)),
    BClass = calculate_bucket_class(ContentLength),
    ?MANIFEST{bkey={Bucket, FileName},
              uuid=UUID,
              state=writing,
              content_length=ContentLength,
              content_type=ContentType,
              content_md5=ContentMd5,
              block_size=BlockSize,
              write_blocks_remaining=Blocks,
              metadata=MetaData,
              acl=Acl,
              props=[{bclass, BClass}|Props],
              cluster_id=ClusterID}.

%% @doc Remove a chunk from the
%%      write_blocks_remaining field of Manifest
remove_write_block(Manifest, Chunk) ->
    Remaining = Manifest?MANIFEST.write_blocks_remaining,
    Updated = ordsets:del_element(Chunk, Remaining),
    ManiState = case Updated of
                    [] ->
                        active;
                    _ ->
                        writing
                end,
    Manifest?MANIFEST{write_blocks_remaining=Updated,
                             state=ManiState,
                             last_block_written_time=os:timestamp()}.

%% @doc Remove a chunk from the `delete_blocks_remaining'
%% field of `Manifest'
remove_delete_block(Manifest, Chunk) ->
    Remaining = Manifest?MANIFEST.delete_blocks_remaining,
    Updated = ordsets:del_element(Chunk, Remaining),
    ManiState = case Updated of
                    [] ->
                        deleted;
                    _ ->
                        scheduled_delete
                end,
    Manifest?MANIFEST{delete_blocks_remaining=Updated,
                             state=ManiState,
                             last_block_deleted_time=os:timestamp()}.


-spec chash_cs_keyfun({binary(), binary()}) -> binary().
chash_cs_keyfun({<<Prefix:3/binary, _/binary>> = Bucket,
                 <<UUID:?UUID_BYTES/binary, BlockNum:?BLOCK_FIELD_SIZE>>})
  when Prefix == ?BLOCK_BUCKET_PREFIX_V0;
       Prefix == ?BLOCK_BUCKET_PREFIX_V1;
       Prefix == ?BLOCK_BUCKET_PREFIX_V2 ->
    Contig = BlockNum div ?FS2_CONTIGUOUS_BLOCKS,
    chash:key_of({Bucket, <<UUID/binary, Contig:?BLOCK_FIELD_SIZE>>});
chash_cs_keyfun({Bucket, Key}) ->
    %% Default object/ring hashing fun, direct passthrough of bkey.
    chash:key_of({Bucket, Key}).

-spec calculate_bucket_class(integer()) -> 'v1' | 'v2'.
calculate_bucket_class(ContentLength) ->
    Dividing = case application:get_env(riak_cs, small_object_divider) of
                   {ok, N} when is_integer(N), N >= 0 ->
                       N;
                   undefined ->
                       125*1024;
                   _ ->
                       0
               end,
    if ContentLength < Dividing ->
            v1;
       true ->
            v2
    end.
