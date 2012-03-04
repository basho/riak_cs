%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% TODO:
%% We're evenutally going to have to start storing more
%% than just a single lfs_manifest record at a key, and store
%% a list of them. This will be used so that we can still have
%% pointers to the blocks to do GC when an update has been made.
%% We also want to be able to continue to serve the "current"
%% version of an object as a new one is being streamed in. While
%% the new one is being streamed in, we're going to want to check point
%% the blocks so that we can do GC is the PUT fails.

-module(riak_moss_lfs_utils).

-include("riak_moss.hrl").

%% 16 bits & 1MB block size = 64GB max object size
%% 24 bits & 1MB block size = 16TB max object size
%% 32 bits & 1MB block size = 4PB max object size
-define(BLOCK_FIELD_SIZE, 16).

%% druuid:v4() uses 16 bytes in raw form.
-define(UUID_BYTES, 16).

%% TODO: Make this configurable via app env?
-define(CONTIGUOUS_BLOCKS, 16).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([block_count/1,
         block_count/2,
         initial_block_keynames/1,
         remaining_block_keynames/1,
         block_keynames/3,
         block_name/3,
         block_name_to_term/1,
         block_size/0,
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
-export([chash_moss_keyfun/1,
         cooked_to_raw_v4/1,
         raw_to_cooked_v4/1]).

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

block_name(_Key, UUID, Number) ->
    %%
    %% Using 16 bits for UUID, we're at 18 bytes for the new scheme
    %% and 55+ bytes for the old scheme.  We don't need the name in
    %% the key (but it might be a very good idea to add the S3 object
    %% name in the key's metadata dictionary), and the benefit of
    %% avoiding the ASCII hexadecimal encoding for the druuid:v4()
    %% UUID is 20 saved bytes.
    RawUUID = cooked_to_raw_v4(UUID),
    <<RawUUID/binary, Number:?BLOCK_FIELD_SIZE>>.

block_name_to_term(Name) ->
    <<RawUUID:77/binary, Number:?BLOCK_FIELD_SIZE>> = Name,
    {raw_to_cooked_v4(RawUUID), Number}.

%% @doc Return the configured block size
-spec block_size() -> pos_integer().
block_size() ->
    case application:get_env(riak_moss, lfs_block_size) of
        undefined ->
            ?DEFAULT_LFS_BLOCK_SIZE;
        {ok, BlockSize} ->
            case BlockSize > ?DEFAULT_LFS_BLOCK_SIZE of
                true ->
                    ?DEFAULT_LFS_BLOCK_SIZE;
                false ->
                    BlockSize
            end
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

%% @doc Default object/ring hashing fun, direct passthrough of bkey.
-spec chash_moss_keyfun({binary(), binary()}) -> binary().
chash_moss_keyfun({<<"b:", _/binary>> = Bucket,
                   <<UUID:?UUID_BYTES/binary, BlockNum:?BLOCK_FIELD_SIZE>>}) ->
    Contig = BlockNum div ?CONTIGUOUS_BLOCKS,
    chash:key_of({Bucket, <<UUID/binary, Contig:?BLOCK_FIELD_SIZE>>});
chash_moss_keyfun({Bucket, Key}) ->
    chash:key_of({Bucket, Key}).

%% This is a temp hack .. I'd like to see the druuid:v4() return the
%% raw data rather than do this kind of silly conversion, all in the
%% name of saving 50% space in the UUID....

cooked_to_raw_v4(<<B1:8/binary, $-:8, B2:4/binary, 
                   $-:8, B3:4/binary, $-:8, B4:4/binary,
                   $-:8, B5:12/binary>>) ->
    N1 = httpd_util:hexlist_to_integer(binary_to_list(B1)),
    N2 = httpd_util:hexlist_to_integer(binary_to_list(B2)),
    N3 = httpd_util:hexlist_to_integer(binary_to_list(B3)),
    N4 = httpd_util:hexlist_to_integer(binary_to_list(B4)),
    N5 = httpd_util:hexlist_to_integer(binary_to_list(B5)),
    <<N1:(4*8), N2:(2*8), N3:(2*8), N4:(2*8), N5:(6*8)>>;
cooked_to_raw_v4(Else) ->
    %% This passthrough clause is for EUnit tests
    Else.

%% This function isn't exactly gracefully efficient, but no code is
%% calling it yet because nobody is calling block_name_to_term/1 yet.

raw_to_cooked_v4(<<N1:(4*8), N2:(2*8), N3:(2*8), N4:(2*8), N5:(6*8)>>) ->
    B1 = hexify(N1, 8),
    B2 = hexify(N2, 4),
    B3 = hexify(N3, 4),
    B4 = hexify(N4, 4),
    B5 = hexify(N5, 12),
    <<B1/binary, $-:8, B2/binary, $-:8, B3/binary, $-:8,
      B4/binary, $-:8, B5/binary>>;
raw_to_cooked_v4(Else) ->
    %% This passthrough clause is for EUnit tests
    Else.

hexify(N, Len) ->
    list_to_binary(pad(string:to_lower(httpd_util:integer_to_hexlist(N)), Len)).

pad(Str, Len) ->
    pad(Str, length(Str), Len).

pad(Str, StrLen, StrLen) ->
    Str;
pad(Str, StrLen, Len) ->
    [$0|pad(Str, StrLen + 1, Len)].

%%%%%%%%%%%%%%%%%%%%%%
%%
%% Tests
%%
%%%%%%%%%%%%%%%%%%%%%%

-ifdef(TEST).

raw_and_cooked_test_() ->
    [{timeout, 300,
     fun() ->
    [true] = lists:usort([begin Ux = druuid:v4(),
                                Ux == riak_moss_lfs_utils:raw_to_cooked_v4(
                                       riak_moss_lfs_utils:cooked_to_raw_v4(Ux))
                          end || _ <- lists:seq(1, 50000)])
     end}].

-endif. % TEST
