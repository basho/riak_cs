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

%% @doc riak_fs2_backend: storage engine based on basic filesystem access
%%
%% Features:
%%
%% * The behavior & features of this backend are closely tied to
%%   the RiakCS application.
%%
%% * We assume that RiakCS's LFS chunks will be written in ascending order.
%%   If they are written out of order, we'll deal with it sanely but
%%   perhaps not in a 100% optimal way.
%%
%% * We assume that it is not a fatal flaw if folding over BKeys/whatever
%%   is not 100% accurate: we might say that a LFS chunk exists in a fold
%%   but sometime later cannot read that chunk.  (RiakCS is expected to
%%   do very few such folds, and very occasionally reporting chunks that
%%   don't really exist is OK.)
%%
%% * We assume that if someone deletes one LFS chunk from an LFS UUID,
%%   then it may or may not be possible to fetch other chunks that
%%   belong to that same UUID.  (If RiakCS deletes a chunk, it's going
%%   to delete the rest of them soon.)
%%
%% TODO list for the multiple-chunk-in-single-file scheme:
%%
%% XX Test-first: create EQC model, test it ruthlessly
%%    XX Include all ops, including folding!
%%       DONE via backend_eqc
%%
%% XX Make certain that max chunk size is strictly enforced.
%% XX Add unit test to make certain that the backend gives graceful
%%    error when chunk size limit is exceeded. DONE: t3_test()
%%
%% XX Add a version number to the ?HEADER_SIZE for all put(),
%% XX Check it on get()s.
%%
%% __ When using with RCS, add a programmatic check for the LFS block size.
%%    Then add extra cushion (e.g. 512 bytes??) and use that for block size?
%%
%% __ Double-check that the api_version & capabilities/0 func is doing
%%    the right thing wrt riak_kv.
%%
%% __ http://erlang.org/pipermail/erlang-questions/2011-September/061147.html
%%    __ Avoid `file` module whenever file_server_2 calls are used.
%%    __ Avoid file:file_name_1() recursion silliness
%%
%% __ Add capability to extend put() and get() to avoid getting an
%%    already-encoded-Riak-object and returning same.  If
%%    riak_kv_vnode gives us an encoded object, we need to unencoded
%%    it just to see if there's a tombstone in the metadata.  We know
%%    that R15B* and R16B releases have scheduler sticking problems
%%    with term_to_binary() conversions.  Doing extra conversions is
%%    both a waste of CPU time and a way to anger the Erlang process
%%    scheduler gods.
%%
%% no Implement the delete op:
%%    __ Update the deleted chunk map:
%%        if undeleted chunks, write new map
%%        if all chunks are deleted, delete file
%%    HRRRMMMMM, this kind of update-in-place is by definition not append-only.
%%    WHAT IF?  1. A fixed size deleted trailer block were appended
%%                 at the offset of the Max+1 block?
%%                 A stat(2) call can tell us if any blocks have been
%%                 deleted yet.  (Or if pread(2) returns EOF)
%%              2. If trailer is present, check last trailer for latest map.
%%    INSTEAD, use hack of one-tombstone-for-all and one-delete-for-all.
%%
%% __ Read cache: if reading 0th chunk, create read-ahead that will
%%    read entire file async'ly and send it as a message to be recieved
%%    later.  When reading 1st chunk, wait for arrival of read-ahead block.
%%    __ Needs a real cache though.  Steal the cache from the memory backend?
%%       Use the entire riak_memory_backend as-is??  Triple-check that ETS
%%       isn't going to copy off-heap/shared binaries....

-module(riak_kv_fs2_backend).
%% -behavior(riak_kv_backend). % Not building within riak_kv

%% KV Backend API
-export([api_version/0,
         capabilities/1,
         capabilities/2,
         start/2,
         stop/1,
         get/3,
         put/5,                 % deprecated in favor of put/6
         delete/4,
         drop/1,
         fold_buckets/4,
         fold_keys/4,
         fold_objects/4,
         is_empty/1,
         status/1,
         callback/3]).
%% KV Backend API based on capabilities
-export([put/6]).
%% Testing
-export([t0/0, t1/0, t2/0, t3/0, t4/0]).

-include("riak_cs_lfs.hrl").
-include_lib("kernel/include/file.hrl").

-define(API_VERSION, 1).
-define(CAPABILITIES, [raw_object, async_fold, write_once_keys]).

%%% -define(TEST_IN_RIAK_KV, true).

%% Borrowed from bitcask.hrl
-define(VALSIZEFIELD, 32).
-define(CRCSIZEFIELD, 32).
%% 8 = header cookie & version number, 4 = Value size, 4 = Erlang CRC32,
-define(HEADER_SIZE,  (8+4+4)). % Differs from bitcask.hrl!
-define(MAXVALSIZE, 2#11111111111111111111111111111111).
-define(COOKIE_SIZE, 64).
-define(COOKIE_V1, 16#c0c00101c00101c0).

%% !@#$!!@#$!@#$!@#$! Erlang's efile_drv does not support the sticky bit.
%% So, we use something else: the setgid bit.
-define(TOMBSTONE_MODE_MARKER, 8#2000).

-ifdef(TEST).
-compile(export_all).
-ifdef(EQC).
%% EQC testing assistants
-export([eqc_filter_delete/3, eqc_filter_list_keys/2]).
-export([prop_nest_ordered/0, eqc_nest_tester/0]).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {
          dir        :: string(),
          block_size :: non_neg_integer(),
          max_blocks :: non_neg_integer(),
          b_depth    :: non_neg_integer(),
          k_depth    :: non_neg_integer(),
          %% Items below used by key listing stack only
          fold_type  :: 'undefined' | 'buckets' | 'keys' | 'objects'
         }).

%% File trailer
-record(t, {
          written_sequentially :: boolean()
         }).

-type state() :: #state{}.
-type config() :: [{atom(), term()}].

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Return the major version of the
%% current API.
-spec api_version() -> {ok, integer()}.
api_version() ->
    {ok, ?API_VERSION}.

%% @doc Return the capabilities of the backend.
-spec capabilities(state()) -> {ok, [atom()]}.
capabilities(State) ->
    capabilities(fake_bucket_name, State).

%% @doc Return the capabilities of the backend.
-spec capabilities(riak_object:bucket(), state()) -> {ok, [atom()]}.
capabilities(_Bucket, _State) ->
    {ok, ?CAPABILITIES}.

%% @doc Start this backend.

-spec start(integer(), config()) -> {ok, state()}.
start(Partition, Config) ->
    PartitionName = integer_to_list(Partition),
    try
        ConfigRoot = get_prop_or_env(data_root, Config, riak_kv,
                                     {{"data_root unset, failing"}}),
        BlockSize = get_prop_or_env(block_size, Config, riak_kv,
                                    {{"block_size unset, failing"}}),
        true = (BlockSize < ?MAXVALSIZE),
        %% MaxBlocks should be present only for testing
        MaxBlocks = case get_prop_or_env(max_blocks_per_file, Config,
                                         riak_kv, undefined) of
                        N when is_integer(N), 1 =< N,
                                              N =< ?FS2_CONTIGUOUS_BLOCKS ->
                            error_logger:warning_msg(
                              "~s: max_blocks_per_file is ~p.  This "
                              "configuration item is only valid for tests.  "
                              "Proceed with caution.",
                              [?MODULE, N]),
                            %% Go ahead and use it
                            N;
                        undefined ->
                            ?FS2_CONTIGUOUS_BLOCKS;
                        _ ->
                            error_logger:warning_msg(
                              "~s: invalid max blocks value, using ~p\n",
                              [?MODULE, ?FS2_CONTIGUOUS_BLOCKS]),
                            ?FS2_CONTIGUOUS_BLOCKS
                    end,
        Dir = filename:join([ConfigRoot,PartitionName]),
        ok = filelib:ensure_dir(Dir),
        BDepth = get_prop_or_env(b_depth, Config, riak_kv, 2),
        KDepth = get_prop_or_env(k_depth, Config, riak_kv, 2),
        ok = create_or_sanity_check_version_file(Dir, BlockSize, MaxBlocks,
                                                 BDepth, KDepth),
        {ok,  #state{dir = Dir,
                     block_size = BlockSize,
                     max_blocks = MaxBlocks,
                     b_depth = BDepth,
                     k_depth = KDepth}}
    catch throw:Error ->
        {error, Error}
    end.

%% @doc Stop the backend
-spec stop(state()) -> ok.
stop(_State) -> ok.

%% @doc Get the object stored at the given bucket/key pair
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()} |
                 {ok, not_found, state()} |
                 {error, term(), state()}.
get(<<?BLOCK_BUCKET_PREFIX, _/binary>> = Bucket,
    <<UUID:?UUID_BYTES/binary, BlockNum:?BLOCK_FIELD_SIZE>>, State) ->
    case read_block(Bucket, UUID, BlockNum, State) of
        Bin when is_binary(Bin) ->
            %% io:format("Got ~p bytes ~P\n", [byte_size(Bin), catch binary_to_term(Bin), 20]),
            %% D = riak_object:get_metadatas(binary_to_term(Bin)),
            %% io:format("MD ~P\n", [D, 125]),
            {ok, Bin, State};
        Reason when is_atom(Reason) ->
            {error, not_found, State}
    end;
get(Bucket, Key, State) ->
    File = location(State, Bucket, Key),
    case filelib:is_file(File) of
        false ->
            {error, not_found, State};
        true ->
            {ok, Bin} = read_file(File),
            case unpack_ondisk(Bin) of
                bad_crc ->
                    %% TODO logging?
                    {error, not_found, State};
                Val ->
                    {ok, Val, State}
            end
    end.

put(Bucket, Key, IndexSpecs, Val, State) ->
    put(Bucket, Key, IndexSpecs, Val, binary_to_term(Val), State).

%% @doc Store Val under Bucket and Key
%%
%% NOTE: Val is a copy of ValRObj that has been encoded by term_to_binary()

-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), riak_object:riak_object(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.
put(<<?BLOCK_BUCKET_PREFIX, _/binary>>,
    <<_:?UUID_BYTES/binary, _:?BLOCK_FIELD_SIZE>>,
    _IndexSpecs, Val, _ValRObj, #state{block_size = BlockSize} = State)
  when size(Val) > BlockSize ->
    {error, invalid_user_argument, State};
put(<<?BLOCK_BUCKET_PREFIX, _/binary>> = Bucket,
    <<UUID:?UUID_BYTES/binary, BlockNum:?BLOCK_FIELD_SIZE>>,
    _IndexSpecs, Val, ValRObj, State) ->
    case put_block(Bucket, UUID, BlockNum, Val, ValRObj, State) of
        ok ->
            {ok, State};
        Reason ->
            {error, Reason, State}
    end;
put(Bucket, PrimaryKey, _IndexSpecs, Val, _ValRObj, State) ->
    File = location(State, Bucket, PrimaryKey),
    case filelib:ensure_dir(File) of
        ok ->
            case atomic_write(File, Val, State) of
                ok         -> {ok, State};
                {error, X} -> {error, X, State}
            end;
        {error, X} -> {error, X, State}
    end.

%% @doc Delete the object stored at BKey
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(<<?BLOCK_BUCKET_PREFIX, _/binary>> = Bucket,
       <<UUID:?UUID_BYTES/binary, BlockNum:?BLOCK_FIELD_SIZE>>,
       _IndexSpecs, State) ->
    Key = convert_blocknum2key(UUID, BlockNum, State),
    File = location(State, Bucket, Key),
    _ = file:delete(File),
    {ok, State};
delete(Bucket, Key, _IndexSpecs, State) ->
    File = location(State, Bucket, Key),
    case file:delete(File) of
        ok -> {ok, State};
        {error, enoent} -> {ok, State};
        {error, Err} -> {error, Err, State}
    end.

%% Notes about folding refactoring:
%%
%% 1. Avoid requiring tremendous amounts of RAM.  lists:foldl/3 requires
%%    knowing all foldable items in advance.
%% 2. We need to move away from */*/*/*/* style globbing, also for RAM
%%    reasons.
%% 3. It would be nice to be able to stream items, e.g. key listing,
%%    in real-time, rather than waiting for something like lists:foldl/3
%%    to finish processing the last key.
%%    * Implementing this requirement can wait a while.
%%
%% Implementation notes/steps
%%
%% a. Create all objects fold first
%% b. The limit object fold on a single bucket by limiting the initial glob.
%% c. Then write key fold
%% d. Then write key fold on a single bucket, using same trick as b.
%% e. Write all buckets fold, using limited glob PLUS a check for a single
%%    undeleted key inside that bucket.

%% @doc Fold over all the buckets.
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(),
                   any(),
                   [],
                   state()) -> {ok, any()} | {async, fun()}.
fold_buckets(FoldBucketsFun, Acc, Opts, State) ->
    fold_common(fold_buckets_fun(FoldBucketsFun), Acc, Opts,
                State#state{fold_type = buckets}).

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()} | {async, fun()}.
fold_keys(FoldKeysFun, Acc, Opts, State) ->
    fold_common(fold_keys_fun(FoldKeysFun), Acc, Opts,
                State#state{fold_type = keys}).

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {async, fun()}.
fold_objects(FoldObjectsFun, Acc, Opts, State) ->
    fold_common(fold_objects_fun(FoldObjectsFun), Acc, Opts,
                State#state{fold_type = objects}).

fold_common(ThisFoldFun, Acc, Opts, State) ->
    Bucket =  proplists:get_value(bucket, Opts),
    Stack = make_start_stack_objects(Bucket, State),
    ObjectFolder =
        fun() ->
                reduce_stack(Stack, ThisFoldFun, Acc, State)
        end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, ObjectFolder};
        false ->
            {ok, ObjectFolder()}
    end.

%% @doc Delete all objects from this backend
%% and return a fresh reference.
-spec drop(state()) -> {ok, state()}.
drop(State=#state{dir=Dir}) ->
    Cmd = io_lib:format("rm -rf ~s", [Dir]),
    _ = os:cmd(Cmd),
    ok = filelib:ensure_dir(Dir),
    {ok, State}.

%% @doc Returns true if this backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean().
is_empty(S) ->
    list_all_keys(S) == [].

%% @doc Get the status information for this fs backend
-spec status(state()) -> [no_status_sorry | {atom(), term()}].
status(_S) ->
    [no_status_sorry_TODO].

%% @doc Register an asynchronous callback
-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(_Ref, _Term, S) ->
    {ok, S}.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @spec atomic_write(File :: string(), Val :: binary()) ->
%%       ok | {error, Reason :: term()}
%% @doc store a atomic value to disk. Write to temp file and rename to
%%       normal path.
atomic_write(File, Val, State) ->
    FakeFile = File ++ ".tmpwrite",
    case write_file(FakeFile, pack_ondisk(Val, State)) of
        ok ->
            file:rename(FakeFile, File);
        X -> X
    end.

%% @private
%% Return a function to fold over the buckets on this backend
fold_buckets_fun(FoldBucketsFun) ->
    fun({Bucket, _Nosuchkey}, _Nosuchvalue, Acc) ->
            FoldBucketsFun(Bucket, Acc)
    end.

%% @private
%% Return a function to fold over keys on this backend
fold_keys_fun(FoldKeysFun) ->
    fun(BKey, _Value, Acc) ->
            {Bucket, Key} = BKey,
            FoldKeysFun(Bucket, Key, Acc)
    end.

%% @private
%% Return a function to fold over the objects on this backend
fold_objects_fun(FoldObjectsFun) ->
    fun(BKey, Value, Acc) ->
            {Bucket, Key} = BKey,
            FoldObjectsFun(Bucket, Key, Value, Acc)
    end.

%% @spec list_all_files_naive_bkeys(state()) -> [{Bucket :: riak_object:bucket(),
%%                                    Key :: riak_object:key()}]
%% @doc Get a list of all bucket/key pairs stored by this backend
list_all_files_naive_bkeys(#state{dir=Dir, b_depth = BDepth,
                                  k_depth = KDepth} = State) ->
    Glob = lists:flatten([
                          lists:duplicate(BDepth, "*/"), % bucket intermediates
                          "*/",                          % bucket
                          lists:duplicate(KDepth, "*/"), % key intermediates
                          "*"                            % key
                         ]),
    % this is slow slow slow
    [location_to_bkey(X, State) || X <- filelib:wildcard(Glob, Dir)].

%% @spec list_all_keys([string()]) -> [{Bucket :: riak_object:bucket(),
%%                                      Key :: riak_object:key()}]
%% @doc Get a list of all bucket/key pairs stored by this backend
list_all_keys(State) ->
    L = lists:foldl(
          fun({<<?BLOCK_BUCKET_PREFIX, _/binary>> = Bucket,
               <<UUID:?UUID_BYTES/binary,
                 BlockBase:?BLOCK_FIELD_SIZE>>}, Acc) ->
                  Chunks = enumerate_chunks_in_file(
                             Bucket, UUID, BlockBase, State),
                  ChunkBKeys = [{Bucket, <<UUID:?UUID_BYTES/binary,
                                           (BlockBase+C):?BLOCK_FIELD_SIZE>>} ||
                                   C <- Chunks],
                  lists:reverse(ChunkBKeys, Acc);
             (BKey, Acc) ->
                  [BKey|Acc]
          end, [], list_all_files_naive_bkeys(State)),
    lists:reverse(L).

%% @spec location(state(), riak_object:bucket(), riak_object:key())
%%          -> string()
%% @doc produce the file-path at which the object for the given Bucket
%%      and Key should be stored
location(State, Bucket) ->
    location(State, Bucket, no_key_provided).

location(#state{dir = Dir, b_depth = BDepth, k_depth = KDepth}, Bucket, Key) ->
    B64 = encode_bucket(Bucket),
    BDirs = if BDepth > 0 -> filename:join(nest(B64, BDepth));
               true       -> ""
            end,
    if Key == no_key_provided ->
            filename:join([Dir, BDirs, B64]);
       true ->
            K64 = encode_key(Key),
            KDirs = if KDepth > 0 -> filename:join(nest(K64, KDepth));
                       true       -> ""
                    end,
            filename:join([Dir, BDirs, B64, KDirs, K64])
    end.

%% @spec location_to_bkey(string(), state()) ->
%%           {riak_object:bucket(), riak_object:key()}
%% @doc reconstruct a Riak bucket/key pair, given the location at
%%      which its object is stored on-disk
location_to_bkey("/" ++ Path, State) ->
    location_to_bkey(Path, State);
location_to_bkey(Path, #state{b_depth = BDepth, k_depth = KDepth}) ->
    [B64|Rest] = drop_from_list(BDepth, string:tokens(Path, "/")),
    [K64] = drop_from_list(KDepth, Rest),
    {decode_bucket(B64), decode_key(K64)}.

%% @spec decode_bucket(string()) -> binary()
%% @doc reconstruct a Riak bucket, given a filename
%% @see encode_bucket/1
decode_bucket(B64) ->
    base64fs2:decode(B64).

%% @spec decode_key(string()) -> binary()
%% @doc reconstruct a Riak object key, given a filename
%% @see encode_key/1
decode_key(K64) ->
    base64fs2:decode(K64).

%% @spec encode_bucket(binary()) -> string()
%% @doc make a filename out of a Riak bucket
encode_bucket(Bucket) ->
    base64fs2:encode_to_string(Bucket).

%% @spec encode_key(binary()) -> string()
%% @doc make a filename out of a Riak object key
encode_key(Key) ->
    base64fs2:encode_to_string(Key).

%% @doc create a directory nesting, to keep the number of
%%      files in a directory smaller
nest(Key, Groups) -> nest(lists:reverse(string:substr(Key, 1, 2*Groups)),
                          Groups, []).
nest(_, 0, Parts) -> Parts;
nest([Nb,Na|Rest],N,Acc) ->
    nest(Rest, N-1, [[Na,Nb]|Acc]);
nest([Na],N,Acc) ->
    nest([],N-1,[[Na]|Acc]);
nest([],N,Acc) ->
    nest([],N-1,["0"|Acc]).

%% Borrowed from bitcask_fileops.erl and then mangled
-spec pack_ondisk(binary(), state()) -> [binary()].
pack_ondisk(Bin, State) ->
    pack_ondisk_v1(Bin, State).

pack_ondisk_v1(Bin, #state{block_size = BlockSize}) ->
    ValueSz = size(Bin),
    if ValueSz =< BlockSize ->
            ok;
       true ->
            exit({size_violation, ?MODULE, ValueSz, '>', BlockSize})
    end,
    Bytes0 = [<<ValueSz:?VALSIZEFIELD>>, Bin],
    [<<?COOKIE_V1:?COOKIE_SIZE, (erlang:crc32(Bytes0)):?CRCSIZEFIELD>>, Bytes0].

-spec unpack_ondisk(binary()) -> binary() | bad_crc.
unpack_ondisk(<<?COOKIE_V1:?COOKIE_SIZE, Crc32:?CRCSIZEFIELD/unsigned,
                ValueSz:?VALSIZEFIELD, Rest/binary>>)
  when size(Rest) >= ValueSz ->
    try
        <<Value:ValueSz/binary, _Tail/binary>> = Rest,
        Crc32 = erlang:crc32([<<ValueSz:?VALSIZEFIELD>>, Value]),
        Value
    catch _:_ ->
            bad_crc
    end;
unpack_ondisk(_Bin) ->
    bad_crc.

calc_block_offset(BlockNum_x, #state{max_blocks = MaxBlocks,
                                     block_size = BlockSize}) ->
    %% CS block numbers start at zero.
    BlockNum = if BlockNum_x == trailer -> MaxBlocks;
                  true                  -> (BlockNum_x rem MaxBlocks)
               end,
    BlockNum * (?HEADER_SIZE + BlockSize).

%% @doc Calculate the largest possible valid block number stored in
%%      this file.  Return -1 if the file's size is zero.
%%
%%      This function isn't meant to be 100% correct in all instances:
%%      it only needs to be good enough.  It might be confused by partial
%%      file writes (which shouldn't be possible, except that a system
%%      crash could cause data loss and so it is possible).
%%      The caller should be tolerant in cases where we return a value
%%      larger than as in a Fully Correct Universe.

calc_max_block(0, _) ->
    -1;
calc_max_block(FileSize, #state{block_size = BlockSize,
                                max_blocks = MaxBlocks}) ->
    case (FileSize - 1) div (?HEADER_SIZE + BlockSize) of
        X when X > (MaxBlocks - 1) ->
            %% Anything written at or beyond MaxBlocks's offset is
            %% in the realm of trailers and is hereby ignored.
            MaxBlocks;
        N ->
            N
    end.

read_block(Bucket, UUID, BlockNum, #state{block_size = BlockSize} = State) ->
    Key = convert_blocknum2key(UUID, BlockNum, State),
    File = location(State, Bucket, Key),
    Offset = calc_block_offset(BlockNum, State),
    try
        {ok, FH} = file:open(File, [read, raw, binary]),
        try
            {ok, PackedBin} = file:pread(FH, Offset, ?HEADER_SIZE + BlockSize),
            try
                Bin = unpack_ondisk(PackedBin),
                Bin
            catch _A:_B ->
                    bad_crc
            end
        catch _X:_Y ->
                %% The pread failed, which means we're in a situation
                %% where we've written only as far as block N but we're
                %% attempting to read ahead, i.e., N+e, where e > 0.
                not_found
        after
            file:close(FH)
        end
    catch _:_ ->
            not_found
    end.

read_file(Path) ->
    io:format(user, "\r\nDEBUG only, deleteme: read_file\r\n", []),
    MaxSize = 4*1024*1024,
    {ok, FH} = file:open(Path, [read, raw, binary]),
    case file:open(Path, [read, raw, binary]) of
        {ok, FH} ->
            try
                {ok, Bin} = file:read(FH, MaxSize),
                case size(Bin) of
                    L when L < MaxSize ->
                        {ok, Bin};
                    _ ->
                        {ok, iolist_to_binary([Bin|read_rest(FH, MaxSize)])}
                end
            catch _:_ ->
                    {error, enoent}
            after
                file:close(FH)
            end;
        Error ->
            Error
    end.

write_file(Path, Data) ->
    case file:open(Path, [write, raw, binary]) of
        {ok, FH} ->
            try
                file:write(FH, Data)
            after
                file:close(FH)
            end;
        Error ->
            Error
    end.

read_rest(FH, MaxSize) ->
    read_rest(file:read(FH, MaxSize), FH, MaxSize).

read_rest({ok, <<>>}, _FH, _MaxSize) ->
    [];
read_rest({ok, Bin}, FH, MaxSize) ->
    [Bin|read_rest(FH, MaxSize)].

put_block(Bucket, UUID, BlockNum, Val, ValRObj, State) ->
    put_block_t_marker_check(Bucket, UUID, BlockNum, Val, ValRObj, State).

%% @doc Check for tombstone on disk before we try to put block
put_block_t_marker_check(Bucket, UUID, BlockNum, Val, ValRObj, State) ->
    Key = convert_blocknum2key(UUID, BlockNum, State),
    File = location(State, Bucket, Key),
    FI_perhaps = file:read_file_info(File),
    case FI_perhaps of
        {ok, FI} ->
            if FI#file_info.mode band ?TOMBSTONE_MODE_MARKER > 0 ->
                    %% This UUID + block of chunks has got a tombstone
                    %% marker set, so there's nothing more to do here.
                    ok;
               true ->
                    put_block_t_check(BlockNum, Val, ValRObj, File, FI_perhaps,
                                      State)
            end;
        _ ->
            put_block_t_check(BlockNum, Val, ValRObj, File, FI_perhaps, State)
    end.

%% @doc Check for tombstone in ValRObj before we try to put block
put_block_t_check(BlockNum, Val, ValRObj, File, FI_perhaps, State) ->
    NewMode = fun(FI) ->
                      NewMode = FI#file_info.mode bor ?TOMBSTONE_MODE_MARKER,
                      ok = file:change_mode(File, NewMode)
              end,
    case riak_object_is_deleted(ValRObj) of
        true ->
            case FI_perhaps of
                {ok, FI} ->
                    ok = NewMode(FI);
                {error, enoent} ->
                    ok = filelib:ensure_dir(File),
                    {ok, FH} = file:open(File, [write, raw]),
                    ok = file:close(FH),
                    ok = NewMode(#file_info{mode = 8#600})
            end;
        _ ->
            put_block3(BlockNum, Val, File, FI_perhaps, State)
    end.

%% @doc Put block: there is no previous tombstone or current tombstone.
put_block3(BlockNum, Val, File, FI_perhaps, State) ->
    Offset = calc_block_offset(BlockNum, State),
    OutOfOrder_p = check_trailer_ooo(FI_perhaps, BlockNum, State),
    try
        case file:open(File, [write, read, raw, binary]) of
            {ok, FH} ->
                try
                    PackedBin = pack_ondisk(Val, State),
                    ok = file:pwrite(FH, Offset, PackedBin),
                    if OutOfOrder_p ->
                            %% It's not an error to write more than one trailer
                            Tr = make_trailer(#t{written_sequentially = false},
                                              State),
                            TrOffset = calc_block_offset(trailer, State),
                            {ok, TrOffset} = file:position(FH, {bof, TrOffset}),
                            ok = file:write(FH, Tr);
                       true ->
                            ok
                    end
                after
                        file:close(FH)
                end;
            {error, enoent} ->
                ok = filelib:ensure_dir(File),
                put_block3(BlockNum, Val, File, FI_perhaps, State);
            {error, _} = Res ->
                Res
        end
    catch
        error:{badmatch, _Y} ->
            lager:warning("~s: badmatch1 err ~p\n", [?MODULE, _Y]),
            {error, badmatch1};
        _X:Y ->
            lager:warning("~s: badmatch2 ~p ~p\n", [?MODULE, _X, Y]),
            {error, badmatch2}
    end.

riak_object_is_deleted(eunit_delete_op_requested) ->
    true;
riak_object_is_deleted(ValRObj) ->
    catch riak_kv_util:is_x_deleted(ValRObj).

enumerate_chunks_in_file(Bucket, UUID, BlockBase,
                         #state{max_blocks=MaxBlocks} = State) ->
    Key = convert_blocknum2key(UUID, BlockBase, State),
    Path = location(State, Bucket, Key),
    case file:read_file_info(Path) of
        {error, _} ->
            [];
        {ok, FI} when FI#file_info.mode band ?TOMBSTONE_MODE_MARKER > 0 ->
            [];
        {ok, FI} ->
            case calc_max_block(FI#file_info.size, State) of
                X when X >= MaxBlocks ->
                    %% A trailer exists, so we need to assume that
                    %% perhaps there might be holes in this file.
                    MaxBlock = MaxBlocks - 1,
                    Filt = fun(BlNum) ->
                                   is_binary(read_block(Bucket, UUID,
                                                        BlockBase + BlNum,
                                                        State))
                           end;
                N ->
                    MaxBlock = N,
                    Filt = fun(_BlNum) ->
                                   true
                           end
            end,
            [BlockNum || BlockNum <- lists:seq(0, MaxBlock),
                         Filt(BlockNum)]
    end.

%% @doc Is the next block number we wish to write out-of-order?

check_trailer_ooo({error, enoent}, BlockNum, #state{max_blocks = MaxBlocks}) ->
    %% BlockNum 0 is ok, all others are out of order
    (BlockNum rem MaxBlocks) /= 0;
check_trailer_ooo({ok, FI}, BlockNum, #state{max_blocks = MaxBlocks} = State) ->
    MaxBlock = calc_max_block(FI#file_info.size, State),
    (BlockNum rem MaxBlocks) == MaxBlock - 1.

make_trailer(Term, State) ->
    Bin = iolist_to_binary(pack_ondisk(term_to_binary(Term), State)),
    Sz = size(Bin),
    [Bin, <<Sz:32>>].

get_prop_or_env(Key, Properties, App, Default) ->
    case proplists:get_value(Key, Properties) of
        undefined ->
            KV_key = list_to_atom("fs2_backend_" ++ atom_to_list(Key)),
            case application:get_env(App, KV_key) of
                undefined ->
                    get_prop_or_env_default(Default);
                Value ->
                    Value
            end;
        Value ->
            Value
    end.

get_prop_or_env_default({{Reason}}) ->
    throw(Reason);
get_prop_or_env_default(Default) ->
    Default.

create_or_sanity_check_version_file(Dir, BlockSize, MaxBlocks,
                                    BDepth, KDepth) ->
    VersionFile = make_version_path(Dir),
    case file:consult(VersionFile) of
        {ok, Terms} ->
            ok = sanity_check_terms(Terms, BlockSize, MaxBlocks,
                                    BDepth, KDepth);
        {error, enoent} ->
            ok = make_version_file(VersionFile, BlockSize, MaxBlocks,
                                   BDepth, KDepth)
    end.

make_version_path(Dir) ->
    Dir ++ "/.version.data".                    % Must hide name from globbing!

make_version_file(File, BlockSize, MaxBlocks, BDepth, KDepth) ->
    ok = filelib:ensure_dir(File),
    {ok, FH} = file:open(File, [write]),
    [ok = io:format(FH, "~p.\n", [X]) ||
        X <- [{backend, ?MODULE},
              {version_number, 1},
              {block_size, BlockSize},
              {max_blocks, MaxBlocks},
              {b_depth, BDepth},
              {k_depth, KDepth}]],
    ok = file:close(FH).

sanity_check_terms(Terms, BlockSize, MaxBlocks, BDepth, KDepth) ->
    ?MODULE = proplists:get_value(backend, Terms),
    1 = proplists:get_value(version_number, Terms),
    %% Use negative number below because if we use the default
    %% 'undefined' for a value that is not present, Erlang term
    %% ordering says integer() < atom().
    Bogus = -9999999999999999999999999999,
    true = (BlockSize =< proplists:get_value(block_size, Terms, Bogus)),
    true = (MaxBlocks =< proplists:get_value(max_blocks, Terms, Bogus)),
    BDepth = proplists:get_value(b_depth, Terms),
    KDepth = proplists:get_value(k_depth, Terms),
    ok.

drop_from_list(0, L) ->
    L;
drop_from_list(N, [_|T]) ->
    drop_from_list(N - 1, T);
drop_from_list(_N, []) ->
    [].

%% Manual stack management for fold operations.
%%
%% * Paths on the stack are relative to #state.dir.
%% * Paths on the stack all start with "/", but they are not absolute paths!

make_start_stack_objects(undefined, #state{b_depth = BDepth}) ->
    case BDepth of
        0 ->
            %% TODO: We have a problem with the version name file here,
            %%       which could collide with a real bucket name.
            [glob_buckets];
        N ->
            [{glob_bucket_intermediate, 1, N, ""}]
    end;
make_start_stack_objects(OnlyBucket, #state{k_depth = KDepth} = State) ->
    Path = location(State, OnlyBucket),
    %% Now mangle to stack path format
    Start = string:sub_string(Path, length(State#state.dir) + 1),
    [{glob_key_intermediate, 1, KDepth, Start}].

%% @doc Reduce (or fold, pick your name) over items in a work stack
reduce_stack([], _FoldFun, Acc, _State) ->
    Acc;
reduce_stack([Op|Rest], FoldFun, Acc, State) ->
    try
        {PushOps, Acc2} = exec_stack_op(Op, FoldFun, Acc, State),
        reduce_stack(PushOps ++ Rest, FoldFun, Acc2, State)
    catch throw:{found_a_bucket, NewAcc} ->
            NewStack = lists:dropwhile(fun(pop_back_to_here) -> false;
                                          (_)                -> true
                                       end, Rest),
            reduce_stack(NewStack, FoldFun, NewAcc, State)
    end.

exec_stack_op({glob_bucket_intermediate, Level, MaxLevel, MidDir},
              _FoldFun, Acc, State)
  when Level < MaxLevel ->
    Ops = [{glob_bucket_intermediate, Level+1, MaxLevel, MidDir ++ "/" ++Dir}||
              Dir <- do_glob("*", MidDir, State)],
    {Ops, Acc};
exec_stack_op({glob_bucket_intermediate, Level, MaxLevel, MidDir},
              _FoldFun, Acc, State)
  when Level == MaxLevel ->
    Ops = [{glob_bucket, MidDir ++ "/" ++ Dir} ||
              Dir <- do_glob("*", MidDir, State)],
    {Ops, Acc};
exec_stack_op({glob_bucket, BDir}, _FoldFun, Acc, #state{k_depth = KDepth} = State) ->
    Optional = if State#state.fold_type == buckets -> [pop_back_to_here];
                  true                             -> []
               end,
    Ops = [[{glob_key_intermediate, 1, KDepth, BDir ++ "/" ++ Dir}|Optional] ||
              Dir <- do_glob("*", BDir, State)],
    {lists:flatten(Ops), Acc};
exec_stack_op({glob_key_intermediate, Level, MaxLevel, MidDir},
              _FoldFun, Acc, State)
  when Level < MaxLevel ->
    Ops = [{glob_key_intermediate, Level+1, MaxLevel, MidDir ++ "/" ++ Dir} ||
              Dir <- do_glob("*", MidDir, State)],
    {Ops, Acc};
exec_stack_op({glob_key_intermediate, Level, MaxLevel, MidDir},
              _FoldFun, Acc, State)
  when Level == MaxLevel ->
    Ops = [{glob_key_file, MidDir ++ "/" ++ Dir} ||
              Dir <- do_glob("*", MidDir, State)],
    {Ops, Acc};
exec_stack_op({glob_key_file, MidDir},  _FoldFun, Acc, State) ->
    Ops = [{key_file, MidDir ++ "/" ++ File} ||
              File <- do_glob("*", MidDir, State)],
    {Ops, Acc};
exec_stack_op({key_file, File},  _FoldFun, Acc, State) ->
    try
        {<<?BLOCK_BUCKET_PREFIX, _/binary>> = Bucket,
         <<UUID:?UUID_BYTES/binary, BlockBase:?BLOCK_FIELD_SIZE>>} =
            location_to_bkey(File, State),
        BKeys = [{Bucket, <<UUID/binary, (BlockBase+Block):?BLOCK_FIELD_SIZE>>}
                 || Block <- enumerate_chunks_in_file(Bucket, UUID, BlockBase,
                                                      State)],
        {BKeys, Acc}
    catch error:_Y ->
            %% Binary pattern matching or base64 decoding failed, or
            %% enumerate_chunks_in_file() did something bad.
            %% TODO: can enumerate_chunks_in_file() fail in a way that will
            %%       make us skip over useful data?
            try
                BKey = location_to_bkey(File, State),
                {[BKey], Acc}
            catch _X2:_Y2 ->
                    %% TODO: shouldn't happen, because all filenames
                    %% should be encoded with base64fs2, so decoding
                    %% them should also always work.
                    io:format("ERROR: key_file ~p: ~p ~p\n", [File, _X2, _Y2]),
                    {[], Acc}
            end
    end;
exec_stack_op(pop_back_to_here, _FoldFun, Acc, _State) ->
    %% If we encounter this in normal processing, then treat it as a
    %% no-op: we didn't find any files that contained at least one
    %% non-tombstone block.
    {[], Acc};
exec_stack_op({_Bucket, _Key} = BKey, FoldFun, Acc,
              #state{fold_type = buckets}) ->
    %% enumerate_chunks_in_file() has figured out for us that the
    %% file doesn't contain tombstones and contains at least one key
    %% (probably).  If those facts weren't true, we couldn't be here.
    %% So, we have high confidence (but not 100% certain) that at least
    %% one key exists in this bucket.  So, let's skip all other keys
    %% in the bucket via a 'throw'
    NewAcc = FoldFun(BKey, value_unused_by_this_modules_fold_wrapper, Acc),
    throw({found_a_bucket, NewAcc});
exec_stack_op({_Bucket, _Key} = BKey, FoldFun, Acc,
              #state{fold_type = keys}) ->
    {[], FoldFun(BKey, value_unused_by_this_modules_fold_wrapper, Acc)};
exec_stack_op({Bucket, Key} = BKey, FoldFun, Acc,
              #state{fold_type = objects} = State) ->
    case get(Bucket, Key, State) of
        {ok, Value, _S} ->
            {[], FoldFun(BKey, Value, Acc)};
        _ ->
            {[], Acc}
    end.

%% Remember: all paths on the stack start with "/" but are not absolute.
do_glob(Glob, Dir, #state{dir = PrefixDir}) ->
    filelib:wildcard(Glob, PrefixDir ++ Dir).

convert_blocknum2key(UUID, BlockNum, #state{max_blocks = MaxBlocks}) ->
    <<UUID/binary, ((BlockNum div MaxBlocks) * MaxBlocks):?BLOCK_FIELD_SIZE>>.

t0() ->
    %% Blocksize must be at last 15 or so: this test writes two blocks
    %% for the same UUID, but it does them out of order, so a trailer
    %% will be created ... and if the block size is too small to
    %% accomodate the trailer, bad things happen.
    BlockSize = 22,
    TestDir = "./delme",
    Bucket = <<?BLOCK_BUCKET_PREFIX, "delme">>,
    K0 = <<0:(?UUID_BYTES*8), 0:?BLOCK_FIELD_SIZE>>,
    V0 = <<42:(BlockSize*8)>>,
    K1 = <<0:(?UUID_BYTES*8), 1:?BLOCK_FIELD_SIZE>>,
    V1 = <<43:(BlockSize*8)>>,
    os:cmd("rm -rf " ++ TestDir),
    {ok, S} = start(-1, [{data_root, TestDir},
                         {block_size, BlockSize}]),
    {ok, S} = put(Bucket, K1, [], V1, ignored, S),
    {ok, S} = put(Bucket, K0, [], V0, ignored, S),
    {ok, V0, S} = get(Bucket, K0, S),
    {ok, V1, S} = get(Bucket, K1, S),
    ok.

t1() ->
    #t{} = #t{},
    TestDir = "./delme",
    Bucket = <<?BLOCK_BUCKET_PREFIX, "delme">>,
    K0 = <<0:(?UUID_BYTES*8), 0:?BLOCK_FIELD_SIZE>>,
    V0 = <<100:64>>,
    K1 = <<0:(?UUID_BYTES*8), 1:?BLOCK_FIELD_SIZE>>,
    V1 = <<101:64>>,
    os:cmd("rm -rf " ++ TestDir),
    {ok, S} = start(-1, [{data_root, TestDir},
                         {block_size, 1024}]),
    {ok, S4} = put(Bucket, K0, [], V0, ignored, S),
    {ok, [{{Bucket, K0}, V0}]} = fold_objects(fun(B, K, V, Acc) ->
                                                      [{{B, K}, V}|Acc]
                                              end, [], [], S4),
    {ok, S5} = put(Bucket, K1, [], V1, ignored, S4),
    {ok, V0, _} = get(Bucket, K0, S5),
    {ok, V1, _} = get(Bucket, K1, S5),
    {ok, X} = fold_objects(fun(B, K, V, Acc) ->
                                   [{{B, K}, V}|Acc]
                           end, [], [], S5),
    [{{Bucket, K0}, V0}, {{Bucket, K1}, V1}] = lists:reverse(X),

    {ok, S6} = delete(Bucket, K1, unused, S5),
    {ok, []} = fold_objects(fun(B, K, V, Acc) ->
                                    [{{B, K}, V}|Acc]
                            end, [], [], S6),
    ok.

t2() ->
    TestDir = "./delme",
    %% Scribble nonsense stuff for SLF temp debugging purposes
    B1 = <<?BLOCK_BUCKET_PREFIX, "delme">>,
    B2 = <<?BLOCK_BUCKET_PREFIX, "delme2">>,
    B3 = <<?BLOCK_BUCKET_PREFIX, "delme22">>,
    os:cmd("rm -rf " ++ TestDir),
    {ok, S} = start(-1, [{data_root, TestDir},
                         {block_size, 1024}]),
    BKVs = [{B, <<UUID:(?UUID_BYTES*8), Seq:?BLOCK_FIELD_SIZE>>,
             list_to_binary(["val ", integer_to_list(Seq)])} ||
               B <- [B1, B2, B3],
               UUID <- [44, 88],
               %% Seq <- lists:seq(0, 5)],
               Seq <- lists:seq(0, ?FS2_CONTIGUOUS_BLOCKS + 5)],
    [{ok, _} = put(B, K, [], V, ignored, S) || {B, K, V} <- BKVs],
    {ok, Res} = fold_objects(fun(B, K, V, Acc) ->
                                     [{B, K, V}|Acc]
                             end, [], [], S),
    %% Lengths are the same
    true = (length(BKVs) == length(Res)),
    %% The original data's sorted order is the same as our traversal
    %% order (once we reverse the fold).
    true = (lists:sort(BKVs) == lists:reverse(Res)),
    ok.

t3() ->
    TestDir = "./delme",
    Bucket = <<?BLOCK_BUCKET_PREFIX, "delme">>,
    K0 = <<0:(?UUID_BYTES*8), 0:?BLOCK_FIELD_SIZE>>,
    BlockSize = 10,
    V0 = <<42:((BlockSize+1)*8)>>,
    os:cmd("rm -rf " ++ TestDir),
    {ok, S} = start(-1, [{data_root, TestDir},
                         {block_size, BlockSize}]),
    {error, invalid_user_argument, S} = put(Bucket, K0, [], V0, ignored, S),
    ok.

%% t4() = folding tests

t4() ->
    t4(1, 3, 2, fun lists:reverse/1).

t4(SmallestBlock, BiggestBlock, BlocksPerFile, OrderFun)
  when SmallestBlock >= 0, SmallestBlock =< BiggestBlock ->
    TestDir = "./delme",
    B1 = <<?BLOCK_BUCKET_PREFIX, "delme1">>,
    B2 = <<?BLOCK_BUCKET_PREFIX, "delme2">>,
    TheTwoBs = [B1, B2],
    BlockSize = 20,
    os:cmd("rm -rf " ++ TestDir),
    {ok, S} = start(-1, [{data_root, TestDir},
                         {block_size, BlockSize},
                         {max_blocks_per_file, BlocksPerFile}]),
    BKVs = [{<<?BLOCK_BUCKET_PREFIX, "delme", Bp:8>>,
             <<0:(?UUID_BYTES*8), X:?BLOCK_FIELD_SIZE>>,
             <<X:((BlockSize)*8)>>} ||
               Bp <- [$1, $2],
               X <- lists:seq(SmallestBlock, BiggestBlock)],
    [{ok, S} = put(B, K, [], V, ignored, S) || {B, K, V} <- OrderFun(BKVs)],

    BKs = [{B, K} || {B, K, _V} <- BKVs],
    [begin
         OnlyB = proplists:get_value(bucket, FoldOpts, all_buckets),
         BucketFilt = fun(B) -> OnlyB == all_buckets orelse B == OnlyB end,

         %% Fold objects
         {ok, FoundBKVs} = fold_objects(fun(B, K, V, Acc) ->
                                                [{B, K, V}|Acc]
                                        end, [], FoldOpts, S),
         OnlyBKVs = [BKV || {B, _, _} = BKV <- BKVs,
                            BucketFilt(B)],
         {OnlyB, true} = {OnlyB, (lists:sort(OnlyBKVs) == lists:reverse(FoundBKVs))},
         %% Fold keys
         {ok, FoundBKs} = fold_keys(fun(B, K, Acc) ->
                                            [{B, K}|Acc]
                                    end, [], FoldOpts, S),
         OnlyBKs = [BK || {B, _} = BK <- BKs,
                          BucketFilt(B)],
         {OnlyB, true} = {OnlyB, (lists:sort(OnlyBKs) == lists:reverse(FoundBKs))}
     end || FoldOpts <- [[], [{bucket, B1}], [{bucket, B2}]] ],

    %% Set up for fold buckets
    RestBegin = $3,             % We already have buckets ending with $1 & $2
    RestEnd = $9,
    RestBlocks = 5,
    RestStatus =
        [begin
             RestB = <<?BLOCK_BUCKET_PREFIX, "delme", Bp:8>>,
             %% Using X like for both UUID and block # will give us one
             %% file that's written in order, UUID=0, and the rest will be
             %% files written out of order.
             [{ok, S} = put(RestB, <<X:(?UUID_BYTES*8), X:?BLOCK_FIELD_SIZE>>,
                            [], <<"val!">>, ignored, S) ||
                 X <- lists:seq(0, RestBlocks)],
             if Bp rem 3 == 0 ->
                     {exists, RestB};
                Bp rem 3 == 1 ->
                     [{ok, S} = put(RestB,
                                    <<X:(?UUID_BYTES*8),X:?BLOCK_FIELD_SIZE>>,
                                    [], <<>>, eunit_delete_op_requested, S) ||
                         X <- lists:seq(0, RestBlocks)],
                     {tombstone, RestB};
                Bp rem 3 == 2 ->
                     [{ok, S} = delete(RestB,
                                      <<X:(?UUID_BYTES*8),X:?BLOCK_FIELD_SIZE>>,
                                      unused, S) ||
                         X <- lists:seq(0, RestBlocks)],
                     {deleted, RestB}
             end
         end || Bp <- lists:seq(RestBegin, RestEnd)],
    RestRemainingBuckets = [B || {exists, B} <- RestStatus],

    %% Fold buckets
    {ok, FoundBs} = fold_buckets(fun(B, Acc) -> [B|Acc] end, [], [], S),
    %% FoundBs should contain only the buckets in TheTwoBs and
    %% the buckets in RestStatus that are 'exists' status.  And
    %% FoundBs should come out in the proper order (don't sort it!).
    true = (lists:sort(TheTwoBs ++ RestRemainingBuckets) == lists:reverse(FoundBs)),

    %% TODO? In the section for setup of the fold buckets, perhaps
    %% use bucket names of varying lengths to try to find an error in
    %% the order that the fold provides (i.e. not in reverse
    %% lexicographic order)?
    %% TODO? In the fold objects and fold keys tests, use bucket names of
    %% different lengths, for the same reason as above?
    ok.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

t0_test() ->
    ok = t0().

t1_test() ->
    ok = t1().

t2_test_() ->
    {spawn,
     [
      {timeout, 60*1000, ?_assertEqual(ok, t2())}
     ]}.

t3_test() ->
    ok = t3().

t4_test() ->
    ok = t4().

%% t4_eqc_test() is now run by eqc_test_(), below, to avoid EUnit timeouts.

eqc_t4_wrapper() ->
    eqc:quickcheck(eqc:numtests(250, prop_t4())).

%% Callbacks for backend_eqc test.

eqc_filter_delete(Bucket, Key, BKVs) ->
    try
        Extract = fun(B, K) ->
                          <<?BLOCK_BUCKET_PREFIX, _/binary>> = B,
                          <<UUID:?UUID_BYTES/binary, _:?BLOCK_FIELD_SIZE>> = K,
                          {B, UUID}
                  end,
        {Bucket, UUID} = Extract(Bucket, Key),
        [BKV || {{B, K}, _V} = BKV <- BKVs,
                {Bx, UUIDx} <- [catch Extract(B, K)],
                not (Bx == Bucket andalso UUIDx == UUID)]
    catch error:{badmatch, _} ->
            BKVs
    end.

eqc_filter_list_keys(Keys, S) ->
    [BKey || {Bucket, Key} = BKey <- Keys,
             (catch element(1, get(Bucket, Key, S))) == ok].

-ifdef(TEST_IN_RIAK_KV).

%% Broken test:
simple_test_foofoo() ->
   ?assertCmd("rm -rf test/fs-backend/*"),
   Config = [{data_root, "test/fs-backend"},
             {block_size, 8}],
   riak_kv_backend:standard_test(?MODULE, Config).

-endif. % TEST_IN_RIAK_KV

nest_test() ->
    ?assertEqual(["ab","cd","ef"],nest("abcdefg", 3)),
    ?assertEqual(["ab","cd","ef"],nest("abcdef", 3)),
    ?assertEqual(["a","bc","de"], nest("abcde", 3)),
    ?assertEqual(["0","ab","cd"], nest("abcd", 3)),
    ?assertEqual(["0","a","bc"],  nest("abc", 3)),
    ?assertEqual(["0","0","ab"],  nest("ab", 3)),
    ?assertEqual(["0","0","a"],   nest("a", 3)),
    ?assertEqual(["0","0","0"],   nest([], 3)).

create_or_sanity_test() ->
    Dir = "/tmp/sanity_test_delete_me",
    BlockSize = 20,
    MaxBlocks = 21,
    BDepth = 2,
    KDepth = 3,

    os:cmd("rm -rf ++ " ++ Dir),
    Base = fun(A, B, C, D) ->
               try
                   ok = create_or_sanity_check_version_file(
                          Dir, BlockSize+A, MaxBlocks+B, BDepth+C, KDepth+D)
               catch _:_ ->
                       bad
               end
           end,
    try
        ok  = Base(0, 0, 0, 0),
        bad = Base(1, 0, 0, 0),
        bad = Base(0, 1, 0, 0),
        bad = Base(0, 0, 1, 0),
        bad = Base(0, 0, 0, 1),
        ok
    after
        os:cmd("rm -rf ++ " ++ Dir)
    end.

-ifdef(EQC).

basic_props() ->
    [{data_root,  "test/fs-backend"},
     {block_size, 1024}].

-ifdef(TEST_IN_RIAK_KV).
eqc_test_() ->
    {spawn,
     [{inorder,
       [{setup,
         fun setup/0,
         fun cleanup/1,
         [
          %% ?_assertEqual(?BLOCK_BUCKET_PREFIX,
          %%               backend_eqc:bucket_prefix_1()),
          %% ?_assertEqual(?UUID_BYTES,
          %%               backend_eqc:key_prefix_1()),
          %% ?_assertEqual(?BLOCK_FIELD_SIZE,
          %%               backend_eqc:key_suffix_1()),
          {timeout, 60000,
           [?_assertEqual(true,
                          backend_eqc:test(?MODULE,
                                           false,
                                           basic_props()))]},
          {timeout, 60000,
           [?_assertEqual(true,
                          eqc_nest_tester())]},
          {timeout, 60000,
           [?_assertEqual(true,
                          eqc_t4_wrapper())]}
         ]}]}]}.
-endif. % TEST_IN_RIAK_KV

eqc_nest_tester() ->
    setup(),
    os:cmd("mkdir -p test/fs-backend"),
    rm_rf_test_dir_contents(),
    ok = file:make_dir("test/fs-backend/foo"),
    case file:make_dir("test/fs-backend/Foo") of
        ok ->
            X = eqc:quickcheck(eqc:numtests(250, prop_nest_ordered())),
            cleanup(x),
            X == true;
        {error, eexist} ->
            io:format(user, "SKIP ~s:eqc_nest_tester: using case insensitive file system\n", [?MODULE]),
            %% cleanup(x),
            true
    end.

prop_t4() ->
    ?FORALL({SmallestBlock, N, BlocksPerFile, OrderFun},
            {choose(0, 20), choose(0, 20), choose(1, 5),
             oneof([fun identity/1, fun lists:reverse/1, fun random/1])},
            true = (ok == t4(SmallestBlock, SmallestBlock + N,
                             BlocksPerFile, OrderFun))).

identity(L) -> L.

random(L) ->
    [X || {_Rnd, X} <- lists:sort([{random:uniform(1000), Y} || Y <- L])].

setup() ->
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger,
                        {file, "riak_kv_fs2_backend_eqc_sasl.log"}),
    error_logger:tty(false),
    error_logger:logfile({open, "riak_kv_fs2_backend_eqc.log"}),
    ok.

cleanup(_) ->
    rm_rf_test_dir_contents().

rm_rf_test_dir_contents() ->
    os:cmd("rm -rf test/fs-backend/*").

prop_nest_ordered() ->
    ?FORALL(BucketList, non_empty(list(gen_bucket())),
            collect(length(BucketList),
            begin
                rm_rf_test_dir_contents(),
                {ok, S} = ?MODULE:start(0, basic_props()),
                [ok = insert_sample_key(Bucket, S) || Bucket <- BucketList],
                {ok, Bs} = ?MODULE:fold_buckets(fun(B, Acc) -> [B|Acc] end, [],
                                                [], S),
                %% rm_rf_test_dir_contents(),
                case lists:usort(BucketList) == lists:reverse(Bs) of
                    true -> true;
                    _ -> {wanted, lists:usort(BucketList),
                          got, lists:reverse(Bs)}
                end
            end)).

gen_bucket() ->
    ?LET(Bucket, frequency([
                            {50, vector(1, char())},
                            {5, vector(2, char())},
                            {5, vector(3, char())},
                            {5, non_empty(list(char()))}
                           ]),
         iolist_to_binary(Bucket)).

insert_sample_key(Bucket, S) ->
    {ok, _} = ?MODULE:put(Bucket, <<"key">>, [], <<"val">>, unused, S),
    ok.

-endif. % EQC
-endif. % TEST
