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
%% * When there's a read-repair triggered by Riak's get FSM, there will
%%   likely be a problem because there will probably be siblings.  These
%%   data blocks are immutable, so repair can only be an option when
%%   there is corruption:
%%     a. Corrupted after landing on disk: our checksum (or Bitcask's)
%%        mean that a backend -> vnode -> get FSM response is not_found.
%%        A repair will get the single correct version, write the
%%        correct version to the corrupted vnode, and all will be fixed.
%%     B. Corrupted before landing on disk: our checksum will look just
%%        fine, because our checksum will be calculated after the
%%        corruption happened.  (E.g., it happened at Riak CS or in
%%        in the network due to a memory error.)
%%        In this case, the get FSM plus allow_mult=true plus
%%        riak_object:reconcile/2 will catch a difference in value
%%        even if the vclocks are identical.  So, we have siblings,
%%        and the get FSM's read repair will try to write the two
%%        siblings to all N copies of key ... and will fail, because
%%        this module will be configured to only allow 1MB chunks + the
%%        largest allowable metadata for the key.
%%
%%        Therefore, Riak CS must do a couple of things:
%%
%%        a. It will also get the siblings.  It must resolve them,
%%           using a checksum header in the metadata to find the
%%           uncorrupted version.
%%        b. Riak CS must do read repair whenever it finds corruption.
%%           As noted above, Riak plus this backend's size limitation
%%           won't allow Riak's own read-repair to succeed most/all of
%%           the time.
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
%% When we encode a bucket name, the encoding has the <<"0b:">> prefix,
%% which doesn't help spread the love of bucket subdir hashing levels.
%% Is this avoidable?  If not, we need a change of hashing the dir levels.
%%
%% __ Add patch to CS that will 1. add Riak header that contains checksum.
%%    2. Will not crash horribly if put fails.
%%    3. Will detect siblings correctly on get and pick the version
%%       that matches the block checksum.
%%    4. Will repair the block when #3 hits.
%%    Also, in conjunction with this backend:
%%    __ Since we're already peeking into the Riak object metadata to
%%       check for tombstones, also check for the block checksum and
%%       refuse to write the block if it is corrupted.  This would
%%       avoid propagation of corruption in a couple of places:
%%       a. AAE replicates a bad block and clobbers a good one
%%       b. Handoff sends a bad block to a vnode and clobbers a good one
%%
%% __ Double-check that the api_version & capabilities/0 func is doing
%% __ Add capability to extend put() and get() to avoid getting an
%%    already-encoded-Riak-object and returning same.  If
%%    riak_kv_vnode gives us an encoded object, we need to unencoded
%%    it just to see if there's a tombstone in the metadata.  We know
%%    that R15B* and R16B releases have scheduler sticking problems
%%    with term_to_binary() conversions.  Doing extra conversions is
%%    both a waste of CPU time and a way to anger the Erlang process
%%    scheduler gods.
%%
%% __ Move to assuming that <<"0b:">> will be assuming 1MB block size.
%%    We need block size flexibility for testing, but that's all for now.
%%    Figure out the max length of S3 bucket (DNS limit) and object name
%%    (AWS S3 docs), and leave room for the rest of the metadata dict.
%%    Answer: max S3 bucket and S3 object name length:
%%    http://aws.amazon.com/articles/1109?_encoding=UTF8&jiveRedirect=1#07
%%    1024 bytes each
%%
%% __ Double-check that the api_version & capabilities/0 func is doing
%%    the right thing wrt riak_kv.
%%
%% __ http://erlang.org/pipermail/erlang-questions/2011-September/061147.html
%%    __ Avoid `file` module whenever file_server_2 calls are used.
%%    __ Avoid file:file_name_1() recursion silliness
%%
%% __ Read cache: if reading 0th chunk, create read-ahead that will
%%    read entire file async'ly and send it as a message to be recieved
%%    later.  When reading 1st chunk, wait for arrival of read-ahead block.
%%    __ Needs a real cache though.  Steal the cache from the memory backend?
%%       Use the entire riak_memory_backend as-is??  Triple-check that ETS
%%       isn't going to copy off-heap/shared binaries....
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

-module(riak_kv_fs2_backend).
%% -behavior(riak_kv_backend). % Not building within riak_kv

%% KV Backend API
-compile(export_all).                          % DEBUGGING ONLY, delme!
-export([api_version/0,
         capabilities/1,
         capabilities/2,
         start/2,
         stop/1,
         get_object/4,                          % capability: uses_r_object
         put_object/5,                          % capability: uses_r_object
         delete/4,
         drop/1,
         fold_buckets/4,
         fold_keys/4,
         fold_objects/4,
         is_empty/1,
         status/1,
         callback/3]).
%% To avoid EUnit test dependency on riak_cs_utils
-export([hexlist_to_binary/1, binary_to_hexlist/1, resolve_robj_siblings/1]).
%% For QuickCheck testing use only
-export([get/3, put/5]).
%% Testing
-export([scan_file/1]).
-export([t0/0, t1/0, t2/0, t3/0, t4/0, t5/0, t6/0]).

-include("riak_cs.hrl").
-include("riak_cs_lfs.hrl").
-include_lib("kernel/include/file.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl"). % ?MD_USERMETA

-define(API_VERSION, 1).
-define(CAPABILITIES, [uses_r_object, async_fold, write_once_keys]).

%%% -define(TEST_FS2_BACKEND_IN_RIAK_KV, true).

-define(FILE1_COOKIE_SIZE, 64).
-define(FILE1_COOKIE, <<">fs2_v1<">>).
-define(FILE1_BLOCK_LEN_SIZE,   32).
-define(FILE1_UNUSED1_LEN_SIZE, 32).
-define(FILE1_UNUSED2_LEN_SIZE, 32).
-define(FILE1_UNUSED3_LEN_SIZE, 32).
-define(FILE1_UNUSED4_LEN_SIZE, 32).
-define(FILE1_UNUSED5_LEN_SIZE, 32).
-define(FILE1_CRC_SIZE,         32).
-define(FILE1_HEADER_SIZE, (?FILE1_COOKIE_SIZE div 8) +
                           (?FILE1_BLOCK_LEN_SIZE div 8) +
                           (?FILE1_UNUSED1_LEN_SIZE div 8) +
                           (?FILE1_UNUSED2_LEN_SIZE div 8) +
                           (?FILE1_UNUSED3_LEN_SIZE div 8) +
                           (?FILE1_UNUSED4_LEN_SIZE div 8) +
                           (?FILE1_UNUSED5_LEN_SIZE div 8) +
                           (?FILE1_CRC_SIZE div 8)).
%% Borrowed from bitcask.hrl
-define(COOKIE_SIZE, 64).
-define(COOKIE_V1, 16#c0c00101c00101c0).
-define(VALSIZEFIELD, 32).
-define(CRCSIZEFIELD, 32).
%% 8 = header cookie & version number, 4 = Value size, 4 = Erlang CRC32,
-define(HEADER_SIZE, (?COOKIE_SIZE div 8) +
                     (?VALSIZEFIELD div 8) +
                     (?CRCSIZEFIELD div 8)). % Differs from bitcask.hrl!
-define(MAXVALSIZE, (1 bsl ?VALSIZEFIELD) - 1).
-define(TYPICAL_BUCKET_PREFIX_LEN, 3).  % To match ?BLOCK_BUCKET_PREFIX length

%% !@#$!!@#$!@#$!@#$! Erlang's efile_drv does not support the sticky bit.
%% So, we use something else: the setuid bit.
%% I'd prefer to use the setgid bit, but HFS+ does not allow non-root
%% users to set the setgid bit on files.  {exasperated}
-define(TOMBSTONE_MODE_MARKER, 8#4000).

%% Dropped data directory suffix: for asynchronously deleting lots of stuff
-define(DROPPED_DIR_SUFFIX, ".dropped").

-ifdef(TEST).
-compile(export_all).
-ifdef(EQC).
%% EQC testing assistants
-export([eqc_filter_delete/3]).
-export([prop_nest_ordered/0, eqc_nest_tester/0]).
-export([backend_eqc_fold_objects_transform/1,
         backend_eqc_filter_orddict_on_delete/4,
         backend_eqc_bucket/0,
         backend_eqc_key/0,
         backend_eqc_postcondition_fold_keys/2]).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifndef(MD_DELETED).
-define(MD_DELETED, <<"X-Riak-Deleted">>).
-endif.

-record(state, {
          dir        :: string(),
          block_size :: non_neg_integer(),
          max_blocks :: non_neg_integer(),
          b_1st_prefixlen :: pos_integer(),
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
        {ConfigRoot, BlockSize, MaxBlocks, B1stPrefixLen, BDepth, KDepth} =
            parse_config_and_env(Config),
        Dir = filename:join([ConfigRoot,PartitionName]),
        ok = filelib:ensure_dir(Dir),
        ok = create_or_sanity_check_version_file(Dir, BlockSize, MaxBlocks,
                                                 BDepth, KDepth),

        %% See also: drop() function
        Cmd = lists:flatten(
                io_lib:format("rm -rf ~s.*~s &", [Dir, ?DROPPED_DIR_SUFFIX])),
        _ = os:cmd(Cmd),

        {ok,  #state{dir = Dir,
                     block_size = BlockSize,
                     max_blocks = MaxBlocks,
                     b_1st_prefixlen = B1stPrefixLen,
                     b_depth = BDepth,
                     k_depth = KDepth}}
    catch throw:Error ->
        {error, Error}
    end.

%% @doc Stop the backend
-spec stop(state()) -> ok.
stop(_State) -> ok.

%% @doc Get the object stored at the given bucket/key pair
-spec get_object(riak_object:bucket(), riak_object:key(), boolean(), state()) ->
                     {ok, binary() | riak_object:riak_object(), state()} |
                     {ok, not_found, state()} |
                     {error, term(), state()}.
get_object(<<?BLOCK_BUCKET_PREFIX, _/binary>> = Bucket,
           <<UUID:?UUID_BYTES/binary, BlockNum:?BLOCK_FIELD_SIZE>> = Key,
           WantsBinary, State) ->
    case read_block(Bucket, Key, UUID, BlockNum, State) of
        Bin when is_binary(Bin), WantsBinary == false ->
            try
                {ok, deserialize_term(Bin), State}
            catch
                _:_ ->
                    {error, not_found, State}
            end;
        Bin when is_binary(Bin), WantsBinary == true ->
            {ok, Bin, State};
        RObj when element(1, RObj) == r_object, WantsBinary == false ->
            {ok, RObj, State};
        RObj when element(1, RObj) == r_object, WantsBinary == true ->
            {ok, serialize_term(RObj), State};
        Reason when is_atom(Reason) ->
            {error, not_found, State}
    end;
get_object(Bucket, Key, WantsBinary, State) ->
    File = location(State, Bucket, Key),
    case filelib:is_file(File) of
        false ->
            {error, not_found, State};
        true ->
            {ok, Bin} = read_file(File),
            case WantsBinary of
                true ->
                    {ok, Bin, State};
                false ->
                    case unpack_ondisk(Bin) of
                        bad_crc ->
                            %% TODO logging?
                            {error, not_found, State};
                        Val ->
                            {ok, deserialize_term(Val), State}
                    end
            end
    end.

%% EQC use only
get(Bucket, Key, State) ->
    case get_object(Bucket, Key, false, State) of
        {ok, RObj, State} ->
            {ok, riak_object:get_value(RObj), State};
        Else ->
            Else
    end.

%% EQC use only
put(Bucket, Key, IdxList, Val, State) ->
    RObj = riak_object:new(Bucket, Key, Val),
    case put_object(Bucket, Key, IdxList, RObj, State) of
        {{ok, _}, _} ->
            ok;
        {Else, _} ->
            Else
    end.

%% @doc Store Val under Bucket and Key
%%
%% NOTE: Val is a copy of ValRObj that has been encoded by serialize_term()

-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put_object(riak_object:bucket(), riak_object:key(), [index_spec()], riak_object:riak_object(), state()) ->
                 {{ok, state()}, EncodedVal::binary()} |
                 {{error, term()}, state()}.
put_object(<<?BLOCK_BUCKET_PREFIX, _/binary>> = Bucket,
           <<UUID:?UUID_BYTES/binary, BlockNum:?BLOCK_FIELD_SIZE>>,
           _IndexSpecs, RObj, State) ->
    case resolve_robj_siblings(riak_object:get_contents(RObj)) of
        {{_MD, V} = MDV, _} when is_binary(V) ->
            RObj2 = case riak_object:get_values(RObj) of
                        [VV] when VV =:= V ->
                            RObj;
                        _ ->
                            riak_object:set_contents(RObj, [MDV])
                    end,
            put_object2(Bucket, UUID, BlockNum, RObj2, State);
        Else ->
            {{error, {has_errors, Else}, State}, invalid_encoded_val_do_not_use}
    end;
put_object(Bucket, PrimaryKey, _IndexSpecs, RObj, State) ->
    File = location(State, Bucket, PrimaryKey),
    case filelib:ensure_dir(File) of
        ok ->
            EncodedVal = serialize_term(RObj),
            case atomic_write(File, EncodedVal, State) of
                ok         -> {{ok, State}, EncodedVal};
                {error, X} -> {{error, X, State}, EncodedVal}
            end;
        {error, X} -> {{error, X, State}, invalid_encoded_val_do_not_use}
    end.

put_object2(Bucket, UUID, BlockNum, RObj, State) ->
    case put_block(Bucket, UUID, BlockNum, RObj, State) of
        {ok, EncodedVal} ->
            {{ok, State}, EncodedVal};
        Reason ->
            {{error, Reason, State}, invalid_encoded_val_do_not_use}
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
    %% Problem: we cannot afford to block the vnode.
    %% Solution:
    %% 1. Move dir out of the way with a unique name
    %% 2. rm -rf in the background
    %% 3. On restart, we must restart rm -rf on items that still exist.
    %%    (see start() func for this part).
    Dropped = dropped_name(Dir),
    ok = file:rename(Dir, Dropped),
    Cmd = io_lib:format("rm -rf ~s &", [Dropped]),
    _ = os:cmd(Cmd),
    ok = filelib:ensure_dir(Dir),
    {ok, State}.

dropped_name(Dir) ->
    Dir ++ lists:flatten(io_lib:format(".~w.~w.~w", tuple_to_list(now()))) ++
        ?DROPPED_DIR_SUFFIX.

%% @doc Returns true if this backend contains any
%% non-tombstone values; otherwise returns false.
-spec is_empty(state()) -> boolean().
is_empty(S) ->
    check_is_empty(S).

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

parse_config_and_env(Config) ->
    ConfigRoot = get_prop_or_env(data_root, Config, riak_kv,
                                 {{"data_root unset, failing"}}),
    %% Avoid operator error by adding internally-necessary fudge factor
    %% size here, rather than relying on operator to do it.
    %% If we want 1 megabyte block size, then internally we need
    %% 1 megabyte + riak object size overhead + riak metadata dictionary
    %% item overhead + etc etc.
    %% See also:
    %% * http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html
    %% * http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingMetadata.html
    %% * size(term_to_binary(riak_object:new(<<>>, <<>>, <<>>))).
    BlockSizeExtra =
        63 +                                    % S3 bucket name length limit
        1024 +                                  % S3 object name length limit
        205 +                                   % #r_object minimum size
        1024,                                   % CS's MD dict entries
    BlockSize = BlockSizeExtra + get_prop_or_env(block_size, Config, riak_kv,
                                               {{"block_size unset, failing"}}),
    true = (BlockSize < ?MAXVALSIZE),
    %% MaxBlocks should be present only for testing
    TestWarning = get(test_warning),
    MaxBlocks = case get_prop_or_env(max_blocks_per_file, Config,
                                     riak_kv, undefined) of
                    N when is_integer(N),
                           1 =< N,
                           N =< ?FS2_CONTIGUOUS_BLOCKS ->
                        if TestWarning == undefined ->
                                error_logger:warning_msg(
                                 "~s: max_blocks_per_file is ~p.  This "
                                 "configuration item is only valid for tests.  "
                                 "Proceed with caution.\n",
                                 [?MODULE, N]);
                           true ->
                                ok
                        end,
                        put(test_warning, done),
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
    B1stPrefixLen = get_prop_or_env(
                  b_1st_prefixlen, Config, riak_kv, ?TYPICAL_BUCKET_PREFIX_LEN),
    BDepth = get_prop_or_env(b_depth, Config, riak_kv, 2),
    KDepth = get_prop_or_env(k_depth, Config, riak_kv, 2),
    {ConfigRoot, BlockSize, MaxBlocks, B1stPrefixLen, BDepth, KDepth}.

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

%% @spec location(state(), riak_object:bucket(), riak_object:key())
%%          -> string()
%% @doc produce the file-path at which the object for the given Bucket
%%      and Key should be stored
location(State, Bucket) ->
    location(State, Bucket, no_key_provided).

location(#state{dir = Dir, b_depth = BDepth, k_depth = KDepth,
               b_1st_prefixlen = B1stPrefixLen}, Bucket, Key) ->
    B64 = encode_bucket(Bucket),
    BDirs = if BDepth > 0 ->
                    %% We know encode_bucket() has exactly 1:2 expansion ratio
                    {BPrefix, BTail} = lists:split(2 * B1stPrefixLen, B64),
                    [First|Rest] = nest(BTail, BDepth),
                    filename:join([(BPrefix ++ First)|Rest]);
               true ->
                    ""
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
    hexlist_to_binary(B64).

%% @spec decode_key(string()) -> binary()
%% @doc reconstruct a Riak object key, given a filename
%% @see encode_key/1
decode_key(K64) ->
    hexlist_to_binary(K64).

%% @spec encode_bucket(binary()) -> string()
%% @doc make a filename out of a Riak bucket
encode_bucket(Bucket) ->
    binary_to_hexlist(Bucket).

%% @spec encode_key(binary()) -> string()
%% @doc make a filename out of a Riak object key
encode_key(Key) ->
    binary_to_hexlist(Key).

%% @doc Convert the passed binary into a string where the numbers are represented in hexadecimal (lowercase and 0 prefilled).
-spec binary_to_hexlist(binary()) -> string().
binary_to_hexlist(<<>>) ->
    [];
binary_to_hexlist(<<A:4, B:4, T/binary>>) ->
    [num2hexchar(A), num2hexchar(B)|binary_to_hexlist(T)].

num2hexchar(N) when N < 10 ->
    N + $0;
num2hexchar(N) when N < 16 ->
    (N - 10) + $a.

%% time_binary_to_hexlist_small() ->
%%     Num = 1000*1000,
%%     Xs = lists:seq(1, Num),
%%     Bin = <<0,255,1,254,3,253>>,
%%     time_binary_to_hexlist(Num, Xs, Bin).

%% time_binary_to_hexlist_big() ->
%%     Num = 50*1000,
%%     Xs = lists:seq(1, Num),
%%     Bin = list_to_binary(lists:duplicate(20, <<0,255,1,254,3,253>>)),
%%     time_binary_to_hexlist(Num, Xs, Bin).

%% time_binary_to_hexlist(Num, Xs, Bin) ->
%%     {USec, _} = timer:tc(fun() ->
%%                                  [binary_to_hexlist(Bin) || _ <- Xs]
%%                          end),
%%     USec / Num.

%% @doc Convert the passed binary into a string where the numbers are represented in hexadecimal (lowercase and 0 prefilled).
-spec hexlist_to_binary(string()) -> binary().
hexlist_to_binary(HS) ->
    list_to_binary(hexlist_to_binary_2(HS)).

hexlist_to_binary_2([]) ->
    [];
hexlist_to_binary_2([A,B|T]) ->
    [hex2byte(A, B)|hexlist_to_binary_2(T)].

hex2byte(A, B) ->
    An = hexchar2num(A),
    Bn = hexchar2num(B),
    <<An:4, Bn:4>>.

hexchar2num(C) when $0 =< C, C =< $9 ->
    C - $0;
hexchar2num(C) when $a =< C, C =< $f ->
    (C - $a) + 10.

%% @doc create a directory nesting, to keep the number of
%%      files in a directory smaller
nest(Key, Groups) -> nest(lists:reverse(string:substr(Key, 1, 2*Groups)),
                          Groups, []).
nest(_, 0, Parts) -> Parts;
nest([Nb,Na|Rest],N,Acc) ->
    nest(Rest, N-1, [[Na,Nb]|Acc]);
nest([Na],N,Acc) ->
    nest([],N-1,[[Na]|Acc]);
nest([],_N,_Acc) ->
    throw(minimum_name_length_violation).
    %% nest([],N-1,["z"|Acc]).                     % $z sorts after $f

%% Borrowed from bitcask_fileops.erl and then mangled
-spec pack_ondisk(binary(), state()) -> [binary()].
pack_ondisk(Bin, State) ->
    pack_ondisk_v1(Bin, State).

%% NOTE: We don't check for size overflow here, we rely on the caller
%%       to do that now, e.g., put_block3().

pack_ondisk_v1(Bin, _S) ->
    ValueSz = size(Bin),
    Bytes0 = [<<ValueSz:?VALSIZEFIELD/unsigned>>, Bin],
    [<<?COOKIE_V1:?COOKIE_SIZE,
       (erlang:crc32(Bytes0)):?CRCSIZEFIELD/unsigned>>,
     Bytes0].

-spec unpack_ondisk(binary()) -> binary() | bad_crc.
unpack_ondisk(<<?COOKIE_V1:?COOKIE_SIZE/unsigned, Crc32:?CRCSIZEFIELD/unsigned,
                ValueSz:?VALSIZEFIELD/unsigned, Rest/binary>>)
  when size(Rest) >= ValueSz ->
    try
        <<Value:ValueSz/binary, _Tail/binary>> = Rest,
        Crc32 = erlang:crc32([<<ValueSz:?VALSIZEFIELD/unsigned>>, Value]),
        Value
    catch _:_ ->
            bad_crc
    end;
unpack_ondisk(_Bin) ->
    bad_crc.

pack_file_header(BlockLen) ->
    pack_file1_header(BlockLen).

pack_file1_header(BlockLen)
  when 0 =< BlockLen, BlockLen =< (1 bsl ?FILE1_BLOCK_LEN_SIZE) - 1 ->
    H = <<?FILE1_COOKIE/binary,
          BlockLen:?FILE1_BLOCK_LEN_SIZE/unsigned,
          0:?FILE1_UNUSED1_LEN_SIZE/unsigned,
          0:?FILE1_UNUSED2_LEN_SIZE/unsigned,
          0:?FILE1_UNUSED3_LEN_SIZE/unsigned,
          0:?FILE1_UNUSED4_LEN_SIZE/unsigned,
          0:?FILE1_UNUSED5_LEN_SIZE/unsigned>>,
    Size = ?FILE1_HEADER_SIZE - (?FILE1_CRC_SIZE div 8),
    Size = byte_size(H),
    CRC = erlang:crc32(H),
    [H, <<CRC:?FILE1_CRC_SIZE/unsigned>>].

unpack_file_header(Hdr) ->
    unpack_file1_header(Hdr).

unpack_file1_header(<<Cookie:(?FILE1_COOKIE_SIZE div 8)/binary,
                      BlockLen:?FILE1_BLOCK_LEN_SIZE/unsigned,
                      _:?FILE1_UNUSED1_LEN_SIZE/unsigned,
                      _:?FILE1_UNUSED2_LEN_SIZE/unsigned,
                      _:?FILE1_UNUSED3_LEN_SIZE/unsigned,
                      _:?FILE1_UNUSED4_LEN_SIZE/unsigned,
                      _:?FILE1_UNUSED5_LEN_SIZE/unsigned,
                      CRC:?FILE1_CRC_SIZE/unsigned>> = Hdr)
  when Cookie == ?FILE1_COOKIE ->
    <<SubHdr:(?FILE1_HEADER_SIZE - (?FILE1_CRC_SIZE div 8))/binary,
      _/binary>> = Hdr,
    case erlang:crc32(SubHdr) of
        X when X == CRC ->
            {BlockLen, 0, 0, 0, 0, 0};
        _ ->
            throw(bad_file1_header)
    end.

read_and_unpack_file_header(FH, #state{block_size = BlockSize}) ->
    Default = {BlockSize, unused, unused, unused, unused, unused},
    case file:pread(FH, 0, ?FILE1_HEADER_SIZE) of
        {ok, Hdr} when byte_size(Hdr) == ?FILE1_HEADER_SIZE ->
            try
                {ok, unpack_file_header(Hdr)}
            catch _X:_Y ->
                    {{parse_error, Hdr}, Default}
            end;
        Else ->
            {{pread_error, Else}, Default}
    end.

read_blocksize_from_file_header(FH, State) ->
    read_blocksize_from_file_header(FH, State, false).

read_blocksize_from_file_header(FH, State, WantDetails) ->
    {Details, {BlockSize,_,_,_,_,_}} = read_and_unpack_file_header(FH, State),
    if WantDetails ->
            {Details, BlockSize};
        true ->
            BlockSize
    end.

calc_block_offset(BlockNum_x, #state{max_blocks = MaxBlocks,
                                     block_size = BlockSize}) ->
    calc_block_offset(BlockNum_x, MaxBlocks, BlockSize).

calc_block_offset(BlockNum_x, #state{max_blocks = MaxBlocks}, BlockSize) ->
    calc_block_offset(BlockNum_x, MaxBlocks, BlockSize);
calc_block_offset(BlockNum_x, MaxBlocks, BlockSize) ->
    %% CS block numbers start at zero.
    BlockNum = if BlockNum_x == trailer -> MaxBlocks;
                  true                  -> (BlockNum_x rem MaxBlocks)
               end,
    ?FILE1_HEADER_SIZE + BlockNum * (?HEADER_SIZE + BlockSize).

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
    case (FileSize - (?FILE1_COOKIE_SIZE div 8) - 1) div (?HEADER_SIZE + BlockSize) of
        X when X > (MaxBlocks - 1) ->
            %% Anything written at or beyond MaxBlocks's offset is
            %% in the realm of trailers and is hereby ignored.
            MaxBlocks;
        N ->
            N
    end.

read_block(Bucket, RiakKey, UUID, BlockNum, State) ->
    Key = convert_blocknum2key(UUID, BlockNum, State),
    File = location(State, Bucket, Key),
    try
        {ok, FI} = file:read_file_info(File),
        if FI#file_info.mode band ?TOMBSTONE_MODE_MARKER > 0 ->
                make_tombstoned_0byte_obj(Bucket, RiakKey);
           true ->
                read_block2(File, BlockNum, State)
        end
    catch
        _:_ ->
            not_found
    end.

read_block2(File, BlockNum, State) ->
    {ok, FH} = file:open(File, [read, raw, binary]),
    BlockSize = read_blocksize_from_file_header(FH, State),
    Offset = calc_block_offset(BlockNum, State, BlockSize),
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
    end.


read_file(Path) ->
    MaxSize = 4*1024*1024,
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

put_block(Bucket, UUID, BlockNum, RObj, State) ->
    put_block_t_marker_check(Bucket, UUID, BlockNum, RObj, State).

%% @doc Check for tombstone on disk before we try to put block
put_block_t_marker_check(Bucket, UUID, BlockNum, RObj, State) ->
    Key = convert_blocknum2key(UUID, BlockNum, State),
    File = location(State, Bucket, Key),
    FI_perhaps = file:read_file_info(File),
    case FI_perhaps of
        {ok, FI} ->
            if FI#file_info.mode band ?TOMBSTONE_MODE_MARKER > 0 ->
                    %% This UUID + block of chunks has got a tombstone
                    %% marker set, so there's nothing more to do here.
                    {ok, serialize_term(make_tombstoned_0byte_obj(RObj))};
               true ->
                    put_block_t_check(BlockNum, RObj, File, FI_perhaps, State)
            end;
        _ ->
            put_block_t_check(BlockNum, RObj, File, FI_perhaps, State)
    end.

%% @doc Check for tombstone in RObj before we try to put block
put_block_t_check(BlockNum, RObj, File, FI_perhaps, State) ->
    NewMode = fun(FI) ->
                      NewMode = FI#file_info.mode bor ?TOMBSTONE_MODE_MARKER,
                      ok = file:change_mode(File, NewMode)
              end,
    case riak_object_is_deleted(RObj) of
        true ->
            case FI_perhaps of
                {ok, FI} ->
                    {ok = NewMode(FI), serialize_term(make_tombstoned_0byte_obj(RObj))};
                {error, enoent} ->
                    ok = filelib:ensure_dir(File),
                    {ok, FH} = file:open(File, [write, raw]),
                    ok = file:close(FH),
                    {ok = NewMode(#file_info{mode = 8#600}), serialize_term(RObj)}
            end;
        _ ->
            put_block3(BlockNum, RObj, File, FI_perhaps, State)
    end.

%% @doc Put block: there is no previous tombstone or current tombstone.
put_block3(BlockNum, RObj, File, FI_perhaps, State) ->
    try
        case file:open(File, [write, read, raw, binary]) of
            {ok, FH} ->
                try
                    {HeaderDetails, BlockSize} =
                        read_blocksize_from_file_header(FH, State, true),
                    EncodedObj = serialize_term(RObj),
                    PackedBin = pack_ondisk(EncodedObj, State),
                    case erlang:iolist_size(PackedBin) of
                        PBS when PBS > BlockSize ->
                            {invalid_data_size, PBS, BlockSize};
                        _ -> 
                            case HeaderDetails of
                                ok ->
                                    ok;
                                _Else ->
                                    Hdr = pack_file_header(BlockSize),
                                    ok = file:pwrite(FH, 0, Hdr)
                            end,
                            Offset = calc_block_offset(BlockNum, State,
                                                       BlockSize),
                            ok = file:pwrite(FH, Offset, PackedBin),
                            {ok, EncodedObj}
                    end
                after
                        file:close(FH)
                end;
            {error, enoent} ->
                ok = filelib:ensure_dir(File),
                put_block3(BlockNum, RObj, File, FI_perhaps, State);
            {error, _} = Error ->
                Error
        end
    catch
        error:{badmatch, Y} ->
            error_logger:warning_msg("~s: badmatch1 err ~p @ ~p\n",
                                     [?MODULE, Y, erlang:get_stacktrace()]),
            {badmatch1, Y};
        X:Y ->
            error_logger:warning_msg("~s: badmatch2 ~p ~p\n", [?MODULE, X, Y]),
            {badmatch2, X, Y}
    end.

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
            %% We don't have to be 100% exact: if there are holes in the
            %% file, then we'll emit block numbers for those holes,
            %% but Riak's AAE will fix things up eventually.
            MB = calc_max_block(FI#file_info.size, State),
            [BlockNum || BlockNum <- lists:seq(0, erlang:max(MB,
                                                             MaxBlocks - 1))];
        {ok, FI} ->
            case calc_max_block(FI#file_info.size, State) of
                X when X >= MaxBlocks ->
                    %% A trailer exists, so we need to assume that
                    %% perhaps there might be holes in this file.
                    MaxBlock = MaxBlocks - 1,
                    Filt = fun(BlNum) ->
                                   %% Key's use here is: a) incorrect, and
                                   %% b) harmless.
                                   is_binary(read_block(Bucket, Key, UUID,
                                                        BlockBase + BlNum,
                                                        State))
                           end,
                    [BlockNum || BlockNum <- lists:seq(0, MaxBlock),
                                 Filt(BlockNum)];
                N ->
                    MaxBlock = N,
                    [BlockNum || BlockNum <- lists:seq(0, MaxBlock)]
            end
    end.

get_prop_or_env(Key, Properties, App, Default) ->
    case proplists:get_value(Key, Properties) of
        undefined ->
            KV_key = list_to_atom("fs2_backend_" ++ atom_to_list(Key)),
            case application:get_env(App, KV_key) of
                undefined ->
                    get_prop_or_env_default(Default);
                {ok, Value} ->
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
                    lager:error("ERROR: key_file ~p: ~p ~p\n", [File, _X2, _Y2]),
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
    case get_object(Bucket, Key, false, State) of
        {ok, Value, _S} ->
            {[], FoldFun(BKey, Value, Acc)};
        _Else ->
            {[], Acc}
    end.

%% Remember: all paths on the stack start with "/" but are not absolute.
do_glob(Glob, Dir, #state{dir = PrefixDir}) ->
    filelib:wildcard(Glob, PrefixDir ++ Dir).

convert_blocknum2key(UUID, BlockNum, #state{max_blocks = MaxBlocks}) ->
    <<UUID/binary, ((BlockNum div MaxBlocks) * MaxBlocks):?BLOCK_FIELD_SIZE>>.

serialize_term(Term) ->
    term_to_binary(Term).

deserialize_term(Bin) ->
    binary_to_term(Bin).

scan_file(_Path) ->
    qqq.

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
    O0 = riak_object:new(Bucket, K0, V0),
    O1 = riak_object:new(Bucket, K1, V1),
    os:cmd("rm -rf " ++ TestDir),
    {ok, S} = start(-1, [{data_root, TestDir},
                         {block_size, base_ext_size() + 100 + BlockSize}]),
    {{ok, S}, _} = put_object(Bucket, K1, [], O1, S),
    {{ok, S}, _} = put_object(Bucket, K0, [], O0, S),
    {ok, O0, S} = get_object(Bucket, K0, false, S),
    {ok, O1, S} = get_object(Bucket, K1, false, S),
    ok.

t1() ->
    #t{} = #t{},
    TestDir = "./delme",
    Bucket = <<?BLOCK_BUCKET_PREFIX, "delme">>,
    K0 = <<0:(?UUID_BYTES*8), 0:?BLOCK_FIELD_SIZE>>,
    V0 = <<100:64>>,
    K1 = <<0:(?UUID_BYTES*8), 1:?BLOCK_FIELD_SIZE>>,
    V1 = <<101:64>>,
    O0 = riak_object:new(Bucket, K0, V0),
    O1 = riak_object:new(Bucket, K1, V1),
    os:cmd("rm -rf " ++ TestDir),
    {ok, S} = start(-1, [{data_root, TestDir},
                         {block_size, base_ext_size() + 1024}]),
    {{ok, S4}, _} = put_object(Bucket, K0, [], O0, S),
    {ok, [{{Bucket, K0}, O0}]} = fold_objects(fun(B, K, O, Acc) ->
                                                      [{{B, K}, O}|Acc]
                                              end, [], [], S4),
    {{ok, S5}, _} = put_object(Bucket, K1, [], O1, S4),
    {ok, O0, _} = get_object(Bucket, K0, false, S5),
    {ok, O1, _} = get_object(Bucket, K1, false, S5),
    {ok, X} = fold_objects(fun(B, K, V, Acc) ->
                                   [{{B, K}, V}|Acc]
                           end, [], [], S5),
    [{{Bucket, K0}, O0}, {{Bucket, K1}, O1}] = lists:reverse(X),

    {ok, S6} = delete(Bucket, K0, unused, S5),
    %% Remember, this backend is different than the usual riak_kv backend:
    %% if someone deletes an object that's stored inside some file-on-disk
    %% F, then any other key that also is mapped to file F will also be
    %% deleted at the same time.
    {ok, []} = fold_objects(fun(B, K, O, Acc) ->
                                    [{{B, K}, O}|Acc]
                            end, [], [], S6),
    ok.

t2() ->
    TestDir = "./delme",
    %% Scribble nonsense stuff for SLF temp debugging purposes
    B1 = <<?BLOCK_BUCKET_PREFIX, "delme">>,
    B2 = <<?BLOCK_BUCKET_PREFIX, "delme2">>,
    B3 = <<?BLOCK_BUCKET_PREFIX, "delme22">>,
    os:cmd("rm -rf " ++ TestDir),
    BlockSize = base_ext_size() + 1024,
    {ok, S} = start(-1, [{data_root, TestDir},
                         {block_size, BlockSize},
                         {max_blocks_per_file, 7}]),
    RoughSize = ?UUID_BYTES + (?BLOCK_FIELD_SIZE div 8) + 8 + base_ext_size(),
    EndSeq = 20 * (BlockSize div RoughSize),

    Os = [riak_object:new(B, <<UUID:(?UUID_BYTES*8), Seq:?BLOCK_FIELD_SIZE>>,
                          list_to_binary(["val ", integer_to_list(Seq)])) ||
             B <- [B1, B2, B3],
             UUID <- [44, 88],
             Seq <- lists:seq(0, EndSeq)],
    [{{ok, _}, _} = put_object(riak_object:bucket(RObj),
                               riak_object:key(RObj),
                               [], RObj, S) || RObj <- Os],
    {ok, Res} = fold_objects(fun(_B, _K, RObj, Acc) ->
                                     [RObj|Acc]
                             end, [], [], S),
    %% Lengths are the same
    true = (length(Os) == length(Res)),
    %% The original data's sorted order is the same as our traversal
    %% order (once we reverse the fold).
    true = (lists:sort(Os) == lists:reverse(Res)),
    os:cmd("rm -rf " ++ TestDir),
    ok.

t3() ->
    TestDir = "./delme",
    Bucket = <<?BLOCK_BUCKET_PREFIX, "delme">>,
    K0 = <<0:(?UUID_BYTES*8), 0:?BLOCK_FIELD_SIZE>>,
    BlockSize = 10,                             % Too small!
    V0 = <<42:((BlockSize+1)*8)>>,
    O0 = riak_object:new(Bucket, K0, V0),
    os:cmd("rm -rf " ++ TestDir),
    {ok, S} = start(-1, [{data_root, TestDir},
                         {block_size, BlockSize}]),
    {{error, {invalid_data_size, _, _}, S}, _EncodedVal} =
        put_object(Bucket, K0, [], O0, S),
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
    BlockSize = 4096,
    os:cmd("rm -rf " ++ TestDir),
    {ok, S} = start(-1, [{data_root, TestDir},
                         {block_size, BlockSize + base_ext_size()},
                         {max_blocks_per_file, BlocksPerFile}]),
    Os = [riak_object:new(<<?BLOCK_BUCKET_PREFIX, "delme", Bp:8>>,
                          <<0:(?UUID_BYTES*8), X:?BLOCK_FIELD_SIZE>>,
                          <<X:((BlockSize)*8)>>) ||
             Bp <- [$1, $2],
             X <- lists:seq(SmallestBlock, BiggestBlock)],
    [{{ok, S}, _} = put_object(riak_object:bucket(O),
                               riak_object:key(O),
                               [], O, S) || O <- OrderFun(Os)],

    put(t4_paranoid_counter, 0),
    [begin
         OnlyB = proplists:get_value(bucket, FoldOpts, all_buckets),
         BucketFilt = fun(B) -> OnlyB == all_buckets orelse B == OnlyB end,
         %% Fold objects
         {ok, FoundOs} = fold_objects(fun(_B, _K, RObj, Acc) ->
                                              [RObj|Acc]
                                      end, [], FoldOpts, S),
         OnlyOs = [O || O <- Os,
                        B <- [riak_object:bucket(O)],
                        BucketFilt(B)],
         {OnlyB, true} = {OnlyB, (lists:sort(OnlyOs) == lists:reverse(FoundOs))},
         %% Fold keys
         {ok, FoundBKs} = fold_keys(fun(B, K, Acc) ->
                                            [{B, K}|Acc]
                                    end, [], FoldOpts, S),
         OnlyBKs = [{riak_object:bucket(O),
                     riak_object:key(O)} || O <- Os,
                                            B <- [riak_object:bucket(O)],
                                            BucketFilt(B)],
         {OnlyB, true} = {OnlyB, sets:is_subset(
                                   sets:from_list(OnlyBKs),
                                   sets:from_list(FoundBKs))},
         put(t4_paranoid_counter, get(t4_paranoid_counter) + 1)
     end || FoldOpts <- [[], [{bucket, B1}], [{bucket, B2}]] ],
    3 = get(t4_paranoid_counter),               % Extra paranoia...

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
             RestOs = [riak_object:new(
                         RestB,
                         <<X:(?UUID_BYTES*8), X:?BLOCK_FIELD_SIZE>>,
                         <<"val!">>) ||
                          X <- lists:seq(0, RestBlocks)],
             [{{ok, S}, _} = put_object(riak_object:bucket(O),
                                        riak_object:key(O),
                                        [], O, S) ||
                 O <- RestOs],
             if Bp rem 3 == 0 ->
                     {exists, RestB};
                Bp rem 3 == 1 ->
                     DelMD = dict:store(?MD_DELETED, true, dict:new()),
                     [begin
                          Od = riak_object:new(
                                 RestB,
                                 <<X:(?UUID_BYTES*8),X:?BLOCK_FIELD_SIZE>>,
                                 <<>>, DelMD),
                         {{ok, S}, _} = put_object(
                                     RestB,
                                     <<X:(?UUID_BYTES*8),X:?BLOCK_FIELD_SIZE>>,
                                     [], Od, S)
                      end || X <- lists:seq(0, RestBlocks)],
                     {tombstone, RestB};
                Bp rem 3 == 2 ->
                     [{ok, S} = delete(RestB,
                                      <<X:(?UUID_BYTES*8),X:?BLOCK_FIELD_SIZE>>,
                                      unused, S) ||
                         X <- lists:seq(0, RestBlocks)],
                     {deleted, RestB}
             end
         end || Bp <- lists:seq(RestBegin, RestEnd)],
    RestRemainingBuckets = [B || {Status, B} <- RestStatus,
                                 Status == exists orelse
                                 Status == tombstone],
    %% Fold buckets
    {ok, FoundBs} = fold_buckets(fun(B, Acc) -> [B|Acc] end, [], [], S),
    %% FoundBs should contain only the buckets in TheTwoBs and
    %% the buckets in RestStatus that are 'exists' status.
    %% The items that we marked with tombstones or explicitly deleted
    %% should be gone, and if they aren't gone, FoundBs will be too big.
    %% Also, FoundBs should come out in the proper order (don't sort it!).
    true = (lists:sort(TheTwoBs ++ RestRemainingBuckets) == lists:reverse(FoundBs)),

    %% TODO? In the section for setup of the fold buckets, perhaps
    %% use bucket names of varying lengths to try to find an error in
    %% the order that the fold provides (i.e. not in reverse
    %% lexicographic order)?
    %% TODO? In the fold objects and fold keys tests, use bucket names of
    %% different lengths, for the same reason as above?
    ok.

t5() ->
    TestDir = "./delme-t5",
    NumBuckets = 5,                           % Must be greater than 1
    EndSeq = 7,                               % Must be greater than 2
    BlockSize = 1024,

    os:cmd("rm -rf ++ " ++ TestDir),
    {ok, S} = start(-1, [{data_root, TestDir},
                         {block_size, BlockSize},
                         {max_blocks_per_file, EndSeq - 2}]),
    true = is_empty(S),

    Bs = [<<?BLOCK_BUCKET_PREFIX, X:32>> || X <- lists:seq(1, NumBuckets)],
    Os = [riak_object:new(B, <<UUID:(?UUID_BYTES*8), Seq:?BLOCK_FIELD_SIZE>>,
                          list_to_binary(["val ", integer_to_list(Seq)])) ||
             B <- Bs,
             UUID <- [55, 56, 57],
             Seq <- lists:seq(0, EndSeq)],
    [{{ok, _}, _} = put_object(riak_object:bucket(RObj),
                               riak_object:key(RObj),
                               [], RObj, S) || RObj <- Os],
    false = is_empty(S),

    LastO = lists:last(Os),
    AllButLastBucketOs =
        [O || O <- Os, riak_object:bucket(O) /= riak_object:bucket(LastO)],
    [{ok, _} = delete(riak_object:bucket(O), riak_object:key(O), [], S) ||
        O <- AllButLastBucketOs],
    false = is_empty(S),

    LastBucketOs = Os -- AllButLastBucketOs,
    DelMD = dict:store(?MD_DELETED, true, dict:new()),
    [{{ok, _}, _} = begin
                        B = riak_object:bucket(O),
                        K = riak_object:key(O),
                        V = riak_object:get_value(O),
                        DelO = riak_object:new(B, K, V, DelMD),
                        put_object(B, K, [], DelO, S)
                    end || O <- LastBucketOs],
    [begin
         {ok, O2, _} = get_object(riak_object:bucket(O), riak_object:key(O),
                                  false, S),
         true = riak_object_is_deleted(O2)
     end || O <- LastBucketOs],
    false = is_empty(S),
    [{ok, _} = delete(riak_object:bucket(O), riak_object:key(O), [], S) ||
        O <- LastBucketOs],
    true = is_empty(S),

    ok.

t6() ->
    TestDir = "./delme-t6",
    NumBuckets = 5,                           % Must be greater than 1
    EndSeq = 7,                               % Must be greater than 2
    BlockSize = 1024,

    os:cmd("rm -rf ++ " ++ TestDir),
    {ok, S} = start(-1, [{data_root, TestDir},
                         {block_size, BlockSize},
                         {max_blocks_per_file, EndSeq - 2}]),
    true = is_empty(S),

    Bs = [<<?BLOCK_BUCKET_PREFIX, X:32>> || X <- lists:seq(1, NumBuckets)],
    Os = [riak_object:new(B, <<UUID:(?UUID_BYTES*8), Seq:?BLOCK_FIELD_SIZE>>,
                          list_to_binary(["val ", integer_to_list(Seq)])) ||
             B <- Bs,
             UUID <- [55, 56, 57],
             Seq <- lists:seq(EndSeq, EndSeq)],
    [{{ok, _}, _} = put_object(riak_object:bucket(RObj),
                               riak_object:key(RObj),
                               [], RObj, S) || RObj <- Os],
    false = is_empty(S).

base_ext_size() ->
    erlang:external_size(riak_object:new(<<>>, <<>>, <<>>)) + 128.

make_tombstoned_0byte_obj(RObj) ->
    make_tombstoned_0byte_obj(riak_object:bucket(RObj),
                              riak_object:key(RObj)).

make_tombstoned_0byte_obj(Bucket, Key) ->
    riak_object:set_vclock(
      riak_object:new(Bucket, Key, <<>>,
                      dict:from_list([{?MD_DELETED, true}])),
      vclock:increment(<<"tombstone_actor">>, vclock:fresh())).

check_is_empty(S) ->
    EmptyFun = fun(_B, _K, _RObj, _Acc) ->
                       throw(it_is_not_empty)
               end,
    try
        {ok, true} = fold_objects(EmptyFun, true, [], S),
        true
    catch
        throw:it_is_not_empty ->
            false
    end.

-type resolve_ok() :: {term(), binary()}.
-type resolve_error() :: {atom(), atom()}.
-spec resolve_robj_siblings(RObj::term()) ->
                      {resolve_ok() | resolve_error(), NeedsRepair::boolean()}.

resolve_robj_siblings(Cs) ->
    [{BestRating, BestMDV}|Rest] = lists:sort([{rate_a_dict(MD, V), MDV} ||
                                                  {MD, V} = MDV <- Cs]),
    if BestRating =< 0 ->
            {BestMDV, length(Rest) > 0};
       true ->
            %% The best has a failing checksum
            {{no_dict_available, bad_checksum}, true}
    end.

%% Corruption simulation:
%% rate_a_dict(_MD, _V) -> case find_rcs_bcsum(_MD) of _ -> 666777888 end.

rate_a_dict(MD, V) ->
    %% The lower the score, the better.
    case dict:find(?MD_DELETED, MD) of
        {ok, true} ->
            -10;                                % Trump everything
        error ->
            case find_rcs_bcsum(MD) of
                CorrectBCSum when is_binary(CorrectBCSum) ->
                    case riak_cs_utils:md5(V) of
                        X when X =:= CorrectBCSum ->
                            -1;                 % Hooray correctness
                        _Bad ->
                            666                 % Boooo
                    end;
                _ ->
                    0                           % OK for legacy data
            end
    end.

find_rcs_bcsum(MD) ->
    case find_md_usermeta(MD) of
        {ok, Ps} ->
            proplists:get_value(<<?USERMETA_BCSUM>>, Ps);
        error ->
            undefined
    end.

find_md_usermeta(MD) ->
    dict:find(?MD_USERMETA, MD).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

riak_object_available() ->
    try
        _ = riak_object:new(<<>>, <<>>, <<>>),
        true
    catch
        error:undef ->
            false
    end.

t0_test() ->
    case riak_object_available() of
        true ->
            ok = t0();
        false ->
            io:format(user, "SKIP: riak_object module is not available\n", [])
    end.

t1_test() ->
    case riak_object_available() of
        true ->
            ok = t1();
        false ->
            io:format(user, "SKIP: riak_object module is not available\n", [])
    end.

t2_test_() ->
    case riak_object_available() of
        true ->
            {spawn,
             [
              {timeout, 60*1000, ?_assertEqual(ok, t2())}
             ]};
        false ->
            io:format(user, "SKIP: riak_object module is not available\n", []),
            {spawn, []}
    end.

t3_test() ->
    case riak_object_available() of
        true ->
            ok = t3();
        false ->
            io:format(user, "SKIP: riak_object module is not available\n", [])
    end.

t4_test() ->
    case riak_object_available() of
        true ->
            ok = t4();
        false ->
            io:format(user, "SKIP: riak_object module is not available\n", [])
    end.

t5_test() ->
    case riak_object_available() of
        true ->
            ok = t5();
        false ->
            io:format(user, "SKIP: riak_object module is not available\n", [])
    end.


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

-ifdef(TEST_FS2_BACKEND_IN_RIAK_KV).

simple_test_() ->
    ?assertCmd("rm -rf test/fs-backend/*"),
    %% This test is not aware of CS-style key structure, so the block_size
    %% checks will never be made, so the block_size that we give here does
    %% not matter: it merely needs to be present.
    Config = [{data_root, "test/fs-backend"},
              {block_size, 0}],
    riak_kv_backend:standard_test(?MODULE, Config).

-endif. % TEST_FS2_BACKEND_IN_RIAK_KV

nest_test() ->
    ?assertEqual(["ab","cd","ef"],nest("abcdefg", 3)),
    ?assertEqual(["ab","cd","ef"],nest("abcdef", 3)),
    ?assertEqual(["a","bc","de"], nest("abcde", 3)),
    ?assertEqual(minimum_name_length_violation, catch nest("abcd", 3)).

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

backend_eqc_fold_objects_transform(RObjs) ->
    [{BKey, riak_object:get_value(RObj)} || {BKey, RObj} <- RObjs].

backend_eqc_filter_orddict_on_delete(
                      <<?BLOCK_BUCKET_PREFIX, _/binary>> = DBucket,
                      <<DUUID:?UUID_BYTES/binary,
                        DBlockNum:?BLOCK_FIELD_SIZE>> = _DKey,
                      Dict, Config) ->
    %% backend_eqc deleted a key that uses our ?BLOCK_BUCKET_PREFIX + UUID
    %% scheme.  Filter out all other blocks that reside in the same file.

    {_ConfigRoot, BlockSize, MaxBlocks, B1stPrefixLen, BDepth, KDepth} =
        parse_config_and_env(Config),
    State = #state{dir = "does/not/matter",
                   block_size = BlockSize,
                   max_blocks = MaxBlocks,
                   b_1st_prefixlen = B1stPrefixLen,
                   b_depth = BDepth,
                   k_depth = KDepth},
    DeletedKey = convert_blocknum2key(DUUID, DBlockNum, State),
    DeletedPath = location(State, DBucket, DeletedKey),
    F = fun({Bucket, <<UUID:?UUID_BYTES/binary, BlockNum:?BLOCK_FIELD_SIZE>>},
            _Value)
           when Bucket == DBucket, UUID == DUUID ->
                Key = convert_blocknum2key(UUID, BlockNum, State),
                Path = location(State, Bucket, Key),
                Path /= DeletedPath;
           (_Key, _Value) ->
                true
        end,
    orddict:filter(F, Dict);
backend_eqc_filter_orddict_on_delete(_Bucket, _Key, Dict, _State) ->
    %% backend_eqc deleted a key that isn't special.
    Dict.

backend_eqc_bucket() ->
    oneof([backend_eqc_bucket_standard(),
           backend_eqc_bucket_v1()]).

backend_eqc_key() ->
    oneof([backend_eqc_key_standard(),
           backend_eqc_key_v1()]).

backend_eqc_postcondition_fold_keys(Expected, Result) ->
    case sets:is_subset(sets:from_list(Expected),
                        sets:from_list(Result)) of
        true ->
            true;
        false ->
            [{mod, ?MODULE}, {line, ?LINE},
             {expected, Expected}, {result, Result}]
    end.

backend_eqc_bucket_standard() ->
    oneof([<<"b1">>, <<"b2">>]).

backend_eqc_bucket_v1() ->
    ?LET(X, oneof([<<"v1_a">>, <<"v1_b">>]),
         <<?BLOCK_BUCKET_PREFIX, X/binary>>).

backend_eqc_key_standard() ->
    oneof([<<"k1">>, <<"k2">>]).

backend_eqc_key_v1() ->
    UUID = 42,
    BigBlockNum = 77239823, % Arbitrary, but bigger than any real
                            % max_blocks_per_file value
    ?LET(X, oneof([0, 1, BigBlockNum]),
         <<UUID:(?UUID_BYTES*8), X:?BLOCK_FIELD_SIZE>>).

basic_props_eqc() ->
    [{b_1st_prefixlen, 0} | basic_props()].

basic_props() ->
    [{data_root,  "test/fs-backend"},
     {block_size, 1024},
     {b_1st_prefixlen, ?TYPICAL_BUCKET_PREFIX_LEN}].

eqc_t4_wrapper(TestTime) ->
    eqc:quickcheck(eqc:testing_time(TestTime, prop_t4())).

-ifdef(TEST_FS2_BACKEND_IN_RIAK_KV).
eqc_test_() ->
    TestTime1 = 30,
    EQC_prop0 = backend_eqc:property(?MODULE, false, basic_props_eqc()),
    EQC_prop1 = eqc_statem:more_commands(75, EQC_prop0),
    EQC_prop2 = eqc:testing_time(TestTime1, EQC_prop1),
    TestTime3 = 10,
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
          {timeout, 2 * TestTime1,
           {"EQC_prop2", [?_assertEqual(true, eqc:quickcheck(EQC_prop2))]}},
          {timeout, 60000,
           {"eqc_nest_tester", [?_assertEqual(true, eqc_nest_tester())]}},
          {timeout, 2 * TestTime3,
           {"eqc_t4_wrapper", [?_assertEqual(true, eqc_t4_wrapper(TestTime3))]}}
         ]}]}]}.
-endif. % TEST_FS2_BACKEND_IN_RIAK_KV

eqc_nest_tester() ->
    setup(),
    os:cmd("mkdir -p test/fs-backend"),
    rm_rf_test_dir_contents(),
    X = eqc:quickcheck(eqc:numtests(250, prop_nest_ordered())),
    cleanup(x),
    X = true.

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
                #state{b_depth = BDepth,
                       b_1st_prefixlen = B1stPrefixLen} = S,
   ?IMPLIES(all_names_at_least_as_long_as(BucketList,
                                          BDepth + B1stPrefixLen),
            begin

                [ok = insert_sample_key(Bucket, S) || Bucket <- BucketList],
                {ok, Bs} = ?MODULE:fold_buckets(fun(B, Acc) -> [B|Acc] end, [],
                                                [], S),
                %% rm_rf_test_dir_contents(),
                case lists:usort(BucketList) == lists:reverse(Bs) of
                    true -> true;
                    _ -> {wanted, lists:usort(BucketList),
                          got, lists:reverse(Bs)}
                end
            end)
            end)).

gen_bucket() ->
    ?LET(CharGen, oneof([$a, char()]),          % Limit to only $a's, sometimes
    ?LET({BPrefix, Bucket},
         {vector(?TYPICAL_BUCKET_PREFIX_LEN, CharGen),
          frequency([
                     {5, vector(3, CharGen)},
                     {5, non_empty(list(CharGen))}
                    ])},
         iolist_to_binary([BPrefix, Bucket]))).

all_names_at_least_as_long_as(Names, Len) ->
    lists:all(fun(N) -> byte_size(N) >= Len end, Names).

insert_sample_key(Bucket, S) ->
    Key = <<"key">>,
    O = riak_object:new(Bucket, Key, <<"val">>),
    {{ok, _}, _} = ?MODULE:put_object(Bucket, Key, [], O, S),
    ok.

-endif. % EQC
-endif. % TEST
