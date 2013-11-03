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

%% For excerpts from filelib.erl:
%%
%% Copyright Ericsson AB 1997-2011. All Rights Reserved.
%%
%% The contents of this file are subject to the Erlang Public License,
%% Version 1.1, (the "License"); you may not use this file except in
%% compliance with the License. You should have received a copy of the
%% Erlang Public License along with this software. If not, it can be
%% retrieved online at http://www.erlang.org/.
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and limitations
%% under the License.

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
%% XX http://erlang.org/pipermail/erlang-questions/2011-September/061147.html
%%    XX Avoid `file` module whenever file_server_2 calls are used.
%%    XX Avoid file:file_name_1() recursion silliness
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
-export([fold_lower_level/4]).
%% To avoid EUnit test dependency on riak_cs_utils
-export([hexlist_to_binary/1, binary_to_hexlist/1, resolve_robj_siblings/1]).
%% For QuickCheck testing use only
-export([get/3, put/5]).
%% Testing
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
-define(BUCKET_PREFIX_LEN, 3).  % To match ?BLOCK_BUCKET_PREFIX length

%% !@#$!!@#$!@#$!@#$! Erlang's efile_drv does not support the sticky bit.
%% So, we use something else: the setuid bit.
%% I'd prefer to use the setgid bit, but HFS+ does not allow non-root
%% users to set the setgid bit on files.  {exasperated}
-define(TOMBSTONE_MODE_MARKER, 8#4000).

%% Dropped data directory suffix: for asynchronously deleting lots of stuff
-define(DROPPED_DIR_SUFFIX, ".dropped").

-define(MAX_LOWLEVEL_FOLD_ERRORS, 100).

-ifdef(TEST).
-compile(export_all).
-ifdef(EQC).
%% EQC testing assistants
-export([eqc_filter_delete/3]).
-export([prop_nest_ordered/0, eqc_nest_tester/1]).
-export([backend_eqc_fold_objects_transform/1,
         backend_eqc_filter_orddict_on_delete/4,
         backend_eqc_bucket/0,
         backend_eqc_key/0,
         backend_eqc_postcondition_fold_keys/2]).
-export([backend_eunit_bucket/1, backend_eunit_key/1]).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

-ifndef(MD_DELETED).
-define(MD_DELETED, <<"X-Riak-Deleted">>).
-endif.

%% From filelib.erl
-define(HANDLE_ERROR(Expr),
        try
            Expr
        catch
            error:{badpattern,_}=UnUsUalVaRiAbLeNaMe ->
                %% Get the stack backtrace correct.
                erlang:error(UnUsUalVaRiAbLeNaMe)
        end).

-record(state, {
          dir        :: string(),
          block_size :: non_neg_integer(),
          max_blocks :: non_neg_integer(),
          i_depth    :: non_neg_integer(),
          %% Items used by reduce_stack only
          last_path  :: string(),
          last_fh    :: term(),
          fold_errors = 0 :: integer(),
          upper_db_type :: atom(),
          upper_db   :: term(),
          upper_db_opts :: term()
          %% %% Items below used by key listing stack only
          %% fold_type  :: 'undefined' | 'buckets' | 'keys' | 'objects'
         }).
-record(upper_opts, {
          open  :: term(),
          read  :: term(),
          write :: term(),
          fold  :: term()
         }).

%% Lower level fold options state
-record(fold_ll_s, {
          opts = [] :: list()
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
        {ConfigRoot, BlockSize, MaxBlocks, IDepth} =
            parse_config_and_env(Config),
        Dir = filename:join([ConfigRoot,PartitionName]),
        ok = ef_ensure_dir(Dir),
        ok = create_or_sanity_check_version_file(Dir, BlockSize, MaxBlocks,
                                                 IDepth),

        %% See also: drop() function
        Cmd = lists:flatten(
                io_lib:format("rm -rf ~s.*~s &", [Dir, ?DROPPED_DIR_SUFFIX])),
        _ = os:cmd(Cmd),

        %% Open our upper-level DB
        OpenOpts = [{create_if_missing, true},
                    {max_open_files, 50},
                    {use_bloomfilter, true},
                    {write_buffer_size, 34410590}],
        ReadOpts = [],
        WriteOpts = [],
        FoldOpts = [{fill_cache,false}],
        AllOpts = #upper_opts{open = OpenOpts,
                              read = ReadOpts,
                              write = WriteOpts,
                              fold = FoldOpts},
        UpperDir = Dir ++ "/.upperdb",
        {ok, UpperDB} = open_db(UpperDir, OpenOpts),

        {ok,  #state{dir = Dir,
                     block_size = BlockSize,
                     max_blocks = MaxBlocks,
                     i_depth = IDepth,
                     upper_db_type = eleveldb,
                     upper_db = UpperDB,
                     upper_db_opts = AllOpts}}
    catch throw:Error ->
        {error, Error}
    end.

%% @doc Stop the backend
-spec stop(state()) -> ok.
stop(State) ->
    catch eleveldb:close(State#state.upper_db),
    ok.

%% @doc Get the object stored at the given bucket/key pair
-spec get_object(riak_object:bucket(), riak_object:key(), boolean(), state()) ->
                     {ok, binary() | riak_object:riak_object(), state()} |
                     {ok, not_found, state()} |
                     {error, term(), state()}.
get_object(<<Prefix:?BUCKET_PREFIX_LEN/binary, _/binary>> = Bucket,
           <<UUID:?UUID_BYTES/binary, BlockNum:?BLOCK_FIELD_SIZE>> = Key,
           WantsBinary, State)
  when Prefix == ?BLOCK_BUCKET_PREFIX_V0;
       Prefix == ?BLOCK_BUCKET_PREFIX_V1;
       Prefix == ?BLOCK_BUCKET_PREFIX_V2 ->
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
    case do_is_file(File) of
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
    %% io:format(user, "get Bucket ~w Key ~w\n", [Bucket, Key]),
    case get_object(Bucket, Key, false, State) of
        {ok, RObj, State} ->
            {ok, riak_object:get_value(RObj), State};
        Else ->
            Else
    end.

%% EQC use only
put(Bucket, Key, IdxList, Val, State) ->
    %% io:format(user, "put Bucket ~w Key ~w\n", [Bucket, Key]),
    RObj = riak_object:new(Bucket, Key, Val),
    case put_object(Bucket, Key, IdxList, RObj, State) of
        %% {{ok, _}, _} ->
        %%     ok;
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
put_object(<<Prefix:?BUCKET_PREFIX_LEN/binary, _/binary>> = Bucket,
           <<UUID:?UUID_BYTES/binary, BlockNum:?BLOCK_FIELD_SIZE>>,
           _IndexSpecs, RObj, State)
  when Prefix == ?BLOCK_BUCKET_PREFIX_V0;
       Prefix == ?BLOCK_BUCKET_PREFIX_V1;
       Prefix == ?BLOCK_BUCKET_PREFIX_V2 ->
    case resolve_robj_siblings(riak_object:get_contents(RObj)) of
        {{_MD, V} = MDV, _} when is_binary(V) ->
            RObj2 = case riak_object:get_values(RObj) of
                        [VV] when VV =:= V ->
                            RObj;
                        _ ->
                            riak_object:set_contents(RObj, [MDV])
                    end,
            put_object_lower(Bucket, UUID, BlockNum, RObj2, State);
        Else ->
            {{error, {has_errors, Else}, State}, invalid_encoded_val_do_not_use}
    end.

put_object_lower(Bucket, UUID, BlockNum, RObj, State) ->
    case put_block(Bucket, UUID, BlockNum, RObj, State) of
        {ok, RObj2, EncodedVal} ->
            ok = put_block_upper(Bucket, UUID, BlockNum, RObj2, State),
            {{ok, State}, EncodedVal};
        Reason ->
            {{error, Reason, State}, invalid_encoded_val_do_not_use}
    end.

put_block_upper(Bucket, UUID, BlockNum, RObj, State) ->
    Key = make_upper_key(Bucket, UUID, BlockNum),
    ValSize = byte_size(riak_object:get_value(RObj)),
    CSum = find_rcs_bcsum_or_else(riak_object:get_metadata(RObj)),
    %% TODO: standardize & versionify this thing
    Val = term_to_binary({yy, riak_object:vclock(RObj), CSum, ValSize}),
    ok = eleveldb:put(State#state.upper_db, Key, Val, (State#state.upper_db_opts)#upper_opts.write),
    ok.

make_upper_key(Bucket, UUID, BlockNum) ->    
    list_to_binary([Bucket,
                    <<UUID:?UUID_BYTES/binary, BlockNum:?BLOCK_FIELD_SIZE>>]).

%% @doc Delete the object stored at BKey
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(<<Prefix:?BUCKET_PREFIX_LEN/binary, _/binary>> = Bucket,
       <<UUID:?UUID_BYTES/binary, BlockNum:?BLOCK_FIELD_SIZE>>,
       _IndexSpecs, State)
  when Prefix == ?BLOCK_BUCKET_PREFIX_V0;
       Prefix == ?BLOCK_BUCKET_PREFIX_V1;
       Prefix == ?BLOCK_BUCKET_PREFIX_V2 ->
    Key = convert_blocknum2key(UUID, BlockNum, State),
    File = location(State, Bucket, Key),
    PerhapsBlocks = enumerate_perhaps_chunks_in_file(File, State),
    %% PerhapsBlocks = [0,1,2,3,4,5,98],

    _ = ef_delete(File),
    if %% PerhapsBlocks == [] ->
       %%      ok;
       true ->
            BaseBlock = (BlockNum div State#state.max_blocks) * State#state.max_blocks,
            DbDeletes = [{delete, make_upper_key(Bucket, UUID, BaseBlock+Block)}
                         || Block <- PerhapsBlocks],
            ok = eleveldb:write(State#state.upper_db, DbDeletes, (State#state.upper_db_opts)#upper_opts.write)
    end,
    {ok, State}.

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
                   proplists:proplist(),
                   state()) -> {ok, any()} | {async, fun()}.
fold_buckets(FoldBucketsFun, Acc, Opts, State) ->
    FiltFun = fun(<<Bucket:(?BUCKET_PREFIX_LEN+?UUID_BYTES)/binary, _Key/binary>>,
                  {LastBucket, _InnerAcc} = FAcc)
                 when Bucket == LastBucket ->
                      FAcc;
                 (<<Bucket:(?BUCKET_PREFIX_LEN+?UUID_BYTES)/binary, _Key/binary>>,
                  {_LastBucket, InnerAcc}) ->
                      {Bucket, FoldBucketsFun(Bucket, InnerAcc)}
              end,
    ObjectFolder =
        fun() ->
                {_, FinalAcc} = eleveldb:fold_keys(State#state.upper_db,
                                                   FiltFun,
                                                   {<<>>, Acc}, []),
                FinalAcc
        end,
    case proplists:get_value(async_fold, Opts, false) of
        true ->
            {async, ObjectFolder};
        _ ->
            {ok, ObjectFolder()}
    end.

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                proplists:proplist(),
                state()) -> {ok, term()} | {async, fun()}.
fold_keys(FoldKeysFun, Acc, Opts, State) ->
    SingleBucket = proplists:get_value(bucket, Opts),
    FoldOpts = make_fold_opts(SingleBucket, State),
    Fun = fun(<<Bucket:(?BUCKET_PREFIX_LEN+?UUID_BYTES)/binary, Key/binary>> = _BKey,
              FAcc) ->
                  if SingleBucket /= undefined, Bucket /= SingleBucket ->
                          throw({break, FAcc});
                     true ->
                          FoldKeysFun(Bucket, Key, FAcc)
                  end
          end,
    ObjectFolder =
        fun() ->
                try
                    eleveldb:fold_keys(State#state.upper_db, Fun,
                                       Acc, FoldOpts)
                catch
                    {break, AccFinal} ->
                        AccFinal
                end
        end,
    case proplists:get_value(async_fold, Opts, false) of
        true ->
            {async, ObjectFolder};
        _ ->
            {ok, ObjectFolder()}
    end.

make_fold_opts(SingleBucket, State) ->
    FirstKey = case SingleBucket of
                   undefined ->
                       <<>>;
                   _ ->
                       SingleBucket
               end,
    [{first_key, FirstKey}|(State#state.upper_db_opts)#upper_opts.fold].

%% @doc Fold over all the objects for one or all buckets.
%%
%% We fold things in upper's DB's order.  This should still be mostly
%% efficient: we won't scan files in directory order, but we *will* scan
%% blocks of a single S3 object in sequential order, which should be good
%% enough for I/O throughput through the OS & file system.

-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   proplists:proplist(),
                   state()) -> {ok, any()} | {async, fun()}.
fold_objects(FoldObjectsFun, Acc, Opts, State) ->
    fold_objects_upper(FoldObjectsFun, Acc, Opts, State).

fold_objects_upper(FoldObjectsFun, Acc, Opts, State) ->
    case proplists:get_value(aae_reconstruction, Opts) of
        true ->
            fold_objects_upper_aae_reconstruction(FoldObjectsFun, Acc, Opts,
                                                  State);
        _ ->
            fold_objects_upper_regular(FoldObjectsFun, Acc, Opts, State)
    end.

fold_objects_upper_aae_reconstruction(FoldObjectsFun, Acc, Opts, State) ->
    SingleBucket = proplists:get_value(bucket, Opts),
    FoldOpts = make_fold_opts(SingleBucket, State),
    Fun = fun({<<Bucket:(?BUCKET_PREFIX_LEN+?UUID_BYTES)/binary, Key/binary>>, Val},
              FAcc) ->
                  if SingleBucket /= undefined, Bucket /= SingleBucket ->
                          throw({break, FAcc});
                     true ->
                          try
                              {yy, VClock, CSum, _Size} = binary_to_term(Val),
                              RObj = make_fake_tiny_obj(Bucket, Key, CSum,
                                                        VClock),
                              FoldObjectsFun(Bucket, Key, RObj, FAcc)
                          catch
                              throw:{break, FinalAcc} ->
                                  %% Rethrow, alas
                                  throw({break, FinalAcc});
                              _:_ ->
                                  FAcc
                          end
                  end
          end,
    ObjectFolder =
        fun() ->
                try
                    eleveldb:fold(State#state.upper_db, Fun, Acc, FoldOpts)
                catch
                    {break, AccFinal} ->
                        AccFinal
                end
        end,
    case proplists:get_value(async_fold, Opts, false) of
        true ->
            {async, ObjectFolder};
        _ ->
            {ok, ObjectFolder()}
    end.

fold_objects_upper_regular(FoldObjectsFun, Acc, Opts, State) ->
    Fun = fun(Bucket, Key, FAcc) ->
                  <<UUID:?UUID_BYTES/binary, BlockNum:?BLOCK_FIELD_SIZE>> = Key,
                  case read_block(Bucket, Key, UUID, BlockNum, State) of
                      Bin when is_binary(Bin) ->
                          try
                              RObj = deserialize_term(Bin),
                              FoldObjectsFun(Bucket, Key, RObj, FAcc)
                          catch
                              throw:it_is_not_empty ->
                                  throw(it_is_not_empty); % Rethrow, alas
                              _:_ ->
                                  FAcc
                          end;
                      RObj when element(1, RObj) == r_object ->
                          try
                              FoldObjectsFun(Bucket, Key, RObj, FAcc)
                          catch
                              throw:it_is_not_empty ->
                                  throw(it_is_not_empty); % Rethrow, alas
                              _:_ ->
                                  FAcc
                          end;
                      not_found ->
                          %% Well, we just witnessed an upper vs. lower sync
                          %% problem.  We can be not_found if there's a
                          %% checksum error or other problem.  Delete it.
                          UpperKey = make_upper_key(Bucket, UUID, BlockNum),
                          _ = eleveldb:delete(State#state.upper_db,
                                              UpperKey, []),
                          FAcc
                  end
          end,
    fold_keys(Fun, Acc, Opts, State).

fold_objects_lower(FoldObjsFun, Acc, FoldOptsList, State) ->
    ______Bucket = proplists:get_value(bucket, FoldOptsList),
    Stack = make_start_stack_objects(FoldOptsList, State),
    SingleBucket = proplists:get_value(bucket, FoldOptsList),
    FoldOpts = (State#state.upper_db_opts)#upper_opts.fold,
    Fun = fun(Bucket, Key, Obj, FAcc) ->
                  if SingleBucket /= undefined, Bucket /= SingleBucket ->
                          %% The lower-level backend doesn't have an efficient
                          %% way of filtering out non-matching bucket names.
                          FAcc;
                     true ->
                          FoldObjsFun(Bucket, Key, Obj, FAcc)
                  end
          end,
    ObjectFolder =
        fun() ->
                %% This catch is never used by the above fun, but here for
                %% symmetry with other fold-evaluation funs.
                try
                    FoldS = make_fold_ll_s(FoldOpts),
                    reduce_stack(Stack, Fun, Acc, FoldS, State)
                catch
                    {break, AccFinal} ->
                        AccFinal
                end
        end,
    case proplists:get_value(async_fold, FoldOptsList, false) of
        true ->
            {async, ObjectFolder};
        _ ->
            {ok, ObjectFolder()}
    end.

make_fold_ll_s(FoldOpts) ->
    #fold_ll_s{opts = FoldOpts}.

%% @doc Fold across all Riak objects in the underlying fs2 store.

-type robj_fold_fun() :: fun((riak_object:riak_object(), term()) -> term()).
-spec fold_lower_level(robj_fold_fun(), term(), list(), #state{}) -> term().
fold_lower_level(FoldFun, Acc, FoldOpts, State)
  when is_function(FoldFun, 4) ->
    FoldS = make_fold_ll_s(FoldOpts),
    reduce_stack(make_start_stack_objects(FoldS, State), FoldFun, Acc, FoldS, State).

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
    ok = ef_rename(Dir, Dropped),
    Cmd = io_lib:format("rm -rf ~s &", [Dropped]),
    _ = os:cmd(Cmd),
    ok = ef_ensure_dir(Dir),
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
    IDepth = get_prop_or_env(i_depth, Config, riak_kv, 3),
    {ConfigRoot, BlockSize, MaxBlocks, IDepth}.

%% @spec atomic_write(File :: string(), Val :: binary()) ->
%%       ok | {error, Reason :: term()}
%% @doc store a atomic value to disk. Write to temp file and rename to
%%       normal path.
atomic_write(File, Val, State) ->
    FakeFile = File ++ ".tmpwrite",
    case write_file(FakeFile, pack_ondisk(Val, State)) of
        ok ->
            ef_rename(FakeFile, File);
        X -> X
    end.

%% @spec location(state(), riak_object:bucket(), riak_object:key())
%%          -> string()
%% @doc produce the file-path at which the object for the given Bucket
%%      and Key should be stored
location(State, Bucket) ->
    location(State, Bucket, <<>>).

location(#state{dir = Dir, i_depth = IDepth}, Bucket, Key) ->
    B64 = encode_thingie(crypto:sha([Bucket, Key])),
    IDirs = if IDepth > 0 ->
                    %% We know encode_thingie() has exactly 1:2 expansion ratio
                    [First|Rest] = nest(B64, IDepth),
                    filename:join([First|Rest]);
               true ->
                    ""
            end,
    filename:join([Dir, IDirs, B64]).

%% @spec location_to_bkeyhash(string(), state()) ->
%%           {riak_object:bucket(), riak_object:key()}
%% @doc reconstruct an hash md5(Riak bucket/key pair), given the location at
%%      which its object is stored on-disk
location_to_bkeyhash("/" ++ Path, State) ->
    location_to_bkeyhash(Path, State);
location_to_bkeyhash(Path, #state{i_depth = IDepth}) ->
    [_B64|Rest] = drop_from_list(IDepth, string:tokens(Path, "/")),
    {decode_thingie(Rest)}.

%% @spec decode_thingie(string()) -> binary()
%% @doc reconstruct a Riak bucket, given a filename
%% @see encode_thingie/1
decode_thingie(B64) ->
    hexlist_to_binary(B64).

%% @spec encode_thingie(binary()) -> string()
%% @doc make a filename out of a Riak bucket
encode_thingie(Bucket) ->
    binary_to_hexlist(Bucket).

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

is_tombstoned_file(File) ->
    {ok, FI} = ef_read_file_info(File),
    if FI#file_info.mode band ?TOMBSTONE_MODE_MARKER > 0 ->
            true;
       true ->
            false
    end.

read_block(Bucket, RiakKey, UUID, BlockNum, State) ->
    Key = convert_blocknum2key(UUID, BlockNum, State),
    File = location(State, Bucket, Key),
    try
        case is_tombstoned_file(File) of
            true ->
                make_tombstoned_0byte_obj(Bucket, RiakKey);
            false ->
                read_block2(File, BlockNum, State)
        end
    catch
        _:_ ->
            not_found
    end.

read_block2(File, BlockNum, State) ->
    FH = if State#state.last_fh == undefined ->
                 {ok, NewFH} = file:open(File, [read, raw, binary]),
                 NewFH;
             true ->
                 State#state.last_fh
         end,
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
        if State#state.last_fh == undefined ->
                file:close(FH);
           true ->
                ok
        end
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
    FI_perhaps = ef_read_file_info(File),
    case FI_perhaps of
        {ok, FI} ->
            if FI#file_info.mode band ?TOMBSTONE_MODE_MARKER > 0 ->
                    %% This UUID + block of chunks has got a tombstone
                    %% marker set, so there's nothing more to do here.
                    RObj2 = make_tombstoned_0byte_obj(RObj),
                    {ok, RObj2, serialize_term(RObj2)};
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
                      ok = ef_change_mode(File, NewMode)
              end,
    case riak_object_is_deleted(RObj) of
        true ->
            case FI_perhaps of
                {ok, FI} ->
                    {ok = NewMode(FI), RObj, serialize_term(make_tombstoned_0byte_obj(RObj))};
                {error, enoent} ->
                    ok = ef_ensure_dir(File),
                    {ok, FH} = file:open(File, [write, raw]),
                    ok = file:close(FH),
                    {ok = NewMode(#file_info{mode = 8#600}), RObj, serialize_term(RObj)}
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
                            %% TODO: This is a sad state of affairs, but
                            %% I hadn't realized the limitation before Riak
                            %% 1.4.0 was finished & released.  The EncodedVal
                            %% that we return to riak_kv_vnode is not an
                            %% opaque blob that will be used as-is by AAE.
                            %% Instead, AAE fully assumes that this EncodedVal
                            %% is a valid encoded Riak object, and it is going
                            %% to unencode it, then update the vclock with
                            %% a sorted one (but we never have more than one
                            %% vclock actor, so this step is a no-op as far
                            %% as we're concerned), re-encodes the r_object,
                            %% and then hashes it with erlang:phash2() and
                            %% then encodes the hash with term_to_binary().
                            %%
                            %% So, we're going to create a fake object
                            %% that's barely big & valid enough to allow AAE
                            %% to do its job.
                            FakeRObj = make_fake_tiny_obj(RObj),
                            {ok, RObj, riak_object:to_binary(v1, FakeRObj)}
                    end
                after
                        file:close(FH)
                end;
            {error, enoent} ->
                ok = ef_ensure_dir(File),
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

enumerate_perhaps_chunks_in_file(Path, #state{max_blocks=MaxBlocks} = State) ->
    case ef_read_file_info(Path) of
        {error, _} ->
            [];
        {ok, FI} ->
            %% We don't have to be 100% exact: if there are holes in the
            %% file, then we'll emit block numbers for those holes,
            MB = calc_max_block(FI#file_info.size, State),
            [BlockNum || BlockNum <- lists:seq(0, erlang:min(MB,
                                                             MaxBlocks - 1))]
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
                                    IDepth) ->
    VersionFile = make_version_path(Dir),
    case file:consult(VersionFile) of
        {ok, Terms} ->
            ok = sanity_check_terms(Terms, BlockSize, MaxBlocks,
                                    IDepth);
        {error, enoent} ->
            ok = make_version_file(VersionFile, BlockSize, MaxBlocks,
                                   IDepth)
    end.

make_version_path(Dir) ->
    Dir ++ "/.version.data".                    % Must hide name from globbing!

make_version_file(File, BlockSize, MaxBlocks, IDepth) ->
    ok = ef_ensure_dir(File),
    {ok, FH} = file:open(File, [write]),
    [ok = io:format(FH, "~p.\n", [X]) ||
        X <- [{backend, ?MODULE},
              {version_number, 1},
              {block_size, BlockSize},
              {max_blocks, MaxBlocks},
              {i_depth, IDepth}]],
    ok = file:close(FH).

sanity_check_terms(Terms, BlockSize, MaxBlocks, IDepth) ->
    ?MODULE = proplists:get_value(backend, Terms),
    1 = proplists:get_value(version_number, Terms),
    %% Use negative number below because if we use the default
    %% 'undefined' for a value that is not present, Erlang term
    %% ordering says integer() < atom().
    Bogus = -9999999999999999999999999999,
    true = (BlockSize =< proplists:get_value(block_size, Terms, Bogus)),
    true = (MaxBlocks =< proplists:get_value(max_blocks, Terms, Bogus)),
    IDepth = proplists:get_value(i_depth, Terms),
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

make_start_stack_objects(_FoldOpts, #state{dir = _Dir, i_depth = IDepth}) ->
    case IDepth of
        0 ->
            %% TODO: The stack machine doesn't support this, is it needed?
            [st_glob_buckets];
        N ->
            [{st_glob_bucket_intermediate, 1, N, ""}]
    end.

%% @doc Reduce (or fold, pick your name) over items in a work stack
%%      We intentionally do *not* return State.
%%      The FoldS state rec isn't used at this time.
reduce_stack([], _FoldFun, Acc, _FoldS, State) ->
    catch file:close(State#state.last_fh),
    Acc;
reduce_stack([Op|Rest], FoldFun, Acc, FoldS, State) ->
    try
        {PushOps, State2, Acc2} = exec_stack_op(Op, FoldFun, Acc, State),
        reduce_stack(PushOps ++ Rest, FoldFun, Acc2, FoldS, State2)
    catch throw:{found_a_bucket, NewAcc} ->
            NewStack = lists:dropwhile(fun(pop_back_to_here) -> false;
                                          (_)                -> true
                                       end, Rest),
            reduce_stack(NewStack, FoldFun, NewAcc, FoldS, State)
    end.

exec_stack_op({st_glob_bucket_intermediate, Level, MaxLevel, MidDir},
              _FoldFun, Acc, State)
  when Level < MaxLevel ->
    Ops = [{st_glob_bucket_intermediate, Level+1, MaxLevel, MidDir ++ "/" ++Dir}||
              Dir <- do_glob("*", MidDir, State)],
    {Ops, State, Acc};
exec_stack_op({st_glob_bucket_intermediate, Level, MaxLevel, MidDir},
              _FoldFun, Acc, State)
  when Level == MaxLevel ->
    Ops = [{st_glob_key_file, MidDir ++ "/" ++ Dir} ||
              Dir <- do_glob("*", MidDir, State)],
    {Ops, State, Acc};
exec_stack_op({st_glob_key_file, MidDir},  _FoldFun, Acc, State) ->
    Ops = [{st_key_file, MidDir ++ "/" ++ File} ||
              File <- do_glob("*", MidDir, State)],
    {Ops, State, Acc};
exec_stack_op({st_key_file, File}, _FoldFun, Acc, #state{dir=Dir} = State) ->
    %% {[], FoldFun(File, value_unused_by_this_modules_fold_wrapper, Acc)}.
    Path = Dir ++ File,
    PerhapsBlocks = enumerate_perhaps_chunks_in_file(Path, State),
    case is_tombstoned_file(Path) of
        false ->
            BKeys = [{file_chunk, Path, Block} || Block <- PerhapsBlocks],
            {BKeys, State, Acc};
        true ->
            case find_any_robj_in_file(Path, PerhapsBlocks, State) of
                not_found ->
                    {[], State, Acc};
                RObj ->
                    Bucket = riak_object:bucket(RObj),
                    <<UUID:?UUID_BYTES/binary, BlockNum:?BLOCK_FIELD_SIZE>> =
                        riak_object:key(RObj),
                    BaseBlock = BlockNum div State#state.max_blocks,
                    Res = [begin
                               K = <<UUID:?UUID_BYTES/binary,
                                     (BaseBlock + NewBlock):?BLOCK_FIELD_SIZE>>,
                               {st_robj, make_tombstoned_0byte_obj(Bucket, K)}
                           end || NewBlock <- PerhapsBlocks],
                    {Res, State, Acc}
            end
    end;
exec_stack_op({file_chunk, Path, Block}, FoldFun, Acc, State0) ->
    State = if Path == State0#state.last_path ->
                    State0;
               true ->
                    catch file:close(State0#state.last_fh),
                    %% TODO: concurrency race (low probability)
                    {ok, NewFH} = file:open(Path, [read, raw, binary]),
                    State0#state{last_path = Path,
                                 last_fh = NewFH}
            end,
    case read_block2(Path, Block, State) of
        Bin when is_binary(Bin) ->
            try
                RObj = deserialize_term(Bin),
                {[], State, FoldFun(riak_object:bucket(RObj),
                                    riak_object:key(RObj),
                                    RObj,
                                    Acc)}
            catch
                throw:{break, FinalAcc} ->
                    %% Rethrow, alas
                    throw({break, FinalAcc});
                EX:EY ->
                    perhaps_log_fold_ll_error(EX, EY,
                                              Path, Block, Acc,
                                              State)
            end;
        _ ->
            {[], State, Acc}
    end;
exec_stack_op({st_robj, RObj}, FoldFun, Acc, State) ->
    try
        {[], State, FoldFun(RObj, Acc)}
    catch EX:EY ->
            perhaps_log_fold_ll_error(EX, EY,
                                      "no-path", "no-block", Acc,
                                      State)
    end.

perhaps_log_fold_ll_error(EX, EY, Path, Block, Acc,
                          #state{fold_errors = Errs} = State)
 when Errs < ?MAX_LOWLEVEL_FOLD_ERRORS  ->
    error_logger:error_msg("file_chunk fold_fun: ~s ~p: ~p ~P @ ~p\n",
                           [Path, Block, EX, EY, 32, erlang:get_stacktrace()]),
    {[], State#state{fold_errors = Errs + 1}, Acc};
perhaps_log_fold_ll_error(_EX, _EY, _Path, _Block, Acc,
                          #state{fold_errors = ?MAX_LOWLEVEL_FOLD_ERRORS} = State) ->
    error_logger:error_msg("file_chunk fold_fun: suppressing errors on dir ~s",
                           [State#state.dir]),
    {[], State#state{fold_errors = ?MAX_LOWLEVEL_FOLD_ERRORS + 1}, Acc};
perhaps_log_fold_ll_error(_EX, _EY, _Path, _Block, Acc, State) ->
    {[], State, Acc}.

%% Remember: all paths on the stack start with "/" but are not absolute.
do_glob(Glob, Dir, #state{dir = PrefixDir}) ->
    ef_wildcard(Glob, PrefixDir ++ Dir).

convert_blocknum2key(UUID, BlockNum, #state{max_blocks = MaxBlocks}) ->
    <<UUID/binary, ((BlockNum div MaxBlocks) * MaxBlocks):?BLOCK_FIELD_SIZE>>.

serialize_term(Term) ->
    term_to_binary(Term).

deserialize_term(Bin) ->
    binary_to_term(Bin).

find_any_robj_in_file(Path, Blocks, State) ->
    lists:foldl(fun(BlockNum, not_found = Acc) ->
                        try
                            try_to_read_block(Path, BlockNum, State)
                        catch _:_ ->
                                Acc
                        end;
                   (_BlockNum, Acc) ->
                        Acc
                end, not_found, Blocks).

try_to_read_block(Path, BlockNum, State) ->
    case read_block2(Path, BlockNum, State) of
        Bin when is_binary(Bin) ->
            deserialize_term(Bin);        % Caller can catch exception
        _ ->
            not_found
    end.

t0() ->
    %% Blocksize must be at last 15 or so: this test writes two blocks
    %% for the same UUID, but it does them out of order, so a trailer
    %% will be created ... and if the block size is too small to
    %% accomodate the trailer, bad things happen.
    BlockSize = 22,
    TestDir = "./delme",
    Bucket = <<?BLOCK_BUCKET_PREFIX_V0:3/binary, "delme">>,
    K0 = <<0:(?UUID_BYTES*8), 0:?BLOCK_FIELD_SIZE>>,
    V0 = <<42:(BlockSize*8)>>,
    K1 = <<0:(?UUID_BYTES*8), 1:?BLOCK_FIELD_SIZE>>,
    V1 = <<43:(BlockSize*8)>>,
    O0 = riak_object:new(Bucket, K0, V0),
    O1 = riak_object:new(Bucket, K1, V1),
    os:cmd("rm -rf " ++ TestDir),
    {ok, S} = start(-1, [{data_root, TestDir},
                         {block_size, base_ext_size() + 100 + BlockSize}]),
    try
        {{ok, S}, _} = put_object(Bucket, K1, [], O1, S),
        {{ok, S}, _} = put_object(Bucket, K0, [], O0, S),
        {ok, O0, S} = get_object(Bucket, K0, false, S),
        {ok, O1, S} = get_object(Bucket, K1, false, S),
        ok
    after
        stop(S)
    end.

t1() ->
    TestDir = "./delme",
    Bucket = <<?BLOCK_BUCKET_PREFIX_V1:3/binary, 16#50:(?UUID_BYTES*8)>>,
    K0 = <<42:(?UUID_BYTES*8), 0:?BLOCK_FIELD_SIZE>>,
    V0 = <<100:64>>,
    K1 = <<42:(?UUID_BYTES*8), 1:?BLOCK_FIELD_SIZE>>,
    V1 = <<101:64>>,
    O0 = riak_object:new(Bucket, K0, V0),
    O1 = riak_object:new(Bucket, K1, V1),
    os:cmd("rm -rf " ++ TestDir),
    {ok, S} = start(-1, [{data_root, TestDir},
                         {block_size, base_ext_size() + 1024}]),
    try
        {{ok, S4}, _} = put_object(Bucket, K0, [], O0, S),
        {ok, O0, _} = get_object(Bucket, K0, false, S4), % DELMEEEEEEEEEEEEEEEEEEEEEEEEEEEEE
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
        ok
    after
        stop(S)
    end.

t2() ->
    TestDir = "./delme",
    %% Scribble nonsense stuff for SLF temp debugging purposes
    B1 = <<?BLOCK_BUCKET_PREFIX_V2:3/binary, 16#50:(?UUID_BYTES*8)>>,
    B2 = <<?BLOCK_BUCKET_PREFIX_V2:3/binary, 16#51:(?UUID_BYTES*8)>>,
    B3 = <<?BLOCK_BUCKET_PREFIX_V2:3/binary, 16#52:(?UUID_BYTES*8)>>,
    os:cmd("rm -rf " ++ TestDir),
    BlockSize = base_ext_size() + 1024,
    {ok, S} = start(-1, [{data_root, TestDir},
                         {block_size, BlockSize},
                         {max_blocks_per_file, 7}]),
    try
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
        ok
    after
        stop(S)
    end.

t3() ->
    TestDir = "./delme",
    Bucket = <<?BLOCK_BUCKET_PREFIX_V0:3/binary, 16#50:(?UUID_BYTES*8)>>,
    K0 = <<0:(?UUID_BYTES*8), 0:?BLOCK_FIELD_SIZE>>,
    BlockSize = 10,                             % Too small!
    V0 = <<42:((4321+BlockSize+1)*8)>>,
    O0 = riak_object:new(Bucket, K0, V0),
    os:cmd("rm -rf " ++ TestDir),
    {ok, S} = start(-1, [{data_root, TestDir},
                         {block_size, BlockSize}]),
    try
        {{error, {invalid_data_size, _, _}, S}, _EncodedVal} =
            put_object(Bucket, K0, [], O0, S),
        ok
    after
        stop(S)
    end.

%% t4() = folding tests

t4() ->
    t4(1, 3, 2, fun lists:reverse/1).

t4(SmallestBlock, BiggestBlock, BlocksPerFile, OrderFun)
  when SmallestBlock >= 0, SmallestBlock =< BiggestBlock ->
    TestDir = "./delme",
    Bucket1Name = <<71:(?UUID_BYTES*8)>>,
    Bucket2Name = <<72:(?UUID_BYTES*8)>>,
    Bucket3Name = <<73:(?UUID_BYTES*8)>>,
    Bucket4Name = <<74:(?UUID_BYTES*8)>>,
    Bucket5Name = <<75:(?UUID_BYTES*8)>>,
    Bucket6Name = <<76:(?UUID_BYTES*8)>>,
    B1 = <<?BLOCK_BUCKET_PREFIX_V1:3/binary, Bucket1Name/binary>>,
    B2 = <<?BLOCK_BUCKET_PREFIX_V1:3/binary, Bucket2Name/binary>>,
    TheTwoBs = [B1, B2],
    BlockSize = 4096,
    os:cmd("rm -rf " ++ TestDir),
    {ok, S} = start(-1, [{data_root, TestDir},
                         {block_size, BlockSize + base_ext_size()},
                         {max_blocks_per_file, BlocksPerFile}]),
    try
        Os = [riak_object:new(<<?BLOCK_BUCKET_PREFIX_V1:3/binary, Bp/binary>>,
                              <<0:(?UUID_BYTES*8), X:?BLOCK_FIELD_SIZE>>,
                              <<X:((BlockSize)*8)>>) ||
                 Bp <- [Bucket1Name, Bucket2Name],
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

        %% Add some new keys.  Then tombstone some of them, and delete some others.
        %% Then fold objects: we see the keys written above should appear PLUS the undeleted
        %%                    objects written below.
        RestBlocks = 5,
        RestStatus =
            [begin
                 RestB = <<?BLOCK_BUCKET_PREFIX_V1:3/binary, Bp/binary>>,
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
                 if Bp == Bucket4Name ->
                         {exists, RestB};
                    Bp == Bucket5Name ->
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
                    Bp == Bucket3Name; Bp == Bucket6Name ->
                         [begin
                              RestK = <<X:(?UUID_BYTES*8),X:?BLOCK_FIELD_SIZE>>,
                              {ok, S} = delete(RestB, RestK, unused, S),
                              {error, not_found, _} = get_object(RestB, RestK, false, S)
                          end || X <- lists:seq(0, RestBlocks)],
                         {deleted, RestB}
                 end
             end || Bp <- [Bucket3Name, Bucket4Name, Bucket5Name, Bucket6Name] ],
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
        ok
    after
        stop(S)
    end.

t5() ->
    TestDir = "./delme-t5",
    NumBuckets = 5,                           % Must be greater than 1
    EndSeq = 7,                               % Must be greater than 2
    BlockSize = 1024,

    os:cmd("rm -rf ++ " ++ TestDir),
    {ok, S} = start(-1, [{data_root, TestDir},
                         {block_size, BlockSize},
                         {max_blocks_per_file, EndSeq - 2}]),
    try
        true = is_empty(S),

        Bs = [<<?BLOCK_BUCKET_PREFIX_V2:3/binary, X:(?UUID_BYTES*8)>> || X <- lists:seq(1, NumBuckets)],
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

        ok
    after
        stop(S)
    end.

t6() ->
    TestDir = "./delme-t6",
    NumBuckets = 5,                           % Must be greater than 1
    EndSeq = 7,                               % Must be greater than 2
    BlockSize = 1024,

    os:cmd("rm -rf ++ " ++ TestDir),
    {ok, S} = start(-1, [{data_root, TestDir},
                         {block_size, BlockSize},
                         {max_blocks_per_file, EndSeq - 2}]),
    try
        true = is_empty(S),

        Bs = [<<?BLOCK_BUCKET_PREFIX_V0:3/binary, X:(?UUID_BYTES*8)>> || X <- lists:seq(1, NumBuckets)],
        Os = [riak_object:new(B, <<UUID:(?UUID_BYTES*8), Seq:?BLOCK_FIELD_SIZE>>,
                              list_to_binary(["val ", integer_to_list(Seq)])) ||
                 B <- Bs,
                 UUID <- [55, 56, 57],
                 Seq <- lists:seq(EndSeq, EndSeq)],
        [{{ok, _}, _} = put_object(riak_object:bucket(RObj),
                                   riak_object:key(RObj),
                                   [], RObj, S) || RObj <- Os],
        false = is_empty(S),
        ok
    after
        stop(S)
    end.

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

%% To make a tiny object that's valid & good enough for AAE to do its job,
%% we create an object that has:
%% 1. an empty MD dict
%% 2. The value of the object is the Riak CS block checksum.

make_fake_tiny_obj(RObj) ->
    Bucket = riak_object:bucket(RObj),
    Key = riak_object:key(RObj),
    FakeBody = find_rcs_bcsum_or_else(riak_object:get_metadata(RObj)),
    VClock = riak_object:vclock(RObj),
    make_fake_tiny_obj(Bucket, Key, FakeBody, VClock).

make_fake_tiny_obj(Bucket, Key, FakeBody, VClock) ->
    riak_object:set_vclock(riak_object:new(Bucket, Key, FakeBody),
                           VClock).

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

find_rcs_bcsum_or_else(MD) ->
    case find_rcs_bcsum(MD) of
        undefined ->
            <<>>;
        Else ->
            Else
    end.

open_db(Dir, OpenOpts) ->
    RetriesLeft = app_helper:get_env(riak_kv, eleveldb_open_retries, 30),
    open_db(Dir, OpenOpts, max(1, RetriesLeft), undefined).

open_db(_Dir, _OpenOpts, 0, LastError) ->
    {error, LastError};
open_db(Dir, OpenOpts, RetriesLeft, _) ->
    case eleveldb:open(Dir, OpenOpts) of
        {ok, _Ref} = OK ->
            OK;
        %% Check specifically for lock error, this can be caused if
        %% a crashed vnode takes some time to flush leveldb information
        %% out to disk.  The process is gone, but the NIF resource cleanup
        %% may not have completed.
        {error, {db_open, OpenErr}=Reason} ->
            case lists:prefix("IO error: lock ", OpenErr) of
                true ->
                    SleepFor = app_helper:get_env(riak_kv, eleveldb_open_retry_delay, 2000),
                    lager:debug("Upper DB retrying ~p in ~p ms after error ~s\n",
                                [Dir, SleepFor, OpenErr]),
                    timer:sleep(SleepFor),
                    open_db(Dir, OpenOpts, RetriesLeft - 1, Reason);
                false ->
                    {error, Reason}
            end;
        {error, Reason} ->
            {error, Reason}
    end.


%% file.erl and filelib.erl replacements
%% filelib.erl code is subject to Ericsson et al. copyright.

%% -spec ef_ensure_dir(Name) -> 'ok' | {'error', Reason} when
%%       Name :: filename() | dirname(),
%%       Reason :: file:posix().
ef_ensure_dir("/") ->
    ok;
ef_ensure_dir(".") ->
    ok;
ef_ensure_dir(F) ->
    Dir = filename:dirname(F),
    case do_is_dir(Dir) of
        true ->
            ok;
        false ->
            ef_ensure_dir(Dir),
            case ef_make_dir(Dir) of
                {error,eexist}=EExist ->
                    case do_is_dir(Dir) of
                        true ->
                            ok;
                        false ->
                            EExist
                    end;
                Err ->
                    Err
            end
    end.

do_is_dir(Dir) ->
    case ef_read_file_info(Dir) of
        {ok, #file_info{type=directory}} ->
            true;
        _ ->
            false
    end.

do_is_file(Dir) ->
    case ef_read_file_info(Dir) of
        {ok, #file_info{type=regular}} ->
            true;
        {ok, #file_info{type=directory}} ->
            true;
        _ ->
            false
    end.

ef_wildcard(Pattern, Cwd) when is_list(Pattern), (is_list(Cwd) or is_binary(Cwd)) ->
    ?HANDLE_ERROR(do_wildcard(Pattern, Cwd)).

do_wildcard(Pattern) when is_list(Pattern) ->
    do_wildcard_comp(do_compile_wildcard(Pattern)).

do_wildcard_comp({compiled_wildcard,{exists,File}}) ->
    case ef_read_file_info(File) of
        {ok,_} -> [File];
        _ -> []
    end;
do_wildcard_comp({compiled_wildcard,[Base|Rest]}) ->
    do_wildcard_1([Base], Rest).

do_wildcard(Pattern, Cwd) when is_list(Pattern), (is_list(Cwd) or is_binary(Cwd)) ->
    do_wildcard_comp(do_compile_wildcard(Pattern), Cwd).

do_wildcard_comp({compiled_wildcard,{exists,File}}, Cwd) ->
    case ef_read_file_info(filename:absname(File, Cwd)) of
        {ok,_} -> [File];
        _ -> []
    end;
do_wildcard_comp({compiled_wildcard,[current|Rest]}, Cwd0) ->
    {Cwd,PrefixLen} = case filename:join([Cwd0]) of
              Bin when is_binary(Bin) -> {Bin,byte_size(Bin)+1};
              Other -> {Other,length(Other)+1}
          end,          %Slash away redundant slashes.
    [
     if
         is_binary(N) ->
             <<_:PrefixLen/binary,Res/binary>> = N,
             Res;
         true ->
             lists:nthtail(PrefixLen, N)
     end || N <- do_wildcard_1([Cwd], Rest)];
do_wildcard_comp({compiled_wildcard,[Base|Rest]}, _Cwd) ->
    do_wildcard_1([Base], Rest).

do_wildcard_1(Files, Pattern) ->
    do_wildcard_2(Files, Pattern, []).

do_wildcard_2([File|Rest], Pattern, Result) ->
    do_wildcard_2(Rest, Pattern, do_wildcard_3(File, Pattern, Result));
do_wildcard_2([], _, Result) ->
    Result.

do_wildcard_3(Base, [Pattern|Rest], Result) ->
    case ef_list_dir(Base) of
        {ok, Files0} ->
            Files = lists:sort(Files0),
            Matches = wildcard_4(Pattern, Files, Base, []),
            do_wildcard_2(Matches, Rest, Result);
        _ ->
            Result
    end;
do_wildcard_3(Base, [], Result) ->
    [Base|Result].

wildcard_4(Pattern, [File|Rest], Base, Result) when is_binary(File) ->
    case wildcard_5(Pattern, binary_to_list(File)) of
        true ->
            wildcard_4(Pattern, Rest, Base, [join(Base, File)|Result]);
        false ->
            wildcard_4(Pattern, Rest, Base, Result)
    end;
wildcard_4(Pattern, [File|Rest], Base, Result) ->
    case wildcard_5(Pattern, File) of
        true ->
            wildcard_4(Pattern, Rest, Base, [join(Base, File)|Result]);
        false ->
            wildcard_4(Pattern, Rest, Base, Result)
    end;
wildcard_4(_Patt, [], _Base, Result) ->
    Result.

wildcard_5([question|Rest1], [_|Rest2]) ->
    wildcard_5(Rest1, Rest2);
wildcard_5([accept], _) ->
    true;
wildcard_5([star|Rest], File) ->
    do_star(Rest, File);
wildcard_5([{one_of, Ordset}|Rest], [C|File]) ->
    case ordsets:is_element(C, Ordset) of
        true  -> wildcard_5(Rest, File);
        false -> false
    end;
wildcard_5([{alt, Alts}], File) ->
    do_alt(Alts, File);
wildcard_5([C|Rest1], [C|Rest2]) when is_integer(C) ->
    wildcard_5(Rest1, Rest2);
wildcard_5([X|_], [Y|_]) when is_integer(X), is_integer(Y) ->
    false;
wildcard_5([], []) ->
    true;
wildcard_5([], [_|_]) ->
    false;
wildcard_5([_|_], []) ->
    false.

do_compile_wildcard(Pattern) ->
    {compiled_wildcard,compile_wildcard_1(Pattern)}.

compile_wildcard_1(Pattern) ->
    [Root|Rest] = filename:split(Pattern),
    case filename:pathtype(Root) of
        relative ->
            compile_wildcard_2([Root|Rest], current);
        _ ->
            compile_wildcard_2(Rest, [Root])
    end.

compile_wildcard_2([Part|Rest], Root) ->
    case compile_part(Part) of
        Part ->
            compile_wildcard_2(Rest, join(Root, Part));
        Pattern ->
            compile_wildcard_3(Rest, [Pattern,Root])
    end;
compile_wildcard_2([], Root) -> {exists,Root}.

compile_wildcard_3([Part|Rest], Result) ->
    compile_wildcard_3(Rest, [compile_part(Part)|Result]);
compile_wildcard_3([], Result) ->
    lists:reverse(Result).

compile_part(Part) ->
    compile_part(Part, false, []).

compile_part_to_sep(Part) ->
    compile_part(Part, true, []).

compile_part([], true, _) ->
    error(missing_delimiter);
compile_part([$,|Rest], true, Result) ->
    {ok, $,, lists:reverse(Result), Rest};
compile_part([$}|Rest], true, Result) ->
    {ok, $}, lists:reverse(Result), Rest};
compile_part([$?|Rest], Upto, Result) ->
    compile_part(Rest, Upto, [question|Result]);
compile_part([$*], Upto, Result) ->
    compile_part([], Upto, [accept|Result]);
compile_part([$*|Rest], Upto, Result) ->
    compile_part(Rest, Upto, [star|Result]);
compile_part([$[|Rest], Upto, Result) ->
    case compile_charset(Rest, ordsets:new()) of
        {ok, Charset, Rest1} ->
            compile_part(Rest1, Upto, [Charset|Result]);
        error ->
            compile_part(Rest, Upto, [$[|Result])
    end;
compile_part([${|Rest], Upto, Result) ->
    case compile_alt(Rest) of
        {ok, Alt} ->
            lists:reverse(Result, [Alt]);
        error ->
            compile_part(Rest, Upto, [${|Result])
    end;
compile_part([X|Rest], Upto, Result) ->
    compile_part(Rest, Upto, [X|Result]);
compile_part([], _Upto, Result) ->
    lists:reverse(Result).

compile_charset([$]|Rest], Ordset) ->
    compile_charset1(Rest, ordsets:add_element($], Ordset));
compile_charset([$-|Rest], Ordset) ->
    compile_charset1(Rest, ordsets:add_element($-, Ordset));
compile_charset([], _Ordset) ->
    error;
compile_charset(List, Ordset) ->
    compile_charset1(List, Ordset).

compile_charset1([Lower, $-, Upper|Rest], Ordset) when Lower =< Upper ->
    compile_charset1(Rest, compile_range(Lower, Upper, Ordset));
compile_charset1([$]|Rest], Ordset) ->
    {ok, {one_of, Ordset}, Rest};
compile_charset1([X|Rest], Ordset) ->
    compile_charset1(Rest, ordsets:add_element(X, Ordset));
compile_charset1([], _Ordset) ->
    error.

compile_range(Lower, Current, Ordset) when Lower =< Current ->
    compile_range(Lower, Current-1, ordsets:add_element(Current, Ordset));
compile_range(_, _, Ordset) ->
    Ordset.

compile_alt(Pattern) ->
    compile_alt(Pattern, []).

compile_alt(Pattern, Result) ->
    case compile_part_to_sep(Pattern) of
        {ok, $,, AltPattern, Rest} ->
            compile_alt(Rest, [AltPattern|Result]);
        {ok, $}, AltPattern, Rest} ->
            NewResult = [AltPattern|Result],
            RestPattern = compile_part(Rest),
            {ok, {alt, [Alt++RestPattern || Alt <- NewResult]}};
        Pattern ->
            error
    end.

join(current, File) -> File;
join(Base, File) -> filename:join(Base, File).

do_star(Pattern, [X|Rest]) ->
    case wildcard_5(Pattern, [X|Rest]) of
        true  -> true;
        false -> do_star(Pattern, Rest)
    end;
do_star(Pattern, []) ->
    wildcard_5(Pattern, []).

do_alt([Alt|Rest], File) ->
    case wildcard_5(Alt, File) of
        true  -> true;
        false -> do_alt(Rest, File)
    end;
do_alt([], _File) ->
    false.

ef_change_mode(Path, NewMode) ->
    do_file_op(write_file_info, Path, #file_info{mode=NewMode}).

ef_delete(File) ->
    do_file_op(delete, File).

ef_list_dir(File) ->
    do_file_op(list_dir, File).

ef_make_dir(Dir) ->
    do_file_op(make_dir, Dir).

ef_read_file_info(File) ->
    do_file_op(read_file_info, File).

ef_rename(Old, New) ->
    do_file_op(rename, Old, New).

do_file_op(FunName, Param1) ->
    Port = get_efile_port(),
    prim_file:FunName(Port, Param1).

do_file_op(FunName, Param1, Param2) ->
    Port = get_efile_port(),
    prim_file:FunName(Port, Param1, Param2).

get_efile_port() ->
    Key = fs2_efile_port,
    case get(Key) of
        undefined ->
            case prim_file_drv_open(efile, [binary]) of
                {ok, Port} ->
                    put(Key, Port),
                    get_efile_port();
                Err ->
                    error_logger:error_msg("get_efile_port: ~p\n", [Err]),
                    timer:sleep(1000),
                    get_efile_port()
            end;
        Port ->
            Port
    end.

prim_file_drv_open(Driver, Portopts) ->
    try erlang:open_port({spawn, Driver}, Portopts) of
        Port ->
            {ok, Port}
    catch
        error:Reason ->
            {error, Reason}
    end.

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


t6_test() ->
    case riak_object_available() of
        true ->
            ok = t6();
        false ->
            io:format(user, "SKIP: riak_object module is not available\n", [])
    end.


%% Callbacks for backend_eqc test.

eqc_filter_delete(Bucket, Key, BKVs) ->
    try
        Extract = fun(B, K) ->
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

    os:cmd("rm -rf ++ " ++ Dir),
    Base = fun(A, B, C, _D) ->
               try
                   ok = create_or_sanity_check_version_file(
                          Dir, BlockSize+A, MaxBlocks+B, BDepth+C)
               catch _:_ ->
                       bad
               end
           end,
    try
        ok  = Base(0, 0, 0, 0),
        bad = Base(1, 0, 0, 0),
        bad = Base(0, 1, 0, 0),
        %% bad = Base(0, 0, 1, 0),
        %% bad = Base(0, 0, 0, 1),
        ok
    after
        os:cmd("rm -rf ++ " ++ Dir)
    end.

file_replacement_test() ->
    Prefix0 = "file_replacement_test",
    os:cmd("rm -rf ./" ++ Prefix0 ++ "*"),
    Prefix = Prefix0 ++ "/",
    ok = ef_make_dir(Prefix),

    ok = ef_make_dir(Prefix ++ "dir1"),
    ok = ef_ensure_dir(Prefix ++ "dir2/dir2b/dir2c/foo"),
    {ok, _} = ef_read_file_info(Prefix ++ "dir1"),
    {ok, _} = ef_read_file_info(Prefix ++ "dir2"),
    {ok, _} = ef_read_file_info(Prefix ++ "dir2/dir2b"),
    {ok, _} = ef_read_file_info(Prefix ++ "dir2/dir2b/dir2c"),
    {error, enoent} = ef_read_file_info(Prefix ++ "dir2/dir2b/dir2c/X"),
    P1 = Prefix ++ "dir1",
    P2 = Prefix ++ "dir2",
    [P1, P2] = ef_wildcard(Prefix ++ "*", "."),
    ["dir1", "dir2"] = ef_wildcard("*", Prefix),

    Content = <<"yo">>,
    file:write_file(Prefix ++ "dir1/file1", Content),
    {error, eexist} = ef_make_dir(Prefix ++ "dir1/file1"),
    {ok, FI1 = #file_info{}} = ef_read_file_info(Prefix ++ "dir1/file1"),
    OldMode = FI1#file_info.mode band 8#777,
    ok = ef_rename(Prefix ++ "dir1/file1", Prefix ++ "dir1/file2"),
    {error, enoent} = ef_rename(Prefix ++ "dir1/file1", Prefix ++ "dir1/file2"),
    {ok, Content} = file:read_file(Prefix ++ "dir1/file2"),

    NewMode = 8#333,
    ok = ef_change_mode(Prefix ++ "dir1/file2", NewMode),
    {ok, FI2 = #file_info{}} = ef_read_file_info(Prefix ++ "dir1/file2"),
    true = (OldMode /= NewMode),
    NewMode = FI2#file_info.mode band 8#777,

    ok.

-ifdef(EQC).

backend_eqc_fold_objects_transform(RObjs) ->
    [{BKey, riak_object:get_value(RObj)} || {BKey, RObj} <- RObjs].

backend_eqc_filter_orddict_on_delete(
                      <<Prefix:3/binary, _/binary>> = DBucket,
                      <<DUUID:?UUID_BYTES/binary,
                        DBlockNum:?BLOCK_FIELD_SIZE>> = _DKey,
                      Dict, Config)
  when Prefix == ?BLOCK_BUCKET_PREFIX_V0 ->
    %% backend_eqc deleted a key that uses our ?BLOCK_BUCKET_PREFIX_V0 + UUID
    %% scheme.  Filter out all other blocks that reside in the same file.

    {_ConfigRoot, BlockSize, MaxBlocks, _IDepth} =
        parse_config_and_env(Config),
    State = #state{dir = "does/not/matter",
                   block_size = BlockSize,
                   max_blocks = MaxBlocks,
                   i_depth = 3},
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
    ?LET(X, frequency([{10, 99000},
                       { 4, 99001},
                       { 1, 99002}]),
         <<?BLOCK_BUCKET_PREFIX_V0:3/binary, X:(?UUID_BYTES*8)>>).

backend_eqc_key() ->
    UUID = 42,
    BigBlockNum = 77239823, % Arbitrary, but bigger than any real
                            % max_blocks_per_file value
    ?LET(X, frequency([{10, 99000},
                       { 5, 99001},
                       { 3, BigBlockNum}]),
         <<UUID:(?UUID_BYTES*8), X:?BLOCK_FIELD_SIZE>>).

backend_eqc_postcondition_fold_keys(Expected, Result) ->
    case sets:is_subset(sets:from_list(Expected),
                        sets:from_list(Result)) of
        true ->
            true;
        false ->
            [{mod, ?MODULE}, {line, ?LINE},
             {expected, Expected}, {result, Result}]
    end.

backend_eunit_bucket(Suffix) ->
    <<?BLOCK_BUCKET_PREFIX_V1:3/binary, Suffix:(?UUID_BYTES*8)>>.

backend_eunit_key(Suffix) ->
    <<4243:(?UUID_BYTES*8), Suffix:?BLOCK_FIELD_SIZE>>.

basic_props_eqc() ->
    basic_props().

basic_props() ->
    [{data_root,  "test/fs-backend"}, {block_size, 1024}].

eqc_t4_wrapper(TestTime) ->
    eqc:quickcheck(eqc:testing_time(TestTime, prop_t4())).

-ifdef(TEST_FS2_BACKEND_IN_RIAK_KV).
eqc_test_() ->
    TestTime1 = 30,
    EQC_prop0 = backend_eqc:property(?MODULE, false, basic_props_eqc()),
    EQC_prop1 = eqc_statem:more_commands(75, EQC_prop0),
    EQC_prop2 = eqc:testing_time(TestTime1, EQC_prop1),
    TestTime2 = TestTime3 = 10,
    {spawn,
     [{inorder,
       [{setup,
         fun setup/0,
         fun cleanup/1,
         [
          %% ?_assertEqual(?BLOCK_BUCKET_PREFIX_V0,
          %%               backend_eqc:bucket_prefix_1()),
          %% ?_assertEqual(?UUID_BYTES,
          %%               backend_eqc:key_prefix_1()),
          %% ?_assertEqual(?BLOCK_FIELD_SIZE,
          %%               backend_eqc:key_suffix_1()),
          {timeout, 2 * TestTime1,
           {"EQC_prop2", [?_assertEqual(true, eqc:quickcheck(EQC_prop2))]}},
          {timeout, 2 * TestTime2,
           {"eqc_nest_tester", [?_assertEqual(true, eqc_nest_tester(TestTime2))]}},
          {timeout, 2 * TestTime3,
           {"eqc_t4_wrapper", [?_assertEqual(true, eqc_t4_wrapper(TestTime3))]}}
         ]}]}]}.
-endif. % TEST_FS2_BACKEND_IN_RIAK_KV

eqc_nest_tester(TestTime) ->
    setup(),
    os:cmd("mkdir -p test/fs-backend"),
    rm_rf_test_dir_contents(),
    X = eqc:quickcheck(eqc:testing_time(TestTime, prop_nest_ordered())),
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
   ?IMPLIES(length(BucketList) > 1,
            begin

                [ok = insert_sample_key(Bucket, S) || Bucket <- BucketList],
                {ok, Bs} = ?MODULE:fold_buckets(fun(B, Acc) -> [B|Acc] end, [],
                                                [], S),
                ?MODULE:stop(S),
                %% rm_rf_test_dir_contents(),
                case lists:usort(BucketList) == lists:reverse(Bs) of
                    true -> true;
                    _ -> {wanted, lists:usort(BucketList),
                          got, lists:reverse(Bs)}
                end
            end)
            end)).

gen_bucket() ->
    ?LET({BPrefix, Bucket},
         {oneof([?BLOCK_BUCKET_PREFIX_V0, ?BLOCK_BUCKET_PREFIX_V1, ?BLOCK_BUCKET_PREFIX_V2]),
          choose(0, (1 bsl 32) - 1)},
         <<BPrefix/binary, Bucket:(?UUID_BYTES*8)>>).

all_names_at_least_as_long_as(Names, Len) ->
    lists:all(fun(N) -> byte_size(N) >= Len end, Names).

insert_sample_key(Bucket, S) ->
    Key = <<55:(?UUID_BYTES*8), 66:?BLOCK_FIELD_SIZE>>,
    O = riak_object:new(Bucket, Key, <<"val">>),
    {{ok, _}, _} = ?MODULE:put_object(Bucket, Key, [], O, S),
    ok.

-endif. % EQC
-endif. % TEST
