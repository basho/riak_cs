%% -------------------------------------------------------------------
%%
%% riak_fs2_backend: storage engine based on basic filesystem access
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
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
%% -------------------------------------------------------------------
%%
%% TODO list for the multiple-chunk-in-single-file scheme:
%%
%% __ Test-first: create EQC model, test it ruthlessly
%%    __ Include all ops, including folding!
%%
%% __ Make certain that max chunk size is strictly enforced.
%% __ Add unit test to make certain that the backend gives graceful
%%    error when chunk size limit is exceeded.
%%
%% __ When using with RCS, add a programmatic check for the LFS block size.
%%    Then add extra cushion (e.g. 512 bytes??) and use that for block size?
%%
%% __ Create & write a real header block on first data chunk write
%%    __ Add space in the header to track deleted chunks.
%%
%% __ Check the header block on all chunk read ops?  (or at least 0th chunk?)
%%
%% __ Implement the delete op:
%%    __ Update the deleted chunk map:
%%        if undeleted chunks, write new map
%%        if all chunks are deleted, delete file
%%    HRRRMMMMM, this kind of update-in-place is by definition not append-only.
%%    WHAT IF?  1. A fixed size deleted trailer block were appended
%%                 at the offset of the Max+1 block?
%%                 A stat(2) call can tell us if any blocks have been
%%                 deleted yet.  (Or if pread(2) returns EOF)
%%              2. If trailer is present, check last trailer for latest map.
%%
%% __ Read cache: if reading 0th chunk, create read-ahead that will
%%    read entire file async'ly and send it as a message to be recieved
%%    later.  When reading 1st chunk, wait for arrival of read-ahead block.
%%    __ Needs a real cache though.  Steal the cache from the memory backend?
%%       Use the entire riak_memory_backend as-is??  Except that using ETS
%%       is going to copy binaries, and we don't want those shared off-heap
%%       binaries copied.  Hrm....

% @doc riak_kv_fs2_backend is filesystem storage system, Mark III

-module(riak_kv_fs2_backend).
%% -behavior(riak_kv_backend). % Not building within riak_kv

%% KV Backend API
-export([api_version/0,
         capabilities/1,
         capabilities/2,
         start/2,
         stop/1,
         get/3,
         put/5,
         delete/4,
         drop/1,
         fold_buckets/4,
         fold_keys/4,
         fold_objects/4,
         is_empty/1,
         status/1,
         callback/3]).
-export([t0/0, t1/0]).

-include("riak_moss_lfs.hrl").
-include_lib("kernel/include/file.hrl").

-define(API_VERSION, 1).
-define(CAPABILITIES, [async_fold, write_once_keys]).

-define(TEST_IN_RIAK_KV, true).

%% Borrowed from bitcask.hrl
-define(VALSIZEFIELD, 32).
-define(CRCSIZEFIELD, 32).
-define(HEADER_SIZE,  8). % Differs from bitcask.hrl!
-define(MAXVALSIZE, 2#11111111111111111111111111111111).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {
          dir        :: string(),
          block_size :: integer()
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
capabilities(_) ->
    {ok, ?CAPABILITIES}.

%% @doc Return the capabilities of the backend.
-spec capabilities(riak_object:bucket(), state()) -> {ok, [atom()]}.
capabilities(_, _) ->
    {ok, ?CAPABILITIES}.

%% @doc Start this backend.  'riak_kv_fs_backend_root' must be set in
%%      Riak's application environment.  It must be set to a string
%%      representing the base directory where this backend should
%%      store its files.
-spec start(integer(), config()) -> {ok, state()}.
start(Partition, Config) ->
    PartitionName = integer_to_list(Partition),
    try
        ConfigRoot = case get_prop_or_env(
                            fs2_backend_data_root, Config, riak_kv) of
                         undefined ->
                             throw("fs2_backend_data_root unset, failing");
                         Else1 ->
                             Else1
                     end,
        BlockSize = case get_prop_or_env(
                           fs2_backend_block_size, Config, riak_kv) of
                        undefined ->
                            throw("fs2_backend_block_size unset, failing");
                        Else2 ->
                            Else2
                    end,
        Dir = filename:join([ConfigRoot,PartitionName]),
        {filelib:ensure_dir(Dir), #state{dir = Dir, block_size = BlockSize}}
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
            {ok, Bin, State};
        Reason when is_atom(Reason) ->
            {error, Reason, State}
    end;
get(Bucket, Key, State) ->
    File = location(State, {Bucket, Key}),
    case filelib:is_file(File) of
        false ->
            {error, not_found, State};
        true ->
            {ok, Bin} = file:read_file(File),
            case unpack_ondisk(Bin) of
                bad_crc ->
                    %% TODO logging?
                    {error, not_found, State};
                Val ->
                    {ok, Val, State}
            end
    end.

%% @doc Store Val under Bkey
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.
put(<<?BLOCK_BUCKET_PREFIX, _/binary>> = Bucket,
    <<UUID:?UUID_BYTES/binary, BlockNum:?BLOCK_FIELD_SIZE>>,
    _IndexSpecs, Val, #state{block_size = BlockSize} = State)
  when size(Val) =< BlockSize ->                % TODO ZZZ FIXME do not fall through!
    File = location(State, {Bucket, UUID}),
    Offset = calc_block_offset(BlockNum, State),
    try
        ok = filelib:ensure_dir(File),
        {ok, FH} = file:open(File, [write, read, raw, binary]),
        try
            PackedBin = pack_ondisk(Val),
            ok = file:pwrite(FH, Offset, PackedBin),
            {ok, State}
        catch _X:_Y ->
                io:format("DBG line ~p err ~p ~p\n", [?LINE, _X, _Y]),
                {error, eBummer, State}
        after
            file:close(FH)
        end
    catch _A:_B ->
            io:format("DBG line ~p err ~p ~p\n", [?LINE, _A, _B]),
            {error, not_found, State}
    end;
put(Bucket, PrimaryKey, _IndexSpecs, Val, State) ->
    File = location(State, {Bucket, PrimaryKey}),
    case filelib:ensure_dir(File) of
        ok         -> {atomic_write(File, Val), State};
        {error, X} -> {error, X, State}
    end.

%% @doc Delete the object stored at BKey
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(Bucket, Key, _IndexSpecs, State) ->
    File = location(State, {Bucket, Key}),
    case file:delete(File) of
        ok -> {ok, State};
        {error, enoent} -> {ok, State};
        {error, Err} -> {error, Err, State}
    end.

%% @doc Fold over all the buckets.
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(),
                   any(),
                   [],
                   state()) -> {ok, any()} | {async, fun()}.
fold_buckets(FoldBucketsFun, Acc, Opts, State) ->
    FoldFun = fold_buckets_fun(FoldBucketsFun),
    BucketFolder =
        fun() ->
                {FoldResult, _} =
                    lists:foldl(FoldFun, {Acc, sets:new()}, list_all_keys(State)),
                FoldResult
        end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, BucketFolder};
        false ->
            {ok, BucketFolder()}
    end.

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()} | {async, fun()}.
fold_keys(FoldKeysFun, Acc, Opts, State) ->
    Bucket =  proplists:get_value(bucket, Opts),
    FoldFun = fold_keys_fun(FoldKeysFun, Bucket),
    KeyFolder =
        fun() ->
                lists:foldl(FoldFun, Acc, list_all_keys(State))
        end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, KeyFolder};
        false ->
            {ok, KeyFolder()}
    end.

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {async, fun()}.
fold_objects(FoldObjectsFun, Acc, Opts, State) ->
    %% Warning: This ain't pretty. Hold your nose.
    Bucket =  proplists:get_value(bucket, Opts),
    FoldFun = fold_objects_fun(FoldObjectsFun, Bucket),
    ObjectFolder =
        fun() ->
                fold(State, FoldFun, Acc)
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
    _ = [file:delete(location(State, BK)) || BK <- list_all_keys(State)],
    Cmd = io_lib:format("rm -Rf ~s", [Dir]),
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
    [no_status_sorry].

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
atomic_write(File, Val) ->
    FakeFile = File ++ ".tmpwrite",
    case file:write_file(FakeFile, pack_ondisk(Val)) of
        ok ->
            file:rename(FakeFile, File);
        X -> X
    end.

%% @private
%% Fold over the keys and objects on this backend
fold(State, Fun0, Acc) ->
    Fun = fun(BKey, AccIn) ->
                  {Bucket, Key} = BKey,
                  case get(Bucket, Key, State) of
                      {ok, Bin, _} ->
                          Fun0(BKey, Bin, AccIn);
                      _ ->
                          AccIn
                  end
          end,
    lists:foldl(Fun, Acc, list_all_keys(State)).

%% @private
%% Return a function to fold over the buckets on this backend
fold_buckets_fun(FoldBucketsFun) ->
    fun(BKey, {Acc, BucketSet}) ->
            {Bucket, _} = BKey,
            case sets:is_element(Bucket, BucketSet) of
                true ->
                    {Acc, BucketSet};
                false ->
                    {FoldBucketsFun(Bucket, Acc),
                     sets:add_element(Bucket, BucketSet)}
            end
    end.

%% @private
%% Return a function to fold over keys on this backend
fold_keys_fun(FoldKeysFun, undefined) ->
    fun(BKey, Acc) ->
            {Bucket, Key} = BKey,
            FoldKeysFun(Bucket, Key, Acc)
    end;
fold_keys_fun(FoldKeysFun, Bucket) ->
    fun(BKey, Acc) ->
            {B, Key} = BKey,
            case B =:= Bucket of
                true ->
                    FoldKeysFun(Bucket, Key, Acc);
                false ->
                    Acc
            end
    end.

%% @private
%% Return a function to fold over the objects on this backend
fold_objects_fun(FoldObjectsFun, undefined) ->
    fun(BKey, Value, Acc) ->
            {Bucket, Key} = BKey,
            FoldObjectsFun(Bucket, Key, Value, Acc)
    end;
fold_objects_fun(FoldObjectsFun, Bucket) ->
    fun(BKey, Value, Acc) ->
            {B, Key} = BKey,
            case B =:= Bucket of
                true ->
                    FoldObjectsFun(Bucket, Key, Value, Acc);
                false ->
                    Acc
            end
    end.

%% @spec list_all_files_naive_bkeys(state()) -> [{Bucket :: riak_object:bucket(),
%%                                    Key :: riak_object:key()}]
%% @doc Get a list of all bucket/key pairs stored by this backend
list_all_files_naive_bkeys(#state{dir=Dir}) ->
    % this is slow slow slow
    %                                              B,N,N,N,K
    [location_to_bkey(X) || X <- filelib:wildcard("*/*/*/*/*",
                                                         Dir)].

%% @spec list_all_keys([string()]) -> [{Bucket :: riak_object:bucket(),
%%                                      Key :: riak_object:key()}]
%% @doc Get a list of all bucket/key pairs stored by this backend
list_all_keys(State) ->
    L = lists:foldl(fun({<<?BLOCK_BUCKET_PREFIX, _/binary>> = Bucket,
                         <<UUID:?UUID_BYTES/binary>> = Key}, Acc) ->
                            Chunks = enumerate_chunks_in_file(Bucket, Key,
                                                              UUID, State),
                            ChunkBKeys = [{Bucket, <<UUID:?UUID_BYTES/binary,
                                                     C:?BLOCK_FIELD_SIZE>>} ||
                                             C <- Chunks],
                            lists:reverse(ChunkBKeys, Acc);
                       (BKey, Acc) ->
                            [BKey|Acc]
                    end, [], list_all_files_naive_bkeys(State)),
    lists:reverse(L).

%% @spec location(state(), {riak_object:bucket(), riak_object:key()})
%%          -> string()
%% @doc produce the file-path at which the object for the given Bucket
%%      and Key should be stored
location(State, {Bucket, Key}) ->
    B64 = encode_bucket(Bucket),
    K64 = encode_key(Key),
    [N1,N2,N3] = nest(K64),
    filename:join([State#state.dir, B64, N1, N2, N3, K64]).

%% @spec location_to_bkey(string()) ->
%%           {riak_object:bucket(), riak_object:key()}
%% @doc reconstruct a Riak bucket/key pair, given the location at
%%      which its object is stored on-disk
location_to_bkey(Path) ->
    [B64,_,_,_,K64] = string:tokens(Path, "/"),
    {decode_bucket(B64), decode_key(K64)}.

%% @spec decode_bucket(string()) -> binary()
%% @doc reconstruct a Riak bucket, given a filename
%% @see encode_bucket/1
decode_bucket(B64) ->
    base64:decode(dirty(B64)).

%% @spec decode_key(string()) -> binary()
%% @doc reconstruct a Riak object key, given a filename
%% @see encode_key/1
decode_key(K64) ->
    base64:decode(dirty(K64)).

%% @spec dirty(string()) -> string()
%% @doc replace filename-troublesome base64 characters
%% @see clean/1
dirty(Str64) ->
    lists:map(fun($-) -> $=;
                 ($_) -> $+;
                 ($,) -> $/;
                 (C)  -> C
              end,
              Str64).

%% @spec encode_bucket(binary()) -> string()
%% @doc make a filename out of a Riak bucket
encode_bucket(Bucket) ->
    clean(base64:encode_to_string(Bucket)).

%% @spec encode_key(binary()) -> string()
%% @doc make a filename out of a Riak object key
encode_key(Key) ->
    clean(base64:encode_to_string(Key)).

%% @spec clean(string()) -> string()
%% @doc remove characters from base64 encoding, which may
%%      cause trouble with filenames
clean(Str64) ->
    lists:map(fun($=) -> $-;
                 ($+) -> $_;
                 ($/) -> $,;
                 (C)  -> C
              end,
              Str64).

%% @spec nest(string()) -> [string()]
%% @doc create a directory nesting, to keep the number of
%%      files in a directory smaller
nest(Key) -> nest(lists:reverse(string:substr(Key, 1, 6)), 3, []).
nest(_, 0, Parts) -> Parts;
nest([Nb,Na|Rest],N,Acc) ->
    nest(Rest, N-1, [[Na,Nb]|Acc]);
nest([Na],N,Acc) ->
    nest([],N-1,[[Na]|Acc]);
nest([],N,Acc) ->
    nest([],N-1,["0"|Acc]).

%% Borrowed from bitcask_fileops.erl and then mangled
-spec pack_ondisk(binary()) -> [binary()].
pack_ondisk(Bin) ->
    ValueSz = size(Bin),
    true = (ValueSz =< ?MAXVALSIZE),
    Bytes0 = [<<ValueSz:?VALSIZEFIELD>>, Bin],
    [<<(erlang:crc32(Bytes0)):?CRCSIZEFIELD>> | Bytes0].

-spec unpack_ondisk(binary()) -> binary() | bad_crc.
unpack_ondisk(<<Crc32:?CRCSIZEFIELD/unsigned,
                ValueSz:?VALSIZEFIELD, Rest/binary>>)
  when size(Rest) >= ValueSz ->
    try
        <<Value:ValueSz/binary, _Tail/binary>> = Rest,
        Crc32 = erlang:crc32([<<ValueSz:?VALSIZEFIELD>>, Value]),
        Value
    catch _:_ ->
            bad_crc
    end;
unpack_ondisk(_) ->
    bad_crc.

calc_block_offset(BlockNum, #state{block_size = BlockSize}) ->
    %% MOSS block numbers start at zero.
    (BlockNum rem ?CONTIGUOUS_BLOCKS) * (?HEADER_SIZE + BlockSize).

calc_max_block(FileSize, #state{block_size = BlockSize}) ->
    FileSize div (?HEADER_SIZE + BlockSize).

read_block(Bucket, UUID, BlockNum, #state{block_size = BlockSize} = State) ->
    File = location(State, {Bucket, UUID}),
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
                io:format("DBG line ~p err ~p ~p\n", [?LINE, _X, _Y]),
                not_found
        after
            file:close(FH)
        end
    catch _:_ ->
            not_found
    end.

enumerate_chunks_in_file(Bucket, _Key, UUID, State) ->
    Path = location(State, {Bucket, UUID}),
    case file:read_file_info(Path) of
        {error, _} ->
            [];
        {ok, FI} ->
            MaxBlock = calc_max_block(FI#file_info.size, State),
            [BlockNum || BlockNum <- lists:seq(0, MaxBlock),
                         is_binary(read_block(Bucket, UUID, BlockNum, State))]
    end.

get_prop_or_env(Key, Properties, App) ->
    case proplists:get_value(Key, Properties) of
        undefined ->
            application:get_env(App, Key);
        Value ->
            Value
    end.

t0() ->
    TestDir = "./delme",
    Bucket = <<?BLOCK_BUCKET_PREFIX, "delme">>,
    K0 = <<0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0>>,
    V0 = <<42:64>>,
    K1 = <<0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1>>,
    V1 = <<43:64>>,
    os:cmd("rm -rf " ++ TestDir),
    {ok, S} = start(-1, [{fs2_backend_data_root, TestDir},
                         {fs2_backend_block_size, 8}]),
    {ok, S} = put(Bucket, K0, [], V0, S),
    {ok, S} = put(Bucket, K1, [], V1, S),
    {ok, V0, S} = get(Bucket, K0, S),
    {ok, V1, S} = get(Bucket, K1, S),
    ok.

t1() ->
    TestDir = "./delme",
    Bucket = <<?BLOCK_BUCKET_PREFIX, "delme">>,
    K0 = <<0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0>>,
    V0 = <<100:64>>,
    K1 = <<0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1>>,
    V1 = <<101:64>>,
    os:cmd("rm -rf " ++ TestDir),
    {ok, S} = start(-1, [{fs2_backend_data_root, TestDir},
                         {fs2_backend_block_size, 1024}]),
    %% {ok, S1} = drop(S),
    %% {ok, S2} = drop(S1),
    %% {ok, S3} = drop(S2),
    {ok, S4} = put(Bucket, K0, [], V0, S),
    {ok, S5} = put(Bucket, K1, [], V1, S4),
    {ok, V0, _} = get(Bucket, K0, S5),
    {ok, V1, _} = get(Bucket, K1, S5),
    {ok, X} = fold_objects(fun(B, K, V, Acc) ->
                                   [{{B, K}, V}|Acc]
                           end, [], [], S5),
    [{{Bucket, K0}, V0}, {{Bucket, K1}, V1}] = lists:reverse(X),
    ok.

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

t0_test() ->
    t0().

-ifdef(TEST_IN_RIAK_KV).

%% Broken test:
simple_test_() ->
   ?assertCmd("rm -rf test/fs-backend"),
   Config = [{fs2_backend_data_root, "test/fs-backend"},
             {fs2_backend_block_size, 8}],
   riak_kv_backend:standard_test(?MODULE, Config).

dirty_clean_test() ->
    Dirty = "abc=+/def",
    Clean = clean(Dirty),
    [ ?assertNot(lists:member(C, Clean)) || C <- "=+/" ],
    ?assertEqual(Dirty, dirty(Clean)).

nest_test() ->
    ?assertEqual(["ab","cd","ef"],nest("abcdefg")),
    ?assertEqual(["ab","cd","ef"],nest("abcdef")),
    ?assertEqual(["a","bc","de"], nest("abcde")),
    ?assertEqual(["0","ab","cd"], nest("abcd")),
    ?assertEqual(["0","a","bc"],  nest("abc")),
    ?assertEqual(["0","0","ab"],  nest("ab")),
    ?assertEqual(["0","0","a"],   nest("a")),
    ?assertEqual(["0","0","0"],   nest([])).

-ifdef(EQC).

eqc_test_() ->
    {spawn,
     [{inorder,
       [{setup,
         fun setup/0,
         fun cleanup/1,
         [
          ?_assertEqual(?BLOCK_BUCKET_PREFIX,
                        backend_eqc:bucket_prefix_1()),
          ?_assertEqual(?UUID_BYTES,
                        backend_eqc:key_prefix_1()),
          ?_assertEqual(?BLOCK_FIELD_SIZE,
                        backend_eqc:key_suffix_1()),
          {timeout, 60000,
           [?_assertEqual(true,
                          backend_eqc:test(?MODULE,
                                           false,
                                           [{fs2_backend_data_root,
                                             "test/fs-backend"},
                                           {fs2_backend_block_size, 1024}]))]}
         ]}]}]}.

setup() ->
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger,
                        {file, "riak_kv_fs2_backend_eqc_sasl.log"}),
    error_logger:tty(false),
    error_logger:logfile({open, "riak_kv_fs2_backend_eqc.log"}),
    ok.

cleanup(_) ->
    os:cmd("rm -rf test/fs-backend/*").

-endif. % EQC
-endif. % TEST_IN_RIAK_KV
-endif. % TEST
