%% -------------------------------------------------------------------
%%
%% riak_fs2_backend: storage engine based on basic filesystem access
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------
%%
%% Features:
%%
%% * The behavior & features of this backend are closely tied to
%%   the RiakCS application.
%%
%% * We assume that RiakCS's LFS chunks will be written in ascending order.
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

%% @doc riak_kv_fs2_backend is filesystem storage system, Mark III

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
-export([t0/0, t1/0]).

-include("riak_moss_lfs.hrl").
-include_lib("kernel/include/file.hrl").

-define(API_VERSION, 1).
-define(CAPABILITIES, [async_fold, write_once_keys, put_plus_object]).

-define(TEST_IN_RIAK_KV, true).

%% Borrowed from bitcask.hrl
-define(VALSIZEFIELD, 32).
-define(CRCSIZEFIELD, 32).
-define(HEADER_SIZE,  8). % Differs from bitcask.hrl!
-define(MAXVALSIZE, 2#11111111111111111111111111111111).

%% !@#$!!@#$!@#$!@#$! Erlang's efile_drv does not support the sticky bit.
%% So, we use something else: the setgid bit.
-define(TOMBSTONE_MODE_MARKER, 8#2000).

-ifdef(TEST).
-ifdef(EQC).
%% EQC testing assistants
-export([eqc_filter_delete/3]).
-export([prop_nest_ordered/0, eqc_nest_tester/0]).
-include_lib("eqc/include/eqc.hrl").
-endif.
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {
          dir        :: string(),
          block_size :: integer(),
          max_blocks :: integer()
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
        %% If the value of DefaultMaxB increases beyond 64, 
        %% please see the delete code _before_ doing so.
        DefaultMaxB = 64,
        MaxBlocks = case get_prop_or_env(fs2_backend_max_blocks_per_file,
                                         Config, riak_kv) of
                        N when is_integer(N), 0 =< N, N =< DefaultMaxB ->
                            N;
                        undefined ->
                            DefaultMaxB;
                        _ ->
                            error_logger:warning_msg("~s: invalid max blocks "
                                                     "value, using ~p\n",
                                                     [?MODULE, DefaultMaxB]),
                            DefaultMaxB
                    end,
        Dir = filename:join([ConfigRoot,PartitionName]),
        {ok = filelib:ensure_dir(Dir), #state{dir = Dir,
                                              block_size = BlockSize,
                                              max_blocks = MaxBlocks}}
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
            {error, not_found, State}
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

put(Bucket, Key, IndexSpecs, Val, State) ->
    put(Bucket, Key, IndexSpecs, Val, binary_to_term(Val), State).

%% @doc Store Val under Bkey
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), riak_object:riak_object(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.
put(<<?BLOCK_BUCKET_PREFIX, _/binary>>,
    <<_:?UUID_BYTES/binary, BlockNum:?BLOCK_FIELD_SIZE>>,
    _IndexSpecs, Val, _ValRObj,
    #state{block_size = BlockSize, max_blocks = MaxBlocks} = State)
  when size(Val) > BlockSize orelse
       BlockNum > (MaxBlocks-1) ->
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
    File = location(State, {Bucket, PrimaryKey}),
    case filelib:ensure_dir(File) of
        ok         -> {atomic_write(File, Val), State};
        {error, X} -> {error, X, State}
    end.

%% @doc Delete the object stored at BKey
-spec delete(riak_object:bucket(), riak_object:key(), [index_spec()], state()) ->
                    {ok, state()} |
                    {error, term(), state()}.
delete(<<?BLOCK_BUCKET_PREFIX, _/binary>>,
    <<_:?UUID_BYTES/binary, BlockNum:?BLOCK_FIELD_SIZE>>,
    _IndexSpecs,
    #state{max_blocks = MaxBlocks} = State)
  when BlockNum > (MaxBlocks-1) ->
    {error, invalid_user_argument, State};
delete(<<?BLOCK_BUCKET_PREFIX, _/binary>> = Bucket,
    <<UUID:?UUID_BYTES/binary, _:?BLOCK_FIELD_SIZE>>, _IndexSpecs, State) ->
    File = location(State, {Bucket, UUID}),
    _ = file:delete(File),
    {ok, State};
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
                         <<UUID:?UUID_BYTES/binary>> = _Key}, Acc) ->
                            Chunks = enumerate_chunks_in_file(Bucket, UUID,
                                                              State),
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
    [N1,N2,N3] = nest3(K64),
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

%% @spec nest3(string()) -> [string()]
%% @doc create a directory nesting, to keep the number of
%%      files in a directory smaller
nest3(Key) -> nest(Key, 3).

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
unpack_ondisk(_Bin) ->
    bad_crc.

calc_block_offset(BlockNum, #state{block_size = BlockSize}) ->
    %% MOSS block numbers start at zero.
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

put_block(Bucket, UUID, BlockNum, Val, ValRObj, State) ->
    put_block_t_marker_check(Bucket, UUID, BlockNum, Val, ValRObj, State).

put_block_t_marker_check(Bucket, UUID, BlockNum, Val, ValRObj, State) ->
    File = location(State, {Bucket, UUID}),
    FI_perhaps = file:read_file_info(File),
    case FI_perhaps of
        {ok, FI} ->
            if FI#file_info.mode band ?TOMBSTONE_MODE_MARKER == 1 ->
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

put_block3(BlockNum, Val, File, FI_perhaps,
           #state{max_blocks = MaxBlocks} = State) ->
    Offset = calc_block_offset(BlockNum, State),
    OutOfOrder_p = check_trailer_ooo(FI_perhaps, BlockNum, State),
    try
        case file:open(File, [write, read, raw, binary]) of
            {ok, FH} ->
                try
                    PackedBin = pack_ondisk(Val),
                    ok = file:pwrite(FH, Offset, PackedBin),
                    if OutOfOrder_p ->
                            %% It's not an error to write more than one trailer
                            Tr = make_trailer(#t{written_sequentially = false}),
                            TrOffset = calc_block_offset(MaxBlocks, State),
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
            io:format(user, "DBG line ~p badmatch1 err ~p\n", [?LINE, _Y]),
            {error, badmatch1};
        _X:Y ->
            io:format(user, "DBG line ~p badmatch2 ~p ~p\n", [?LINE, _X, Y]),
            {error, badmatch2}
    end.

riak_object_is_deleted(delete_op_requested) ->
    true;
riak_object_is_deleted(ValRObj) ->
    catch riak_kv_util:is_x_deleted(ValRObj).

enumerate_chunks_in_file(Bucket, UUID, #state{max_blocks=MaxBlocks} = State) ->
    Path = location(State, {Bucket, UUID}),
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
                                   is_binary(read_block(Bucket, UUID, BlNum,
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

check_trailer_ooo({error, enoent}, BlockNum, _State) ->
    %% BlockNum 0 is ok, all others are out of order
    BlockNum /= 0;
check_trailer_ooo({ok, FI}, BlockNum, State) ->
    MaxBlock = calc_max_block(FI#file_info.size, State),
    BlockNum == MaxBlock - 1.

make_trailer(Term) ->
    Bin = iolist_to_binary(pack_ondisk(term_to_binary(Term))),
    Sz = size(Bin),
    [Bin, <<Sz:32>>].

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
    {ok, S} = put(Bucket, K1, [], V1, ignored, S),
    {ok, S} = put(Bucket, K0, [], V0, ignored, S),
    {ok, V0, S} = get(Bucket, K0, S),
    {ok, V1, S} = get(Bucket, K1, S),
    ok.

t1() ->
    #t{} = #t{},
    TestDir = "./delme",
    Bucket = <<?BLOCK_BUCKET_PREFIX, "delme">>,
    K0 = <<0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0>>,
    V0 = <<100:64>>,
    K1 = <<0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,1>>,
    V1 = <<101:64>>,
    os:cmd("rm -rf " ++ TestDir),
    {ok, S} = start(-1, [{fs2_backend_data_root, TestDir},
                         {fs2_backend_block_size, 1024}]),
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

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

t0_test() ->
    t0().

t1_test() ->
    t1().

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

-ifdef(TEST_IN_RIAK_KV).

%% Broken test:
simple_test_() ->
   ?assertCmd("rm -rf test/fs-backend/*"),
   Config = [{fs2_backend_data_root, "test/fs-backend"},
             {fs2_backend_block_size, 8}],
   riak_kv_backend:standard_test(?MODULE, Config).

nest_test() ->
    ?assertEqual(["ab","cd","ef"],nest3("abcdefg")),
    ?assertEqual(["ab","cd","ef"],nest3("abcdef")),
    ?assertEqual(["a","bc","de"], nest3("abcde")),
    ?assertEqual(["0","ab","cd"], nest3("abcd")),
    ?assertEqual(["0","a","bc"],  nest3("abc")),
    ?assertEqual(["0","0","ab"],  nest3("ab")),
    ?assertEqual(["0","0","a"],   nest3("a")),
    ?assertEqual(["0","0","0"],   nest3([])).

-ifdef(EQC).

basic_props() ->
    [{fs2_backend_data_root,  "test/fs-backend"},
     {fs2_backend_block_size, 1024}].

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
                                           basic_props()))]},
          {timeout, 60000,
           [?_assertEqual(true,
                          eqc_nest_tester())]}
         ]}]}]}.

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
                            %% %% base64:encode([0])  -> <<"AA==">>
                            %% %% base64:encode([$h]) -> <<"aA==">>
                            %% %% A weight of this case ~10:1 will almost always
                            %% %% find the counterexample within 50 tests.
                            %% {50, oneof([[0], [$h]])},
                            {50, vector(1, char())},
                            {5, vector(2, char())},
                            {5, vector(3, char())},
                            {5, vector(4, char())} %%,
                            %% {5, vector(5, char())},
                            %% {5, vector(6, char())},
                            %% {5, vector(16, char())},
                            %% {5, non_empty(list(char()))}
                           ]),
         iolist_to_binary(Bucket)).

insert_sample_key(Bucket, S) ->
    {ok, _} = ?MODULE:put(Bucket, <<"key">>, [], <<"val">>, unused, S),
    ok.

-endif. % EQC
-endif. % TEST_IN_RIAK_KV
-endif. % TEST
