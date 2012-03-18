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

% @doc riak_kv_fs2_backend is filesystem storage system, Mark III

-module(riak_kv_fs2_backend).
-behavior(riak_kv_backend).

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

-define(API_VERSION, 1).
-define(CAPABILITIES, [async_fold]).

%% Borrowed from bitcask.hrl
-define(VALSIZEFIELD, 32).
-define(CRCSIZEFIELD, 32).
-define(HEADER_SIZE,  8). % Differs from bitcask.hrl!
-define(MAXVALSIZE, 2#11111111111111111111111111111111).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-record(state, {dir}).
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
-spec start(integer(), config()) -> {ok, state()} | {error, term()}.
start(Partition, Config) ->
    PartitionName = integer_to_list(Partition),
    ConfigRoot = app_helper:get_prop_or_env(fs2_backend_data_root, Config, riak_kv),
    if
        ConfigRoot =:= undefined ->
            riak:stop("fs_backend_data_root unset, failing");
        true ->
            ok
    end,
    Dir = filename:join([ConfigRoot,PartitionName]),
    {filelib:ensure_dir(Dir), #state{dir=Dir}}.

-spec stop(state()) -> ok.
stop(_State) -> ok.

%% @doc Get the object stored at the given bucket/key pair
-spec get(riak_object:bucket(), riak_object:key(), state()) ->
                 {ok, any(), state()} |
                 {ok, not_found, state()} |
                 {error, term(), state()}.
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

%% @doc Store Val under Bkey
-type index_spec() :: {add, Index, SecondaryKey} | {remove, Index, SecondaryKey}.
-spec put(riak_object:bucket(), riak_object:key(), [index_spec()], binary(), state()) ->
                 {ok, state()} |
                 {error, term(), state()}.
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



%% @spec list(state()) -> [{Bucket :: riak_object:bucket(),
%%                          Key :: riak_object:key()}]
%% @doc Get a list of all bucket/key pairs stored by this backend
list(#state{dir=Dir}) ->
    % this is slow slow slow
    %                                              B,N,N,N,K
    [location_to_bkey(X) || X <- filelib:wildcard("*/*/*/*/*",
                                                         Dir)].

-ifdef(SLF_COMMENT).

%% @spec list_bucket(state(), riak_object:bucket()) ->
%%           [riak_object:key()]
%% @doc Get a list of the keys in a bucket
list_bucket(State, Bucket) ->
    case Bucket of
        '_' ->
            lists:usort(lists:map(fun({B, _}) -> B end, list(State)));
        {filter, B, Fun} ->
            [ hd(K) || K <-
                lists:filter(Fun,
                    [ EV || EV <- lists:map(fun(K) ->
                                                case K of
                                                    {B, Key} -> [Key];
                                                    _ -> []
                                                end
                                            end, list(State)),
                            EV /= [] ]) ];
        _ ->
            B64 = encode_bucket(Bucket),
            L = length(State#state.dir),
            [ K || {_,K} <- [ location_to_bkey(lists:nthtail(L, X)) ||
                                X <- filelib:wildcard(
                                       filename:join([State#state.dir,
                                                      B64,"*/*/*/*"])) ]]
    end.

is_empty(State) -> ?MODULE:list(State) =:= [].

fold(State, Fun0, Acc) ->
    Fun = fun(BKey, AccIn) ->
                  case ?MODULE:get(State, BKey) of
                      {ok, Bin} ->
                          Fun0(BKey, Bin, AccIn);
                      _ ->
                          AccIn
                  end
          end,
    lists:foldl(Fun, Acc, ?MODULE:list(State)).

-endif. % SLF_COMMENT

%% @doc Fold over all the keys for one or all buckets.
-spec fold_keys(riak_kv_backend:fold_keys_fun(),
                any(),
                [{atom(), term()}],
                state()) -> {ok, term()} | {async, fun()} | {error, term()}.
fold_keys(_FoldKeysFun, Acc, _Opts, _S) ->
    Acc.                                        % lazy!

%% @doc Fold over all the objects for one or all buckets.
-spec fold_objects(riak_kv_backend:fold_objects_fun(),
                   any(),
                   [{atom(), term()}],
                   state()) -> {ok, any()} | {async, fun()} | {error, term()}.
fold_objects(_FoldObjectsFun, Acc, _Opts, _S) ->
    Acc.                                        % lazy!


%% @doc Fold over all the buckets.
-spec fold_buckets(riak_kv_backend:fold_buckets_fun(),
                   any(),
                   [],
                   state()) -> {ok, any()} | {async, fun()} | {error, term()}.
fold_buckets(FoldBucketsFun, Acc, Opts, State) ->
    %% case Bucket of
    %%     '_' ->
    %%         lists:usort(lists:map(fun({B, _}) -> B end, list(State)));
    %%     {filter, B, Fun} ->
    %%         [ hd(K) || K <-
    %%             lists:filter(Fun,
    %%                 [ EV || EV <- lists:map(fun(K) ->
    %%                                             case K of
    %%                                                 {B, Key} -> [Key];
    %%                                                 _ -> []
    %%                                             end
    %%                                         end, list(State)),
    %%                         EV /= [] ]) ];
    %%     _ ->
    %%         B64 = encode_bucket(Bucket),
    %%         L = length(State#state.dir),
    %%         [ K || {_,K} <- [ location_to_bkey(lists:nthtail(L, X)) ||
    %%                             X <- filelib:wildcard(
    %%                                    filename:join([State#state.dir,
    %%                                                   B64,"*/*/*/*"])) ]]
    %% end.


    FoldFun = fold_buckets_fun(FoldBucketsFun),
    BucketFolder =
        fun() ->
                {FoldResult, _} =
                    lists:foldl(FoldFun, {Acc, sets:new()}, list(State)),
                FoldResult
        end,
    case lists:member(async_fold, Opts) of
        true ->
            {async, BucketFolder};
        false ->
            {ok, BucketFolder()}
    end.

%% %% @doc Fold over all the keys for one or all buckets.
%% -spec fold_keys(riak_kv_backend:fold_keys_fun(),
%%                 any(),
%%                 [{atom(), term()}],
%%                 state()) -> {ok, term()} | {async, fun()} | {error, term()}.
%% fold_keys(FoldKeysFun, Acc, Opts, #state{opts=BitcaskOpts,
%%                                          data_dir=DataFile,
%%                                          ref=Ref,
%%                                          root=DataRoot}) ->
%%     Bucket =  proplists:get_value(bucket, Opts),
%%     FoldFun = fold_keys_fun(FoldKeysFun, Bucket),
%%     case lists:member(async_fold, Opts) of
%%         true ->
%%             ReadOpts = set_mode(read_only, BitcaskOpts),
%%             KeyFolder =
%%                 fun() ->
%%                         case bitcask:open(filename:join(DataRoot, DataFile),
%%                                           ReadOpts) of
%%                             Ref1 when is_reference(Ref1) ->
%%                                 try
%%                                     bitcask:fold_keys(Ref1, FoldFun, Acc)
%%                                 after
%%                                     bitcask:close(Ref1)
%%                                 end;
%%                             {error, Reason} ->
%%                                 {error, Reason}
%%                         end
%%                 end,
%%             {async, KeyFolder};
%%         false ->
%%             FoldResult = bitcask:fold_keys(Ref, FoldFun, Acc),
%%             case FoldResult of
%%                 {error, _} ->
%%                     FoldResult;
%%                 _ ->
%%                     {ok, FoldResult}
%%             end
%%     end.

%% %% @doc Fold over all the objects for one or all buckets.
%% -spec fold_objects(riak_kv_backend:fold_objects_fun(),
%%                    any(),
%%                    [{atom(), term()}],
%%                    state()) -> {ok, any()} | {async, fun()} | {error, term()}.
%% fold_objects(FoldObjectsFun, Acc, Opts, #state{opts=BitcaskOpts,
%%                                                data_dir=DataFile,
%%                                                ref=Ref,
%%                                                root=DataRoot}) ->
%%     Bucket =  proplists:get_value(bucket, Opts),
%%     FoldFun = fold_objects_fun(FoldObjectsFun, Bucket),
%%     case lists:member(async_fold, Opts) of
%%         true ->
%%             ReadOpts = set_mode(read_only, BitcaskOpts),
%%             ObjectFolder =
%%                 fun() ->
%%                         case bitcask:open(filename:join(DataRoot, DataFile),
%%                                           ReadOpts) of
%%                             Ref1 when is_reference(Ref1) ->
%%                                 try
%%                                     bitcask:fold(Ref1, FoldFun, Acc)
%%                                 after
%%                                     bitcask:close(Ref1)
%%                                 end;
%%                             {error, Reason} ->
%%                                 {error, Reason}
%%                         end
%%                 end,
%%             {async, ObjectFolder};
%%         false ->
%%             FoldResult = bitcask:fold(Ref, FoldFun, Acc),
%%             case FoldResult of
%%                 {error, _} ->
%%                     FoldResult;
%%                 _ ->
%%                     {ok, FoldResult}
%%             end
%%     end.




-spec drop(state()) -> {ok, state()} | {error, term(), state()}.
drop(State) ->
    [file:delete(location(State, BK)) || BK <- ?MODULE:list(State)],
    Cmd = io_lib:format("rm -Rf ~s", [State#state.dir]),
    os:cmd(Cmd),
    {ok, State}.

-spec is_empty(state()) -> boolean().
is_empty(S) ->
    ?MODULE:list(S) == [].

-spec status(state()) -> [{atom(), term()}].
status(_S) ->
    [no_status_sorry].

-spec callback(reference(), any(), state()) -> {ok, state()}.
callback(_Ref, _Term, S) ->
    {ok, S}.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @private
%% Return a function to fold over the buckets on this backend
fold_buckets_fun(FoldBucketsFun) ->
    fun(Location, {Acc, BucketSet}) ->
            %% {Bucket, _} = location_to_bkey(Location),
            {Bucket, _} = Location,

            case sets:is_element(Bucket, BucketSet) of
                true ->
                    {Acc, BucketSet};
                false ->
                    {FoldBucketsFun(Bucket, Acc),
                     sets:add_element(Bucket, BucketSet)}
            end
    end.

%% %% @private
%% %% Return a function to fold over keys on this backend
%% fold_keys_fun(FoldKeysFun, undefined) ->
%%     %% Fold across everything...
%%     fun(StorageKey, Acc) ->
%%             case from_object_key(StorageKey) of
%%                 {Bucket, Key} ->
%%                     FoldKeysFun(Bucket, Key, Acc);
%%                 _ ->
%%                     throw({break, Acc})
%%             end
%%     end;
%% fold_keys_fun(FoldKeysFun, {bucket, FilterBucket}) ->
%%     %% Fold across a specific bucket...
%%     fun(StorageKey, Acc) ->
%%             case from_object_key(StorageKey) of
%%                 {Bucket, Key} when Bucket == FilterBucket ->
%%                     FoldKeysFun(Bucket, Key, Acc);
%%                 _ ->
%%                     throw({break, Acc})
%%             end
%%     end;
%% fold_keys_fun(FoldKeysFun, {index, FilterBucket, {eq, <<"$bucket">>, _}}) ->
%%     %% 2I exact match query on special $bucket field...
%%     fold_keys_fun(FoldKeysFun, {bucket, FilterBucket});
%% fold_keys_fun(FoldKeysFun, {index, FilterBucket, {eq, FilterField, FilterTerm}}) ->
%%     %% Rewrite 2I exact match query as a range...
%%     NewQuery = {range, FilterField, FilterTerm, FilterTerm},
%%     fold_keys_fun(FoldKeysFun, {index, FilterBucket, NewQuery});
%% fold_keys_fun(FoldKeysFun, {index, FilterBucket, {range, <<"$key">>, StartKey, EndKey}}) ->
%%     %% 2I range query on special $key field...
%%     fun(StorageKey, Acc) ->
%%             case from_object_key(StorageKey) of
%%                 {Bucket, Key} when FilterBucket == Bucket,
%%                                    StartKey =< Key,
%%                                    EndKey >= Key ->
%%                     FoldKeysFun(Bucket, Key, Acc);
%%                 _ ->
%%                     throw({break, Acc})
%%             end
%%     end;
%% fold_keys_fun(FoldKeysFun, {index, FilterBucket, {range, FilterField, StartTerm, EndTerm}}) ->
%%     %% 2I range query...
%%     fun(StorageKey, Acc) ->
%%             case from_index_key(StorageKey) of
%%                 {Bucket, Key, Field, Term} when FilterBucket == Bucket,
%%                                                 FilterField == Field,
%%                                                 StartTerm =< Term,
%%                                                 EndTerm >= Term ->
%%                     FoldKeysFun(Bucket, Key, Acc);
%%                 _ ->
%%                     throw({break, Acc})
%%             end
%%     end;
%% fold_keys_fun(_FoldKeysFun, Other) ->
%%     throw({unknown_limiter, Other}).

%% %% @private
%% %% Return a function to fold over the objects on this backend
%% fold_objects_fun(FoldObjectsFun, FilterBucket) ->
%%     %% 2I does not support fold objects at this time, so this is much
%%     %% simpler than fold_keys_fun.
%%     fun({StorageKey, Value}, Acc) ->
%%             case from_object_key(StorageKey) of
%%                 {Bucket, Key} when FilterBucket == undefined;
%%                                    Bucket == FilterBucket ->
%%                     FoldObjectsFun(Bucket, Key, Value, Acc);
%%                 _ ->
%%                     throw({break, Acc})
%%             end
%%     end.


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

-ifdef(SLF_COMMENT).


-endif. % SLF_LAZY

-ifdef(TEST).
-endif. % TEST

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
-spec pack_ondisk(binary()) -> binary().
pack_ondisk(Bin) ->
    ValueSz = size(Bin),
    true = (ValueSz =< ?MAXVALSIZE),
    Bytes0 = [<<ValueSz:?VALSIZEFIELD>>, Bin],
    [<<(erlang:crc32(Bytes0)):?CRCSIZEFIELD>> | Bytes0].

-spec unpack_ondisk(binary()) -> binary() | bad_crc.
unpack_ondisk(<<Crc32:?CRCSIZEFIELD/unsigned, Bytes/binary>>) ->
    case erlang:crc32(Bytes) of
        Crc32 ->
            <<ValueSz:?VALSIZEFIELD, Value:ValueSz/bytes>> = Bytes,
            Value;
        _BadCrc ->
            bad_crc
    end.

%%
%% Test
%%
-ifdef(TEST).

%% Broken test:
simple_test_() ->
   ?assertCmd("rm -rf test/fs-backend"),
   Config = [{fs2_backend_data_root, "test/fs-backend"}],
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
%% Broken test:
%% eqc_test() ->
%%     Cleanup = fun(_State,_Olds) -> os:cmd("rm -rf test/fs-backend") end,
%%     Config = [{riak_kv_fs_backend_root, "test/fs-backend"}],
%%     ?assertCmd("rm -rf test/fs-backend"),
%%     ?assertEqual(true, backend_eqc:test(?MODULE, false, Config, Cleanup)).

eqc_test_() ->
    {spawn,
     [{inorder,
       [{setup,
         fun setup/0,
         fun cleanup/1,
         [
          {timeout, 60000,
           [?_assertEqual(true,
                          backend_eqc:test(?MODULE,
                                           false,
                                           [{data_root,
                                             "test/fs-backend"}]))]}
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
-endif. % TEST
