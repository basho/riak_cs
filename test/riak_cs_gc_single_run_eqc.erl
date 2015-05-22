%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc EQC test module for single gc run.
%% Test targets is a combination of `riak_cs_gc_batch' and `riak_cs_gc_worker'.
%% All calls to riak, 2i/GET/DELETE, are mocked away by `meck'.

-module(riak_cs_gc_single_run_eqc).

-include("riak_cs_gc.hrl").

-ifdef(EQC).
-include_lib("eqc/include/eqc.hrl").
-include_lib("eunit/include/eunit.hrl").

-compile(export_all).

-type fileset_keys_input() :: {num_fileset_keys(), no_error}.
-type num_fileset_keys() :: non_neg_integer().
-type error_or_not() :: no_error | {error, error_type()}.
%% Some kinds of errors in communication with riak are considered.
%% - `in_fileset_fetch': errors in GET from `riak-cs-gc' bucket.
%%   This affects the counters `batch_count' and `batch_skips'.
%% - `in_fileset_delete': errors in DELETE from `riak-cs-gc' bucket.
%%   No particular effects in this test scope. In a real situation,
%%   keys are re-handled in next GC run.
%% - `in_block_delete': errors in DELETE of blocks.
%%   This affects the counter `block_count'. In addition, the related
%%   fileset key should NOT be deleted. See `dummy_delete_object/2'.
-type error_type() :: in_fileset_fetch | in_fileset_delete | in_block_delete.

%% number of keys in riak-cs-gc bucket
-define(GC_KEY_NUM, 131).
%% number of manifests per fileset
-define(MANIFEST_NUM_IN_FILESET, 7).
%% number of blocks per manifest
-define(BLOCK_NUM_IN_MANIFEST, 20).

-define(QC_OUT(P),
        eqc:on_output(fun(Str, Args) -> io:format(user, Str, Args) end, P)).
-define(TESTING_TIME, 30).

%%====================================================================
%% Eunit tests
%%====================================================================

eqc_test_() ->
    {foreach,
     fun() ->
             application:set_env(riak_cs, gc_batch_size, 7),
             meck:new(riak_cs_gc_manager, []),

             meck:new(riakc_pb_socket, [passthrough]),
             %% For riak_cs_gc_worker, it starts/stops pool worker directly.
             meck_pool_worker(),
             %% GET/DELETE filesets from riak-cs-gc bucket
             meck_fileset_get_and_delete(),
             %% Also uses delete fsm and get number of deleted blocks.
             meck_delete_fsm_sup()
     end,
     fun(_) ->
             meck:unload(),
             stop_and_wait_for_gc_batch()
     end,
     [
      {timeout, ?TESTING_TIME*2,
       ?_assert(quickcheck(eqc:testing_time(?TESTING_TIME,
                                            ?QC_OUT(prop_gc_batch(no_error)))))},
      {timeout, ?TESTING_TIME*2,
       ?_assert(quickcheck(eqc:testing_time(?TESTING_TIME,
                                            ?QC_OUT(prop_gc_batch(with_errors)))))}
     ]}.

%% EQC of single GC runs.
%% 1. EQC generates `ListOfFilesetKeysInput', for exapmle
%%    `[{3, no_error}, {14, with_errors}, {15, with_errors}, {92, no_error}]'.
%% 2. `riak_cs_gc_batch' requests 2i for `riak-cs-gc' and gets response.
%%    including list of fileset keys, such as `[<<"1">>, <<"2">>, <<"3">>]'.
%% 3. `riak_cs_gc_batch' starts workers for each fileset key,
%%    GET the fileset, spawns riak_cs_delete_fsm and DELETE fileset key at the end.
%% 4. `riak_cs_gc_batch' gathers workers' results and this test asserts them.
prop_gc_batch(ErrorOrNot) ->
    ?FORALL(ListOfFilesetKeysInput,
            non_empty(list(fileset_keys_input(ErrorOrNot))),
            begin
                Self = self(),
                meck:expect(riak_cs_gc_manager, finished,
                            fun(State) ->
                                    Self ! {batch_finished, State}
                            end),
                Res = gc_batch(ListOfFilesetKeysInput),
                {ExpectedBatchCount,
                 ExpectedBatchSkips,
                 ExpectedManifCount,
                 ExpectedBlockCount} = expectations(ListOfFilesetKeysInput),
                stop_and_wait_for_gc_batch(),
                ?WHENFAIL(
                   begin
                       eqc:format("ListOfFilesetKeysInput: ~p~n",
                                  [ListOfFilesetKeysInput])
                   end,
                   conjunction([{batch_count, equals(ExpectedBatchCount, element(1, Res))},
                                {batch_skips, equals(ExpectedBatchSkips, element(2, Res))},
                                {manif_count, equals(ExpectedManifCount, element(3, Res))},
                                {block_count, equals(ExpectedBlockCount, element(4, Res))}]))
            end).

stop_and_wait_for_gc_batch() ->
    Pid = whereis(riak_cs_gc_batch),
    catch riak_cs_gc_batch:stop(),
    wait_for_stop(Pid).

wait_for_stop(undefined) ->
    ok;
wait_for_stop(Pid) ->
    case is_process_alive(Pid) of
        true ->
            timer:sleep(200),
            wait_for_stop(Pid);
        false ->
            ok
    end.

-spec gc_batch([fileset_keys_input()]) -> eqc:property().
gc_batch(ListOfFilesetKeysInput) ->
    %% For `riak-cs-gc' 2i query, use a process to hold `ListOfFilesetKeysInput'.
    %% ?debugVal(ListOfFilesetKeysInput),
    meck:expect(riakc_pb_socket, get_index_range,
                dummy_get_index_range_fun(ListOfFilesetKeysInput)),
    SortedKeys = lists:sort(ListOfFilesetKeysInput),
    {StartKey, _} = hd(SortedKeys),
    {EndKey, _} = lists:last(SortedKeys),
    BatchStart = riak_cs_gc:timestamp(),
    {ok, _} = riak_cs_gc_batch:start_link(#gc_batch_state{
                                             batch_start=BatchStart,
                                             start_key=StartKey,
                                             end_key=EndKey,
                                             max_workers=5,
                                             leeway=1}),
    receive
        {batch_finished, #gc_batch_state{batch_count=BatchCount,
                                         batch_skips=BatchSkips,
                                         manif_count=ManifCount,
                                         block_count=BlockCount} = _State} ->
            {BatchCount, BatchSkips, ManifCount, BlockCount};
        OtherMsg ->
            eqc:format("OtherMsg: ~p~n", [OtherMsg]),
            {error, error, error, error}
    end.


-spec expectations([fileset_keys_input()]) ->
                          {non_neg_integer(), non_neg_integer(),
                           non_neg_integer(), non_neg_integer()}.
expectations(ListOfFilesetKeysInput) ->
    AllFilesetCount = lists:sum([N || {N, _} <- ListOfFilesetKeysInput]),
    FilesetFetchErrorCount = lists:sum([N || {N, {error, in_fileset_fetch}}
                                                 <- ListOfFilesetKeysInput]),
    BlockDeleteErrorBatchCount = lists:sum([N || {N, {error, in_block_delete}}
                                                     <- ListOfFilesetKeysInput]),
    ExpectedBatchCount = AllFilesetCount - FilesetFetchErrorCount,
    ExpectedBatchSkips = FilesetFetchErrorCount,

    ExpectedManifCount = ExpectedBatchCount*?MANIFEST_NUM_IN_FILESET,
    BlockDeleteErrorManifCount = BlockDeleteErrorBatchCount*?MANIFEST_NUM_IN_FILESET,
    ExpectedBlockCount =
        (ExpectedManifCount - BlockDeleteErrorManifCount)*?BLOCK_NUM_IN_MANIFEST,
    {ExpectedBatchCount, ExpectedBatchSkips,
     ExpectedManifCount, ExpectedBlockCount}.

%%====================================================================
%% Generators
%%====================================================================

%% Generator of numbers of fileset keys included in a single object
%% of the `riak-cs-gc' bucket, with information of error injection.
-spec fileset_keys_input(no_error | with_errors) ->
                                eqc_gen:gen({non_neg_integer(), error_or_not()}).
fileset_keys_input(no_error) ->
    {num_fileset_keys(), no_error};
fileset_keys_input(with_errors) ->
    frequency([{1, {num_fileset_keys(), no_error}},
               {1, {num_fileset_keys(), {error, in_fileset_fetch}}},
               {1, {num_fileset_keys(), {error, in_fileset_delete}}},
               {1, {num_fileset_keys(), {error, in_block_delete}}}]).

-spec num_fileset_keys() -> eqc_gen:gen(Positive::integer()).
num_fileset_keys() ->
    ?LET(N, nat(), N+1).


%%====================================================================
%% A server process holding `ListOfFilesetKeysInput' and reply elements in sequence.
%% Replis are type of `{NumFilesetKeys::integer(), ErrorType::term()'.
%% Currently `ErrorType' is only `in_block_delete'.
%%====================================================================

-spec fileset_keys_input_server([fileset_keys_input()]) -> no_return().
fileset_keys_input_server([]) ->
    receive
        {next, {From, Ref}} ->
            From ! {Ref, {0, no_error}}
    end;
fileset_keys_input_server([Reply | ListOfFilesetKeysInput]) ->
    receive
        {next, {From, Ref}} ->
            From ! {Ref, Reply},
            fileset_keys_input_server(ListOfFilesetKeysInput)
    end.

%% Client helper for the above server process.
-spec next_fileset_keys_input(pid()) -> fileset_keys_input().
next_fileset_keys_input(KeysInputServer) ->
    Ref = make_ref(),
    KeysInputServer ! {next, {self(), Ref}},
    receive
        {Ref, KeysInput} -> KeysInput
    end.

%% ====================================================================
%% Mock helpers for `riakc_pb_socket:get_index_range/6'
%% ====================================================================
-spec dummy_get_index_range_fun([fileset_keys_input()]) -> fun().
dummy_get_index_range_fun(ListOfFilesetKeysInput) ->
    KeysInputServer =
        spawn_link(fun() -> fileset_keys_input_server(ListOfFilesetKeysInput) end),
    fun(_Pbc, _B, _K, _Start, _End, Opts) ->
            FilesetKeysInput = next_fileset_keys_input(KeysInputServer),
            dummy_get_index_range(FilesetKeysInput, Opts)
    end.

%% {ok, Reply} for 2i request which includes `NumFilesetKeys' keys
-spec dummy_get_index_range(fileset_keys_input(), proplists:proplist()) ->
                                   {ok, ?INDEX_RESULTS{}}.
dummy_get_index_range({NumFilesetKeys, no_error}, Opts) ->
    dummy_get_index_range(NumFilesetKeys, <<"no_error">>, Opts);
dummy_get_index_range({NumFilesetKeys, {error, in_fileset_fetch}}, Opts) ->
    dummy_get_index_range(NumFilesetKeys, <<"error:in_fileset_fetch/">>, Opts);
dummy_get_index_range({NumFilesetKeys, {error, in_fileset_delete}}, Opts) ->
    dummy_get_index_range(NumFilesetKeys, <<"error:in_fileset_delete/">>, Opts);
dummy_get_index_range({NumFilesetKeys, {error, in_block_delete}}, Opts) ->
    dummy_get_index_range(NumFilesetKeys, <<"error:in_block_delete/">>, Opts).

dummy_get_index_range(NumFilesetKeys, Prefix, Opts) ->
    Offset = case proplists:get_value(continuation, Opts) of
                 undefined -> 1;
                 Value -> Value
             end,
    Continuation = case NumFilesetKeys of
                       0 -> undefined;
                       _ -> Offset + NumFilesetKeys
                   end,
    {ok, ?INDEX_RESULTS{
            keys=[<<Prefix/binary, (i2b(I))/binary>> ||
                     I <- lists:seq(Offset, Offset + NumFilesetKeys - 1)],
            continuation=Continuation}}.


%% ====================================================================
%% Mock helpers for `riak_cs_delete_fsm_sup:start_delete_fsm/2'
%% which spawns a dummy process and returns its pid.
%% ====================================================================
meck_delete_fsm_sup() ->
    meck:new(riak_cs_delete_fsm_sup, [passthrough]),
    meck:expect(riak_cs_delete_fsm_sup, start_delete_fsm,
                fun dummy_start_delete_fsm/2).

dummy_start_delete_fsm(_Node, [_RcPid, {_UUID, ?MANIFEST{bkey={_, K}}=_Manifest},
                               From, _GCKey, _Args]) ->
    TotalBlocks = ?BLOCK_NUM_IN_MANIFEST,
    NumDeleted = case re:run(K, <<"^error:in_block_delete/">>) of
                     nomatch -> TotalBlocks;
                     {match, _} -> 0
                 end,
    DummyDeleteFsmPid =
        spawn(fun() -> gen_fsm:sync_send_event(
                         From,
                         {self(), {ok, {NumDeleted, TotalBlocks}}})
              end),
    {ok, DummyDeleteFsmPid}.

%% ====================================================================
%% Mock helpers for GET/DELETE of filesets.
%% GET returns fileset which include some pairs of `{UUID, Manifest}'.
%% DELETE returns simply `ok' or `error'.
%% ====================================================================
meck_fileset_get_and_delete() ->
    meck:new(riak_cs_pbc, [passthrough]),
    meck:expect(riak_cs_pbc, get_object, fun dummy_get_object/4),
    meck:expect(riakc_pb_socket, is_connected, fun always_true/1),
    meck:expect(riakc_pb_socket, delete_obj, fun dummy_delete_object/4).

dummy_get_object(_Pbc, <<"riak-cs-gc">>=B, K, _Opt) ->
    case re:run(K, <<"^error:in_fileset_fetch/">>) of
        nomatch ->
            {ok, riakc_obj:new_obj(B, K, vclock,
                                   [{dict:new(),
                                     build_fileset_bin(K, ?MANIFEST_NUM_IN_FILESET)}])};
        {match, _} ->
            {error, {dummy_error, in_fileset_fetch}}
    end;
dummy_get_object(_Pbc, _B, _K, _Opt) ->
    error.

always_true(_) -> true.

dummy_delete_object(_Pbc, RiakObj, _Opts, _Timeout) ->
    Key = riakc_obj:key(RiakObj),
    case re:run(Key, <<"^error:in_block_delete/">>) of
        nomatch ->
            ok;
        {match, _} ->
            %% A fileset which has errors in deleting blocks should NOT be deleted.
            throw({error, "Must not a delete fileset of '{error, in_block_delete'"})
    end,
    case re:run(Key, <<"^error:in_fileset_delete/">>) of
        nomatch ->
            ok;
        {match, _} ->
            {error, {dummy_error, in_fileset_delete}}
    end.

%% Build a binary of fileset which contains `Count' pair of UUID and manifest.
build_fileset_bin(FilesetKey, Count) ->
    FileSet = lists:foldl(fun(Index, FileSetAcc) ->
                                  BinIndex = i2b(Index),
                                  Key = <<FilesetKey/binary, "/", BinIndex/binary>>,
                                  twop_set:add_element(
                                    {<<"UUID", FilesetKey/binary, BinIndex/binary>>,
                                     ?MANIFEST{block_size=1024*1024,
                                               bkey={<<"bucket">>, Key},
                                               state=pending_delete}}, FileSetAcc)
                          end, twop_set:new(), lists:seq(1, Count)),
    term_to_binary(FileSet).

%% ====================================================================
%% Mock helpers for `riak_cs_riakc_pool_worker''s `start_link' and `stop'.
%% ====================================================================
meck_pool_worker() ->
    meck:new(riak_cs_utils, [passthrough]),
    meck:expect(riak_cs_utils, riak_connection,
                fun(_Pool) ->
                        Pid = spawn_link(fun dummy_pbc/0),
                        {ok, Pid}
                end),
    meck:expect(riak_cs_utils, close_riak_connection,
                fun(_Pool, Pid) ->
                        Pid ! stop,
                        ok
                end).

%% Although this dummy process is actually not needed for EQC,
%% it would still be useful for debugging when missing mock
%% and riakc is called directly.
dummy_pbc() ->
    receive
        stop -> ok;
        M -> eqc:format("dummy_worker received M: ~p~n", [M]),
             dummy_pbc()
    end.

-spec i2b(integer()) -> binary().
i2b(Integer) ->
    list_to_binary(integer_to_list(Integer)).

-endif.
