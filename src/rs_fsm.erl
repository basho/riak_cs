%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(rs_fsm).

-behaviour(gen_fsm).
-compile(export_all).                           % XXX debugging only

-include("rs_erasure_encoding.hrl").

-include_lib("eqc/include/eqc.hrl").

%% API
-export([write/8, write/11,
         read/7, read/10]).
-export([get_local_riak_client/0, free_local_riak_client/1]).

%% gen_fsm callbacks
-export([init/1, prepare_write/2, prepare_write/3, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).
-export([t_write/0, t_write_test/0,
         t_read/0, t_read_test/0]).

-define(MD_USERMETA, <<"X-Riak-Meta">>).  %% TODO Define this the Right Way

-type partnum() :: non_neg_integer().

-record(state, {
          caller :: pid(),
          mode :: 'read' | 'write',
          alg :: rs_ec_algorithm(),
          k :: pos_integer(),
          m :: pos_integer(),
          rbucket :: binary(),
          rsuffix :: binary(),
          data :: binary(),
          opts :: proplist:proplist(),
          tref :: undefined | reference(),
          get_client_fun :: fun(),
          free_client_fun :: fun(),
          robj_mod :: atom(),
          ops_procs :: undefined | [pid()],
          waiting_replies = 0 :: integer(),
          read_replies = [] :: [{partnum(), binary()}]
         }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc Start a new write op using Reed-Solomon erasure coding
%%
%% We will use a Reed-Solomon-style algorithm Alg, factors K & M,
%% using Riak bucket RBucket and RSuffix for the Riak key suffix.
%%
%% K = number of data pieces
%% M = number of parity pieces
%%
%% The Alg atom will encode some algorithm assumptions that the rest
%% of this code will also assume, e.g.
%%   * ?ALG_CAUCHY_GOOD_V0 will use w=4 *and* it will assume a very
%%     particular version of the NIF-to-be's implementation of
%%     the cauchy_good algorithm.  For example, if the NIF can't
%%     provide that exact version, then we must fail.  Paranoia!
%%
%% @spec start() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
write(Alg, K, M, RBucket, RSuffix, Data, Opts, Timeout) ->
    write(Alg, K, M, RBucket, RSuffix, Data, Opts, Timeout,
          fun get_local_riak_client/0, fun free_local_riak_client/1,
          riak_object).

write(Alg, K, M, RBucket, RSuffix, Data, Opts, Timeout,
      GetClientFun, FreeClientFun, RObjMod) ->
    {ok, Pid} = start_write(Alg, K, M, RBucket, RSuffix, Data, Opts, Timeout,
                            GetClientFun, FreeClientFun, RObjMod),
    wait_for_sync_reply(Pid, Timeout).

start_write(Alg, K, M, RBucket, RSuffix, Data, Opts, Timeout,
            GetClientFun, FreeClientFun, RObjMod)
  when Alg == ?ALG_FAKE_V0; Alg == ?ALG_CAUCHY_GOOD_V0 ->
    gen_fsm:start(
      ?MODULE, {write, Alg, K, M, RBucket, RSuffix, Data, Opts, Timeout, self(),
                GetClientFun, FreeClientFun, RObjMod}, []).

read(Alg, K, M, RBucket, RSuffix, Opts, Timeout) ->
    read(Alg, K, M, RBucket, RSuffix, Opts, Timeout,
         fun get_local_riak_client/0, fun free_local_riak_client/1,
         riak_object).

read(Alg, K, M, RBucket, RSuffix, Opts, Timeout,
      GetClientFun, FreeClientFun, RObjMod) ->
    {ok, Pid} = start_read(Alg, K, M, RBucket, RSuffix, Opts, Timeout,
                           GetClientFun, FreeClientFun, RObjMod),
    wait_for_sync_reply(Pid, Timeout).

start_read(Alg, K, M, RBucket, RSuffix, Opts, Timeout,
           GetClientFun, FreeClientFun, RObjMod)
  when Alg == ?ALG_FAKE_V0; Alg == ?ALG_CAUCHY_GOOD_V0 ->
    gen_fsm:start(
      ?MODULE, {read, Alg, K, M, RBucket, RSuffix, Opts, Timeout, self(),
                GetClientFun, FreeClientFun, RObjMod}, []).

get_local_riak_client() ->
    riak:local_client().

free_local_riak_client(_) ->
    ok.

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @spec init(Args) -> {ok, StateName, State} |
%%                     {ok, StateName, State, Timeout} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
%%--------------------------------------------------------------------
init({write, Alg, K, M, RBucket, RSuffix, Data, Opts, Timeout, Caller,
      GetClientFun, FreeClientFun, RObjMod})
    when is_integer(K) andalso K > 0 andalso
         is_integer(M) andalso M > 0 andalso
         is_binary(RBucket) andalso is_binary(RSuffix) andalso
         is_binary(Data) ->
    TRef = if Timeout == infinity ->
                   undefined;
              true ->
                   erlang:send_after(Timeout, self(), final_timeout)
           end,
    {ok, prepare_write, #state{caller = Caller,
                               mode = write,
                               alg = Alg,
                               k = K,
                               m = M,
                               rbucket = RBucket,
                               rsuffix = RSuffix,
                               data = Data,
                               opts = Opts,
                               tref = TRef,
                               get_client_fun = GetClientFun,
                               free_client_fun = FreeClientFun,
                               robj_mod = RObjMod}, 0};
init({read, Alg, K, M, RBucket, RSuffix, Opts, Timeout, Caller,
      GetClientFun, FreeClientFun, RObjMod})
    when is_integer(K) andalso K > 0 andalso
         is_integer(M) andalso M > 0 andalso
         is_binary(RBucket) andalso is_binary(RSuffix) ->
    if Alg == ?ALG_FAKE_V0        -> ok;
       Alg == ?ALG_CAUCHY_GOOD_V0 -> io:format("Warning: read code broken!\n")
    end,
    TRef = if Timeout == infinity ->
                   undefined;
              true ->
                   erlang:send_after(Timeout, self(), final_timeout)
           end,
    {ok, prepare_read, #state{caller = Caller,
                              mode = write,
                              alg = Alg,
                              k = K,
                              m = M,
                              rbucket = RBucket,
                              rsuffix = RSuffix,
                              opts = Opts,
                              tref = TRef,
                              get_client_fun = GetClientFun,
                              free_client_fun = FreeClientFun,
                              robj_mod = RObjMod}, 0}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same
%% name as the current state name StateName is called to handle
%% the event. It is also called if a timeout occurs.
%%
%% @spec prepare_write(Event, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
prepare_write(timeout, S) ->
    BKFrags = ec_encode(S#state.data, S),
    Writers = spawn_frag_writers(size(S#state.data), BKFrags, S),
    {next_state, write_waiting_replies,
     S#state{ops_procs = Writers,
             waiting_replies = S#state.k + S#state.m}}.

write_waiting_replies({?MODULE, asyncreply, FromPid, ok},
                      #state{waiting_replies = WaitingRepliesX,
                             ops_procs = OpsProcsX} = S) ->
    OpsProcs = OpsProcsX -- [FromPid],
    case WaitingRepliesX - 1 of
        0 ->
            send_sync_reply(S#state.caller, ok),
            {stop, normal, S#state{ops_procs = OpsProcs,
                                   waiting_replies = 0}};
        WaitingReplies when OpsProcs == [] ->
            {next_state, write_partial_failure,
             S#state{ops_procs = OpsProcs,
                     waiting_replies = WaitingReplies},
             0};
        WaitingReplies ->
            {next_state, write_waiting_replies,
             S#state{ops_procs = OpsProcs,
                     waiting_replies = WaitingReplies}}
    end;
write_waiting_replies({?MODULE, asyncreply, _FromPid, NotOk}, S) ->
    %% TODO Fix
    send_sync_reply(S#state.caller, {error, {dunno_why, NotOk}}),
    {stop, normal, S};
write_waiting_replies(timeout, S) ->
    {next_state, write_partial_failure, S, 0}.

write_partial_failure(timeout, S) ->
    %% TODO: figure out what to do here
    send_sync_reply(S#state.caller, timeout_bummer),
    {stop, normal, S}.

prepare_read(timeout, S) ->
    %% TODO Fix assumption about k readers
    BKs = lists:sublist(ec_encode_read_keys(S), S#state.k),
    PartNumBKs = lists:zip(lists:seq(0, length(BKs) - 1), BKs),
    Readers = spawn_frag_readers(PartNumBKs, S),
    {next_state, read_waiting_replies,
     S#state{ops_procs = Readers,
             waiting_replies = S#state.k}}.

read_waiting_replies({?MODULE, asyncreply, FromPid,
                      {PartNum, {ok, BigBinSize, Bin}}},
                      #state{waiting_replies = WaitingRepliesX,
                             ops_procs = OpsProcsX,
                             read_replies = Replies} = S) ->
    OpsProcs = OpsProcsX -- [FromPid],
    NewReplies = [{PartNum, Bin}|Replies],
    case WaitingRepliesX - 1 of
        0 ->
            PadBin = list_to_binary([B || {_, B} <- lists:sort(NewReplies)]),
            <<BigBin:BigBinSize/binary, _Pad/binary>> = PadBin,
            send_sync_reply(S#state.caller, {ok, BigBin}),
            {stop, normal, S#state{ops_procs = OpsProcs,
                                   waiting_replies = 0}};
        WaitingReplies when OpsProcs == [] ->
            {next_state, read_partial_failure,
             S#state{ops_procs = OpsProcs,
                     waiting_replies = WaitingReplies},
             0};
        WaitingReplies ->
            {next_state, read_waiting_replies,
             S#state{ops_procs = OpsProcs,
                     waiting_replies = WaitingReplies,
                     read_replies = NewReplies}}
    end;
read_waiting_replies({?MODULE, asyncreply, _FromPid, NotOk}, S) ->
    %% TODO Fix
    send_sync_reply(S#state.caller, {error, {dunno_why, NotOk}}),
    {stop, normal, S};
read_waiting_replies(timeout, S) ->
    {next_state, read_partial_failure, S, 0}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_event/[2,3], the instance of this function with
%% the same name as the current state name StateName is called to
%% handle the event.
%%
%% @spec prepare_write(Event, From, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
prepare_write(_Event, _From, State) ->
    Reply = neverNEVAHHH,
    {reply, Reply, prepare_write, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @spec handle_sync_event(Event, From, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @spec handle_info(Info,StateName,State)->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, S) ->
    erlang:cancel_timer(S#state.tref),
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% --- Riak bucket & key encoding/decoding

encode_key(Frag, #state{alg = Alg, k = K, m = M, rsuffix = Suffix}) ->
    encode_key(Alg, K, M, Frag, Suffix).

encode_key(Alg, K, M, Frag, Suffix) ->
    Code = alg_to_code(Alg),
    <<"st", Code:8, K:8, M:8, Frag:8, Suffix/binary>>.

decode_key(<<"st", Code:8, K:8, M:8, Frag:8, Suffix/binary>>) ->
    {code_to_alg(Code), K, M, Frag, Suffix}.

alg_to_code(?ALG_FAKE_V0)        -> $f;
alg_to_code(?ALG_CAUCHY_GOOD_V0) -> $g.

code_to_alg($f) -> ?ALG_FAKE_V0;
code_to_alg($g) -> ?ALG_CAUCHY_GOOD_V0.

%% --- Fake erasure encoding/decoding

ec_encode(Bin, #state{alg = Alg, k = K, m = M, opts = Opts,
                      rbucket = Bucket, rsuffix = Suffix}) ->
    ec_encode(Bin, Alg, K, M, Bucket, Suffix, Opts).

ec_encode(<<>>, _, _, _, _, _, _) ->
    [];
ec_encode(Bin, Alg, K, M, Bucket, Suffix, Opts) ->
    NumFrags = K + M,
    Buckets = lists:duplicate(NumFrags, Bucket),
    Keys = ec_encode_keys(Alg, K, M, Suffix, lists:seq(0, NumFrags - 1)),
    Frags = ec_encode_data(Bin, Alg, K, M, Opts),
    lists:zip3(Buckets, Keys, Frags).

ec_encode_read_keys(#state{alg = Alg, k = K, m = M,
                           rbucket = Bucket, rsuffix = Suffix}) ->
    ec_encode_read_keys(Alg, K, M, Bucket, Suffix).

ec_encode_read_keys(Alg, K, M, Bucket, Suffix) ->
    Buckets = lists:duplicate(K + M, Bucket),
    Keys = ec_encode_keys(Alg, K, M, Suffix, lists:seq(0, K + M - 1)),
    lists:zip(Buckets, Keys).

ec_encode_keys(Alg, K, M, Suffix, Frags) ->
    [encode_key(Alg, K, M, Frag, Suffix) || Frag <- Frags].

ec_encode_data(Bin, ?ALG_FAKE_V0, K, M, _Opts) ->
    BinSize = size(Bin),
    BinSizeRemK = BinSize rem K,
    if K == 1 ->
            FragSize = BinSize,
            PadSize = 0;
       BinSizeRemK == 0 ->
            FragSize = BinSize div K,
            PadSize = 0;
       true ->
            FragSize = (BinSize div K) + 1,
            PadSize = K - BinSizeRemK
    end,
    ec_fake_split_bin(FragSize, Bin, K, PadSize) ++
        [<<(X+42):(FragSize * 8)>> || X <- lists:seq(0, M - 1)];
ec_encode_data(Bin, ?ALG_CAUCHY_GOOD_V0, K, M, Opts) ->
    EncPid = proplists:get_value(rs_dumb_ext_pid, Opts, no_such_encoder_pid),
    {Ks, Ms} = rs_dumb_ext:encode(EncPid, alg_cauchy_good0, K, M, Bin),
    Ks ++ Ms.

ec_fake_split_bin(1, Bin, K, PadSize) when size(Bin) < K ->
    ec_fake_split_small(Bin) ++ lists:duplicate(PadSize, <<0>>);
ec_fake_split_bin(FragSize, Bin, _K, Padsize) ->
    ec_fake_split_bin_big(FragSize, Bin, Padsize).

ec_fake_split_small(<<>>) ->
    [];
ec_fake_split_small(<<Head:1/binary, Rest/binary>>) ->
    [Head|ec_fake_split_small(Rest)].

ec_fake_split_bin_big(FragSize, Bin, PadBytes)
  when size(Bin) >= FragSize ->
    <<Head:FragSize/binary, Rest/binary>> = Bin,
    [Head|ec_fake_split_bin_big(FragSize, Rest, PadBytes)];
ec_fake_split_bin_big(_, <<>>, 0) ->
    [];
ec_fake_split_bin_big(FragSize, Last, PadBytes) when FragSize < PadBytes ->
    Pad1Size = FragSize - size(Last),
    NumPad2s = PadBytes div FragSize,
    Pad2s = lists:duplicate(NumPad2s, <<0:(8*FragSize)>>),
    if Last == <<>> ->
            Pad2s;
       true ->
            [<<Last/binary, 0:(8*Pad1Size)>>|Pad2s]
    end;
ec_fake_split_bin_big(_FragSize, Last, PadBytes) ->
    [<<Last/binary, 0:(8*PadBytes)>>].

%% --- Misc

spawn_frag_writers(BinSize, BKFrags, S) ->
    Me = self(),
    [spawn_link(fun() ->
                        frag_writer(Me, BinSize, BKFrag, S)
                end) || BKFrag <- BKFrags].

%% TODO I'm not very good at using try/catch/after or don't understand
%%      it, or try/catch/after is an ugly tool?

frag_writer(Parent, BinSize, {Bucket, Key, FragData}, S) ->
    Res = try
              {ok, Client} = (S#state.get_client_fun)(),
              try
                  RObj0 = (S#state.robj_mod):new(Bucket, Key, FragData),
                  MD = dict:from_list([{?MD_USERMETA, [{<<"BinSize">>,
                                                        BinSize}]}]),
                  RObj = (S#state.robj_mod):update_metadata(RObj0, MD),

                  %% TODO What to do about replica placement here?
                  %% We cannot specify n_val in the put FSM's Options
                  %% list, alas, and there are other things that we
                  %% cannot specify either but could make our job a
                  %% lot easier.
                  Client:put(RObj)
              catch
                  Xi:Yi ->
                      {error, {Xi, Yi, erlang:get_stacktrace()}}
              after
                  (S#state.free_client_fun)(Client)
              end
          catch
              Xo:Yo ->
                  {error, {Xo, Yo, erlang:get_stacktrace()}}
          end,
    send_frag_reply(Parent, Res),
    exit(normal).

spawn_frag_readers(PartNumBKs, S) ->
    Me = self(),
    [spawn_link(fun() -> frag_reader(Me, PBK, S) end) || PBK <- PartNumBKs].

%% TODO I'm not very good at using try/catch/after or don't understand
%%      it, or try/catch/after is an ugly tool?

frag_reader(Parent, {PartNum, {Bucket, Key}}, S) ->
    Res = try
              {ok, Client} = (S#state.get_client_fun)(),
              try
                  case Client:get(Bucket, Key) of
                      {ok, RObj} ->
                          [MD|_] = (S#state.robj_mod):get_metadatas(RObj),
                          {ok, UMD} = dict:find(?MD_USERMETA, MD),
                          {value, {_, BinSize}} = lists:keysearch(
                                                    <<"BinSize">>, 1, UMD),
                          {ok, BinSize, (S#state.robj_mod):get_value(RObj)};
                      Else ->
                          Else
                  end
              catch
                  Xi:Yi ->
                      {error, {Xi, Yi, erlang:get_stacktrace()}}
              after
                  (S#state.free_client_fun)(Client)
              end
          catch
              Xo:Yo ->
                  {error, {Xo, Yo, erlang:get_stacktrace()}}
          end,
    send_frag_reply(Parent, {PartNum, Res}),
    exit(normal).

zip_1xx(_A, [], []) ->
    [];
zip_1xx(A, [B|Bx], [C|Cx]) ->
    [{A, B, C}|zip_1xx(A, Bx, Cx)].

monitor_pid(Pid) ->
    erlang:monitor(process, Pid).

demonitor_pid(Ref) when is_reference(Ref) ->
    erlang:demonitor(Ref),
    receive
        {'DOWN', Ref, _, _, _} ->
            true
    after 0 ->
            true
    end.

send_frag_reply(Pid, Reply) ->
    gen_fsm:send_event(Pid, {?MODULE, asyncreply, self(), Reply}).

send_sync_reply(Pid, Reply) ->
    Pid ! {?MODULE, syncreply, Reply}.

%% Note that we assume that the pid we're waiting for is going to
%% do the right thing wrt timeouts.

wait_for_sync_reply(Pid, Timeout0) ->
    Timeout = if Timeout0 == infinity -> infinity;
                 true                 -> Timeout0 + 200
              end,
    WRef = monitor_pid(Pid),
    try
        receive
            {?MODULE, syncreply, Reply} ->
                Reply;
            {'DOWN', WRef, _, _, Info} ->
                {error, Info}
        after
            Timeout ->
                timeout
        end
    after
        demonitor_pid(WRef)
    end.

t_write() ->
    write(?ALG_FAKE_V0, 3, 2, <<"rb">>, <<"rs">>, <<"data">>, [], 500).

t_write_test() ->
    ok = t_write().

t_read() ->
    read(?ALG_FAKE_V0, 3, 2, <<"rb">>, <<"rs">>, [], 500).

t_read_test() ->
    ok = t_read().

t_fake_encode_test() ->
    Bins = [list_to_binary(lists:seq(1, X)) || X <- lists:seq(1, 40)],
    Ks = lists:seq(1, 13),
    Ms = lists:seq(0, 13),
    Bucket = <<"bucket">>,
    Suffix = <<"suffix">>,
    [begin
         %% io:format("size(Bin) ~p K ~p M ~p\n", [size(Bin), K, M]),
         Es = ec_encode(Bin, ?ALG_FAKE_V0, K, M, Bucket, Suffix, []),
         %% We won't check the parity frags, since they're all bogus.
         %% Instead, compare the original Bin + plus any addtional padding
         %% to the concatenation of all of the K data frags.
         %% and then compare to the K data
         %% frags.
         BinSize = size(Bin),
         PadBytes = if K == 1 ->
                           0;
                      size(Bin) rem K == 0 ->
                           0;
                      true ->
                           K - (size(Bin) rem K)
                   end,
         %% PadBytes = if K == size(Bin) ->
         %%                   0;
         %%              %% size(Bin) < K ->
         %%              %%      K - size(Bin);
         %%              size(Bin) rem K == 0 ->
         %%                   0;
         %%              true ->
         %%                   K - (size(Bin) rem K)
         %%           end,
         Pad = list_to_binary([0 || _ <- lists:seq(0, PadBytes-1)]),
         CatKFrags = list_to_binary([Frag || {_, _, Frag} <-
                                                 lists:sublist(Es, K)]),
         {Bin, K, M, Pad, <<Bin:BinSize/binary, Pad/binary>>} =
             {Bin, K, M, Pad, CatKFrags}
     end || Bin <- Bins, K <- Ks, M <- Ms],
    ok.

%% --- QuickCheck model stuff

gen_proto_reply_seq() ->
    ?LET({K, M}, {choose(1, 9), choose(0, 5)},
         %% 1st vector = K replicas, sequence range 0 - 1000
         %% 2nd vector = M replicas, sequence range 100 - 1100
         %% Goal: M replica replies will tend to be a bit later than K replies
         {K, M, vector(K, gen_seq_reply(500)), vector(M, gen_seq_reply(600))}).

gen_seq_reply(N) ->
    {choose(N - 500, N + 500), gen_reply()}.

gen_reply() ->
    frequency([{ 7, ok},
               { 2, not_found},
               { 1, timeout}]).

%% %% Sanity check: no m reply appears before a k not-ok.
%% %% Check by keeping a counter of failures: +1 when a k server
%% %% fails, and -1 when an m server responds w
%% Sanity1 = lists:foldl(fun({_, k, _, Fail}, Fails) when Fail /= ok ->
%%                               Fails + 1;
%%                          ({_, m, _, _}, Fails) ->
%%                               Fails - 1;
%%                          (_, Fails) ->
%%                               Fails
%%                       end, 0, FullSeq),
%%
%% This is a bogus test.  Consider this sequence:
%%
%% [{0,k,1,not_found},{100,m,1,not_found},{100,m,2,not_found}]
%% k1's not_found means that m1 should be considered.
%% m1's not_found means that m2 should be considered.
%% m2's not_found means that the sanity score is negative.
%% The sequence is legit, so a negative score tells us not-enough.

prop_yoyo() ->
    ?FORALL(
       {K, _M, K_vec, M_vec}, gen_proto_reply_seq(),
       begin
           FullSeq = make_reply_sequence(K_vec, M_vec),
           %% io:format("K: ~p ~p\n", [K, K_vec]),
           %% io:format("M: ~p ~p\n", [M, M_vec]),
           %% io:format("r: ~p\n", [FullSeq]),
           NumOKs = length([x || {_, _, _, ok} <- FullSeq]),
           NumNotFs = length([x || {_, _, _, not_found} <- FullSeq]),
           _NumTOs = length([x || {_, _, _, timeout} <- FullSeq]),
           _Class = if NumOKs >= K ->
                           ok;
                      NumNotFs >= K ->
                           not_found;
                      true ->
                           future_hazy_try_again_later
                   end,
           ?WHENFAIL(
              io:format("S = ~p\n", [FullSeq]),
              (NumOKs >= K orelse NumNotFs >= K)
              andalso
              %% TODO replay FullSeq into the FSM
              true)
       end).

conv_proto_reply_seq2sfar(FragType, Vector) ->
    %% sfar = sequence number, frag type, server #, reply
    Pairs = lists:zip(lists:seq(1, length(Vector)), Vector),
    [{SequenceNum, FragType, Server, Reply} ||
        {Server, {SequenceNum, Reply}} <- Pairs].

make_reply_sequence(K_vec0, M_vec0) ->
    K_vec = lists:sort(conv_proto_reply_seq2sfar(k, K_vec0)),
    M_vec = lists:sort(conv_proto_reply_seq2sfar(m, M_vec0)),
    make_reply_seq(K_vec, M_vec).

make_reply_seq(K_vec, M_vec) ->
    make_reply_seq2(K_vec, M_vec, 1).

%% For example, if:
%%
%%    K_vec = [{49,timeout},{24,timeout},{315,ok}]
%%    M_vec = [{564,ok},{219,ok},{586,ok}]
%%
%% ... the model will assume that K frag #2 will timeout @ 24 and K
%% frag #1 will timeout at 49.  So we mix in the replies from M frag
%% #2 and M frag #1.  That will give a total order over time of:
%%
%% [{24,k,2,timeout},{49,k,1,timeout},{219,m,2,ok},{315,k,3,ok},{564,m,1,ok}]

make_reply_seq2([], _, _) ->
    [];
make_reply_seq2([{_Seq, _FragType, _Server, Cmd} = K|Krest], M_vec, NextM) ->
    %% Here's where we embed an assumption of the model.
    case Cmd of
        ok ->
            %% If reply == ok, then no action will be required later
            %% by one of the m parity servers.
            [K|make_reply_seq2(Krest, M_vec, NextM)];
        _ ->
            %% If a reply is not ok, then we'll
            %% try to find an m server's reply and mix it in (sorted!)
            %% with the rest of our command sequence.  Otherwise,
            %% keep processing as normal.
            case lists:keytake(NextM, 3, M_vec) of
                {value, Msfar, M_vec2} ->
                    [K|make_reply_seq2(lists:sort([Msfar|Krest]),
                                         M_vec2, NextM + 1)];
                _ ->
                    [K|make_reply_seq2(Krest, M_vec, NextM)]
            end
    end.
