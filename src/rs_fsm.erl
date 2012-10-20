%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(rs_fsm).

-behaviour(gen_fsm).

%% API
-export([write/7, write/9]).
-export([start_write/9]).
-export([get_local_riak_client/0, free_local_riak_client/1]).

%% gen_fsm callbacks
-export([init/1, prepare_write/2, prepare_write/3, handle_event/3,
         handle_sync_event/4, handle_info/3, terminate/3, code_change/4]).
-export([t0/0]).

-define(ALG_LIBER8TION_V0, 'liber8tion0').

-record(state, {
          caller :: pid(),
          mode :: 'read' | 'write',
          alg :: 'liber8tion0',
          k :: pos_integer(),
          m :: pos_integer(),
          rbucket :: binary(),
          rsuffix :: binary(),
          tref :: undefined | reference(),
          get_client_fun :: fun(),
          free_client_fun :: fun(),
          riak_client :: undefined | term()
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
%%   * ?ALG_LIBER8TION_V0 will use w=8 *and* it will assume a very
%%     particular version of the NIF-to-be's implementation of
%%     the liber8tion algorithm.  For example, if the NIF can't
%%     provide that exact version, then we must fail.  Paranoia!
%%
%% @spec start() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
write(Alg, K, M, RBucket, RSuffix, Data, Timeout) ->
    write(Alg, K, M, RBucket, RSuffix, Data, Timeout,
          fun get_local_riak_client/0, fun free_local_riak_client/1).

write(Alg, K, M, RBucket, RSuffix, Data, Timeout,
      GetClientFun, FreeClientFun) ->
    {ok, Pid} = start_write(Alg, K, M, RBucket, RSuffix, Data, Timeout,
                            GetClientFun, FreeClientFun),
    wait_for_reply(Pid, Timeout).

start_write(Alg, K, M, RBucket, RSuffix, Data, Timeout,
            GetClientFun, FreeClientFun) ->
    gen_fsm:start(
      ?MODULE, {write, Alg, K, M, RBucket, RSuffix, Data, Timeout, self(),
                GetClientFun, FreeClientFun}, []).

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
init({write, Alg, K, M, RBucket, RSuffix, Data, Timeout, Caller,
      GetClientFun, FreeClientFun})
    when is_integer(K) andalso K > 0 andalso
         is_integer(M) andalso M > 0 andalso
         is_binary(RBucket) andalso is_binary(RSuffix) andalso
         is_binary(Data) ->
    Alg = ?ALG_LIBER8TION_V0,

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
                               tref = TRef,
                               get_client_fun = GetClientFun,
                               free_client_fun = FreeClientFun}, 0}.

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
    {ok, Client} = (S#state.get_client_fun)(),
    send_reply(S#state.caller, boom),
    {stop, normal, S#state{riak_client = Client}}.
    %% {next_state, prepare_write, S}.

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
    Reply = ok,
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
    (S#state.free_client_fun)(S#state.riak_client),
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

send_reply(Pid, Reply) ->
    Pid ! {?MODULE, reply, Reply}.

%% Note that we assume that the pid we're waiting for is going to
%% do the right thing wrt timeouts.

wait_for_reply(Pid, Timeout0) ->
    Timeout = if Timeout0 == infinity -> infinity;
                 true                 -> Timeout0 + 200
              end,
    WRef = monitor_pid(Pid),
    try
        receive
            {?MODULE, reply, Reply} ->
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

t0() ->
    write(?ALG_LIBER8TION_V0, 3, 2, <<"rb">>, <<"rs">>, <<"data">>, 500).
