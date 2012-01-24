%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Module to manage storage of objects and files

-module(riak_moss_delete_fsm).

-behaviour(gen_fsm).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Test API
-export([test_link/4,
         current_state/1]).

-endif.

%% API
-export([start_link/3,
         send_event/2]).

%% gen_fsm callbacks
-export([init/1,
         initialize/2,
         waiting_file_info/2,
         waiting_root_update/2,
         waiting_blocks_delete/2,
         waiting_root_delete/2,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-record(state, {bucket :: binary(),
                filename :: binary(),
                block_count :: non_neg_integer(),
                blocks_remaining :: non_neg_integer(),
                deleter_pid :: undefined | pid(),
                waiter :: undefined | {reference(), pid()},
                timeout :: timeout()}).
-type state() :: #state{}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Start a `riak_moss_delete_fsm'.
-spec start_link(binary(),
                 string(),
                 timeout()) ->
                        {ok, pid()} | ignore | {error, term()}.
start_link(Bucket, Name, Timeout) ->
    Args = [Bucket, Name, Timeout],
    gen_fsm:start_link(?MODULE, Args, []).

%% @doc Send an event to a `riak_moss_delete_fsm'.
-spec send_event(pid(), term()) -> ok.
send_event(Pid, Event) ->
    gen_fsm:send_event(Pid, Event).

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

%% @doc Initialize the fsm.
-spec init([binary() | timeout()]) ->
                  {ok, initialize, state(), 0} |
                  {ok, write_root, state()}.
init([Bucket, Name, Timeout]) ->
    %% @TODO Get rid of this once sure that we can
    %% guarantee file name will be passed in as a binary.
    case is_binary(Name) of
        true ->
            FileName = Name;
        false ->
            FileName = list_to_binary(Name)
    end,

    State = #state{bucket=Bucket,
                   filename=FileName,
                   timeout=Timeout},
    {ok, initialize, State, 0};
init({test, Args, StateProps}) ->
    {ok, initialize, State, 0} = init(Args),
    %% Update the state with entries from StateProps
    Fields = record_info(fields, state),
    FieldPos = lists:zip(Fields, lists:seq(2, length(Fields)+1)),
    ModStateFun = fun({Field, Value}, State0) ->
                          Pos = proplists:get_value(Field, FieldPos),
                          setelement(Pos, State0, Value)
                  end,
    TestState = lists:foldl(ModStateFun, State, StateProps),
    {ok, waiting_file_info, TestState}.

%% @doc First state of the delete fsm
-spec initialize(timeout, state()) ->
                        {next_state, write_root, state(), timeout()} |
                        {stop, term(), state()}.
initialize(timeout, State=#state{bucket=Bucket,
                                 filename=FileName,
                                 timeout=Timeout}) ->
    %% Start the worker to perform the writing
    case start_deleter() of
        {ok, DeleterPid} ->
            link(DeleterPid),
            %% Provide the deleter with the file details
            riak_moss_deleter:initialize(DeleterPid,
                                         self(),
                                         Bucket,
                                         FileName),
            UpdState = State#state{deleter_pid=DeleterPid},
            {next_state, waiting_file_info, UpdState, Timeout};
        {error, Reason} ->
            lager:error("Failed to start the delete fsm deleter process. Reason: ",
                        [Reason]),
            {stop, Reason, State}
    end.

%% @doc State to receive information about the file or object to be
%% deleted from the deleter process.
-spec waiting_file_info({deleter_ready, object |
                         {file, non_neg_integer()} |
                         {error, term()}},
                        state()) ->
                               {next_state,
                                waiting_root_delete | waiting_blocks_delete,
                                state(),
                                timeout()}.
waiting_file_info({deleter_ready, object}, State=#state{deleter_pid=DeleterPid,
                                                        timeout=Timeout}) ->
    %% Send request to the deleter to delete the object
    riak_moss_deleter:delete_root(DeleterPid),
    {next_state, waiting_root_delete, State, Timeout};
waiting_file_info({deleter_ready, {file, BlockCount}},
                  State=#state{deleter_pid=DeleterPid,
                               timeout=Timeout}) ->
    riak_moss_deleter:update_root(DeleterPid, set_inactive),
    UpdState = State#state{block_count=BlockCount,
                           blocks_remaining=BlockCount},
    {next_state, waiting_root_update, UpdState, Timeout};
waiting_file_info({deleter_ready, {error, Reason}},
                  State=#state{deleter_pid=DeleterPid}) ->
    lager:warning("The deletion process encountered an error. Reason: ~p", [Reason]),
    riak_moss_deleter:stop(DeleterPid),
    {stop, normal, State}.

%% @doc State to receive notifcation when the root file object has
%% been updated to mark the file as inactive.
-spec waiting_root_update(root_inactive, state()) ->
                                 {next_state,
                                  waiting_blocks_delete,
                                  state(),
                                  timeout()}.
waiting_root_update(root_inactive, State=#state{block_count=BlockCount,
                                                deleter_pid=DeleterPid,
                                                timeout=Timeout}) ->

    %% Send request to the deleter to delete the file blocks
    %% @TODO Backpressure or concurrency of deletion needed. Currently
    %% all block delete requests will serialize at the `riak_moss_deleter'.
    case BlockCount of
        0 ->
            riak_moss_deleter:delete_root(DeleterPid),
            {next_state, waiting_root_delete, State, Timeout};
        _ ->
            [riak_moss_deleter:delete_block(DeleterPid, BlockID) ||
                BlockID <- lists:seq(0, BlockCount-1)],
            {next_state, waiting_blocks_delete, State, Timeout}
    end.

%% @doc State for waiting for responses about file data blocks
%% being deleted.
-spec waiting_blocks_delete({block_deleted, non_neg_integer()}, state()) ->
                                   {next_state,
                                    waiting_root_delete | waiting_blocks_delete,
                                    state(),
                                    non_neg_integer()}.
waiting_blocks_delete({block_deleted, _},
                      State=#state{blocks_remaining=BlocksRemaining,
                                   deleter_pid=DeleterPid,
                                   timeout=Timeout}) ->
    UpdBlocksRemaining = BlocksRemaining-1,
    UpdState = State#state{blocks_remaining=UpdBlocksRemaining},
    case UpdBlocksRemaining of
        0 ->
            %% All blocks deleted, transition to `waiting_root_delete'.
            %% @TODO The method for tracking compelted blocks could stand
            %% to be more robust.
            riak_moss_deleter:delete_root(DeleterPid),
            {next_state, waiting_root_delete, UpdState, Timeout};
        _ ->
            {next_state, waiting_blocks_delete, UpdState, Timeout}
    end.

%% @doc State for waiting for response about object or file root
%% being deleted.
-spec waiting_root_delete(root_deleted, state()) ->
                                 {stop, normal, state()}.
waiting_root_delete(root_deleted, State=#state{deleter_pid=DeleterPid}) ->
    riak_moss_deleter:stop(DeleterPid),
    {stop, normal, State}.

%% @doc Handle events that should be handled
%% the same regardless of the current state.
-spec handle_event(term(), atom(), state()) ->
                          {stop, badmsg, state()}.
handle_event(_Event, _StateName, State) ->
    {stop, badmsg, State}.

%% @doc Handle synchronous events that should be handled
%% the same regardless of the current state.
-spec handle_sync_event(term(), term(), atom(), state()) ->
                               {reply, term(), atom(), state()} |
                               {next_state, atom(), state()}.
handle_sync_event(current_state, _From, StateName, State) ->
    {reply, StateName, StateName, State};
handle_sync_event(_Event, _From, StateName, State) ->
    {next_state, StateName, State}.

%% @doc @TODO
-spec handle_info(term(), atom(), state()) ->
                         {next_state, atom(), state(), timeout()} |
                         {stop, badmsg, state()}.
handle_info({'EXIT', _Pid, _Reason}, StateName, State=#state{timeout=Timeout}) ->
    {next_state, StateName, State, Timeout};
handle_info({_ReqId, {ok, _Pid}},
            StateName,
            State=#state{timeout=Timeout}) ->
    {next_state, StateName, State, Timeout};
handle_info(_Info, _StateName, State) ->
    {stop, badmsg, State}.

%% @doc Unused.
-spec terminate(term(), atom(), state()) -> ok.
terminate(Reason, _StateName, _State) ->
    Reason.

%% @doc Unused.
-spec code_change(term(), atom(), state(), term()) ->
                         {ok, atom(), state()}.
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%% ====================================================================
%% Internal functions
%% ====================================================================

%% @private
%% @doc Start a `riak_moss_deleter' process to perform the actual work
%% of writing data to Riak.
-spec start_deleter() -> {ok, pid()} | {error, term()}.
start_deleter() ->
    riak_moss_deleter_sup:start_deleter(node(), []).

%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).

%% @doc Start a `riak_moss_delete_fsm' for testing.
-spec test_link([{atom(), term()}],
                binary(),
                string(),
                timeout()) ->
                       {ok, pid()} | ignore | {error, term()}.
test_link(StateProps, Bucket, Name, Timeout) ->
    Args = [Bucket, Name, Timeout],
    gen_fsm:start_link(?MODULE, {test, Args, StateProps}, []).

%% @doc Get the current state of the fsm for testing inspection
-spec current_state(pid()) -> atom() | {error, term()}.
current_state(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, current_state).

-endif.
