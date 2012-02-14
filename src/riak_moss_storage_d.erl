%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc The daemon that calculates moss storage on the configured
%% schedule.

-module(riak_moss_storage_d).

-behaviour(gen_fsm).

%% API
-export([start_link/0,
         status/0,
         start_batch/0,
         read_storage_schedule/0]).

%% gen_fsm callbacks
-export([init/1,

         idle/2, idle/3,
         calculating/2, calculating/3,

         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-include("riak_moss.hrl").

-define(SERVER, ?MODULE).

-record(state, {
          schedule,      %% the times that archives are stored
          riak,          %% client we're currently using
          target,        %% the ideal time the batch would start
          batch_start,   %% the time we started 
          batch_count=0, %% count of users processed so far
          batch=[]       %% users left to process in this batch
         }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, [], []).

status() ->
    gen_fsm:sync_send_event(?SERVER, status).

start_batch() ->
    gen_fsm:sync_send_event(?SERVER, start_batch, infinity).

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
init([]) ->
    Schedule = read_storage_schedule(),
    {ok, idle, #state{schedule=Schedule}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_event/2, the instance of this function with the same
%% name as the current state name StateName is called to handle
%% the event. It is also called if a timeout occurs.
%%
%% @spec state_name(Event, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------

idle(_, State) ->
    {next_state, idle, State}.

calculating(continue, #state{batch=[]}=State) ->
    %% finished with this batch
    lager:info("Finished storage calculation in ~b seconds.",
               [elapsed(State#state.batch_start)]),
    riak_moss_utils:close_connection(State#state.riak),
    {next_state, idle, State#state{riak=undefined}};
calculating(continue, State) ->
    NewState = calculate_next_user(State),
    %% more to do yet
    gen_fsm:send_event(?SERVER, continue),
    {next_state, calculating, NewState};
calculating(_, State) ->
    {next_state, calculating, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_event/[2,3], the instance of this function with
%% the same name as the current state name StateName is called to
%% handle the event.
%%
%% @spec state_name(Event, From, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
idle(status, _From, State) ->
    Props = [{schedule, State#state.schedule}],
    {reply, {ok, {idle, Props}}, idle, State};
idle(start_batch, _From, #state{schedule=Schedule}=State) ->
    BatchStart = calendar:universal_time(),
    Target = target_time(BatchStart, Schedule),

    %% TODO: probably want to do this fetch streaming, to avoid
    %% accidental memory pressure at other points
    {ok, Riak} = riak_moss_utils:riak_connection(),
    Batch = fetch_user_list(Riak),

    NewState = State#state{batch_start=BatchStart,
                           target=Target,
                           riak=Riak,
                           batch=Batch,
                           batch_count=0},

    gen_fsm:send_event(?SERVER, continue),
    {reply, ok, calculating, NewState};
idle(_, _From, State) ->
    {reply, ok, idle, State}.

calculating(status, _From, State) ->
    Props = [{schedule, State#state.schedule},
             {target, State#state.target},
             {elapsed, elapsed(State#state.batch_start)},
             {users_done, State#state.batch_count},
             {users_left, length(State#state.batch)}],
    {reply, {ok, {calculating, Props}}, calculating, State};
calculating(start_batch, _From, State) ->
    {reply, {error, already_calculating}, calculating, State};
calculating(_, _From, State) ->
    {reply, ok, calculating, State}.

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
terminate(_Reason, _StateName, _State) ->
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

%% Returns the schedules sorted such that the next time to start
%% calculation is the head of the list.
read_storage_schedule() ->
    lists:usort(read_storage_schedule1()).

read_storage_schedule1() ->
    case application:get_env(riak_moss, storage_schedule) of
        undefined ->
            lager:warning("No storage schedule defined."),
            [];
        {ok, []} ->
            lager:warning("No storage schedule defined."),
            [];
        {ok, Sched} ->
            case catch parse_time(Sched) of
                {ok, Time} ->
                    %% user provided just one time
                    [Time];
                {'EXIT',_} ->
                    Times = [ {S, catch parse_time(S)} || S <- Sched ],
                    case [ Bad || {_,{'EXIT',_}}=Bad <- Times ] of
                        [] ->
                            %% all times user provided were good
                            [ Parsed || {_, {ok, Parsed}} <- Times];
                        Errors ->
                            lager:error("Bad storage schedule elements ~p,"
                                        " ignoring given schedule.",
                                        [ [S || {S,_} <- Errors] ]),
                            []
                    end
            end
    end.

parse_time({Hour, Min}) when (Hour >= 0 andalso Hour =< 23),
                             (Min >= 0 andalso Min =< 59) ->
    {ok, {Hour, Min}};
parse_time(HHMM) when is_list(HHMM) ->
    case io_lib:fread("~2d~2d", HHMM) of
        {ok, [Hour, Min], []} ->
            %% make sure numeric bounds apply
            parse_time({Hour, Min})
    end.
                            
fetch_user_list(Riak) ->
    case riakc_pb_socket:list_keys(Riak, ?USER_BUCKET) of
        {ok, Users} -> Users;
        {error, Error} ->
            lager:error("Storage calculator was unable"
                        " to fetch list of users (~p)",
                       [Error]),
            []
    end.

calculate_next_user(#state{riak=Riak,
                           batch=[User|Rest]}=State) ->
    Start = calendar:universal_time(),
    case riak_moss_storage:sum_user(Riak, User) of
        {ok, BucketList} ->
            End = calendar:universal_time(),
            store_user(State, User, BucketList, Start, End);
        {error, Error} ->
            lager:error("Error computing storage for user ~s (~p)", 
                        [User, Error])
    end,
    State#state{batch=Rest, batch_count=1+State#state.batch_count}.

store_user(#state{riak=Riak, target=Time},
           User, BucketList, Start, End) ->
    Obj = riak_moss_storage:make_object(Time, User, BucketList, Start, End),
    case riakc_pb_socket:put(Riak, Obj) of
        ok -> ok;
        {error, Error} ->
            lager:error("Error storing storage for user ~s (~p)",
                        [User, Error])
    end.

elapsed(Time) ->
    calendar:datetime_to_gregorian_seconds(calendar:universal_time())
        -calendar:datetime_to_gregorian_seconds(Time).

target_time({Today,{BSH, BSM,_}}=BatchStart, Schedule) ->
    case {BSH, BSM} < hd(Schedule) of
        true ->
            %% this batch was meant to start yesterday
            {Yesterday,_} = calendar:gregorian_seconds_to_datetime(
                              calendar:datetime_to_gregorian_seconds(
                                BatchStart)-24*60*60),
            [{H, M}|_] = Schedule,
            {Yesterday, {H, M, 0}};
        false ->
            %% find the latest time that is still before BatchStart
            [{H, M}|_] = [ {H, M} || {H, M} <- lists:reverse(Schedule),
                                     H < BSH orelse
                                               (H == BSH andalso M < BSM) ],
            {Today, {H, M, 0}}
    end.
                                         
            
