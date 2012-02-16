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

start_link() ->
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, [], []).

status() ->
    gen_fsm:sync_send_event(?SERVER, status).

start_batch() ->
    gen_fsm:sync_send_event(?SERVER, start_batch, infinity).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%% @doc Read the storage schedule and go to idle.
init([]) ->
    Schedule = read_storage_schedule(),
    {ok, idle, #state{schedule=Schedule}}.

%% Asynchronous events

%% @doc Transitions out of idle are all synchronous events
idle(_, State) ->
    {next_state, idle, State}.

%% @doc Async transitions from calculating are all due to messages the
%% FSM sends itself, in order to have opportunities to handle messages
%% from the outside world (like `status').
calculating(continue, #state{batch=[]}=State) ->
    %% finished with this batch
    lager:info("Finished storage calculation in ~b seconds.",
               [elapsed(State#state.batch_start)]),
    riak_moss_utils:close_riak_connection(State#state.riak),
    {next_state, idle, State#state{riak=undefined}};
calculating(continue, State) ->
    %% more to do yet
    NewState = calculate_next_user(State),
    gen_fsm:send_event(?SERVER, continue),
    {next_state, calculating, NewState};
calculating(_, State) ->
    {next_state, calculating, State}.

%% Synchronous events

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

%% @doc there are no all-state events for this fsm
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%% @doc there are no all-state events for this fsm
handle_sync_event(_Event, _From, StateName, State) ->
    Reply = ok,
    {reply, Reply, StateName, State}.

%% @doc this fsm does not expect to receive any non-event messages
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%% @doc TODO: log warnings if this fsm is asked to terminate in the
%% middle of running a calculation
terminate(_Reason, _StateName, _State) ->
    ok.

%% @doc this fsm has no special upgrade process
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

store_user(#state{riak=Riak}, User, BucketList, Start, End) ->
    Obj = riak_moss_storage:make_object(User, BucketList, Start, End),
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
                                         
            
