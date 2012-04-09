%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Archives riak_moss_access_logger data to Riak for later
%% processing.
%%
%% Data to be archived is given to this process by means of an ETS
%% transfer.  The `riak_moss_access_logger' accumulates accesses for a
%% time slice into a private ETS table, then ships that ETS table to
%% this process when it rolls over to a new one.  This process does
%% the archiving and then deletes the table.
-module(riak_moss_access_archiver).

-behaviour(gen_fsm).

%% API
-export([start_link/0, archive/2]).
-export([status/1]).

%% gen_fsm callbacks
-export([init/1,
         idle/2, idle/3,
         archiving/2, archiving/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-include("riak_moss.hrl").

-define(SERVER, ?MODULE). 

-record(state,
        {
          riak,      %% the riak connection we're using
          table,     %% the logger table being archived
          slice,     %% the logger slice bieng archived
          next,      %% the next entry in the table to archive
          backlog=[] %% tables that need to be archived when we're done
        }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_fsm:start_link({local, ?SERVER}, ?MODULE, [], []).

%% @doc Interface for riak_moss_access_logger to transfer aggregation
%% table.  When this function is finished, the caller should assume
%% that the referenced `Table' will be deleted.
archive(Table, Slice) ->
    try 
        %% TODO: might be a good idea to check archiver mailbox
        %% backlog here, in case stats logging is taking much longer
        %% than expected
        ets:give_away(Table, whereis(?SERVER), Slice)
    catch error:badarg ->
            lager:error("~p was not available, access stats for ~p lost",
                        [?SERVER, Slice]),
            %% if the archiver had been alive just now, but crashed
            %% during operation, the stats also would have been lost,
            %% so also losing them here is just an efficient way to
            %% work around accidental memory bloat via repeated
            %% archiver error; TODO: using disk instead of memory
            %% storage for access stats may be better protection
            %% against memory bloat
            ets:delete(Table),
            false %% opposite of ets:give_away/3 success
    end.

%% @doc Find out what the archiver is up to.  Should return `{ok,
%% State, Props}` where `State` is `idle` or `archiving` if the
%% archiver had time to respond, or `busy` if the request timed out.
%% `Props` will include additional details about what the archiver is
%% up to.
status(Timeout) ->
    case catch gen_fsm:sync_send_all_state_event(?SERVER, status, Timeout) of
        {State, Props} when State == idle; State == archiving ->
            {ok, State, Props};
        {'EXIT',{timeout,_}} ->
            %% if the response times out, the number of logs waiting
            %% to be archived (including the currently archiving one)
            %% should be roughly equal to the number of messages in
            %% the process's mailbox (modulo status query messages),
            %% since that's how ETS tables are transfered.
            [{message_queue_len, MessageCount}] =
                process_info(whereis(?SERVER), [message_queue_len]),
            {ok, busy, [{message_queue_len, MessageCount}]};
        {'EXIT',{Reason,_}} ->
            {error, Reason}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    rts:check_bucket_props(?ACCESS_BUCKET),
    {ok, idle, #state{}}.

idle(_Request, State) ->
    {next_state, idle, State}.

archiving(continue, #state{next='$end_of_table', table=OldTable,
                           backlog=Backlog, riak=Riak}=State) ->
    ets:delete(OldTable),
    case Backlog of
        [{Table,Slice}|RestBacklog] ->
            %% start working on the pile we were ignoring
            start_archiving(Table, Slice,
                            State#state{backlog=RestBacklog});
        [] ->
            %% nothing to do, rest
            riak_moss_riakc_pool_worker:stop(Riak),
            {next_state, idle, State#state{riak=undefined,
                                           table=undefined,
                                           slice=undefined,
                                           next=undefined}}
    end;
archiving(continue, #state{riak=Riak, table=Table,
                           next=Key, slice=Slice}=State) ->
    archive_user(Key, Riak, Table, Slice),
    Next = ets:next(Table, Key),
    continue(State#state{next=Next});
archiving(_Request, State) ->
    {next_state, archiving, State}.

idle(_Request, _From, State) ->
    {reply, ok, idle, State}.

archiving(_Request, _From, State) ->
    {reply, ok, archiving, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(status, _From, StateName,
                  #state{slice=Slice, backlog=Backlog}=State) ->
    Props = [{backlog, length(Backlog)}]
        ++[{slice, Slice} || Slice /= undefined],
    {reply, {StateName, Props}, StateName, State};
handle_sync_event(_Event, _From, StateName, State) ->
    {reply, ok, StateName, State}.

handle_info({'ETS-TRANSFER', Table, _From, Slice}, StateName, State) ->
    case StateName of
        idle ->
            %% this does not check out a worker from the riak
            %% connection pool; instead it creates a fresh new worker,
            %% the idea being that we don't want to foul up the access
            %% archival just because the pool is empty; pool workers
            %% just happen to be literally the socket process, so
            %% "starting" one here is the same as opening a
            %% connection, and avoids duplicating the configuration
            %% lookup code
            case riak_moss_riakc_pool_worker:start_link([]) of
                {ok, Riak} ->
                    start_archiving(Table, Slice, State#state{riak=Riak});
                {error, Reason} ->
                    lager:error(
                      "Access archiver connection to Riak failed (~p), "
                      "stats for ~p were lost",
                      [Reason, Slice]),
                    {next_state, idle, State}
            end;
        _ ->
            %% TODO: maybe trigger a timeout of the in-progress archival
            Backlog = State#state.backlog++[{Table, Slice}],
            NewState = State#state{backlog=Backlog},
            {next_state, StateName, NewState}
    end;
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, idle, _State) ->
    ok;
terminate(_Reason, _StateName, _State) ->
    lager:warn("Access archiver stopping with work left to do;"
               " logs will be dropped"),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Fold the data stored in the table, and store it in Riak, then
%% delete the table.  Each user referenced in the table generates one
%% Riak object.
start_archiving(Table, Slice, State) ->
    Next = ets:first(Table),
    continue(State#state{next=Next, table=Table, slice=Slice}).

continue(State) ->
    gen_fsm:send_event(?SERVER, continue),
    {next_state, archiving, State}.

archive_user(User, Riak, Table, Slice) ->
    Accesses = [ A || {_, A} <- ets:lookup(Table, User) ],
    Record = riak_moss_access:make_object(User, Accesses, Slice),
    store(User, Riak, Record, Slice).

store(User, Riak, Record, Slice) ->
    %% TODO: whole-archive timeout, so we don't get a backup
    %% from each put being relatively fast, but the sum being
    %% longer than the period
    case riakc_pb_socket:put(Riak, Record) of
        ok ->
            lager:debug("Archived access stats for ~s ~p",
                        [User, Slice]);
        {error, Reason} ->
            %% TODO: check for "disconnected" explicitly & attempt reconnect?
            lager:error("Access archiver storage failed (~p), "
                        "stats for ~s ~p:~p were lost",
                        [Reason, User, Slice])
    end.

