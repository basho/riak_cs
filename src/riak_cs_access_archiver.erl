%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

%% @doc Archives riak_cs_access_logger data to Riak for later
%% processing.
%%
%% Data to be archived is given to this process by means of an ETS
%% transfer.  The `riak_cs_access_logger' accumulates accesses for a
%% time slice into a private ETS table, then ships that ETS table to
%% this process when it rolls over to a new one.  This process does
%% the archiving and then deletes the table.
-module(riak_cs_access_archiver).

-behaviour(gen_fsm).

%% API
-export([start_link/0, archive/2]).
-export([status/1]).

%% gen_fsm callbacks
-export([init/1,
         check_bucket_props/2, check_bucket_props/3,
         idle/2, idle/3,
         archiving/2, archiving/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-include("riak_cs.hrl").

-define(SERVER, ?MODULE).
-define(DEFAULT_MAX_BACKLOG, 2). % 2 ~= forced and next

-record(state,
        {
          riak,      %% the riak connection we're using
          mon,       %% the monitor watching our riak connection
          table,     %% the logger table being archived
          slice,     %% the logger slice being archived
          next,      %% the next entry in the table to archive
          max_backlog, %% max number of entries to allow in backlog
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

%% @doc Interface for riak_cs_access_logger to transfer aggregation
%% table.  When this function is finished, the caller should assume
%% that the referenced `Table' will be deleted.
archive(Table, Slice) ->
    try
        ets:give_away(Table, whereis(?SERVER), Slice)
    catch error:badarg ->
            _ = lager:error("~p was not available, access stats for ~p lost",
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
%% State, Props}' where `State' is `idle' or `archiving' if the
%% archiver had time to respond, or `busy' if the request timed out.
%% `Props' will include additional details about what the archiver is
%% up to.  May also return `{error, Reason}' if something else went
%% wrong.
status(Timeout) ->
    case catch gen_fsm:sync_send_all_state_event(?SERVER, status, Timeout) of
        {State, Props} when State == idle;
                            State == archiving;
                            State == check_bucket_props ->
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
    MaxBacklog = case application:get_env(
                        riak_cs, access_archiver_max_backlog) of
                     {ok, MB} when is_integer(MB) -> MB;
                     _ ->
                         _ = lager:warning(
                               "access_archiver_max_backlog was unset or"
                               " invalid; overriding with default of ~b",
                               [?DEFAULT_MAX_BACKLOG]),
                         ?DEFAULT_MAX_BACKLOG
                 end,
    gen_fsm:send_event(?SERVER, check),
    {ok, check_bucket_props, #state{max_backlog=MaxBacklog}}.

check_bucket_props(check, State) ->
    case riak_cs_access_archiver_sup:start_client() of
        {ok, Riak} ->
            case rts:check_bucket_props(?ACCESS_BUCKET, Riak) of
                ok ->
                    Mon = erlang:monitor(process, Riak),
                    maybe_idle(State#state{riak=Riak, mon=Mon});
                 _Error ->
                    ok = riak_cs_access_archiver_sup:stop_client(),
                    maybe_idle(State)
            end;
        {error, Reason} ->
            _ = lager:warning("Unable to verify ~s bucket settings (~p).",
                              [?ACCESS_BUCKET, Reason]),
            maybe_idle(State)
    end;
check_bucket_props(_Request, State) ->
    {next_state, check_bucket_props, State}.

idle(_Request, State) ->
    {next_state, idle, State, hibernate}.

archiving(continue, #state{next='$end_of_table', table=OldTable}=State) ->
    ets:delete(OldTable),
    maybe_idle(State#state{table=undefined, slice=undefined, next=undefined});
archiving(continue, #state{riak=Riak, table=Table,
                           next=Key, slice=Slice}=State) ->
    case archive_user(Key, Riak, Table, Slice) of
        retry ->
            %% wait for 'DOWN' to reconnect client,
            %% then retry this user
            continue(State);
        _ ->
            %% both ok and error move us on to the next user
            Next = ets:next(Table, Key),
            continue(State#state{next=Next})
    end;
archiving(_Request, State) ->
    {next_state, archiving, State}.

check_bucket_props(_Request, _From, State) ->
    {reply, ok, check_bucket_props, State}.

idle(_Request, _From, State) ->
    {reply, ok, idle, State, hibernate}.

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

handle_info({'ETS-TRANSFER', Table, _From, Slice}, _StateName, State) ->
    Backlog = State#state.backlog++[{Table, Slice}],
    case length(Backlog) =< State#state.max_backlog of
        true ->
            %% room in the backlog; leave things be
            NewState = State#state{backlog=Backlog};
        false when State#state.table == undefined ->
            %% too much in the backlog, and we're not working on
            %% anything; drop the first item in the backlog
            [{DropTable, DropSlice}|RestBacklog] = Backlog,
            ets:delete(DropTable),
            ok = lager:error("Skipping archival of accesses ~p to"
                             " catch up on backlog",
                             [DropSlice]),
            NewState = State#state{backlog=RestBacklog};
        false ->
            %% too much in the backlog, drop the work under way to
            %% make room
            ets:delete(State#state.table),
            ok = lager:error("Skipping partial archival of accesses ~p to"
                             " catch up on backlog",
                             [State#state.slice]),
            NewState = State#state{backlog=Backlog,
                                   table=undefined,
                                   slice=undefined,
                                   next=undefined}
    end,
    maybe_idle(NewState);
handle_info({'DOWN', Mon, process, Riak, _Reason},
            _StateName,
            #state{riak=Riak, mon=Mon}=State) ->
    case riak_cs_access_archiver_sup:start_client() of
        {ok, NewRiak} ->
            NewMon = erlang:monitor(process, Riak),
            maybe_idle(State#state{riak=NewRiak, mon=NewMon});
        {error, Reason} ->
            case State#state.table of
                undefined -> ok;
                Table ->
                    ets:delete(Table),
                    ok = lager:error(
                           "Access archiver connection to Riak failed"
                           " (~p), stats for ~p were lost",
                           [Reason, State#state.slice])
            end,
            maybe_idle(
              State#state{riak=undefined, mon=undefined,
                          table=undefined, slice=undefined, next=undefined})
    end;
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, idle, _State) ->
    ok;
terminate(_Reason, _StateName, _State) ->
    _ = lager:warning("Access archiver stopping with work left to do;"
                      " logs will be dropped"),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Decide whether to go idle or start processing a log.  If going
%% idle, disconnect the riak client (if one is open).  If starting
%% processing, connect the riak client (if it's not already open).
maybe_idle(#state{table=T}=State) when T =/= undefined ->
    {next_state, archiving, State};
maybe_idle(#state{backlog=[_|_], riak=undefined}=State) ->
    %% things to do, but no client to do them with

    %% this does not check out a worker from the riak
    %% connection pool; instead it creates a fresh new worker,
    %% the idea being that we don't want to foul up the access
    %% archival just because the pool is empty
    case riak_cs_access_archiver_sup:start_client() of
        {ok, Riak} ->
            Mon = erlang:monitor(process, Riak),
            maybe_idle(State#state{riak=Riak, mon=Mon});
        {error, Reason} ->
            [{Table,Slice}|Backlog] = State#state.backlog,
            ets:delete(Table),
            ok = lager:error(
                   "Access archiver connection to Riak failed (~p), "
                   "stats for ~p were lost",
                   [Reason, Slice]),
            maybe_idle(State#state{backlog=Backlog})
    end;
maybe_idle(#state{backlog=[{Table, Slice}|Backlog]}=State) ->
    %% things to do, and we already have a client
    continue(start_archiving(Table, Slice, State#state{backlog=Backlog}));
maybe_idle(#state{backlog=[], riak=Riak, mon=Mon}=State)
  when Riak /= undefined ->
    %% nothing to do, but we're holding a client
    erlang:demonitor(Mon, [flush]),
    ok = riak_cs_access_archiver_sup:stop_client(),
    {next_state, idle, State#state{riak=undefined, mon=undefined}, hibernate};
maybe_idle(State) ->
    %% nothing to do, no client
    {next_state, idle, State, hibernate}.

%% @doc Fold the data stored in the table, and store it in Riak, then
%% delete the table.  Each user referenced in the table generates one
%% Riak object.
start_archiving(Table, Slice, State) ->
    Next = ets:first(Table),
    State#state{next=Next, table=Table, slice=Slice}.

continue(State) ->
    gen_fsm:send_event(?SERVER, continue),
    {next_state, archiving, State}.

archive_user(User, Riak, Table, Slice) ->
    Accesses = [ A || {_, A} <- ets:lookup(Table, User) ],
    Record = riak_cs_access:make_object(User, Accesses, Slice),
    store(User, Riak, Record, Slice).

store(User, Riak, Record, Slice) ->
    case catch riakc_pb_socket:put(Riak, Record) of
        ok ->
            ok = lager:debug("Archived access stats for ~s ~p",
                             [User, Slice]);
        {error, Reason} ->
            ok = lager:error("Access archiver storage failed (~p), "
                             "stats for ~s ~p were lost",
                             [Reason, User, Slice]),
            {error, Reason};
        {'EXIT', {noproc, _}} ->
            %% just haven't gotten the 'DOWN' yet
            retry
    end.
