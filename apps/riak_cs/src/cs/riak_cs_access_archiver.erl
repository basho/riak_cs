%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
%%               2021 TI Tokyo    All Rights Reserved.
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
-export([start_link/0]).
-export([status/2]).

%% gen_fsm callbacks
-export([init/1,
         idle/2, idle/3,
         archiving/2, archiving/3,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-include("riak_cs.hrl").

-record(state,
        {
          riak_client, %% the riak connection we're using
          mon,         %% the monitor watching our riak connection
          table,       %% the logger table being archived
          slice,       %% the logger slice being archived
          next         %% the next entry in the table to archive
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
    gen_fsm:start_link(?MODULE, [], []).

%% @doc Find out what the archiver is up to.  Should return `{ok,
%% State, Props}' where `State' is `idle' or `archiving' if the
%% archiver had time to respond, or `busy' if the request timed out.
%% `Props' will include additional details about what the archiver is
%% up to.  May also return `{error, Reason}' if something else went
%% wrong.
status(Pid, Timeout) ->
    case catch gen_fsm:sync_send_all_state_event(Pid, status, Timeout) of
        {State, Props} when State == idle;
                            State == archiving ->
            {ok, State, Props};
        {'EXIT',{timeout,_}} ->
            %% if the response times out, the number of logs waiting
            %% to be archived (including the currently archiving one)
            %% should be roughly equal to the number of messages in
            %% the process's mailbox (modulo status query messages),
            %% since that's how ETS tables are transfered.
            [{message_queue_len, MessageCount}] =
                process_info(Pid, [message_queue_len]),
            {ok, busy, [{message_queue_len, MessageCount}]};
        {'EXIT',{Reason,_}} ->
            {error, Reason}
    end.

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init([]) ->
    {ok, RcPid} = start_riak_client(),
    Mon = erlang:monitor(process, RcPid),
    {ok, idle, #state{riak_client=RcPid, mon=Mon}}.

idle(_Request, State) ->
    {next_state, idle, State}.

archiving(continue, #state{next='$end_of_table'}=State) ->
    {stop, normal, State};
archiving(continue, #state{riak_client=RcPid, table=Table,
                           next=Key, slice=Slice}=State) ->
    case archive_user(Key, RcPid, Table, Slice) of
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

idle(_Request, _From, State) ->
    {reply, ok, idle, State}.

archiving(_Request, _From, State) ->
    {reply, ok, archiving, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(status, _From, StateName, #state{slice=Slice}=State) ->
    Props = [{slice, Slice} || Slice /= undefined],
    {reply, {StateName, Props}, StateName, State};
handle_sync_event(_Event, _From, StateName, State) ->
    {reply, ok, StateName, State}.

handle_info({'ETS-TRANSFER', Table, _From, Slice}, _StateName, State) ->
    continue(State#state{next=ets:first(Table), table=Table, slice=Slice});
handle_info({'DOWN', _Mon, process, _RcPid, _Reason}, StateName, State) ->
    {ok, NewRcPid} = start_riak_client(),
    NewMon = erlang:monitor(process, NewRcPid),
    gen_fsm:send_event(self(), continue),
    {next_state, StateName, State#state{riak_client=NewRcPid, mon=NewMon}};
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(Reason, StateName, #state{table=Table,
                                    riak_client=RcPid,
                                    mon=Mon})
  when Reason =:= normal; StateName =:= idle ->
    cleanup(Table, RcPid, Mon),
    ok;
terminate(_Reason, _StateName, #state{table=Table,
                                      riak_client=RcPid,
                                      mon=Mon}) ->
    logger:warning("Access archiver stopping with work left to do;"
                   " logs will be dropped"),
    cleanup(Table, RcPid, Mon),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

start_riak_client() ->
    riak_cs_riak_client:start_link([]).

cleanup(Table, RcPid, Mon) ->
    cleanup_table(Table),
    cleanup_monitor(Mon),
    cleanup_riak_client(RcPid).

cleanup_table(undefined) ->
    ok;
cleanup_table(Table) ->
    ets:delete(Table).

cleanup_monitor(undefined) ->
    ok;
cleanup_monitor(Mon) ->
    erlang:demonitor(Mon, [flush]).

cleanup_riak_client(undefined) ->
    ok;
cleanup_riak_client(RcPid) ->
    riak_cs_riak_client:stop(RcPid).

continue(State) ->
    gen_fsm:send_event(self(), continue),
    {next_state, archiving, State}.

archive_user(User, RcPid, Table, Slice) ->
    Accesses = [ A || {_, A} <- ets:lookup(Table, User) ],
    Record = riak_cs_access:make_object(User, Accesses, Slice),
    store(User, RcPid, Record, Slice).

store(User, RcPid, Record, Slice) ->
    {ok, MasterPbc} = riak_cs_riak_client:master_pbc(RcPid),
    Timeout = riak_cs_config:put_access_timeout(),
    case catch riak_cs_pbc:put(MasterPbc, Record, Timeout, [riakc, put_access]) of
        ok ->
            logger:debug("Archived access stats for ~s ~p", [User, Slice]);
        {error, Reason} ->
            riak_cs_pbc:check_connection_status(MasterPbc,
                                                "riak_cs_access_archiver:store/4"),
            riak_cs_access:flush_access_object_to_log(User, Record, Slice),
            {error, Reason};
        {'EXIT', {noproc, _}} ->
            %% just haven't gotten the 'DOWN' yet
            retry
    end.
