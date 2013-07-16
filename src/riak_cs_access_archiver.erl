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
          riak,      %% the riak connection we're using
          mon,       %% the monitor watching our riak connection
          table,     %% the logger table being archived
          slice,     %% the logger slice being archived
          next      %% the next entry in the table to archive
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
    gen_fsm:start_link({local, ?MODULE}, ?MODULE, [], []).

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
                process_info(whereis(?MODULE), [message_queue_len]),
            {ok, busy, [{message_queue_len, MessageCount}]};
        {'EXIT',{Reason,_}} ->
            {error, Reason}
    end.

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init([]) ->
    {ok, Riak} = riak_connection(),
    Mon = erlang:monitor(process, Riak),
    {ok, idle, #state{riak=Riak, mon=Mon}}.

idle(_Request, State) ->
    {next_state, idle, State}.

archiving(continue, #state{next='$end_of_table'}=State) ->
    {stop, normal, State};
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
handle_info({'DOWN', _Mon, process, _Riak, _Reason}, StateName, State) ->
    {ok, NewRiak} = riak_connection(),
    NewMon = erlang:monitor(process, NewRiak),
    gen_fsm:send_event(?MODULE, continue),
    {next_state, StateName, State#state{riak=NewRiak, mon=NewMon}};
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(Reason, StateName, #state{table=Table,
                                riak=Riak,
                                mon=Mon})
  when Reason =:= normal; StateName =:= idle ->
    cleanup(Table, Riak, Mon),
    ok;
terminate(_Reason, _StateName, #state{table=Table,
                                      riak=Riak,
                                      mon=Mon}) ->
    _ = lager:warning("Access archiver stopping with work left to do;"
                      " logs will be dropped"),
    cleanup(Table, Riak, Mon),
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

riak_connection() ->
    {Host, Port} = riak_host_port(),
    Timeout = case application:get_env(riak_cs, riakc_connect_timeout) of
                  {ok, ConfigValue} ->
                      ConfigValue;
                  undefined ->
                      10000
              end,
    StartOptions = [{connect_timeout, Timeout},
                    {auto_reconnect, true}],
    riakc_pb_socket:start_link(Host, Port, StartOptions).

%% @TODO This is duplicated code from
%% `riak_cs_riakc_pool_worker'. Move this to `riak_cs_config' once
%% that has been merged.
-spec riak_host_port() -> {string(), pos_integer()}.
riak_host_port() ->
    case application:get_env(riak_cs, riak_ip) of
        {ok, Host} ->
            ok;
        undefined ->
            Host = "127.0.0.1"
    end,
    case application:get_env(riak_cs, riak_pb_port) of
        {ok, Port} ->
            ok;
        undefined ->
            Port = 8087
    end,
    {Host, Port}.

cleanup(Table, Pid, Mon) ->
    cleanup_table(Table),
    cleanup_monitor(Mon),
    cleanup_socket(Pid).

cleanup_table(undefined) ->
    ok;
cleanup_table(Table) ->
    ets:delete(Table).

cleanup_monitor(undefined) ->
    ok;
cleanup_monitor(Mon) ->
    erlang:demonitor(Mon, [flush]).

cleanup_socket(undefined) ->
    ok;
cleanup_socket(Pid) ->
    riakc_pb_socket:stop(Pid).

continue(State) ->
    gen_fsm:send_event(?MODULE, continue),
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
