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

%% @doc Manages `riak_cs_access_archiver' workers that archive
%% `riak_cs_access_logger' data to Riak for later processing.
%%
-module(riak_cs_access_archiver_manager).

-behaviour(gen_server).

%% API
-export([start_link/0, archive/2]).
-export([status/1]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include("riak_cs.hrl").

-define(DEFAULT_MAX_BACKLOG, 2). % 2 ~= forced and next
-define(DEFAULT_MAX_ARCHIVERS, 2).

-record(state,
        {
          max_workers, %% max number of living worker processes
          workers=[], %% list of worker pids
          max_backlog, %% max number of entries to allow in backlog
          backlog=[] %% tables that need to be archived when we're done
        }).

%%%===================================================================
%%% API
%%%===================================================================

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%% @doc Interface for `riak_cs_access_logger' to transfer aggregation
%% table.  When this function is finished, the caller should assume
%% that the referenced `Table' will be deleted.
-spec archive(term(), term()) -> term().
archive(Table, Slice) ->
    case gen_server:call(?MODULE, archive, infinity) of
        {ok, Pid} ->
            try
                ets:give_away(Table, Pid, Slice)
            catch error:badarg ->

                    _ = lager:error("~p was not available, access stats for ~p lost",
                                    [?MODULE, Slice]),
                    %% if the archiver had been alive just now, but crashed
                    %% during operation, the stats also would have been lost,
                    %% so also losing them here is just an efficient way to
                    %% work around accidental memory bloat via repeated
                    %% archiver error; TODO: using disk instead of memory
                    %% storage for access stats may be better protection
                    %% against memory bloat
                    ets:delete(Table),
                    false %% opposite of ets:give_away/3 success
            end;
        _ ->
            _ = lager:error("~p was not available, access stats for ~p lost",
                            [?MODULE, Slice]),
            ets:delete(Table),
            false
    end.

%% @doc Find out what the archiver is up to.  Should return `{ok,
%% State, Props}' where `State' is `idle' or `archiving' if the
%% archiver had time to respond, or `busy' if the request timed out.
%% `Props' will include additional details about what the archiver is
%% up to.  May also return `{error, Reason}' if something else went
%% wrong.
status(Timeout) ->
    case catch gen_server:call(?MODULE, status, Timeout) of
        {ok, Props} ->
            {ok, Props};
        {'EXIT', {timeout,_}} ->
            %% if the response times out, the number of logs waiting
            %% to be archived (including the currently archiving one)
            %% should be roughly equal to the number of messages in
            %% the process's mailbox (modulo status query messages),
            %% since that's how ETS tables are transfered.
            [{message_queue_len, MessageCount}] =
                process_info(whereis(?MODULE), [message_queue_len]),
            {error, {busy, MessageCount}};
        {'EXIT', {Reason,_}} ->
            {error, Reason}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

init([]) ->
    process_flag(trap_exit, true),
    %% @TODO Move this into `riak_cs_config' once it is merged in.
    MaxWorkers = case application:get_env(
                        riak_cs, access_archiver_max_workers) of
                     {ok, Workers} when is_integer(Workers) -> Workers;
                     _ ->
                         _ = lager:warning(
                               "access_archiver_max_backlog was unset or"
                               " invalid; overriding with default of ~b",
                               [?DEFAULT_MAX_ARCHIVERS]),
                         ?DEFAULT_MAX_ARCHIVERS
                 end,
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
    {ok, #state{max_workers=MaxWorkers,
                max_backlog=MaxBacklog}}.

handle_call(status, _From, State=#state{backlog=Backlog, workers=Workers}) ->
    Props = [{backlog, length(Backlog)},
             {workers, Workers}],
        {reply, {ok, Props}, State};
handle_call({archive, _, _}, _From, State=#state{workers=Workers,
                                                 max_workers=MaxWorkers})
  when length(Workers) >= MaxWorkers ->
    %% All workers are busy so the manager takes ownership and adds an
    %% entry to the backlog
    {reply, {ok, self()}, State};
handle_call(archive, _From, State=#state{workers=Workers}) ->
    LinkRes = riak_cs_access_archiver:start_link(),
    UpdWorkers = case LinkRes of
                     {ok, Pid} ->
                         [Pid | Workers];
                     _ ->
                         Workers
                 end,
    {reply, LinkRes, State#state{workers=UpdWorkers}};
handle_call(stop, _From, State) ->
    {stop, normal, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, _Reason}, State=#state{workers=Workers}) ->
    {noreply, State#state{workers=lists:delete(Pid, Workers)}};
handle_info({'ETS-TRANSFER', Table, _From, Slice}, State) ->
    Backlog = State#state.backlog,
    NewState =
        case length(Backlog) < State#state.max_backlog of
            true ->
                %% room in the backlog; just add to it
                State#state{backlog=Backlog++[{Table, Slice}]};
            false ->
                %% too much in the backlog, drop the first item in the backlog
                [{_DropTable, DropSlice}|RestBacklog] = Backlog,
                ok = lager:error("Skipping archival of accesses ~p to"
                                 " catch up on backlog",
                                 [DropSlice]),
                 State#state{backlog=RestBacklog++[{Table, Slice}]}
        end,
    {noreply, NewState};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #state{backlog=[]}) ->
    ok;
terminate(_Reason, _State) ->
    _ = lager:warning("Access archiver manager stopping with a backlog;"
                      " logs will be dropped"),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
