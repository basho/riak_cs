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

-behaviour(gen_server).

%% API
-export([start_link/0, archive/2]).
-export([status/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-include("riak_moss.hrl").

-define(SERVER, ?MODULE). 

-record(state, {}).

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
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

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

%% @doc Find out what the archiver is up to.  Should return `{ok, N}`
%% where N is the number of logs waiting to be archived.
status(Timeout) ->
    case catch gen_server:call(?SERVER, status, Timeout) of
        idle ->
            %% the server won't respond while it's archiving, so if it
            %% does respond, we know there's nothing to archive
            {ok, 0};
        {'EXIT',{timeout,_}} ->
            %% if the response times out, the number of logs waiting
            %% to be archived (including the currently archiving one)
            %% should be roughly equal to the number of messages in
            %% the process's mailbox (modulo status query messages),
            %% since that's how ETS tables are transfered.
            [{message_queue_len, MessageCount}] =
                process_info(whereis(?SERVER), [message_queue_len]),
            {ok, MessageCount};
        {'EXIT',{Reason,_}} ->
            {error, Reason}
    end.

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init([]) ->
    rts:check_bucket_props(?ACCESS_BUCKET),
    {ok, #state{}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(status, _From, State) ->
    %% this is used during a request to flush the current access log;
    %% therefore, if we have time to read this message, we are idle
    {reply, idle, State};
handle_call(_Request, _From, State) ->
    Reply = ok,
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(_Msg, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info({'ETS-TRANSFER', Table, _From, Slice}, State) ->
    do_archive(Table, Slice),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Fold the data stored in the table, and store it in Riak, then
%% delete the table.  Each user referenced in the table generates one
%% Riak object.
do_archive(Table, Slice) ->
    case riak_moss_utils:riak_connection() of
        {ok, Riak} ->
            archive_user(ets:first(Table), Riak, Table, Slice),
            riakc_pb_socket:stop(Riak);
        {error, Reason} ->
            lager:error("Access archiver connection to Riak failed (~p), "
                        "stats for ~p were lost",
                        [Reason, Slice])
    end,
    ets:delete(Table).

archive_user('$end_of_table', _, _, _) ->
    ok;
archive_user(User, Riak, Table, Slice) ->
    Accesses = [ A || {_, A} <- ets:lookup(Table, User) ],
    Record = riak_moss_access:make_object(User, Accesses, Slice),
    store(User, Riak, Record, Slice),
    archive_user(ets:next(Table, User), Riak, Table, Slice).

store(User, Riak, Record, Slice) ->
    %% TODO: whole-archive timeout, so we don't get a backup
    %% from each put being relatively fast, but the sum being
    %% longer than the period
    case riakc_pb_socket:put(Riak, Record) of
        ok ->
            lager:debug("Archived access stats for ~s ~p",
                        [User, Slice]);
        {error, Reason} ->
            lager:error("Access archiver storage failed (~p), "
                        "stats for ~s ~p:~p were lost",
                        [Reason, User, Slice])
    end.

