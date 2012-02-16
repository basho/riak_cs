%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Log access to Riak MOSS.  This is where I/O stats are
%% computed and recorded.
%%
%% I/O stats are expected as notes in the webmachine log data, with
%% keys of the form `{access, KEY}'.  I/O stats are only logged if a
%% note is included with the key `user' and a value that is a
%% `#moss_user{}' record.
%%
%% That is, to log I/O stats for a request, call
%%
%% ```
%% wrq:add_note({access, user}, User=#moss_user{}, RD)
%% '''
%%
%% somewhere in your resource.  To add another stat, for instance
%% `gets', add another note:
%%
%% ```
%% wrq:add_note({access, gets}, 1, RD)
%% '''
%%
%% Notes other than `user' are expected to be simple numbers, and all
%% notes for each key for a user will be summed for archival
%% periodically.
%%
%% The stat `bytes_out' is logged automatically from the log data
%% field `bytes'.
%%
%% The archive period is controlled by the `riak_moss' application
%% environment variable `access_archive_period', specified as an
%% integer number of seconds.  Archives are always made at the same
%% time each day (for a given period), to allow for a predictable
%% storage key.
-module(riak_moss_access_logger).

-behaviour(gen_server).
-include_lib("webmachine/src/webmachine_logger.hrl").
-include("riak_moss.hrl").

%% API
-export([start_link/1, log_access/1]).
-export([set_user/2, set_stat/3]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE). 

-record(state, {
          period :: integer(),         %% time between aggregation archivals
          current :: {calendar:datetime(), calendar:datetime()},
                     %% current agg. slice
          archive :: reference(),      %% reference for archive msg
          table :: ets:tid()           %% the table aggregating stats
         }).

-type state() :: #state{}.

%%%===================================================================
%%% Non-server API (Webmachine Notes)
%%%===================================================================

-define(STAT(Name), {access, Name}).

%% @doc Set the MOSS user for this request.  Stats are not recorded if
%% the user is not set.
set_user(#moss_user{}=User, RD) ->
    wrq:add_note(?STAT(user), User, RD);
set_user(unknown, RD) ->
    RD.

%% @doc Set the value of the named stat for this request.
set_stat(Name, Value, RD) when (is_atom(Name) orelse is_binary(Name)),
                               is_number(Value) ->
    wrq:add_note(?STAT(Name), Value, RD).

%%%===================================================================
%%% Server API (Final Logging)
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(_BaseDir) ->
    case riak_moss_access:archive_period() of
        {ok, ArchivePeriod} ->
            gen_server:start_link({local, ?SERVER}, ?MODULE,
                                  [{period, ArchivePeriod}],
                                  []);
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc webmachine logging callback
log_access(LogData) ->
    %% calls to this function are spawned in Webmachine, so calling
    %% instead of casting is correct
    gen_server:call(?SERVER, {log_access, LogData}).

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
init(Props) ->
    {period, P} = lists:keyfind(period, 1, Props),
    T = fresh_table(),

    %% accuracy in recording: say the first slice starts *now*, not
    %% at the just-passed boundary
    Start = calendar:universal_time(),
    {_,End} = rts:slice_containing(Start, P),
    C = {Start, End},

    InitState = #state{period=P, table=T, current=C},
    {ok, schedule_archival(InitState)}.

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
handle_call({log_access, LogData}, _From, #state{table=T}=State) ->
    case access_record(LogData) of
        {ok, Access} -> ets:insert(T, Access);
        _            -> ok
    end,
    %% TODO: probably want to check table size as a trigger for
    %% archival, in addition to time period
    {reply, ok, State};
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
handle_info({archive, Ref}, #state{archive=Ref}=State) ->
    NewState = do_archive(State),
    {noreply, schedule_archival(NewState)};
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

%% @doc Create a new ets table to accumulate accesses in.
-spec fresh_table() -> ets:tid().
fresh_table() ->
    ets:new(?SERVER, [private, duplicate_bag, {keypos, 1}]).    

%% @doc Schedule a message to be sent when it's time to archive this
%% slice's accumulated accesses.
-spec schedule_archival(state()) -> state().
schedule_archival(#state{current={_,E}}=State) ->
    Ref = make_ref(),

    %% TimeLeft should almost always be zero, except for the first
    %% slice in which this server starts
    %% TODO: check that TL is non-negative, and consider throwing away
    %% backlog if we're that far behind?
    Now = calendar:datetime_to_gregorian_seconds(
            calendar:universal_time()),
    TL = calendar:datetime_to_gregorian_seconds(E)-Now,
    lager:debug("Next access archival in ~b seconds", [TL]),

    %% time left is in seconds, we need milliseconds
    erlang:send_after(TL*1000, self(), {archive, Ref}),
    State#state{archive=Ref}.

%% @doc Send the current slice's accumulated accesses to the archiver
%% for storage.  Create a clean table to store the next slice's accesses.
-spec do_archive(state()) -> state().
do_archive(#state{period=P, table=T, current=C}=State) ->
    lager:debug("Rolling access for ~p", [C]),
    %% archiver takes ownership of the table, and deletes it when done
    riak_moss_access_archiver:archive(T, C),

    %% create a fresh table for use here
    NewT = fresh_table(),
    NewC = rts:next_slice(C, P),
    State#state{table=NewT, current=NewC}.

%% @doc Digest a Webmachine log data record, and produce a record for
%% the access table.
-spec access_record(#wm_log_data{})
         -> {ok, {riak_moss:user_key(), [{atom()|binary(), number()}]}}
          | ignore.
access_record(#wm_log_data{response_length=BytesOut, notes=Notes}) ->
    case lists:keytake(?STAT(user), 1, Notes) of
        {value, {_, #moss_user{key_id=Key}}, OtherNotes} ->
            {ok, {Key, [{bytes_out, BytesOut}|access_notes(OtherNotes)]}};
        _ ->
            ignore
    end.

%% @doc Find just the access notes in the list of notes extracted from
%% the WM log data.
-spec access_notes(list()) -> [{atom()|binary(), number()}].
access_notes(Notes) ->
    [ {K, V} || {?STAT(K), V} <- Notes,
                (is_atom(K) orelse is_binary(K)),
                is_number(V) ].
