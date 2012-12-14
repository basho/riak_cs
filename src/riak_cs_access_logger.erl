%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Log access to Riak CS.  This is where I/O stats are
%% computed and recorded.
%%
%% I/O stats are expected as notes in the webmachine log data, with
%% keys of the form `{access, KEY}'.  I/O stats are only logged if a
%% note is included with the key `user' and a value that is a
%% `#rcs_user_v2{}' record.
%%
%% That is, to log I/O stats for a request, call
%%
%% ```
%% wrq:add_note({access, user}, User=#rcs_user_v2{}, RD)
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
%% The log is flushed to Riak at an interval specified by the
%% `riak_cs' application environment variable
%% `access_log_flush_interval'.  The value is the maximum number of
%% seconds between flushes.  This number should be less than or equal
%% to the `access_archive_period' setting, and should also evenly
%% divide that setting, or results of later queries may miss
%% information.
-module(riak_cs_access_logger).

-behaviour(gen_server).
-include_lib("webmachine/src/webmachine_logger.hrl").
-include("riak_cs.hrl").

%% API
-export([start_link/1, log_access/1]).
-export([set_user/2, expect_bytes_out/2, set_bytes_in/2]).
-export([flush/1]).

%% gen_server callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(SERVER, ?MODULE).

-record(state, {
          period :: integer(),         %% time between aggregation archivals
          max_size :: integer(),       %% max accesses between archivals
          size :: integer(),           %% num. accesses since last archival
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
-define(EXPECT_BYTES_OUT, expect_bytes_out).
-define(BYTES_IN, bytes_in).

%% @doc Set the Riak CS user for this request.  Stats are not recorded if
%% the user is not set.
set_user(KeyID, RD) when is_list(KeyID) ->
    wrq:add_note(?STAT(user), KeyID, RD);
set_user(?RCS_USER{key_id=KeyID}, RD) ->
    wrq:add_note(?STAT(user), KeyID, RD);
set_user(undefined, RD) ->
    RD;
set_user(unknown, RD) ->
    RD.

%% @doc Tell the logger that this resource expected to send `Count'
%% bytes, such that it can classify the count it actually receives as
%% complete or incomplete.
expect_bytes_out(Count, RD) when is_integer(Count) ->
    wrq:add_note(?EXPECT_BYTES_OUT, Count, RD).

%% @doc Note that this resource received `Count' bytes from the
%% request body.
set_bytes_in(Count, RD) when is_integer(Count) ->
    wrq:add_note(?BYTES_IN, Count, RD).

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
    case {riak_cs_access:log_flush_interval(),
          riak_cs_access:max_flush_size()} of
        {{ok, LogPeriod}, {ok, FlushSize}} ->
            gen_server:start_link({local, ?SERVER}, ?MODULE,
                                  [{period, LogPeriod},
                                   {max_size, FlushSize}],
                                  []);
        {{error, Reason}, _} ->
            _ = lager:error("Error starting access logger: ~s", [Reason]),
            %% can't simply {error, Reason} out here, because
            %% webmachine/mochiweb will just ignore the failed
            %% startup; using init:stop/0 here so that the user isn't
            %% suprised later when there are no logs
            init:stop();
        {_, {error, Reason}} ->
            _ = lager:error("Error starting access logger: ~s", [Reason]),
            init:stop()
    end.

%% @doc webmachine logging callback
log_access(LogData) ->
    %% this happens in a fun spawned by webmachine_decision_core, but
    %% there's no reason to make the process wait around for an ok
    %% response -- just cast
    gen_server:cast(?SERVER, {log_access, LogData}).

flush(Timeout) ->
    Now = calendar:universal_time(),
    case catch gen_server:call(?SERVER, {flush, Now}, Timeout) of
        ok -> ok;
        {'EXIT',{Reason,_}} -> Reason
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
init(Props) ->
    {period, P} = lists:keyfind(period, 1, Props),
    {max_size, MS} = lists:keyfind(max_size, 1, Props),
    T = fresh_table(),

    %% accuracy in recording: say the first slice starts *now*, not
    %% at the just-passed boundary
    Start = calendar:universal_time(),
    {_,End} = rts:slice_containing(Start, P),
    C = {Start, End},

    InitState = #state{period=P, table=T, current=C, max_size=MS, size=0},
    case schedule_archival(InitState) of
        {ok, SchedState} -> ok;
        {error, _Behind} ->
            %% startup was right on a boundary, just try again,
            %% and fail if this one also fails
            {ok, SchedState} = schedule_archival(InitState)
    end,
    {ok, SchedState}.

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
handle_call({flush, FlushEnd}, _From, State) ->
    NewState = force_archive(State, FlushEnd),
    {reply, ok, NewState};
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
handle_cast({log_access, LogData},
            #state{table=T, size=S, max_size=MaxS}=State) ->
    case access_record(LogData) of
        {ok, Access} ->
            ets:insert(T, Access),
            case S+1 < MaxS of
                true ->
                    %% still a "small" log; keep going
                    {noreply, State#state{size=S+1}};
                false ->
                    %% log is now "big"; flush it
                    {noreply, force_archive(State, calendar:universal_time())}
            end;
        _ ->
            {noreply, State}
    end;
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
    case schedule_archival(NewState) of
        {ok, SchedState} ->
            {noreply, SchedState};
        {error, Behind} ->
            %% if the logger is so far behind that it has already
            %% missed the time that the next archival should happen,
            %% just bounce the server to clear up the backlog -- this
            %% decision could be changed to some heuristic based on
            %% number of seconds and number of messages behind, if the
            %% simple "missed window" is too lossy
            [{message_queue_len, MessageCount}] =
                process_info(self(), [message_queue_len]),
            _ = lager:error("Access logger is running ~b seconds behind,"
                            " skipping ~p log messages to catch up",
                            [Behind, MessageCount]),
            {stop, behind, NewState}
    end;
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
-spec schedule_archival(state()) -> {ok, state()} | {error, integer()}.
schedule_archival(#state{current={_,E}}=State) ->
    Ref = make_ref(),

    Now = calendar:datetime_to_gregorian_seconds(
            calendar:universal_time()),
    TL = calendar:datetime_to_gregorian_seconds(E)-Now,
    case TL < 0 of
        false ->
            _ = lager:debug("Next access archival in ~b seconds", [TL]),

            %% time left is in seconds, we need milliseconds
            erlang:send_after(TL*1000, self(), {archive, Ref}),
            {ok, State#state{archive=Ref}};
        true ->
            {error, -TL}
    end.

force_archive(#state{current=C}=State, FlushEnd) ->
    %% record this archive as not filling the whole slice
    {SliceStart, SliceEnd} = C,
    NewState = do_archive(State#state{current={SliceStart, FlushEnd}}),

    %% Now continue waiting for the archive message for this slice,
    %% but mark the next archive as not filling the whole slice as well
    NewState#state{current={FlushEnd, SliceEnd}}.

%% @doc Send the current slice's accumulated accesses to the archiver
%% for storage.  Create a clean table to store the next slice's accesses.
-spec do_archive(state()) -> state().
do_archive(#state{period=P, table=T, current=C}=State) ->
    _ = lager:debug("Rolling access for ~p", [C]),
    %% archiver takes ownership of the table, and deletes it when done
    riak_cs_access_archiver:archive(T, C),

    %% create a fresh table for use here
    NewT = fresh_table(),
    NewC = rts:next_slice(C, P),
    State#state{table=NewT, current=NewC, size=0}.

%% @doc Digest a Webmachine log data record, and produce a record for
%% the access table.
-spec access_record(#wm_log_data{})
         -> {ok, {iodata(), {binary(), list()}}}
          | ignore.
access_record(#wm_log_data{notes=undefined,
                           method=Method,path=Path,headers=Headers}=_X) ->
    error_logger:error_msg("No WM route: ~p ~s ~p\n", [Method, Path, Headers]),
    ignore;
access_record(#wm_log_data{notes=Notes}=Log) ->
    case lists:keyfind(?STAT(user), 1, Notes) of
        {?STAT(user), Key} ->
            {ok, {Key, {operation(Log), stats(Log)}}};
        false ->
            ignore
    end.

operation(#wm_log_data{resource_module=riak_cs_wm_usage}) ->
    <<"UsageRead">>;
operation(#wm_log_data{resource_module=riak_cs_wm_buckets}) ->
    <<"ListBuckets">>;
operation(#wm_log_data{resource_module=riak_cs_wm_user}) ->
    <<"AccountRead">>;
operation(#wm_log_data{resource_module=riak_cs_wm_bucket_acl,
                       method='GET'}) ->
    <<"BucketReadACL">>;
operation(#wm_log_data{resource_module=riak_cs_wm_bucket_acl,
                       method='HEAD'}) ->
    <<"BucketStatACL">>;
operation(#wm_log_data{resource_module=riak_cs_wm_bucket_acl,
                       method='PUT'}) ->
    <<"BucketWriteACL">>;
operation(#wm_log_data{resource_module=riak_cs_wm_bucket_acl}) ->
    <<"BucketUnknownACL">>;
operation(#wm_log_data{resource_module=riak_cs_wm_bucket,
                       method='HEAD'}) ->
    <<"BucketStat">>;
operation(#wm_log_data{resource_module=riak_cs_wm_bucket,
                       method='PUT'}) ->
    <<"BucketCreate">>;
operation(#wm_log_data{resource_module=riak_cs_wm_bucket,
                       method='DELETE'}) ->
    <<"BucketDelete">>;
operation(#wm_log_data{resource_module=riak_cs_wm_bucket}) ->
    <<"BucketUnknown">>;
operation(#wm_log_data{resource_module=riak_cs_wm_objects}) ->
    <<"BucketRead">>;
operation(#wm_log_data{resource_module=riak_cs_wm_object_acl,
                       method='GET'}) ->
    <<"KeyReadACL">>;
operation(#wm_log_data{resource_module=riak_cs_wm_object_acl,
                       method='HEAD'}) ->
    <<"KeyStatACL">>;
operation(#wm_log_data{resource_module=riak_cs_wm_object_acl,
                       method='PUT'}) ->
    <<"KeyWriteACL">>;
operation(#wm_log_data{resource_module=riak_cs_wm_object_acl}) ->
    <<"KeyUnknownACL">>;
operation(#wm_log_data{resource_module=riak_cs_wm_object,
                       method='GET'}) ->
    <<"KeyRead">>;
operation(#wm_log_data{resource_module=riak_cs_wm_object,
                       method='HEAD'}) ->
    <<"KeyStat">>;
operation(#wm_log_data{resource_module=riak_cs_wm_object,
                       method='PUT'}) ->
    <<"KeyWrite">>;
operation(#wm_log_data{resource_module=riak_cs_wm_object,
                       method='DELETE'}) ->
    <<"KeyDelete">>;
operation(#wm_log_data{resource_module=riak_cs_wm_object}) ->
    <<"KeyUnknown">>;
operation(#wm_log_data{method=Method}) ->
    iolist_to_binary([<<"Unknown">>, atom_to_binary(Method, latin1)]).

stats(#wm_log_data{response_code=Code,
                   notes=Notes,
                   headers=Headers,
                   response_length=Length}) ->
    Prefix = if Code >= 500 -> <<"SystemError">>;
                Code >= 400 -> <<"UserError">>;
                true        -> <<"">>
             end,
    BytesIn = case lists:keyfind(?BYTES_IN, 1, Notes) of
                  {?BYTES_IN, BI} -> BI;
                  false ->
                      CLS = mochiweb_headers:get_value(
                              "content-length", Headers),
                      case catch list_to_integer(CLS) of
                          CL when is_integer(CL) -> CL;
                          _                      -> 0
                      end
              end,
    BytesOutType = case lists:keyfind(?EXPECT_BYTES_OUT, 1, Notes) of
                       {?EXPECT_BYTES_OUT, EL} when EL /= Length ->
                           <<"Incomplete">>;
                       false ->
                           <<"">>
                   end,
    %% KEEP THIS IN ORDER, so that it's an orddict
    lists:flatten(
      [[{iolist_to_binary([Prefix,<<"BytesIn">>]), BytesIn}
        || BytesIn > 0],
       [{iolist_to_binary([Prefix,<<"BytesOut">>,BytesOutType]), Length}
        || Length > 0],
       {iolist_to_binary([Prefix,<<"Count">>]), 1}]).
