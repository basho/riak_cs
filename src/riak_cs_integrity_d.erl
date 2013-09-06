-module(riak_cs_integrity_d).
-behaviour(gen_fsm).

-include("riak_cs.hrl").

%% API
-export([start_link/0,
         start_audit/0,
         pause_audit/0,
         continue_audit/0,
         stop_audit/0]).

%% gen_fsm callbacks
-export([init/1,
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

%% gen_fsm states
-export([stopped/3,
         list_buckets/2,
         waiting_bucket_list/2,
         audit_legacy/2,
         audit/2]).

-record(state, {riakc_pid         :: pid(),
                riak_version     :: binary(),
                buckets=[]       :: list(binary()),
                request_id       :: term(),
                bucket_fold_opts :: list()}).

%% ====================================================================
%% API
%% ====================================================================

-spec start_link() -> {ok, pid()} | {error, term()}.
start_link() ->
    gen_fsm:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec start_audit() -> ok | {error, audit_in_progress}.
start_audit() ->
    gen_fsm:sync_send_event(?MODULE, start_audit).

-spec stop_audit() -> ok | {error, audit_not_running}.
stop_audit() ->
    gen_fsm:sync_send_event(?MODULE, stop_audit).

-spec pause_audit() -> ok | {error, audit_not_running}.
pause_audit() ->
    gen_fsm:sync_send_event(?MODULE, pause_audit).

-spec continue_audit() -> ok | {error, audit_in_progress}.
continue_audit() ->
    gen_fsm:sync_send_event(?MODULE, continue_audit).

%% ====================================================================
%% gen_fsm states
%% ====================================================================
stopped(stop_audit, _, State) ->
    {reply, {error, audit_not_running}, stopped, State};
stopped(start_audit, _From, State) ->
    {reply, ok, list_buckets, State, 0}.

list_buckets(timeout, S=#state{riakc_pid=undefined})
  when S#state.riak_version < <<"1.4">> ->
    {ok, Pid} = riak_cs_utils:riak_connection(),
    {ok, Buckets} = riakc_pb_socket:list_keys(Pid, ?BUCKETS_BUCKET),
    NewState = S#state{riakc_pid=Pid, buckets=Buckets},
    {next_state, audit_legacy, NewState, 0};

list_buckets(timeout, S=#state{riakc_pid=undefined}) ->
    {ok, Pid} = riak_cs_utils:riak_connection(),
    EndKey = riak_cs_list_objects_fsm_v2:big_end_key(128),

    %% TODO: Make this configurable
    Opts = [{max_results, 1000},
            {start_key, <<>>},
            {end_key, EndKey}],
    {ok, ReqId} = riakc_pb_socket:cs_bucket_fold(Pid, ?BUCKETS_BUCKET, Opts),
    State = S#state{riakc_pid=Pid, 
                    bucket_fold_opts=Opts,
                    request_id=ReqId},
    {next_state, waiting_bucket_list, State};

list_buckets(timeout, S=#state{riakc_pid=Pid, bucket_fold_opts=Opts}) ->
    {ok, ReqId} = riakc_pb_socket:cs_bucket_fold(Pid, ?BUCKETS_BUCKET, Opts),
    State = S#state{request_id=ReqId},
    {next_state, waiting_bucket_list, State}.

waiting_bucket_list({ReqId, {ok, Buckets}}, S=#state{request_id=ReqId,
                                                     buckets=B}) ->
    State = S#state{buckets=B++Buckets},
    {next_state, waiting_bucket_list, State};

waiting_bucket_list({ReqId, {done, _Continuation}}, S=#state{request_id=ReqId}) ->
    {next_state, audit, S, 0}.

audit_legacy(timeout, State=#state{riakc_pid=Pid}) ->
    lager:info("Buckets = ~p~n", [State#state.buckets]),
    riak_cs_utils:close_riak_connection(Pid),
    {next_state, stopped, State#state{riakc_pid=undefined}}.

audit(timeout, State=#state{riakc_pid=Pid}) ->
    lager:info("Buckets = ~p~n", [State#state.buckets]),
    riak_cs_utils:close_riak_connection(Pid),
    {next_state, stopped, State#state{riakc_pid=undefined}}.

    
%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

init([]) ->
    {ok, Pid} = riak_cs_utils:riak_connection(),
    {ok, ServerInfo} = riakc_pb_socket:get_server_info(Pid),
    Version = proplists:get_value(server_version, ServerInfo),
    riak_cs_utils:close_riak_connection(Pid),
    {ok, stopped, #state{riak_version=Version}}.


handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, {error, no_such_event}, StateName, State}.

%% the responses from `riakc_pb_socket:get_index_range'
%% come back as regular messages, so just pass
%% them along as if they were gen_server events.
handle_info(Info, _StateName, State) ->
    waiting_bucket_list(Info, State).

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.
