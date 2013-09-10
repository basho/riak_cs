-module(riak_cs_integrity_d).
-behaviour(gen_fsm).

-include("riak_cs.hrl").

%% API
-export([start_link/0,
         start_audit/0,
         pause_audit/0,
         continue_audit/0,
         continue_audit/1,
         stop_audit/0,
         start_repair/0]).

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
         audit/2]).

-record(audit_info, {bad_buckets = []   :: list()}).

-record(state, {riakc_pid                :: pid(),
                riak_version             :: binary(),
                users=[]                 :: list(binary()),
                buckets=[]               :: list(binary()),
                request_id               :: term(),
                bucket_fold_opts         :: list(),
                worker                   :: tuple(),
                audit_info=#audit_info{} :: #audit_info{}}).

-define(BUCKET_TOMBSTONE, <<"0">>).
-define(PAUSE_TIME, 0).
-define(BUCKET_PAGE_SIZE, 1000).

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

-spec continue_audit(binary()) -> ok | {error, audit_in_progress}.
continue_audit(StartKey) ->
    gen_fsm:sync_send_event(?MODULE, {continue_audit, StartKey}).

-spec start_repair() -> ok | {error, atom()}.
start_repair() ->
    gen_fsm:sync_send_event(?MODULE, start_repair).

%% ====================================================================
%% gen_fsm states
%% ====================================================================
stopped(stop_audit, _, State) ->
    {reply, {error, audit_not_running}, stopped, State};
stopped(start_audit, _From, State) ->
    {reply, ok, list_buckets, State, 0};
stopped(start_repair, _From, State) ->
    {reply, ok, repair, State, 0};
stopped(stop_repair, _, State) ->
    {reply, {error, repair_not_running}, stopped, State}.

list_buckets(timeout, S=#state{riakc_pid=undefined})
  when S#state.riak_version < <<"1.4">> ->
    {ok, Pid} = riak_cs_utils:riak_connection(),
    %% It's unlikely the user's bucket will be very large so just read it in for now
    {ok, Users} = riakc_pb_socket:list_keys(Pid, ?USER_BUCKET),
    {ok, Buckets} = riakc_pb_socket:list_keys(Pid, ?BUCKETS_BUCKET),
    NewState = S#state{riakc_pid=Pid, buckets=Buckets, users=Users},
    {next_state, audit, NewState, 0};

list_buckets(timeout, S=#state{riakc_pid=undefined}) ->
    {ok, Pid} = riak_cs_utils:riak_connection(),
    EndKey = riak_cs_list_objects_fsm_v2:big_end_key(128),
    {ok, Users} = riakc_pb_socket:list_keys(Pid, ?USER_BUCKET),

    %% TODO: Make this configurable
    Opts = [{max_results, ?BUCKET_PAGE_SIZE},
            {start_key, <<>>},
            {end_key, EndKey}],
    {ok, ReqId} = riakc_pb_socket:cs_bucket_fold(Pid, ?BUCKETS_BUCKET, Opts),
    State = S#state{buckets=[],
                    users=Users,
                    riakc_pid=Pid, 
                    bucket_fold_opts=Opts,
                    request_id=ReqId},
    {next_state, waiting_bucket_list, State};

list_buckets(continue, S=#state{riakc_pid=Pid, bucket_fold_opts=Opts}) ->
    {ok, ReqId} = riakc_pb_socket:cs_bucket_fold(Pid, ?BUCKETS_BUCKET, Opts),
    State = S#state{request_id=ReqId},
    {next_state, waiting_bucket_list, State}.

waiting_bucket_list({ReqId, {ok, Buckets}}, S=#state{request_id=ReqId,
                                                     buckets=B}) ->
    Keys = [begin lager:info("Obj = ~p~n~n", [Obj]), riakc_obj:key(Obj) end || Obj <- Buckets],
    State = S#state{buckets=Keys++B},
    {next_state, waiting_bucket_list, State};

waiting_bucket_list({ReqId, {done, _Continuation}}, S=#state{request_id=ReqId}) ->
    {next_state, audit, S, 0}.

audit(timeout, State) ->
    {AuditPid, AuditRef} = audit_buckets(State),
    NewState = State#state{worker={AuditPid, AuditRef}},
    {next_state, audit, NewState};

audit({audit_buckets_result, BadBuckets}, 
  State=#state{riakc_pid=Pid, audit_info=AI, riak_version=Version}) ->
    AuditInfo = AI#audit_info{bad_buckets=BadBuckets++AI#audit_info.bad_buckets},
    case Version < <<"1.4">> of
        true ->
            riak_cs_utils:close_riak_connection(Pid),
            print_audit_results(AuditInfo),
            NewState = State#state{riakc_pid=undefined, audit_info=AuditInfo, buckets=[]},
            {next_state, stopped, NewState};
        false ->
            case length(State#state.buckets) < ?BUCKET_PAGE_SIZE of
                true ->
                    riak_cs_utils:close_riak_connection(Pid),
                    print_audit_results(AuditInfo),
                    NewState = State#state{riakc_pid=undefined, 
                                           worker=undefined,
                                           audit_info=AuditInfo,
                                           buckets=[]},
                    {next_state, stopped, NewState};
                false ->
                    NewState = State#state{audit_info=AuditInfo, worker=undefined},
                    NewState2 = update_bucket_fold_opts(NewState),
                    gen_fsm:send_event(?MODULE, continue),
                    {next_state, list_buckets, NewState2}
            end
    end.

repair(timeout, State={audit_info=AuditInfo}) ->
    case is_clean(AuditInfo) of
        true ->
            lager:info("No problems have been detected on your system.~n");
        false ->
            {ok, Pid} = riak_cs_utils:riak_connection(),
            print_audit_info(AuditInfo),
            Worker = repair_buckets(AuditInfo),
            NewState = State#state{worker=Worker, riakc_pid=Pid},
            {next_state, repair, NewState}
    end;
repair(repair_buckets_complete, State=#state{riakc_pid=Pid}) ->
    riak_cs_utils:close_riak_connection(Pid),
    NewState = reset_state(State),
    {next_state, stopped, NewState}.

%% ====================================================================
%% Internal Functions
%% ====================================================================

repair_buckets(Pid, #audit_info{bad_buckets=BadBuckets) ->
    spawn_monitor(fun() ->
        [delete_user_bucket(Bucket) || Bucket <- BadBuckets],
    end).



audit_buckets(#state{buckets=Buckets, riakc_pid=Pid, users=Users}) ->
    spawn_monitor(fun() ->
        DeletedBuckets = extract_deleted_buckets(Buckets, Pid),
        BadBuckets = find_inconsistent_buckets(Users, DeletedBuckets, Pid),
        gen_fsm:send_event(?MODULE, {audit_buckets_result, BadBuckets})
    end).

print_audit_info(#audit_info{bad_buckets=BadBuckets}) ->
    lager:info("An audit of your system found the following errors:~n"),
    lager:info("Number of Bad Buckets: ~p~n", length(BadBuckets)).

is_clean(#audit_info{bad_buckets=[]}) ->
    true;
is_clean(_) ->
    false.

reset_state(State) ->
    #state{riak_version=State#state.riak_version}.

update_bucket_fold_opts(S=#state{bucket_fold_opts=Opts, buckets=Buckets}) ->
    NewOpts = lists:keyreplace(start_key, 1, Opts, 
        {start_key, hd(lists:reverse(Buckets))}),
    S#state{buckets=[], bucket_fold_opts=NewOpts}.

print_audit_results(AuditInfo) ->
    io:format("Found the following inconsistent buckets: ~n", []),
    [io:format("Bucket: ~p Owner Id: ~p~n", [B, O]) ||
        {B, O} <- AuditInfo#audit_info.bad_buckets],
    io:format("Audit Complete.~n",[]).

extract_deleted_buckets(Buckets, Pid) ->
    FilterFun =
    fun(X) ->
            get_bucket_value(X, Pid) =:= ?BUCKET_TOMBSTONE
    end,
    lists:filter(FilterFun, Buckets).

get_bucket_value(Bucket, Pid) ->
    case riakc_pb_socket:get(Pid, ?BUCKETS_BUCKET, Bucket) of
        {ok, Obj} ->
            hd(riakc_obj:get_values(Obj));
        _ ->
            false
    end.

find_inconsistent_buckets(Users, DeletedBuckets, Pid) ->
    find_inconsistent_buckets(Users, DeletedBuckets, [], Pid).

find_inconsistent_buckets([], _DeletedBuckets, BadBuckets, _Pid) ->
    BadBuckets;
find_inconsistent_buckets([User | RestUsers], DeletedBuckets, BadBuckets, Pid) ->
    UpdBadBuckets = bad_buckets_for_user(User, DeletedBuckets, BadBuckets, Pid),
    timer:sleep(?PAUSE_TIME),
    find_inconsistent_buckets(RestUsers, DeletedBuckets, UpdBadBuckets, Pid).

bad_buckets_for_user(UserId, DeletedBuckets, BadBuckets, Pid) ->
    Buckets = get_user_buckets(riakc_pb_socket:get(Pid, ?USER_BUCKET, UserId)),
    FoldFun = fun(Bucks, Acc) ->
            find_bad_buckets(UserId, Bucks, DeletedBuckets, Acc)
    end,
    lists:foldl(FoldFun, BadBuckets, Buckets).

delete_user_bucket({Bucket, UserId}) ->
    {ok, Obj} = riakc_pb_socket:get(Pid, ?USER_BUCKET, UserId),
    NewUser = lists:filter(fun(V) ->
                  Bucket =/= element(8, binary_to_term(V))
              end, riakc_obj:get_values(Obj)),
    

get_user_buckets({ok, Obj}) ->
    [element(8, binary_to_term(V)) || V <- riakc_obj:get_values(Obj)];
get_user_buckets(_) ->
    [].

find_bad_buckets(UserId, Buckets, DeletedBuckets, BadBuckets) ->
    FoldFun = fun(Bucket, Acc) ->
            case is_bad_bucket(Bucket, DeletedBuckets) of
                true ->
                    [{Bucket, UserId} | Acc];
                false ->
                    Acc
            end
    end,
    lists:foldl(FoldFun, BadBuckets, Buckets).

is_bad_bucket(Bucket, _) when element(3, Bucket) =:= deleted ->
    false;
is_bad_bucket(Bucket, DeletedBuckets) ->
    Name = list_to_binary(element(2, Bucket)),
    lists:member(Name, DeletedBuckets).

%% ====================================================================
%% gen_fsm callbacks
%% ====================================================================

init([]) ->
    {ok, Pid} = riak_cs_utils:riak_connection(),
    {ok, ServerInfo} = riakc_pb_socket:get_server_info(Pid),
    %% TODO: Need to handle mixed clusters here
    Version = proplists:get_value(server_version, ServerInfo),
    riak_cs_utils:close_riak_connection(Pid),
    {ok, stopped, #state{riak_version=Version}}.


handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(_Event, _From, StateName, State) ->
    {reply, {error, no_such_event}, StateName, State}.

handle_info({'DOWN', Ref, _, Pid, Status}, StateName, 
  S=#state{worker={_, Ref}}) ->
    lager:error("Audit process failed in state ~p for Pid=~p, Status=~p~n",
        [StateName, Pid, Status]),
    lager:error("Failure State = ~p~n", [S]),
    NewState = reset_state(S),
    {next_state, stopped, NewState};

%% Expected shutdown of the process. Refs don't match.
handle_info({'DOWN', _, _, _, _}, StateName, State) ->
    {next_state, StateName, State};

%% the responses from `riakc_pb_socket:get_index_range'
%% come back as regular messages, so just pass
%% them along as if they were gen_server events.
handle_info(Info, _StateName, State) ->
    waiting_bucket_list(Info, State).

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.
