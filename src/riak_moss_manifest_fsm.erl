%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_moss_manifest_fsm).

-include("riak_moss.hrl").

-behaviour(gen_fsm).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Test API
-export([test_link/2]).

-endif.

%% API
-export([start_link/3,
         get_active_manifest/1,
         add_new_manifest/2,
         update_manifest/2,
         mark_active_as_pending_delete/1,
         update_manifest_with_confirmation/2,
         stop/1]).

%% gen_fsm callbacks
-export([init/1,

         %% async
         waiting_command/2,
         waiting_update_command/2,

         %% sync
         waiting_command/3,
         waiting_update_command/3,

         %% rest
         handle_event/3,
         handle_sync_event/4,
         handle_info/3,
         terminate/3,
         code_change/4]).

-define(SERVER, ?MODULE).

-record(state, {bucket :: binary(),
                key :: binary(),
                riak_object :: term(),

                %% an orddict mapping
                %% UUID -> Manifest
                %% TODO:
                %% maybe this can just
                %% be pulled out of the
                %% riak object every time?
                manifests :: term(),

                riakc_pid :: pid()
            }).

%%%===================================================================
%%% API
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Creates a gen_fsm process which calls Module:init/1 to
%% initialize. To ensure a synchronized start-up procedure, this
%% function does not return until Module:init/1 has returned.
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Bucket, Key, RiakPid) ->
    gen_fsm:start_link(?MODULE, [Bucket, Key, RiakPid], []).

get_active_manifest(Pid) ->
    gen_fsm:sync_send_event(Pid, get_active_manifest, infinity).

add_new_manifest(Pid, Manifest) ->
    gen_fsm:send_event(Pid, {add_new_manifest, Manifest}).

mark_active_as_pending_delete(Pid) ->
    gen_fsm:sync_send_event(Pid, mark_active_as_pending_delete, infinity).

update_manifest(Pid, Manifest) ->
    gen_fsm:send_event(Pid, {update_manifest, Manifest}).

-spec update_manifest_with_confirmation(term(), lfs_manifest()) -> ok | {error, term()}.
update_manifest_with_confirmation(Pid, Manifest) ->
    gen_fsm:sync_send_event(Pid, {update_manifest_with_confirmation, Manifest},
                           infinity).

stop(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, stop, infinity).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm is started using gen_fsm:start/[3,4] or
%% gen_fsm:start_link/[3,4], this function is called by the new
%% process to initialize.
%%
%% @spec init(Args) -> {ok, StateName, State} |
%%                     {ok, StateName, State, Timeout} |
%%                     ignore |
%%                     {stop, StopReason}
%% @end
%%--------------------------------------------------------------------
init([Bucket, Key, RiakPid]) ->
    process_flag(trap_exit, true),
    {ok, waiting_command, #state{bucket=Bucket,
                                 key=Key,
                                 riakc_pid=RiakPid}};
init([test, Bucket, Key]) ->
    %% skip the prepare phase
    %% and jump right into waiting command,
    %% creating the "mock" riakc_pb_socket
    %% gen_server here
    {ok, Pid} = riakc_pb_socket_fake:start_link(),
    {ok, waiting_command, #state{bucket=Bucket, key=Key, riakc_pid=Pid}}.

%% This clause is for adding a new
%% manifest that doesn't exist yet.
%% Once it has been called _once_
%% with a particular UUID, update_manifest
%% should be used from then on out.
waiting_command({add_new_manifest, Manifest}, State=#state{riakc_pid=RiakcPid,
                                                           bucket=Bucket,
                                                           key=Key}) ->
    ok = get_and_update(RiakcPid, Manifest, Bucket, Key),
    {next_state, waiting_update_command, State}.

waiting_update_command({update_manifest, Manifest}, State=#state{riakc_pid=RiakcPid,
                                                                 bucket=Bucket,
                                                                 key=Key,
                                                                 riak_object=undefined,
                                                                 manifests=undefined}) ->
    ok = get_and_update(RiakcPid, Manifest, Bucket, Key),
    {next_state, waiting_update_command, State};
waiting_update_command({update_manifest, Manifest}, State=#state{riakc_pid=RiakcPid,
                                                                 riak_object=PreviousRiakObject,
                                                                 manifests=PreviousManifests}) ->

    WrappedManifest = riak_moss_manifest:new(Manifest#lfs_manifest_v2.uuid, Manifest),
    Resolved = riak_moss_manifest_resolution:resolve([PreviousManifests, WrappedManifest]),
    RiakObject = riakc_obj:update_value(PreviousRiakObject, term_to_binary(Resolved)),
    %% TODO:
    %% currently we don't do
    %% anything to make sure
    %% this call succeeded
    ok = riakc_pb_socket:put(RiakcPid, RiakObject),
    {next_state, waiting_update_command, State#state{riak_object=undefined, manifests=undefined}}.


%%--------------------------------------------------------------------
%% @private
%% @doc
%% There should be one instance of this function for each possible
%% state name. Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_event/[2,3], the instance of this function with
%% the same name as the current state name StateName is called to
%% handle the event.
%%
%% @spec state_name(Event, From, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
waiting_command(get_active_manifest, _From, State) ->
    {Reply, NewState} = active_manifest(State),
    {reply, Reply, waiting_update_command, NewState};
waiting_command(mark_active_as_pending_delete, _From, State) ->
    Reply = set_active_manifest_pending_delete(State),
    {stop, normal, Reply, State}.

waiting_update_command({update_manifest_with_confirmation, Manifest}, _From,
                                            State=#state{riakc_pid=RiakcPid,
                                            bucket=Bucket,
                                            key=Key,
                                            riak_object=undefined,
                                            manifests=undefined}) ->
    Reply = get_and_update(RiakcPid, Manifest, Bucket, Key),
    {reply, Reply, waiting_update_command, State};
waiting_update_command({update_manifest_with_confirmation, Manifest}, _From,
                                            State=#state{riakc_pid=RiakcPid,
                                            riak_object=PreviousRiakObject,
                                            manifests=PreviousManifests}) ->
    WrappedManifest = riak_moss_manifest:new(Manifest#lfs_manifest_v2.uuid, Manifest),
    Resolved = riak_moss_manifest_resolution:resolve([PreviousManifests, WrappedManifest]),
    RiakObject = riakc_obj:update_value(PreviousRiakObject, term_to_binary(Resolved)),
    %% TODO:
    %% currently we don't do
    %% anything to make sure
    %% this call succeeded
    Reply = riakc_pb_socket:put(RiakcPid, RiakObject),
    {reply, Reply, waiting_update_command, State#state{riak_object=undefined,
                                                       manifests=undefined}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:send_all_state_event/2, this function is called to handle
%% the event.
%%
%% @spec handle_event(Event, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Whenever a gen_fsm receives an event sent using
%% gen_fsm:sync_send_all_state_event/[2,3], this function is called
%% to handle the event.
%%
%% @spec handle_sync_event(Event, From, StateName, State) ->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {reply, Reply, NextStateName, NextState} |
%%                   {reply, Reply, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState} |
%%                   {stop, Reason, Reply, NewState}
%% @end
%%--------------------------------------------------------------------
handle_sync_event(stop, _From, _StateName, State) ->
    Reply = ok,
    {stop, normal, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it receives any
%% message other than a synchronous or asynchronous event
%% (or a system message).
%%
%% @spec handle_info(Info,StateName,State)->
%%                   {next_state, NextStateName, NextState} |
%%                   {next_state, NextStateName, NextState, Timeout} |
%%                   {stop, Reason, NewState}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_fsm when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_fsm terminates with
%% Reason. The return value is ignored.
%%
%% @spec terminate(Reason, StateName, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _StateName, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, StateName, State, Extra) ->
%%                   {ok, StateName, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Get the active manifest for an object.
-spec active_manifest(#state{}) -> {{ok, lfs_manifest()}, #state{}} | {{error, notfound}, #state{}}.
active_manifest(State=#state{riakc_pid=RiakcPid,
                            bucket=Bucket,
                            key=Key}) ->
    %% Retrieve the (resolved) value
    %% from Riak and return the active
    %% manifest, if there is one. Then
    %% stash the riak_object in the state
    %% so that the next time we write
    %% we write with the correct vector
    %% clock.
    case get_manifests(RiakcPid, Bucket, Key) of
        {ok, RiakObject, Resolved} ->
            Reply = case riak_moss_manifest:active_manifest(Resolved) of
                {ok, _Active}=ActiveReply ->
                    ActiveReply;
                {error, no_active_manifest} ->
                    {error, notfound}
            end,
            NewState = State#state{riak_object=RiakObject, manifests=Resolved},
            {Reply, NewState};
        {error, notfound}=NotFound ->
            {NotFound, State}
    end.

%% @doc
-spec get_manifests(pid(), binary(), binary()) ->
    {ok, term(), term()} | {error, notfound}.
get_manifests(RiakcPid, Bucket, Key) ->
    ManifestBucket = riak_moss_utils:to_bucket_name(objects, Bucket),
    case riakc_pb_socket:get(RiakcPid, ManifestBucket, Key) of
        {ok, Object} ->
            Siblings = riakc_obj:get_values(Object),
            DecodedSiblings = lists:map(fun erlang:binary_to_term/1, Siblings),
            Resolved = riak_moss_manifest_resolution:resolve(DecodedSiblings),
            {ok, Object, Resolved};
        {error, notfound}=NotFound ->
            NotFound
    end.

get_and_update(RiakcPid, Manifest, Bucket, Key) ->
    %% retrieve the current (resolved) value at {Bucket, Key},
    %% add the new manifest, and then write the value
    %% back to Riak
    %% NOTE: it would also be nice to assert that the
    %% UUID being added doesn't already exist in the
    %% dict
    WrappedManifest = riak_moss_manifest:new(Manifest#lfs_manifest_v2.uuid, Manifest),
    ObjectToWrite = case get_manifests(RiakcPid, Bucket, Key) of
        {ok, RiakObject, Manifests} ->
            NewManiAdded = riak_moss_manifest_resolution:resolve([WrappedManifest, Manifests]),
            OverriddenMarkedAsPendingDelete = riak_moss_manifest:mark_overwritten(NewManiAdded),
            riakc_obj:update_value(RiakObject, term_to_binary(OverriddenMarkedAsPendingDelete));
        {error, notfound} ->
            ManifestBucket = riak_moss_utils:to_bucket_name(objects, Bucket),
            riakc_obj:new(ManifestBucket, Key, term_to_binary(WrappedManifest))
    end,

    %% TODO:
    %% currently we don't do
    %% anything to make sure
    %% this call succeeded
    riakc_pb_socket:put(RiakcPid, ObjectToWrite).

%% @doc Set the active manifest to the pending_delete state.
-spec set_active_manifest_pending_delete(#state{}) -> ok | {error, notfound}.
set_active_manifest_pending_delete(#state{riakc_pid=RiakcPid,
                                          bucket=Bucket,
                                          key=Key}) ->
    case get_manifests(RiakcPid, Bucket, Key) of
        {ok, RiakObject, Resolved} ->
            Marked = orddict:map(fun(_Key, Value) ->
                        if
                            Value#lfs_manifest_v2.state == active ->
                                Value#lfs_manifest_v2{state=pending_delete,
                                                      delete_marked_time=erlang:now()};
                            true ->
                                Value
                        end end, Resolved),
            NewRiakObject = riakc_obj:update_value(RiakObject, term_to_binary(Marked)),
            ok = riakc_pb_socket:put(RiakcPid, NewRiakObject),
            ok;
        {error, notfound}=NotFound ->
            NotFound
    end.

%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).

test_link(Bucket, Key) ->
    gen_fsm:start_link(?MODULE, [test, Bucket, Key], []).

-endif.
