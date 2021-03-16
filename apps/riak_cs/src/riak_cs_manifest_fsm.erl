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

-module(riak_cs_manifest_fsm).

-include("riak_cs.hrl").

-behaviour(gen_fsm).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

%% Test API
-export([test_link/2]).

-endif.

%% API
-export([start_link/3,
         get_all_manifests/1,
         get_active_manifest/1,
         get_specific_manifest/2,
         add_new_manifest/2,
         update_manifest/2,
         update_manifests/2,
         delete_specific_manifest/2,
         gc_specific_manifest/2,
         update_manifest_with_confirmation/2,
         update_manifests_with_confirmation/2,
         maybe_stop_manifest_fsm/1,
         stop/1]).
-export([update_md_with_multipart_2i/4]).

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
                manifests :: term(), % an orddict mapping UUID -> Manifest
                riak_client :: riak_client()
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
%% @end
%%--------------------------------------------------------------------
start_link(Bucket, Key, RcPid) ->
    gen_fsm:start_link(?MODULE, [Bucket, Key, RcPid], []).

get_all_manifests(Pid) ->
    gen_fsm:sync_send_event(Pid, get_manifests, infinity).

get_active_manifest(Pid) ->
    Response = gen_fsm:sync_send_event(Pid, get_manifests, infinity),
    riak_cs_utils:active_manifest_from_response(Response).

get_specific_manifest(Pid, UUID) ->
    case gen_fsm:sync_send_event(Pid, get_manifests, infinity) of
        {ok, Manifests} ->
            case orddict:fetch(UUID, Manifests) of
                {ok, _}=Result ->
                    Result;
                error ->
                    {error, notfound}
            end;
        {error, notfound}=NotFound ->
            NotFound
    end.

add_new_manifest(Pid, Manifest) ->
    Dict = riak_cs_manifest_utils:new_dict(Manifest?MANIFEST.uuid, Manifest),
    gen_fsm:send_event(Pid, {add_new_dict, Dict}).

update_manifests(Pid, Manifests) ->
    gen_fsm:send_event(Pid, {update_manifests, Manifests}).

update_manifest(Pid, Manifest) ->
    Dict = riak_cs_manifest_utils:new_dict(Manifest?MANIFEST.uuid, Manifest),
    update_manifests(Pid, Dict).

%% @doc Delete a specific manifest version from a manifest and
%% update the manifest value in riak or delete the manifest key from
%% riak if there are no manifest versions remaining.
-spec delete_specific_manifest(pid(), binary()) -> ok | {error, term()}.
delete_specific_manifest(Pid, UUID) ->
    gen_fsm:sync_send_event(Pid, {delete_manifest, UUID}, infinity).

%% @doc Not only delete a specific manifest version from a manifest
%% and update the manifest value in riak or delete the manifest key
%% from riak if there are no manifest versions remaining, but also
%% moves them into GC bucket.
-spec gc_specific_manifest(pid(), binary()) -> ok | {error, term()}.
gc_specific_manifest(Pid, UUID) ->
    gen_fsm:sync_send_event(Pid, {gc_specific_manifest, UUID}, infinity).

-spec update_manifests_with_confirmation(pid(), orddict:orddict()) -> ok | {error, term()}.
update_manifests_with_confirmation(Pid, Manifests) ->
    gen_fsm:sync_send_event(Pid, {update_manifests_with_confirmation, Manifests},
                           infinity).

-spec update_manifest_with_confirmation(pid(), lfs_manifest()) -> ok | {error, term()}.
update_manifest_with_confirmation(Pid, Manifest) ->
    Dict = riak_cs_manifest_utils:new_dict(Manifest?MANIFEST.uuid, Manifest),
    update_manifests_with_confirmation(Pid, Dict).

-spec maybe_stop_manifest_fsm(undefined | pid()) -> ok.
maybe_stop_manifest_fsm(undefined) ->
    ok;
maybe_stop_manifest_fsm(ManiPid) ->
    stop(ManiPid),
    ok.

stop(Pid) ->
    gen_fsm:sync_send_all_state_event(Pid, stop, infinity).

%%%===================================================================
%%% gen_fsm callbacks
%%%===================================================================

init([Bucket, Key, RcPid]) ->
    process_flag(trap_exit, true),
    {ok, waiting_command, #state{bucket=Bucket,
                                 key=Key,
                                 riak_client=RcPid}};
init([test, Bucket, Key]) ->
    %% creating the "mock" riakc_pb_socket
    %% gen_server here
    {ok, FakePbc} = riakc_pb_socket_fake:start_link(),
    {ok, waiting_command, #state{bucket=Bucket, key=Key, riak_client=FakePbc}}.

%% This clause is for adding a new
%% manifest that doesn't exist yet.
%% Once it has been called _once_
%% with a particular UUID, update_manifest
%% should be used from then on out.
waiting_command({add_new_dict, WrappedManifest},
                State=#state{riak_client=RcPid,
                             bucket=Bucket,
                             key=Key}) ->
    {_, RiakObj, Manifests} = get_and_update(RcPid, WrappedManifest, Bucket, Key),
    UpdState = State#state{riak_object=RiakObj, manifests=Manifests},
    {next_state, waiting_update_command, UpdState}.

waiting_update_command({update_manifests, WrappedManifests},
                       State=#state{riak_client=RcPid,
                                    bucket=Bucket,
                                    key=Key,
                                    riak_object=undefined,
                                    manifests=undefined}) ->
    _Res = get_and_update(RcPid, WrappedManifests, Bucket, Key),
    {next_state, waiting_update_command, State};
waiting_update_command({update_manifests, WrappedManifests},
                       State=#state{riak_client=RcPid,
                                    bucket=Bucket,
                                    key=Key,
                                    riak_object=PreviousRiakObject,
                                    manifests=PreviousManifests}) ->
    _ = update_from_previous_read(RcPid,
                                  PreviousRiakObject,
                                  Bucket, Key,
                                  PreviousManifests,
                                  WrappedManifests),
    {next_state, waiting_update_command, State#state{riak_object=undefined, manifests=undefined}}.

waiting_command(get_manifests, _From, State) ->
    {Reply, NewState} = handle_get_manifests(State),
    {reply, Reply, waiting_update_command, NewState};
waiting_command({delete_manifest, UUID},
                       _From,
                       State=#state{riak_client=RcPid,
                                    bucket=Bucket,
                                    key=Key,
                                    riak_object=undefined,
                                    manifests=undefined}) ->
    Reply = get_and_delete(RcPid, UUID, Bucket, Key),
    {reply, Reply, waiting_update_command, State};
waiting_command({update_manifests_with_confirmation, _}=Cmd, From, State) ->
    %% Used by multipart commit: this FSM was just started a moment
    %% ago, and we don't need this FSM to re-do work that multipart
    %% commit has already done.
    waiting_update_command(Cmd, From, State).


waiting_update_command({update_manifests_with_confirmation, WrappedManifests}, _From,
                                            State=#state{riak_client=RcPid,
                                                         bucket=Bucket,
                                                         key=Key,
                                                         riak_object=undefined,
                                                         manifests=undefined}) ->
    {Reply, _, _} = get_and_update(RcPid, WrappedManifests, Bucket, Key),
    {reply, Reply, waiting_update_command, State};
waiting_update_command({update_manifests_with_confirmation, WrappedManifests}, _From,
                                            State=#state{riak_client=RcPid,
                                                         bucket=Bucket,
                                                         key=Key,
                                                         riak_object=PreviousRiakObject,
                                                         manifests=PreviousManifests}) ->
    Reply =
        case riak_cs_config:read_before_last_manifest_write() of
            true ->
                {R, _, _} = get_and_update(RcPid, WrappedManifests, Bucket, Key),
                R;
            false ->
                update_from_previous_read(RcPid,
                                          PreviousRiakObject,
                                          Bucket,
                                          Key,
                                          PreviousManifests,
                                          WrappedManifests)
        end,
    {reply, Reply, waiting_update_command, State#state{riak_object=undefined,
                                                       manifests=undefined}};
waiting_update_command({gc_specific_manifest, UUID}, _From,
                       #state{
                          riak_object = RiakObj0,
                          bucket = Bucket,
                          key = Key,
                          riak_client = RcPid
                         } = State) ->
    %% put_fsm has issued delete_manifest caused by force_stop
    Res = case RiakObj0 of
              undefined ->
                  case riak_cs_manifest:get_manifests(RcPid, Bucket, Key) of
                      {ok, RiakObj, _} ->
                          riak_cs_gc:gc_specific_manifests([UUID], RiakObj,
                                                           Bucket, Key, RcPid);
                      Error ->
                          Error
                  end;
              RiakObj ->
                  riak_cs_gc:gc_specific_manifests([UUID], RiakObj,
                                                   Bucket, Key, RcPid)
          end,
    {stop, normal, Res, State}.

handle_event(_Event, StateName, State) ->
    {next_state, StateName, State}.

handle_sync_event(stop, _From, _StateName, State) ->
    Reply = ok,
    {stop, normal, Reply, State}.

handle_info(_Info, StateName, State) ->
    {next_state, StateName, State}.

terminate(_Reason, _StateName, _State) ->
    ok.

code_change(_OldVsn, StateName, State, _Extra) ->
    {ok, StateName, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Return all (resolved) manifests, or notfound
-spec handle_get_manifests(#state{}) ->
    {{ok, [lfs_manifest()]}, #state{}} | {{error, notfound}, #state{}}.
handle_get_manifests(State=#state{riak_client=RcPid,
                           bucket=Bucket,
                           key=Key}) ->
    case riak_cs_manifest:get_manifests(RcPid, Bucket, Key) of
        {ok, RiakObject, Resolved} ->
            Reply = {ok, Resolved},
            NewState = State#state{riak_object=RiakObject, manifests=Resolved},
            {Reply, NewState};
        {error, notfound}=NotFound ->
            {NotFound, State}
    end.

%% @doc Retrieve the current (resolved) value at {Bucket, Key},
%% delete the manifest corresponding to `UUID', and then
%% write the value back to Riak or delete the manifest value
%% if there are no manifests remaining.
-spec get_and_delete(riak_client(), binary(), binary(), binary()) -> ok |
                                                             {error, term()}.
get_and_delete(RcPid, UUID, Bucket, Key) ->
    case riak_cs_manifest:get_manifests(RcPid, Bucket, Key) of
        {ok, RiakObject, Manifests} ->
            ResolvedManifests = riak_cs_manifest_resolution:resolve([Manifests]),
            UpdatedManifests = orddict:erase(UUID, ResolvedManifests),
            case UpdatedManifests of
                [] ->
                    DeleteTimeout = riak_cs_config:delete_manifest_timeout(),
                    riak_cs_pbc:delete_obj(manifest_pbc(RcPid), RiakObject, [],
                                           DeleteTimeout, [riakc, delete_manifest]);
                _ ->
                    ObjectToWrite0 =
                        riak_cs_utils:update_obj_value(
                          RiakObject, riak_cs_utils:encode_term(UpdatedManifests)),
                    ObjectToWrite = update_md_with_multipart_2i(
                                      ObjectToWrite0, UpdatedManifests, Bucket, Key),
                    PutTimeout = riak_cs_config:put_manifest_timeout(),
                    riak_cs_pbc:put(manifest_pbc(RcPid), ObjectToWrite,
                                    PutTimeout, [riakc, put_manifest])
            end;
        {error, notfound} ->
            ok
    end.

-spec get_and_update(riak_client(), orddict:orddict(), binary(), binary()) ->
                            {ok | error, undefined | riakc_obj:riakc_obj(),
                             undefined | orddict:orddict()}.
get_and_update(RcPid, WrappedManifests, Bucket, Key) ->
    case riak_cs_manifest:get_manifests(RcPid, Bucket, Key) of
        {ok, RiakObject, Manifests} ->
            case update(RcPid, Manifests, RiakObject, WrappedManifests, Bucket, Key) of
                {ok, _, _} = Res ->
                    case maybe_backpressure_sleep(riakc_obj:value_count(RiakObject)) of
                        true ->
                            %% Backpressure sleep has been triggered. Current object
                            %% is to be discarded to prevent unnecessary interleaving
                            %% with other concurrent manifest updates while sleeping.
                            {ok, undefined, undefined};
                        false ->
                            Res
                    end;
                OtherRes ->
                    OtherRes
            end;
        {error, notfound} ->
            ManifestBucket = riak_cs_utils:to_bucket_name(objects, Bucket),
            ObjectToWrite0 = riakc_obj:new(ManifestBucket, Key, riak_cs_utils:encode_term(WrappedManifests)),
            ObjectToWrite = update_md_with_multipart_2i(
                              ObjectToWrite0, WrappedManifests, Bucket, Key),
            Timeout = riak_cs_config:put_manifest_timeout(),
            PutResult = riak_cs_pbc:put(manifest_pbc(RcPid), ObjectToWrite,
                                        Timeout, [riakc, put_manifest]),
            {PutResult, undefined, undefined}
    end.

%% If backpressure is needed, sleep some interval and return `true'.
%% Otherwise, return `false'.
-spec maybe_backpressure_sleep(non_neg_integer()) -> boolean().
maybe_backpressure_sleep(Siblings) ->
    BackpressureThreshold = riak_cs_config:get_env(
                              riak_cs, manifest_siblings_bp_threashold, 5),
    maybe_backpressure_sleep(Siblings, BackpressureThreshold).

-spec maybe_backpressure_sleep(non_neg_integer(), non_neg_integer() | infinity) -> boolean().
maybe_backpressure_sleep(Siblings, BackpressureThreshold)
  when Siblings < BackpressureThreshold ->
    false;
maybe_backpressure_sleep(Siblings, _BackpressureThreshold) ->
    MaxSleep = riak_cs_config:get_env(riak_cs, manifest_siblings_bp_max_sleep, 30*1000),
    Coefficient = riak_cs_config:get_env(riak_cs, manifest_siblings_bp_coefficient, 200),
    MeanSleepMS = min(Coefficient * Siblings, MaxSleep),
    Delta = MeanSleepMS div 2,
    SleepMS = crypto:rand_uniform(MeanSleepMS - Delta, MeanSleepMS + Delta),
    lager:debug("maybe_backpressure_sleep: Siblings=~p, SleepMS=~p~n", [Siblings, SleepMS]),
    ok = riak_cs_stats:countup([manifest, siblings_bp_sleep]),
    ok = timer:sleep(SleepMS),
    true.

update(RcPid, OldManifests, OldRiakObject, WrappedManifests, Bucket, Key) ->
    NewManiAdded = riak_cs_manifest_resolution:resolve([WrappedManifests, OldManifests]),
    %% Update the object here so that if there are any
    %% overwritten UUIDs, then gc_specific_manifests() will
    %% operate on NewManiAdded and save it to Riak when it is
    %% finished.
    ObjectToWrite0 = riak_cs_utils:update_obj_value(
                       OldRiakObject, riak_cs_utils:encode_term(NewManiAdded)),
    ObjectToWrite = update_md_with_multipart_2i(
                      ObjectToWrite0, NewManiAdded, Bucket, Key),
    {Result, NewRiakObject} =
        case riak_cs_manifest_utils:overwritten_UUIDs(NewManiAdded) of
            [] ->
                Timeout = riak_cs_config:put_manifest_timeout(),
                riak_cs_pbc:put(manifest_pbc(RcPid), ObjectToWrite, [return_body],
                                Timeout, [riakc, put_manifest]);
            OverwrittenUUIDs ->
                riak_cs_gc:gc_specific_manifests(OverwrittenUUIDs,
                                                 ObjectToWrite,
                                                 Bucket, Key,
                                                 RcPid)
        end,
    UpdatedManifests = riak_cs_manifest:manifests_from_riak_object(NewRiakObject),
    {Result, NewRiakObject, UpdatedManifests}.

manifest_pbc(RcPid) ->
    {ok, ManifestPbc} = riak_cs_riak_client:manifest_pbc(RcPid),
    ManifestPbc.

-spec update_from_previous_read(riak_client(), riakc_obj:riakc_obj(),
                                binary(), binary(),
                                orddict:orddict(), orddict:orddict()) ->
    ok | {error, term()}.
update_from_previous_read(RcPid, RiakObject, Bucket, Key,
                          PreviousManifests, NewManifests) ->
    Resolved = riak_cs_manifest_resolution:resolve([PreviousManifests,
            NewManifests]),
    NewRiakObject0 = riak_cs_utils:update_obj_value(RiakObject,
                                                    riak_cs_utils:encode_term(Resolved)),
    NewRiakObject = update_md_with_multipart_2i(NewRiakObject0, Resolved,
                                                Bucket, Key),
    %% TODO:
    %% currently we don't do
    %% anything to make sure
    %% this call succeeded
    Timeout = riak_cs_config:put_manifest_timeout(),
    riak_cs_pbc:put(manifest_pbc(RcPid), NewRiakObject, [],
                    Timeout, [riakc, put_manifest]).

update_md_with_multipart_2i(RiakObject, WrappedManifests, Bucket, Key) ->
    %% During testing, it's handy to delete Riak keys in the
    %% S3 bucket, e.g., cleaning up from a previous test.
    %% Let's not trip over tombstones here.
    MD0 = case ([MD || {MD, V} <- riakc_obj:get_contents(RiakObject),
                       V /= <<>>]) of
              []  ->
                  dict:new();
              MDs ->
                  merge_dicts(MDs)
          end,
    {K_i, V_i} = riak_cs_mp_utils:calc_multipart_2i_dict(
                   [M || {_, M} <- WrappedManifests], Bucket, Key),
    MD = dict:store(K_i, V_i, MD0),
    riakc_obj:update_metadata(RiakObject, MD).

merge_dicts([MD|MDs]) ->
    %% Smash all the dicts together, arbitrarily picking
    %% one value in case of conflict.
    Pick1 = fun(_K, V1, _V2) -> V1 end,
    lists:foldl(fun(D, DMerged) ->
                        dict:merge(Pick1, D, DMerged)
                end, MD, MDs).

%% ===================================================================
%% Test API
%% ===================================================================

-ifdef(TEST).

test_link(Bucket, Key) ->
    gen_fsm:start_link(?MODULE, [test, Bucket, Key], []).

mash_test() ->
    L1 = [{a,1}, {b,2}, {c,3}],
    L2 = [{d,4}, {b,3}, {e,5}],
    D1 = dict:from_list(L1),
    D2 = dict:from_list(L2),
    [{a,1},{b,3},{c,3},{d,4},{e,5}] =
        lists:sort(dict:to_list(merge_dicts([D1, D2]))),
    L1 = lists:sort(dict:to_list(merge_dicts([D1]))).

-endif.
