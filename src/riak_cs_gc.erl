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

%% @doc Utility module for garbage collection of files.

-module(riak_cs_gc).

-include("riak_cs_gc.hrl").
-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% export Public API
-export([decode_and_merge_siblings/2,
         gc_interval/0,
         set_gc_interval/1,
         initial_gc_delay/0,
         gc_retry_interval/0,
         gc_max_workers/0,
         gc_active_manifests/3,
         gc_specific_manifests/5,
         epoch_start/0,
         leeway_seconds/0,
         set_leeway_seconds/1,
         max_scheduled_delete_manifests/0,
         move_manifests_to_gc_bucket/2,
         timestamp/0]).

%% export for repl debugging and testing
-export([get_active_manifests/3]).

%%%===================================================================
%%% Public API
%%%===================================================================

%% @doc Keep requesting manifests until there are no more active manifests or
%% there is an error. This requires the following to be occur:
%% 1) All previously active multipart manifests have had their unused parts cleaned
%%    and become active+multipart_clean
%% 2) All active manifests and active+multipart_clean manifests for multipart are GC'd
%%
%% Note that any error is irrespective of the current position of the GC states.
%% Some manifests may have been GC'd and then an error occurs. In this case the
%% client will only get the error response.
-spec gc_active_manifests(binary(), binary(), riak_client()) ->
    {ok, [binary()]} | {error, term()}.
gc_active_manifests(Bucket, Key, RcPid) ->
    gc_active_manifests(Bucket, Key, RcPid, []).

%% @private
-spec gc_active_manifests(binary(), binary(), riak_client(), [binary]) ->
    {ok, [binary()]} | {error, term()}.
gc_active_manifests(Bucket, Key, RcPid, UUIDs) ->
   case get_active_manifests(Bucket, Key, RcPid) of
        {ok, _RiakObject, []} ->
            {ok, UUIDs};
        {ok, RiakObject, Manifests} ->
            UnchangedManifests = clean_manifests(Manifests, RcPid),
            case gc_manifests(UnchangedManifests, RiakObject, Bucket, Key, RcPid) of
                {error, _}=Error ->
                    Error;
                NewUUIDs ->
                    gc_active_manifests(Bucket, Key, RcPid, UUIDs ++ NewUUIDs)
            end;
        {error, notfound} ->
            {ok, UUIDs};
        {error, _}=Error ->
            Error
    end.

-spec get_active_manifests(binary(), binary(), riak_client()) ->
    {ok, riakc_obj:riakc_obj(), [lfs_manifest()]} | {error, term()}.
get_active_manifests(Bucket, Key, RcPid) ->
    active_manifests(riak_cs_manifest:get_manifests(RcPid, Bucket, Key)).

-spec active_manifests({ok, riakc_obj:riakc_obj(), [lfs_manifest()]}) ->
                          {ok, riakc_obj:riakc_obj(), [lfs_manifest()]};
                      ({error, term()}) ->
                          {error, term()}.
active_manifests({ok, RiakObject, Manifests}) ->
    {ok, RiakObject, riak_cs_manifest_utils:active_manifests(Manifests)};
active_manifests({error, _}=Error) ->
    Error.

-spec clean_manifests([lfs_manifest()], riak_client()) -> [lfs_manifest()].
clean_manifests(ActiveManifests, RcPid) ->
    [M || M <- ActiveManifests, clean_multipart_manifest(M, RcPid)].

-spec clean_multipart_manifest(lfs_manifest(), riak_client()) -> true | false.
clean_multipart_manifest(M, RcPid) ->
    is_multipart_clean(riak_cs_mp_utils:clean_multipart_unused_parts(M, RcPid)).

is_multipart_clean(same) ->
    true;
is_multipart_clean(updated) ->
    false.

-spec gc_manifests(Manifests :: [lfs_manifest()],
                   RiakObject :: riakc_obj:riakc_obj(),
                   Bucket :: binary(),
                   Key :: binary(),
                   RcPid :: riak_client()) ->
    [binary()] | {error, term()}.
gc_manifests(Manifests, RiakObject, Bucket, Key, RcPid) ->
    F = fun(_M, {error, _}=Error) ->
               Error;
           (M, UUIDs) ->
               gc_manifest(M, RiakObject, Bucket, Key, RcPid, UUIDs)
        end,
    lists:foldl(F, [], Manifests).

-spec gc_manifest(M :: lfs_manifest(),
                  RiakObject :: riakc_obj:riakc_obj(),
                  Bucket :: binary(),
                  Key :: binary(),
                  RcPid :: riak_client(),
                  UUIDs :: [binary()]) ->
      [binary()] | no_return().
gc_manifest(M, RiakObject, Bucket, Key, RcPid, UUIDs) ->
    UUID = M?MANIFEST.uuid,
    check(gc_specific_manifests_to_delete([UUID], RiakObject, Bucket, Key, RcPid), [UUID | UUIDs]).

check({ok, _}, Val) ->
    Val;
check({error, _}=Error, _Val) ->
    Error.

-spec gc_specific_manifests_to_delete(UUIDsToMark :: [binary()],
                   RiakObject :: riakc_obj:riakc_obj(),
                   Bucket :: binary(),
                   Key :: binary(),
                   RcPid :: riak_client()) ->
    {error, term()} | {ok, riakc_obj:riakc_obj()}.
gc_specific_manifests_to_delete(UUIDsToMark, RiakObject, Bucket, Key, RcPid) ->
    MarkedResult = mark_as_deleted(UUIDsToMark, RiakObject, Bucket, Key, RcPid),
    handle_mark_as_pending_delete(MarkedResult, Bucket, Key, UUIDsToMark, RcPid).

%% @private
-spec gc_specific_manifests(UUIDsToMark :: [binary()],
                   RiakObject :: riakc_obj:riakc_obj(),
                   Bucket :: binary(),
                   Key :: binary(),
                   RcPid :: riak_client()) ->
    {error, term()} | {ok, riakc_obj:riakc_obj()}.
gc_specific_manifests([], RiakObject, _Bucket, _Key, _RcPid) ->
    {ok, RiakObject};
gc_specific_manifests(UUIDsToMark, RiakObject, Bucket, Key, RcPid) ->
    MarkedResult = mark_as_pending_delete(UUIDsToMark,
                                          RiakObject,
                                          Bucket, Key,
                                          RcPid),
    handle_mark_as_pending_delete(MarkedResult, Bucket, Key, UUIDsToMark, RcPid).

%% @private
-spec handle_mark_as_pending_delete({ok, riakc_obj:riakc_obj()},
                                    binary(), binary(),
                                    [binary()],
                                    riak_client()) ->
    {error, term()} | {ok, riakc_obj:riakc_obj()};

    ({error, term()}, binary(), binary(), [binary()], riak_client()) ->
    {error, term()} | {ok, riakc_obj:riakc_obj()}.
handle_mark_as_pending_delete({ok, RiakObject}, Bucket, Key, UUIDsToMark, RcPid) ->
    Manifests = riak_cs_manifest:manifests_from_riak_object(RiakObject),
    PDManifests = riak_cs_manifest_utils:manifests_to_gc(UUIDsToMark, Manifests),
    MoveResult = move_manifests_to_gc_bucket(PDManifests, RcPid),
    PDUUIDs = [UUID || {UUID, _} <- PDManifests],
    handle_move_result(MoveResult, RiakObject, Bucket, Key, PDUUIDs, RcPid);
handle_mark_as_pending_delete({error, _Error}=Error, _Bucket, _Key, _UUIDsToMark, _RcPid) ->
    _ = lager:warning("Failed to mark as pending_delete, reason: ~p", [Error]),
    Error.

%% @private
-spec handle_move_result(ok | {error, term()},
                         riakc_obj:riakc_obj(),
                         binary(), binary(),
                         [binary()],
                         riak_client()) ->
    {ok, riakc_obj:riakc_obj()} | {error, term()}.
handle_move_result(ok, RiakObject, Bucket, Key, PDUUIDs, RcPid) ->
    mark_as_scheduled_delete(PDUUIDs, RiakObject, Bucket, Key, RcPid);
handle_move_result({error, _Reason}=Error, _RiakObject, _Bucket, _Key, _PDUUIDs, _RcPid) ->
    Error.

%% @doc Return the number of seconds to wait after finishing garbage
%% collection of a set of files before starting the next.
-spec gc_interval() -> non_neg_integer() | infinity.
gc_interval() ->
    case application:get_env(riak_cs, gc_interval) of
        undefined ->
            ?DEFAULT_GC_INTERVAL;
        {ok, Interval} ->
            Interval
    end.

-spec set_gc_interval(infinity | non_neg_integer()) -> ok | {error, invalid_value}.
set_gc_interval(infinity) ->
    application:set_env(riak_cs, gc_interval, infinity);
set_gc_interval(Interval) when is_integer(Interval) andalso Interval > 0 ->
    application:set_env(riak_cs, gc_interval, Interval);
set_gc_interval(_Interval) ->
    {error, invalid_value}.

%% @doc Return the number of seconds to wait in addition to the
%% specified GC interval before scheduling the initial GC collection.
-spec initial_gc_delay() -> non_neg_integer().
initial_gc_delay() ->
    case application:get_env(riak_cs, initial_gc_delay) of
        undefined ->
            0;
        {ok, Delay} ->
            Delay
    end.

%% @doc Return the number of seconds to wait before rescheduling a
%% `pending_delete' manifest for garbage collection.
-spec gc_retry_interval() -> non_neg_integer().
gc_retry_interval() ->
    case application:get_env(riak_cs, gc_retry_interval) of
        undefined ->
            ?DEFAULT_GC_RETRY_INTERVAL;
        {ok, RetryInterval} ->
            RetryInterval
    end.

%% @doc Return the max number of workers which can run concurrently.
-spec gc_max_workers() -> non_neg_integer().
gc_max_workers() ->
    case application:get_env(riak_cs, gc_max_workers) of
        undefined ->
            ?DEFAULT_GC_WORKERS;
        {ok, Workers} ->
            Workers
    end.

%% @doc Return the start of GC epoch represented as a binary.
%% This is the time that the GC daemon uses to  begin collecting keys
%% from the `riak-cs-gc' bucket.
-spec epoch_start() -> binary().
epoch_start() ->
    case application:get_env(riak_cs, epoch_start) of
        undefined ->
            ?EPOCH_START;
        {ok, EpochStart} ->
            EpochStart
    end.

%% @doc Return the minimum number of seconds a file manifest waits in
%% the `scheduled_delete' state before being garbage collected.
-spec leeway_seconds() -> non_neg_integer().
leeway_seconds() ->
    case application:get_env(riak_cs, leeway_seconds) of
        undefined ->
            ?DEFAULT_LEEWAY_SECONDS;
        {ok, LeewaySeconds} ->
            LeewaySeconds
    end.

-spec set_leeway_seconds(non_neg_integer()) -> ok | {error, invalid_value}.
set_leeway_seconds(LeewaySeconds)
  when is_integer(LeewaySeconds) andalso LeewaySeconds > 0 ->
    application:set_env(riak_cs, leeway_seconds, LeewaySeconds);
set_leeway_seconds(_LeewaySeconds) ->
    {error, invalid_value}.

%% @doc Return the maximimum number of manifests that can be in the
%% `scheduled_delete' state for a given key. `unlimited' means there
%% is no maximum, and pruning will not happen based on count. This is
%% the default.
-spec max_scheduled_delete_manifests() -> non_neg_integer() | unlimited.
max_scheduled_delete_manifests() ->
    case application:get_env(riak_cs, max_scheduled_delete_manifests) of
        undefined ->
            unlimited;
        {ok, MaxCount} ->
            MaxCount
    end.

%% @doc Generate a key for storing a set of manifests for deletion.
-spec timestamp() -> non_neg_integer().
timestamp() ->
    timestamp(os:timestamp()).

-spec timestamp(erlang:timestamp()) -> non_neg_integer().
timestamp(ErlangTime) ->
    riak_cs_utils:second_resolution_timestamp(ErlangTime).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Mark a list of manifests as `pending_delete' based upon the
%% UUIDs specified, and also add {deleted, true} to the props member
%% to signify an actual delete, and not an overwrite.
-spec mark_as_deleted([binary()], riakc_obj:riakc_obj(), binary(), binary(), riak_client()) ->
    {ok, riakc_obj:riakc_obj()} | {error, term()}.
mark_as_deleted(UUIDsToMark, RiakObject, Bucket, Key, RcPid) ->
    mark_manifests(RiakObject, Bucket, Key, UUIDsToMark,
                   fun riak_cs_manifest_utils:mark_deleted/2,
                   RcPid).

%% @doc Mark a list of manifests as `pending_delete' based upon the
%% UUIDs specified.
-spec mark_as_pending_delete([binary()], riakc_obj:riakc_obj(), binary(), binary(), riak_client()) ->
    {ok, riakc_obj:riakc_obj()} | {error, term()}.
mark_as_pending_delete(UUIDsToMark, RiakObject, Bucket, Key, RcPid) ->
    mark_manifests(RiakObject, Bucket, Key, UUIDsToMark,
                   fun riak_cs_manifest_utils:mark_pending_delete/2,
                   RcPid).

%% @doc Mark a list of manifests as `scheduled_delete' based upon the
%% UUIDs specified.
-spec mark_as_scheduled_delete([binary()], riakc_obj:riakc_obj(), binary(), binary(), riak_client()) ->
    {ok, riakc_obj:riakc_obj()} | {error, term()}.
mark_as_scheduled_delete(UUIDsToMark, RiakObject, Bucket, Key, RcPid) ->
    mark_manifests(RiakObject, Bucket, Key, UUIDsToMark,
                   fun riak_cs_manifest_utils:mark_scheduled_delete/2,
                   RcPid).


%% @doc Call a `riak_cs_manifest_utils' function on a set of manifests
%% to update the state of the manifests specified by `UUIDsToMark'
%% and then write the updated values to riak.
-spec mark_manifests(riakc_obj:riakc_obj(), binary(), binary(), [binary()], fun(), riak_client()) ->
                    {ok, riakc_obj:riakc_obj()} | {error, term()}.
mark_manifests(RiakObject, Bucket, Key, UUIDsToMark, ManiFunction, RcPid) ->
    Manifests = riak_cs_manifest:manifests_from_riak_object(RiakObject),
    Marked = ManiFunction(Manifests, UUIDsToMark),
    UpdObj0 = riak_cs_utils:update_obj_value(RiakObject,
                                             riak_cs_utils:encode_term(Marked)),
    UpdObj = riak_cs_manifest_fsm:update_md_with_multipart_2i(
               UpdObj0, Marked, Bucket, Key),

    %% use [returnbody] so that we get back the object
    %% with vector clock. This allows us to do a PUT
    %% again without having to re-retrieve the object
    {ok, ManifestPbc} = riak_cs_riak_client:manifest_pbc(RcPid),
    riak_cs_pbc:put(ManifestPbc, UpdObj, [return_body], riak_cs_config:put_gckey_timeout()).

%% @doc Copy data for a list of manifests to the
%% `riak-cs-gc' bucket to schedule them for deletion.
-spec move_manifests_to_gc_bucket([cs_uuid_and_manifest()], riak_client()) ->
    ok | {error, term()}.
move_manifests_to_gc_bucket([], _RcPid) ->
    ok;
move_manifests_to_gc_bucket(Manifests, RcPid) ->
    Key = generate_key(),
    ManifestSet = build_manifest_set(Manifests),
    {ok, ManifestPbc} = riak_cs_riak_client:manifest_pbc(RcPid),
    Timeout = riak_cs_config:get_gckey_timeout(),
    ObjectToWrite =
        case riakc_pb_socket:get(ManifestPbc, ?GC_BUCKET, Key, Timeout) of
            {error, notfound} ->
                %% There was no previous value, so we'll
                %% create a new riak object and write it
                riakc_obj:new(?GC_BUCKET, Key, riak_cs_utils:encode_term(ManifestSet));
            {ok, PreviousObject} ->
                %% There is a value currently stored here,
                %% so resolve all the siblings and add the
                %% new set in as well. Write this
                %% value back to riak
                Resolved = decode_and_merge_siblings(PreviousObject, ManifestSet),
                riak_cs_utils:update_obj_value(PreviousObject,
                                               riak_cs_utils:encode_term(Resolved))
        end,

    %% Create a set from the list of manifests
    _ = lager:debug("Manifests scheduled for deletion: ~p", [ManifestSet]),
    Timeout1 = riak_cs_config:put_gckey_timeout(),
    riak_cs_pbc:put(ManifestPbc, ObjectToWrite, Timeout1).

-spec build_manifest_set([cs_uuid_and_manifest()]) -> twop_set:twop_set().
build_manifest_set(Manifests) ->
    lists:foldl(fun twop_set:add_element/2, twop_set:new(), Manifests).

%% @doc Generate a key for storing a set of manifests in the
%% garbage collection bucket.
-spec generate_key() -> binary().
generate_key() ->
    list_to_binary([integer_to_list(timestamp()),
                    $_,
                    key_suffix(os:timestamp())]).

-spec key_suffix(erlang:timestamp()) -> string().
key_suffix(Time) ->
    _ = random:seed(Time),
    integer_to_list(random:uniform(riak_cs_config:gc_key_suffix_max())).

%% @doc Given a list of riakc_obj-flavored object (with potentially
%%      many siblings and perhaps a tombstone), decode and merge them.
-spec decode_and_merge_siblings(riakc_obj:riakc_obj(), twop_set:twop_set()) ->
      twop_set:twop_set().
decode_and_merge_siblings(Obj, OtherManifestSets) ->
    Some = [binary_to_term(V) || {_, V}=Content <- riakc_obj:get_contents(Obj),
                                 not riak_cs_utils:has_tombstone(Content)],
    twop_set:resolve([OtherManifestSets | Some]).

%% ===================================================================
%% EUnit tests
%% ===================================================================
-ifdef(TEST).

-endif.
