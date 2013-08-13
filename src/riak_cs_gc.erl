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

-include("riak_cs.hrl").
-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% export Public API
-export([decode_and_merge_siblings/2,
         gc_interval/0,
         gc_retry_interval/0,
         gc_active_manifests/3,
         gc_specific_manifests/5,
         epoch_start/0,
         leeway_seconds/0,
         move_manifests_to_gc_bucket/3,
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
-spec gc_active_manifests(binary(), binary(), pid()) -> 
    {ok, [binary()]} | {error, term()}.
gc_active_manifests(Bucket, Key, RiakcPid) ->
    gc_active_manifests(Bucket, Key, RiakcPid, []).

%% @private
-spec gc_active_manifests(binary(), binary(), pid(), [binary]) ->
    {ok, [binary()]} | {error, term()}.
gc_active_manifests(Bucket, Key, RiakcPid, UUIDs) ->
   case get_active_manifests(Bucket, Key, RiakcPid) of
        {ok, _RiakObject, []} -> 
            {ok, UUIDs};
        {ok, RiakObject, Manifests} ->
            UnchangedManifests = clean_manifests(Manifests, RiakcPid),
            case gc_manifests(UnchangedManifests, RiakObject, Bucket, Key, RiakcPid) of
                {error, _}=Error -> 
                    Error;
                NewUUIDs -> 
                    gc_active_manifests(Bucket, Key, RiakcPid, UUIDs ++ NewUUIDs)
            end;
        {error, notfound} ->
            {ok, UUIDs};
        {error, _}=Error -> 
            Error
    end.

-spec get_active_manifests(binary(), binary(), pid()) ->
    {ok, riakc_obj:riakc_obj(), [lfs_manifest()]} | {error, term()}.
get_active_manifests(Bucket, Key, RiakcPid) ->
    active_manifests(riak_cs_utils:get_manifests(RiakcPid, Bucket, Key)).

-spec active_manifests({ok, riakc_obj:riakc_obj(), [lfs_manifest()]}) ->
                          {ok, riakc_obj:riakc_obj(), [lfs_manifest()]}; 
                      ({error, term()}) ->
                          {error, term()}.
active_manifests({ok, RiakObject, Manifests}) -> 
    {ok, RiakObject, riak_cs_manifest_utils:active_manifests(Manifests)};
active_manifests({error, _}=Error) -> 
    Error.

-spec clean_manifests([lfs_manifest()], pid()) -> [lfs_manifest()].
clean_manifests(ActiveManifests, RiakcPid) ->
    [M || M <- ActiveManifests, clean_multipart_manifest(M, RiakcPid)]. 

-spec clean_multipart_manifest(lfs_manifest(), pid()) -> true | false.
clean_multipart_manifest(M, RiakcPid) ->
    is_multipart_clean(riak_cs_mp_utils:clean_multipart_unused_parts(M, RiakcPid)).

is_multipart_clean(same) -> 
    true;
is_multipart_clean(updated) -> 
    false.

-spec gc_manifests(Manifests :: [lfs_manifest()],
                   RiakObject :: riakc_obj:riakc_obj(),
                   Bucket :: binary(),
                   Key :: binary(),
                   RiakcPid :: pid()) ->
    [binary()] | {error, term()}.
gc_manifests(Manifests, RiakObject, Bucket, Key, RiakcPid) ->
    F = fun(_M, {error, _}=Error) ->
               Error;
           (M, UUIDs) -> 
               gc_manifest(M, RiakObject, Bucket, Key, RiakcPid, UUIDs) 
        end, 
    lists:foldl(F, [], Manifests).

-spec gc_manifest(M :: lfs_manifest(),
                  RiakObject :: riakc_obj:riakc_obj(),
                  Bucket :: binary(),
                  Key :: binary(),
                  RiakcPid :: pid(),
                  UUIDs :: [binary()]) ->
      [binary()] | no_return().
gc_manifest(M, RiakObject, Bucket, Key, RiakcPid, UUIDs) ->
    UUID = M?MANIFEST.uuid,
    check(gc_specific_manifests_to_delete([UUID], RiakObject, Bucket, Key, RiakcPid), [UUID | UUIDs]).

check({ok, _}, Val) -> 
    Val;
check({error, _}=Error, _Val) -> 
    Error.

-spec gc_specific_manifests_to_delete(UUIDsToMark :: [binary()],
                   RiakObject :: riakc_obj:riakc_obj(),
                   Bucket :: binary(),
                   Key :: binary(),
                   RiakcPid :: pid()) ->
    {error, term()} | {ok, riakc_obj:riakc_obj()}.
gc_specific_manifests_to_delete(UUIDsToMark, RiakObject, Bucket, Key, RiakcPid) ->
    MarkedResult = mark_as_deleted(UUIDsToMark, RiakObject, Bucket, Key, RiakcPid),
    handle_mark_as_pending_delete(MarkedResult, Bucket, Key, UUIDsToMark, RiakcPid).

%% @private
-spec gc_specific_manifests(UUIDsToMark :: [binary()],
                   RiakObject :: riakc_obj:riakc_obj(),
                   Bucket :: binary(),
                   Key :: binary(),
                   RiakcPid :: pid()) ->
    {error, term()} | {ok, riakc_obj:riakc_obj()}.
gc_specific_manifests([], RiakObject, _Bucket, _Key, _RiakcPid) ->
    {ok, RiakObject};
gc_specific_manifests(UUIDsToMark, RiakObject, Bucket, Key, RiakcPid) ->
    MarkedResult = mark_as_pending_delete(UUIDsToMark,
                                          RiakObject,
                                          Bucket, Key,
                                          RiakcPid),
    handle_mark_as_pending_delete(MarkedResult, Bucket, Key, UUIDsToMark, RiakcPid).

%% @private
-spec handle_mark_as_pending_delete({ok, riakc_obj:riakc_obj()},
                                    binary(), binary(),
                                    [binary()],
                                    pid()) ->
    {error, term()} | {ok, riakc_obj:riakc_obj()};

    ({error, term()}, binary(), binary(), [binary()], pid()) ->
    {error, term()} | {ok, riakc_obj:riakc_obj()}.
handle_mark_as_pending_delete({ok, RiakObject}, Bucket, Key, UUIDsToMark, RiakcPid) ->
    Manifests = riak_cs_utils:manifests_from_riak_object(RiakObject),
    PDManifests = riak_cs_manifest_utils:manifests_to_gc(UUIDsToMark, Manifests),
    MoveResult = move_manifests_to_gc_bucket(PDManifests, RiakcPid),
    %% riak_cs_gc.erl:89: The pattern [{UUID, _} | _] can never match
    %% the type [] Oi, this is a stumper.  Just overwrite a file once,
    %% and it's obvious that PDManifests is a non-empty list and
    %% indeed has 2-tuples.  And the spec for
    %% riak_cs_manifest_utils:manifests_to_gc/2 appears correct.
    PDUUIDs = [UUID || {UUID, _} <- PDManifests],
    handle_move_result(MoveResult, RiakObject, Bucket, Key, PDUUIDs, RiakcPid);
handle_mark_as_pending_delete({error, Error}=Error, _Bucket, _Key, _UUIDsToMark, _RiakcPid) ->
    _ = lager:warning("Failed to mark as pending_delete, reason: ~p", [Error]),
    Error.

%% @private
-spec handle_move_result(ok | {error, term()},
                         riakc_obj:riakc_obj(),
                         binary(), binary(),
                         [binary()],
                         pid()) ->
    {ok, riakc_obj:riakc_obj()} | {error, term()}.
handle_move_result(ok, RiakObject, Bucket, Key, PDUUIDs, RiakcPid) ->
    mark_as_scheduled_delete(PDUUIDs, RiakObject, Bucket, Key, RiakcPid);
handle_move_result({error, _Error}=Error, _RiakObject, _Bucket, _Key, _PDUUIDs, _RiakcPid) ->
    Error.

%% @doc Return the number of seconds to wait after finishing garbage
%% collection of a set of files before starting the next.
-spec gc_interval() -> non_neg_integer().
gc_interval() ->
    case application:get_env(riak_cs, gc_interval) of
        undefined ->
            ?DEFAULT_GC_INTERVAL;
        {ok, Interval} ->
            Interval
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

%% @doc Generate a key for storing a set of manifests for deletion.
-spec timestamp() -> non_neg_integer().
timestamp() ->
    riak_cs_utils:second_resolution_timestamp(os:timestamp()).

%%%===================================================================
%%% Internal functions
%%%===================================================================

%% @doc Mark a list of manifests as `pending_delete' based upon the
%% UUIDs specified, and also add {deleted, true} to the props member
%% to signify an actual delete, and not an overwrite.
-spec mark_as_deleted([binary()], riakc_obj:riakc_obj(), binary(), binary(), pid()) ->
    {ok, riakc_obj:riakc_obj()} | {error, term()}.
mark_as_deleted(UUIDsToMark, RiakObject, Bucket, Key, RiakcPid) ->
    mark_manifests(RiakObject, Bucket, Key, UUIDsToMark,
                   fun riak_cs_manifest_utils:mark_deleted/2,
                   RiakcPid).

%% @doc Mark a list of manifests as `pending_delete' based upon the
%% UUIDs specified.
-spec mark_as_pending_delete([binary()], riakc_obj:riakc_obj(), binary(), binary(), pid()) ->
    {ok, riakc_obj:riakc_obj()} | {error, term()}.
mark_as_pending_delete(UUIDsToMark, RiakObject, Bucket, Key, RiakcPid) ->
    mark_manifests(RiakObject, Bucket, Key, UUIDsToMark,
                   fun riak_cs_manifest_utils:mark_pending_delete/2,
                   RiakcPid).

%% @doc Mark a list of manifests as `scheduled_delete' based upon the
%% UUIDs specified.
-spec mark_as_scheduled_delete([binary()], riakc_obj:riakc_obj(), binary(), binary(), pid()) ->
    {ok, riakc_obj:riakc_obj()} | {error, term()}.
mark_as_scheduled_delete(UUIDsToMark, RiakObject, Bucket, Key, RiakcPid) ->
    mark_manifests(RiakObject, Bucket, Key, UUIDsToMark,
                   fun riak_cs_manifest_utils:mark_scheduled_delete/2,
                   RiakcPid).


%% @doc Call a `riak_cs_manifest_utils' function on a set of manifests
%% to update the state of the manifests specified by `UUIDsToMark'
%% and then write the updated values to riak.
-spec mark_manifests(riakc_obj:riakc_obj(), binary(), binary(), [binary()], fun(), pid()) ->
                    {ok, riakc_obj:riakc_obj()} | {error, term()}.
mark_manifests(RiakObject, Bucket, Key, UUIDsToMark, ManiFunction, RiakcPid) ->
    Manifests = riak_cs_utils:manifests_from_riak_object(RiakObject),
    Marked = ManiFunction(Manifests, UUIDsToMark),
    UpdObj0 = riak_cs_utils:update_obj_value(RiakObject,
                                             riak_cs_utils:encode_term(Marked)),
    UpdObj = riak_cs_manifest_fsm:update_md_with_multipart_2i(
               UpdObj0, Marked, Bucket, Key),

    %% use [returnbody] so that we get back the object
    %% with vector clock. This allows us to do a PUT
    %% again without having to re-retrieve the object
    riak_cs_utils:put(RiakcPid, UpdObj, [return_body]).

move_manifests_to_gc_bucket(Manifests, RiakcPid) ->
    move_manifests_to_gc_bucket(Manifests, RiakcPid, true).

%% @doc Copy data for a list of manifests to the
%% `riak-cs-gc' bucket to schedule them for deletion.
-spec move_manifests_to_gc_bucket([lfs_manifest()], pid(), boolean()) ->
    ok | {error, term()}.
move_manifests_to_gc_bucket([], _RiakcPid, _AddLeewayP) ->
    ok;
move_manifests_to_gc_bucket(Manifests, RiakcPid, AddLeewayP) ->
    Key = generate_key(AddLeewayP),
    ManifestSet = build_manifest_set(Manifests),
    ObjectToWrite = case riakc_pb_socket:get(RiakcPid, ?GC_BUCKET, Key) of
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
    riak_cs_utils:put(RiakcPid, ObjectToWrite).

-spec build_manifest_set([lfs_manifest()]) -> twop_set:twop_set().
build_manifest_set(Manifests) ->
    lists:foldl(fun twop_set:add_element/2, twop_set:new(), Manifests).

%% @doc Generate a key for storing a set of manifests in the
%% garbage collection bucket.
-spec generate_key(boolean()) -> binary().
generate_key(AddLeewayP) ->
    list_to_binary(
      integer_to_list(
        timestamp() + if not AddLeewayP -> 0;
                         true           -> leeway_seconds()
                      end)).

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
