%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_cs_console).

-export([
         status/1,
         cluster_info/1,
         audit_bucket_ownership/1,
         cleanup_orphan_multipart/0,
         cleanup_orphan_multipart/1
        ]).

-include("riak_cs.hrl").
-include_lib("riakc/include/riakc.hrl").

%%%===================================================================
%%% Public API
%%%===================================================================

status([]) ->
    Stats = riak_cs_stats:get_stats(),
    _ = [io:format("~p : ~p~n", [Name, Value])
         || {Name, Value} <- Stats],
    ok.

%% in progress.
cluster_info([OutFile]) ->
    try
        case cluster_info:dump_local_node(OutFile) of
            [ok] -> ok;
            Result ->
                io:format("Cluster_info failed ~p~n", [Result]),
                error
        end
    catch
        error:{badmatch, {error, eacces}} ->
            io:format("Cluster_info failed, permission denied writing to ~p~n", [OutFile]);
        error:{badmatch, {error, enoent}} ->
            io:format("Cluster_info failed, no such directory ~p~n", [filename:dirname(OutFile)]);
        error:{badmatch, {error, enotdir}} ->
            io:format("Cluster_info failed, not a directory ~p~n", [filename:dirname(OutFile)]);
        Exception:Reason ->
            lager:error("Cluster_info failed ~p:~p", [Exception, Reason]),
            io:format("Cluster_info failed, see log for details~n"),
            error
    end.

%% @doc Audit bucket ownership by comparing information between
%%      users bucket and buckets bucket.
audit_bucket_ownership(_Args) when is_list(_Args) ->
    audit_bucket_ownership().

audit_bucket_ownership() ->
    {ok, RcPid} = riak_cs_riak_client:start_link([]),
    try audit_bucket_ownership0(RcPid) of
        [] ->
            io:format("No Inconsistencies found.~n");
        Inconsistencies ->
            io:format("~p.~n", [Inconsistencies])
    after
        riak_cs_riak_client:stop(RcPid)
    end.

%% Construct two sets of {User, Bucket} tuples and
%% return differences of subtraction in both directions, also output logs.
audit_bucket_ownership0(RcPid) ->
    FromUsers = ownership_from_users(RcPid),
    {FromBuckets, OwnedBy} = ownership_from_buckets(RcPid),
    lager:debug("FromUsers: ~p~n", [lists:usort(gb_sets:to_list(FromUsers))]),
    lager:debug("FromBuckets: ~p~n", [lists:usort(gb_sets:to_list(FromBuckets))]),
    lager:debug("OwnedBy: ~p~n", [lists:usort(gb_trees:to_list(OwnedBy))]),
    Inconsistencies0 =
        gb_sets:fold(
          fun ({U, B}, Acc) ->
                  lager:info(
                    "Bucket is not tracked by user: {User, Bucket} = {~s, ~s}",
                    [U, B]),
                  [{not_tracked, {U, B}} | Acc]
          end, [], gb_sets:subtract(FromBuckets, FromUsers)),
    gb_sets:fold(
      fun({U,B}, Acc) ->
              case gb_trees:lookup(B, OwnedBy) of
                  none ->
                      lager:info(
                        "Bucket does not exist: {User, Bucket} = {~s, ~s}", [U, B]),
                      [{no_bucket_object, {U, B}} | Acc];
                  {value, DifferentUser} ->
                      lager:info(
                        "Bucket is owned by different user: {User, Bucket, DifferentUser} = "
                        "{~s, ~s, ~s}", [U, B, DifferentUser]),
                      [{different_user, {U, B, DifferentUser}} | Acc]
              end
      end, Inconsistencies0, gb_sets:subtract(FromUsers, FromBuckets)).

ownership_from_users(RcPid) ->
    {ok, UserKeys} = riak_cs_user:fetch_user_keys(RcPid),
    lists:foldl(
      fun(UserKey, Ownership) ->
              UserStr = binary_to_list(UserKey),
              {ok, {RCSUser, _Obj}} = riak_cs_user:get_user(UserStr, RcPid),
              ?RCS_USER{buckets=Buckets} = RCSUser,
              lists:foldl(
                fun(?RCS_BUCKET{name=BucketStr}, InnerOwnership) ->
                        gb_sets:add_element({UserKey, list_to_binary(BucketStr)},
                                            InnerOwnership)
                end, Ownership, Buckets)
      end, gb_sets:new(), UserKeys).

ownership_from_buckets(RcPid) ->
    {ok, BucketKeys} = riak_cs_bucket:fetch_bucket_keys(RcPid),
    lists:foldl(
      fun(BucketKey, {Ownership, OwnedBy}) ->
              case riak_cs_bucket:fetch_bucket_object(BucketKey, RcPid) of
                  {error, no_such_bucket} ->
                      {Ownership, OwnedBy};
                  {ok, BucketObj} ->
                      OwnerIds = riakc_obj:get_values(BucketObj),
                      %% Raise badmatch when something wrong, too sloppy?
                      {ok, Owner} =
                          case lists:usort(OwnerIds) of
                              [Uniq] -> {ok, Uniq};
                              [] ->     {error, {no_bucket_owner, BucketKey}};
                              Owners -> {error, {multiple_owners, BucketKey, Owners}}
                          end,
                      {gb_sets:add_element({Owner, BucketKey}, Ownership),
                       gb_trees:enter(BucketKey, Owner, OwnedBy)}
              end
      end, {gb_sets:new(), gb_trees:empty()}, BucketKeys).

%% @doc This function is for operation, esp cleaning up multipart
%% uploads which not completed nor aborted, and after that - bucket
%% deleted. Due to riak_cs/#475, this had been possible and Riak CS
%% which has been running earlier versions before 1.4.x may need this
%% cleanup.  This functions takes rather long time, because it
%% 1. iterates all existing and deleted buckets in moss.buckets
%% 2. if the bucket is deleted then search for all uncompleted
%%    multipart uploads.
%% usage:
%% $ riak-cs attach
%% 1> riak_cs_console:cleanup_orphan_multipart().
%% cleaning up with timestamp 2014-05-11-....
-spec cleanup_orphan_multipart() -> no_return().
cleanup_orphan_multipart() ->
    cleanup_orphan_multipart([]).

-spec cleanup_orphan_multipart(string()|binary()) -> no_return().
cleanup_orphan_multipart([]) ->
    cleanup_orphan_multipart(riak_cs_wm_utils:iso_8601_datetime());
cleanup_orphan_multipart(Timestamp) when is_list(Timestamp) ->
    cleanup_orphan_multipart(list_to_binary(Timestamp));
cleanup_orphan_multipart(Timestamp) when is_binary(Timestamp) ->

    {ok, RcPid} = riak_cs_riak_client:start_link([]),
    _ = lager:info("cleaning up with timestamp ~s", [Timestamp]),
    _ = io:format("cleaning up with timestamp ~s", [Timestamp]),
    Fun = fun(RcPidForOneBucket, BucketName, GetResult, Acc0) ->
                  ok = maybe_cleanup_csbucket(RcPidForOneBucket, BucketName,
                                              GetResult, Timestamp),
                  Acc0
          end,
    _ = riak_cs_bucket:fold_all_buckets(Fun, [], RcPid),

    ok = riak_cs_riak_client:stop(RcPid),
    _ = lager:info("All old unaborted orphan multipart uploads have been deleted.", []),
    _ = io:format("~nAll old unaborted orphan multipart uploads have been deleted.~n", []).


%%%===================================================================
%%% Internal functions
%%%===================================================================


-spec maybe_cleanup_csbucket(riak_client(),
                             binary(),
                             {ok, riakc_obj()}|{error, term()},
                             binary()) -> ok | {error, term()}.
maybe_cleanup_csbucket(RcPidForOneBucket, BucketName, {ok, RiakObj}, Timestamp) ->
    case riakc_obj:get_values(RiakObj) of
        [?FREE_BUCKET_MARKER] ->
            %% deleted bucket, ensure if no uploads exists
            io:format("\rchecking bucket ~s:", [BucketName]),
            case riak_cs_bucket:delete_old_uploads(BucketName, RcPidForOneBucket,
                                                   Timestamp) of
                {ok, 0} -> ok;
                {ok, Count} ->  io:format(" aborted ~p uploads.~n",
                                          [Count]);
                Error ->
                    lager:warning("Error in deleting old uploads: ~p~n", [Error]),
                    io:format("Error in deleting old uploads: ~p <<< ~n", [Error]),
                    Error
            end;

        [<<>>] -> %% tombstone, can't happen
            io:format("tombstone found on bucket ~s~n", [BucketName]),
            ok;
        [_] -> %% active bucket, do nothing
            ok;
        L when is_list(L) andalso length(L) > 1 -> %% siblings!! whoa!!
            io:format("siblings found on bucket ~s~n", [BucketName]),
            ok
    end;
maybe_cleanup_csbucket(_, _, {error, notfound}, _) ->
    ok;
maybe_cleanup_csbucket(_, BucketName, {error, _} = Error, _) ->
    lager:error("~p on processing ~s", [Error, BucketName]),
    io:format("Error: ~p on processing ~s\n", [Error, BucketName]),
    Error.
