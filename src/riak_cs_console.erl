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
         check_user_buckets/1,
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
    [io:format("~p : ~p~n", [Name, Value])
     || {Name, Value} <- Stats].

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
            lager:error("Cluster_info failed ~p:~p",
                [Exception, Reason]),
            io:format("Cluster_info failed, see log for details~n"),
            error
    end.

check_user_buckets(_Argv) ->
    {ok, RcPid} = riak_cs_riak_client:start_link([]),
    {ok, Users} = riak_cs_user:fetch_user_list(RcPid),
    [ begin
          UserStr = binary_to_list(User),
          io:format("Checking user ~s ..~n", [UserStr]),
          {ok, {RCSUser, _Obj}} = riak_cs_user:get_user(UserStr, RcPid),
          ?RCS_USER{buckets=Buckets} = RCSUser,
          [verify_bucket(RcPid, RCSUser, Bucket)
           || Bucket <- Buckets]
      end
      || User <- Users ].

verify_bucket(RcPid,
              ?RCS_USER{key_id=_KeyId} = _RCSUser,
              ?RCS_BUCKET{last_action=LastAction, name=BucketName} = _Bucket) ->
    case riak_cs_bucket:fetch_bucket_object(BucketName, RcPid) of
        {ok, Obj} ->
            case riakc_obj:value_count(Obj) of
                1 ->
                    %% Run check logic here
                    ok;
                N when N > 0 ->
                    %% warning, continue with first value
                    warning
            end;
        {error, notfound} when LastAction =:= deleted ->
            ok; %% maybe inconsistent ... shouldbe, but okay
        {error, notfound} when LastAction =:= created ->
            bad;
        {error, _} = E ->
            E %% somethings is wrong; should stop the system now
    end.

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
