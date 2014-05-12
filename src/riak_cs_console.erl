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
         cluster_info/1,
         cleanup_orphan_multipart/0,
         cleanup_orphan_multipart/1
        ]).

-include("riak_cs.hrl").
-include_lib("riakc/include/riakc.hrl").

%%%===================================================================
%%% Public API
%%%===================================================================

%% in progress.
cluster_info([OutFile]) ->
    try
        cluster_info:dump_local_node(OutFile)
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

%% @doc This function is for operation, esp cleaning up multipart
%% uploads which not completed nor aborted, and after that - bucket
%% deleted. Due to riak_cs/#475, this had been possible and Riak CS
%% which has been running earlier versions before 1.4.x may need this
%% cleanup.  This functions takes rather long time, because it
%% 1. iterates all existing and deleted buckets in moss.buckets
%% 2. if the bucket is deleted then search for all uncompleted
%%    multipart uploads, by calling stanchion_server:cleanup_nonexistent_bucket
%%    because this sequence should not be interrupted by concurrent
%%    bucket creation.
%% usage:
%% $ riak-cs attach
%% 1> riak_cs_console:cleanup_orphan_multipart().
%% cleaning up with timestamp 2014-05-11-....
-spec cleanup_orphan_multipart() -> no_return().
cleanup_orphan_multipart() ->
    cleanup_orphan_multipart(riak_cs_wm_utils:iso_8601_datetime()).

-spec cleanup_orphan_multipart(string()|binary()) -> no_return().
cleanup_orphan_multipart(Timestamp) when is_list(Timestamp) ->
    cleanup_orphan_multipart(list_to_binary(Timestamp));
cleanup_orphan_multipart(Timestamp) when is_binary(Timestamp) ->
    {Host, Port} = riak_cs_config:riak_host_port(),
    Options = [{connect_timeout, riak_cs_config:connect_timeout()}],
    {ok, Pid} = riakc_pb_socket:start_link(Host, Port, Options),
    Bucket = ?BUCKETS_BUCKET,
    {ok, Results} = riakc_pb_socket:get_index_range(Pid, Bucket,
                                                    <<"$key">>,
                                                    <<0>>, <<255>>,
                                                    [{max_results, 1024}]),
    _ = io:format("cleaning up with timestamp ~s", [Timestamp]),
    _ = iterate_csbuckets(Pid, Results, [], Timestamp),
    ok = riakc_pb_socket:stop(Pid),
    _ = io:format("all unaborted orphan multipart uploads before ~s has deleted",
                  [Timestamp]).

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec iterate_csbuckets(pid(), ?INDEX_RESULTS{}, [], binary()) -> ok | {error, term()}.
iterate_csbuckets(_Pid, ?INDEX_RESULTS{keys=[]}, _List, _) -> ok;

iterate_csbuckets(Pid,
                  _IndexResults0 = ?INDEX_RESULTS{keys=Keys0,
                                                  terms=_Terms,
                                                  continuation=Cont},
                  List, Timestamp) ->

    %% process Keys0 here
    _ = [ maybe_cleanup_csbucket(Pid, Key, Timestamp) || Key <- Keys0 ],

    Options = [{max_results, 1024}, {continuation, Cont}],
    case riakc_pb_socket:get_index_range(Pid, ?BUCKETS_BUCKET,
                                         <<"$key">>,
                                         <<0>>, <<255>>,
                                         Options) of
        {ok, IndexResults} ->
            iterate_csbuckets(Pid, IndexResults, List ++ Keys0, Timestamp);
        Error ->
            io:format("~p", [Error])
    end.

-spec maybe_cleanup_csbucket(pid(), binary(), binary()) -> ok.
maybe_cleanup_csbucket(Pid, BucketName, Timestamp) ->
    case riakc_pb_socket:get(Pid, ?BUCKETS_BUCKET, BucketName) of
        {ok, RiakObj} ->
            case riakc_obj:get_values(RiakObj) of
                [<<"0">>] -> %% deleted bucket, ensure if no uploads exists
                    io:format("checking bucket ~s:~n", [BucketName]),
                    riak_cs_bucket:delete_old_uploads(BucketName, Pid, Timestamp),
                    io:format("done.~n", []);

                [<<>>] -> %% tombstone, can't happen
                    io:format("tombstone found on bucket ~s", [BucketName]),
                    ok;
                [_] -> %% active bucket, do nothing
                    ok;
                L when is_list(L) andalso length(L) > 1 -> %% siblings!! whoa!!
                    io:format("siblings found on bucket ~s", [BucketName]),
                    ok
            end;
        {error, notfound} ->
            ok;
        {error, _} = Error ->
            io:format("Error: ~p on processing ~s", [Error, BucketName]),
            Error
    end.
