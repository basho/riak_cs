#!/usr/bin/env escript

%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2014 Basho Technologies, Inc.  All Rights Reserved,.
%%               2021 TI Tokyo    All Rights Reserved.
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

-module(repair_gc_bucket).

%% Scan the gc bucket and repair manfiest status in it.
%%
%% Once invalid state manifests happens in the GC bucket [1].
%% To proceed GC work for other manifests with appropriate status,
%% invalid state manifests are just skipped in GC [2].
%% This script complements [2] with repairing invalid state manifests.
%%
%% [1] Invalid state in GC manifest seen
%%   https://github.com/basho/riak_cs/issues/827
%% [2] Bugfix: Skip invalid state manifests in GC bucket
%%   https://github.com/basho/riak_cs/pull/964
%%
%% Details
%% - list manifets in the GC bucket
%% - skip all the manifests except `active' status
%% - for `active' manifests, get actual manifest from the object bucket
%%   and check its state.
%% - when the state is already `pending_delete' or `scheduled_delete',
%%   change status to `pending_delete' and update riak object in the
%%   GC bucket.
%% - if its state is `writing' or `active', output error log and leave
%%   it as is.

-export([main/1]).

-mode(compile).

-include_lib("riakc/include/riakc.hrl").
-include_lib("riak_cs/include/riak_cs.hrl").

main(Args) ->
    _ = application:load(lager),
    ok = application:set_env(lager, handlers, [{lager_console_backend, [{level, info}]}]),
    ok = lager:start(),
    {ok, {Options, _PlainArgs}} = getopt:parse(option_spec(), Args),
    LogLevel = case proplists:get_value(debug, Options) of
                   0 ->
                       info;
                   _ ->
                       ok = lager:set_loglevel(lager_console_backend, debug),
                       debug
               end,
    debug("Log level is set to ~p", [LogLevel]),
    debug("Options: ~p", [Options]),
    case proplists:get_value(host, Options) of
        undefined ->
            getopt:usage(option_spec(), "riak-cs escript /path/to/repair_gc_bucket.erl"),
            halt(1);
        Host ->
            Port = proplists:get_value(port, Options),
            debug("Connecting to Riak ~s:~B...", [Host, Port]),
            case riakc_pb_socket:start_link(Host, Port) of
                {ok, Pbc} ->
                    repair(Pbc, Options),
                    timer:sleep(100);
                {error, Reason} ->
                    err("Connection to Riak failed ~p", [Reason]),
                    halt(2)
            end
    end.

option_spec() ->
    [
     {host, $h, "host", string, "Host of Riak PB"},
     {port, $p, "port", {integer, 8087}, "Port number of Riak PB"},
     {debug, $d, "debug", {integer, 0}, "Enable debug (-dd for more verbose)"},
     {leeway_seconds, $l, "leeway-seconds", {integer, 24*60*60}, "Specify leeway seconds"},
     {page_size, $s, "page-size", {integer, 1000}, "Specify page size for 2i key listing"},
     {dry_run, undefined, "dry-run", {boolean, false}, "if set, actual update does not happen"}
    ].

err(Format, Args) ->
    log(error, Format, Args).

debug(Format, Args) ->
    log(debug, Format, Args).

verbose(Options, Format, Args) ->
    {debug, DebugLevel} = lists:keyfind(debug, 1, Options),
    case DebugLevel of
        Level when 2 =< Level ->
            debug(Format, Args);
        _ ->
            ok
    end.

info(Format, Args) ->
    log(info, Format, Args).

log(Level, Format, Args) ->
    lager:log(Level, self(), Format, Args).

-spec repair(pid(), proplists:proplist()) -> term().
repair(Pbc, Options) ->
    fetch_2i_keys(Pbc, Options, undefined).

fetch_2i_keys(Pbc, Options, Continuation) ->
    {page_size, MaxResults} = lists:keyfind(page_size, 1, Options),
    debug("Fetching next ~p keys, Continuation=~p", [MaxResults, Continuation]),
    QueryOptions = [{max_results, MaxResults},
                    {continuation, Continuation}],
    Now = riak_cs_gc:timestamp(),
    Leeway = proplists:get_value(leeway_seconds, Options),
    StartTime = riak_cs_gc:epoch_start(),
    EndTime = list_to_binary(integer_to_list(Now - Leeway)),
    debug("StartTime=~p, EndTime=~p", [StartTime, EndTime]),
    QueryResult = riakc_pb_socket:get_index_range(
                    Pbc,
                    ?GC_BUCKET, ?KEY_INDEX,
                    StartTime, EndTime,
                    QueryOptions),
    handle_2i_result(Pbc, Options, QueryResult).

handle_2i_result(Pbc, _Options, {error, Reason}) ->
    err("2i query failed: ~p", [Reason]),
    cleanup(Pbc),
    halt(2);
handle_2i_result(Pbc, Options,
                 {ok, ?INDEX_RESULTS{keys=Keys, continuation=Continuation}}) ->
    Head = case Keys of
               [] -> undefined;
               [H | _] -> H
           end,
    debug("~p Keys fetched: First key=~p, Continuation=~p",
          [length(Keys), Head, Continuation]),
    verbose(Options, "All keys: ~p~n", [Keys]),
    process_gc_keys(Pbc, Options, Continuation, Keys).

-spec process_gc_keys(pid(), proplists:proplist(),
                      Continuation::binary(), [binary()]) -> ok.
process_gc_keys(Pbc, _Options, undefined, []) ->
    ok = info("Repare GC bucket finished", []),
    cleanup(Pbc);
process_gc_keys(Pbc, Options, Continuation, []) ->
    fetch_2i_keys(Pbc, Options, Continuation);
process_gc_keys(Pbc, Options, Continuation, [GCKey | Keys]) ->
    ok = repair_manifests_for_gc_key(Pbc, Options, GCKey),
    process_gc_keys(Pbc, Options, Continuation, Keys).

-spec repair_manifests_for_gc_key(pid(), proplists:proplist(), binary()) ->
                                         ok |
                                         {error, term()}.
repair_manifests_for_gc_key(Pbc, Options, GCKey) ->
    Timeout = riak_cs_config:get_gckey_timeout(),
    case riakc_pb_socket:get(Pbc, ?GC_BUCKET, GCKey, [], Timeout) of
        {ok, GCObj} ->
            FileSet = riak_cs_gc:decode_and_merge_siblings(
                        GCObj, twop_set:new()),
            repair_manifests_for_gc_key(Pbc, Options, GCKey, GCObj,
                                        FileSet, twop_set:to_list(FileSet), unmodified);
        {error, notfound} ->
            %% Tombstone or deleted by another process. Proceed to the next.
            ok;
        {error, Reason} ->
            %% Error happend, but just log it and proceed anyway.
            err("Repairing GCKey=~p failed: ~p", [GCKey, Reason]),
            ok
    end.

%% Repair manifests in a fileset of specific GC Key.

%% First iterate manifests and modify the state fields in manifests
%% and, at the end, write it back to GC bucket only if at least one
%% manifest is changed.  If all manifests are unmodified, it does not
%% write back the object.
%%
%% The error case is that the actual manifest (in `0o:*' bucket)
%% corresponding to the one in GC bucket is `writing' or `active'. In
%% the case, this script output error log and do NOT modify the state.
-spec repair_manifests_for_gc_key(pid(), proplists:proplist(),
                                  binary(), riakc_obj:riakc_obj(),
                                  twop_set:twop_set(), [lfs_manifest()],
                                  unmodified | modified) ->
                                         ok.
repair_manifests_for_gc_key(_Pbc, _Options, _GCKey, _GCObj, _FileSet, [], unmodified) ->
    ok;
repair_manifests_for_gc_key(Pbc, Options, GCKey, GCObj, FileSet, [], modified) ->
    update_gc_key(Pbc, Options, GCKey, GCObj, FileSet);
repair_manifests_for_gc_key(Pbc, Options, GCKey, GCObj, FileSet,
                            [{UUID, ?MANIFEST{state=active, bkey={B,K}, uuid=UUID}=M} |
                             Manifests], IsModified) ->
    ActualManifestState = get_actual_manifest_state(Pbc, B, K, UUID),
    case ActualManifestState of
        {error, Reason} ->
            err("Fetch actual manifest for "
                "bucket=~p key=~p uuid=~p gckey=~p failed: ~p",
                [B, K, UUID, GCKey, Reason]),
            repair_manifests_for_gc_key(Pbc, Options, GCKey, GCObj,
                                        FileSet, Manifests, IsModified);
        {ok, active} ->
            err("Actual manifest is 'active': "
                "bucket=~p key=~p uuid=~p GC-key=~p, NOT repaired",
                [B, K, UUID, GCKey]),
            repair_manifests_for_gc_key(Pbc, Options, GCKey, GCObj,
                                        FileSet, Manifests, IsModified);
        {ok, writing} ->
            err("Actual manifest is 'writing': "
                "bucket=~p key=~p uuid=~p GC-key=~p, NOT repaired",
                [B, K, UUID, GCKey]),
            repair_manifests_for_gc_key(Pbc, Options, GCKey, GCObj,
                                        FileSet, Manifests, IsModified);
        {ok, _Res} ->
            %% notfound / pending_delete / scheduled_delete
            NewFileSet = modify_manifest_state(FileSet, {UUID, M}),
            info("Manifest state modified: "
                 "GC-key=~p, bucket=~p key=~p uuid=~p",
                 [GCKey, B, K, UUID]),
            repair_manifests_for_gc_key(Pbc, Options, GCKey, GCObj,
                                        NewFileSet, Manifests, modified)
    end;
repair_manifests_for_gc_key(Pbc, Options, GCKey, GCObj, FileSet,
                            [_M | Manifests], IsModified) ->
    repair_manifests_for_gc_key(Pbc, Options, GCKey, GCObj, FileSet,
                                Manifests, IsModified).

modify_manifest_state(FileSet, {UUID, M}) ->
    Deleted = twop_set:del_element({UUID, M}, FileSet),
    twop_set:add_element({UUID, M?MANIFEST{state=pending_delete,
                                           delete_marked_time=os:timestamp()}},
                         Deleted).

update_gc_key(Pbc, Options, GCKey, GCObj, FileSet) ->
    case lists:keyfind(dry_run, 1, Options) of
        {dry_run, true} ->
            ok;
        {dry_run, false} ->
            NewValue = riak_cs_utils:encode_term(FileSet),
            NewObj = riak_cs_utils:update_obj_value(GCObj, NewValue),
            case riakc_pb_socket:put(Pbc, NewObj) of
                ok ->
                    ok;
                {error, Reason} ->
                    err("Writing back GC object at gckey=~p failed: ~p", [GCKey, Reason]),
                    %% Updating this GC key failed, but proceed to repair further keys
                    ok
            end
    end.

cleanup(Pbc) ->
    _ = riakc_pb_socket:stop(Pbc),
    ok.

%% CS Utilities

-spec get_actual_manifest_state(pid(), string(), string(), string()) -> {ok, atom()} |
                                                                        {error, term()}.
get_actual_manifest_state(Pbc, Bucket, Key, UUID)->
    RiakBucket = riak_cs_utils:to_bucket_name(objects, Bucket),
    RiakKey = iolist_to_binary(Key),
    case riakc_pb_socket:get(Pbc, RiakBucket, RiakKey, []) of
        {ok, RiakObj} ->
            ManifestDict = riak_cs_manifest:manifests_from_riak_object(RiakObj),
            case rcs_common_manifest_utils:active_manifest(ManifestDict) of
                {ok, ?MANIFEST{uuid=UUID}=M} -> {ok, M?MANIFEST.state};
                {ok, ?MANIFEST{}} -> {ok, notfound};
                {error, no_active_manifest} -> {ok, notfound}
            end;
        {error, notfound} ->
            {ok, notfound};
        {error, Reason} ->
            {error, Reason}
    end.
