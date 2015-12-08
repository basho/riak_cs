#!/usr/bin/env escript

%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_cs_inspector).

%% CAUTION
%% Some commands are potentially DANGEROUS, for example executes
%% many requests to Riak.
%% To use this scripts, you MUST at least know
%% - status of data in Riak, # of buckets, # of objects and so on
%% - how many requests are executed and
%% - what kind of requests are executed, list keys or get object
%% - Riak CS data structure at both at CS level and Riak level

%% Fetch and print some information to inspect Riak CS.
%% Information is fetched from Riak, **not through Riak CS**.
%% Usage of utility functions in Riak CS modules should be
%% compatible both with Riak CS 1.2.x and Riak CS 1.3.x.

%% This scripts based on @kuenishi's work.

%% TODOs:
%% - MP manifest support (part manifest are not pretty-printed yet)
%% - key streaming instead of key listing, or use 2i streaming to keep result sorted

-export([main/1]).
-export([print_object/3, list_blocks/4]).
-export([rec_pp_fun/2]).

-mode(compile).

-include_lib("riak_cs/include/riak_cs.hrl").

-define(rec_pp_fun(RecName),
        rec_pp_fun(RecName, N) ->
               case record_info(size, RecName) - 1 of
                   N -> record_info(fields, RecName);
                   _ -> no
               end).

-define(user_attr(Attr),
        user_attr(Attr, #rcs_user_v2{Attr = Value}) ->
           Value;
        user_attr(Attr, #moss_user_v1{Attr = Value}) ->
           Value;
        user_attr(Attr, #moss_user{Attr = Value}) ->
           Value).

-define(m_attr(Attr),
        m_attr(Attr, #lfs_manifest_v3{Attr = Value}) ->
           Value;
        m_attr(Attr, #lfs_manifest_v2{Attr = Value}) ->
           Value).

%% TODO: buckets names can be retrieved by bucket list? (possible but expensive)
-define(KNOWN_BUCKETS, ["tmp", "test", "test1", "test2", "test-mp", "test-tmp"]).

%% option specs

-define(global_opt_spec,
        [
         {host, $h, "host", {string, "localhost"}, "Riak host. default:localhost"},
         {port, $p, "port", {integer, 8087}, "Riak Protocol Buffer port. default:8087"},
         {dev,  undefined, "dev", {integer, 0}, "devrel alias"},
         {help, $H, "help", boolean, "Show this message"}
        ]).

-define(sort_opt_spec,   [{sort, $s, "sort", {boolean, false}, "Sort the results"}]).
-define(bucket_opt_spec, [{bucket, undefined, undefined, string, "bucket name"}]).
-define(object_opt_spec, [{object, undefined, undefined, string, "object name"}]).

%% command specs

-define(bucket_cmd_spec,
        {bucket, [{list, ?sort_opt_spec ++
                       [{all, undefined, "all", boolean, "list buckets meta buckets and known buckets only"}]},
                  {show, ?bucket_opt_spec}]}).
-define(object_cmd_spec,
        {object, [{list,  ?sort_opt_spec ++ ?bucket_opt_spec},
                  {show,  ?bucket_opt_spec ++ ?object_opt_spec},
                  {count, ?bucket_opt_spec}]}).
-define(block_cmd_spec,
        {block, [{list,  ?bucket_opt_spec ++ ?sort_opt_spec ++
                      [{key, undefined, undefined, string, "key"},
                       {prefix,  undefined, undefined,  string, "prefix of UUIDs"}]},
                 {show,  ?bucket_opt_spec ++
                      [{uuid, undefined, undefined, string, "UUID"},
                       {seq,  undefined, undefined, string, "sequence ID"}]},
                 {count, ?bucket_opt_spec ++ ?sort_opt_spec}
                ]}).
-define(gc_cmd_spec,
        {gc, [{list,  [{file, $f, "file", string, "input file"}]++?sort_opt_spec},
              {show,  [{key, undefined, undefined, string, "gc key"}]},
              {count, []}
             ]}).
-define(access_cmd_spec,
        {access, [{list, ?sort_opt_spec},
                  {show, [{key, undefined, undefined, string, "access stats key"}]}]}).
-define(storage_cmd_spec,
        {storage, [{list, ?sort_opt_spec},
                   {show, [{key, undefined, undefined, string, "storage stats key"},
                           {print_zeros, undefined, "print-zeros", {boolean, false},
                            "print zeros for detaild stat items"}]}]}).
-define(user_cmd_spec,
        {user, [{list, ?sort_opt_spec},
                {show, [{key, undefined, "key", string, "User ID"},
                        {name, undefined, "name", string, "Username"}]}]}).

main(Args) ->
    init_specs(),
    case getopt:parse(get_merged_spec(), Args) of
        {error, {Reason, Data}} ->
            exit_with_usage({Reason, Data});
        {ok, {_Opts, []}} ->
            usage();
        {ok, {_Opts, [Cmd]}} ->
            maybe_exit_with_usage(Cmd),
            usage(Cmd);
        {ok, {_Opts, [Cmd, SubCmd | _Rest]}} ->
            maybe_exit_with_usage(Cmd, SubCmd),
            OptSpec = get_opt_spec(Cmd, SubCmd),
            CmdArgs = [A || A <- Args, A =/= Cmd, A =/= SubCmd],
            case getopt:parse(OptSpec, CmdArgs) of
                {error, {Reason, Data}} ->
                    exit_with_usage({Reason, Data});
                {ok, {Opts, _NonOpts}} ->
                    maybe_exit_with_usage(Cmd, SubCmd, Opts),
                    {ok, RiakcPid} = connect(Opts),
                    process_command(list_to_atom(Cmd), list_to_atom(SubCmd),
                                    Opts, RiakcPid)
            end
    end.

process_command(bucket, list, Opts, RiakcPid) ->
    case proplists:get_value(all, Opts) of
        true ->
            list_buckets(RiakcPid, lookup_listbucket_fun(Opts));
        _ ->
            list_cs_buckets(RiakcPid, lookup_listkey_fun(Opts))
    end;
process_command(bucket, show, Opts, RiakcPid) ->
    Bucket = proplists:get_value(bucket, Opts),
    show_bucket(RiakcPid, Bucket);
process_command(object, list, Opts, RiakcPid) ->
    case proplists:get_value(bucket, Opts) of
        undefined ->
            usage(object, list);
        CSBucketName ->
            list_objects(RiakcPid, CSBucketName, lookup_listkey_fun(Opts))
    end;
process_command(object, show, Opts, RiakcPid) ->
    show_manifest(RiakcPid,
                  proplists:get_value(bucket, Opts),
                  proplists:get_value(object, Opts));
process_command(object, count, Opts, RiakcPid) ->
    Bucket= proplists:get_value(bucket, Opts),
    count_manifest(RiakcPid, Bucket);
process_command(user, list, Opts, RiakcPid) ->
    list_users(RiakcPid, lookup_listkey_fun(Opts));
process_command(user, show, Opts, RiakcPid) ->
    show_user(RiakcPid, Opts);
process_command(access, list, Opts, RiakcPid) ->
    list_accesses(RiakcPid, lookup_listkey_fun(Opts));
process_command(access, show, Opts, RiakcPid) ->
    show_access(RiakcPid, proplists:get_value(key, Opts));
process_command(gc, list, Opts, RiakcPid) ->
    list_gc(RiakcPid, proplists:get_value(file, Opts), lookup_listkey_fun(Opts));
process_command(gc, show, Opts, RiakcPid) ->
    show_gc(RiakcPid, proplists:get_value(key, Opts));
process_command(gc, count, _Opts, RiakcPid) ->
    count_riak_bucket(RiakcPid, "riak-cs-gc", "riak-cs-gc", 100*1000);
process_command(storage, list, Opts, RiakcPid) ->
    list_storage(RiakcPid, lookup_listkey_fun(Opts));
process_command(storage, show, Opts, RiakcPid) ->
    show_storage(RiakcPid, proplists:get_value(key, Opts),
                 proplists:get_value(print_zeros, Opts));
process_command(block, list, Opts, RiakcPid) ->
    list_blocks(RiakcPid,
               proplists:get_value(bucket, Opts),
               proplists:get_value(key, Opts),
               proplists:get_value(prefix, Opts));
process_command(block, show, Opts, RiakcPid) ->
    show_block(RiakcPid,
               proplists:get_value(bucket, Opts),
               proplists:get_value(uuid, Opts),
               proplists:get_value(seq, Opts));
process_command(block, count, Opts, RiakcPid) ->
    Bucket = proplists:get_value(bucket, Opts),
    count_blocks(RiakcPid, Bucket);
process_command(_, _, _, _) ->
    init_specs(),
    usage(),
    halt(1).

connect(Opts) ->
    IP = proplists:get_value(host, Opts),
    Port = case proplists:get_value(dev, Opts) of
               DevNum when DevNum > 0 ->
                   10007 + 10*DevNum;
               _ ->
                   proplists:get_value(port, Opts)
    end,
    heading("Connecting to ~s:~B...~n", [IP, Port]),
    riakc_pb_socket:start_link(IP, Port).

e(Fmt, Args) ->
    io:format(standard_error, Fmt, Args).

e(Line) -> e(Line, []).

heading(Fmt) -> e(Fmt, []).
heading(Fmt, Args) -> e(Fmt, Args).

list_buckets(RiakcPid, ListFun) ->
    %% TODO(shino): Can include all names of [a-z][0-9a-zA-Z-]{2..4} ?
    KnownNames = [{riak_cs_utils:md5(list_to_binary(B)), B}
                  || B <- ?KNOWN_BUCKETS],
    heading("All buckets:~n"),
    heading("[~-7s] ~-32..=s ~-32..=s~n",
            [type, "cs-bucket-name ", "riak-bucket-name "]),
    PrintFun = fun(RiakBuckets) ->
                       [io:format("[~-7s] ~-32s ~w~n", cs_bucket_info(RiakBucket, KnownNames))
                        || RiakBucket <- RiakBuckets ]
               end,
    ListFun(RiakcPid, PrintFun).

cs_bucket_info(RiakBucket, KnownNames) ->
    try riak_cs_utils:from_bucket_name(RiakBucket) of
        {Type,  CsBucketHash} ->
            case lists:keyfind(CsBucketHash, 1, KnownNames) of
                false ->
                    [Type, lists:duplicate(30, "*"), RiakBucket];
                {_, KnownName} ->
                    [Type, KnownName, RiakBucket]
            end
    catch
        _:_ ->
            ['riak-cs', RiakBucket, RiakBucket]
    end.

list_cs_buckets(RiakcPid, ListFun)->
    heading("~-64..=s ~-8..=s ~-40..=s~n",
            ["CS Bucket Name ", "Sibl. ", "Owner Key "]),
    PrintFun = fun(Keys) ->
                       lists:foreach(fun(Key) ->
                                             [io:format("~-64s ~-8B ~-40s~n", [Key, SiblingNo, V])
                                              || {SiblingNo, {_MD, V}}
                                                     <- get_riak_object(RiakcPid, "moss.buckets", Key)]
                                     end, Keys)
               end,
    ListFun(RiakcPid, "moss.buckets", PrintFun).

list_objects(RiakcPid, Bucket, ListFun)->
    print_manifest_summary(pid, bucket, sibling_no, header),
    ManifestBucketName = riak_cs_utils:to_bucket_name(objects, Bucket),
    PrintFun = fun(ManifestKeys) ->
                       [print_manifest_summary(RiakcPid, Bucket, SiblingNo, M)
                        || Key <- ManifestKeys,
                           {SiblingNo, _UUID, M} <- get_manifest(RiakcPid, Bucket, Key)]
               end,
    ListFun(RiakcPid, ManifestBucketName, PrintFun).

list_users(RiakcPid, ListFun)->
    heading("~-40..=s ~-8..=s ~-40..=s ~-40..=s~n",
            ["Key ID ", "Sibl. ", "Name ", "Secret "]),
    PrintFun = fun(Keys) ->
                       %% TODO: only support #rcs_user_v2{}
                       [[io:format("~-40s ~8B ~-40s ~-40s~n",
                                   [user_attr(key_id, User),
                                    SiblingNo,
                                    user_attr(name, User),
                                    user_attr(key_secret, User)])
                         || {SiblingNo, {MD, V}} <- get_riak_object(RiakcPid, "moss.users", Key),
                            User <- [case MD of
                                         tombstone -> tombstone;
                                         _ -> binary_to_term(V)
                                     end]]
                        || Key <- Keys]
               end,
    ListFun(RiakcPid, "moss.users", PrintFun).

show_user(RiakcPid, Opts) ->
    case {proplists:get_value(key, Opts), proplists:get_value(name, Opts)} of
        {undefined, undefined} ->
            e("Error: --key or --name is required."),
            io:nl(),
            usage(user, show),
            halt(1);
        {Key, undefined} ->
            print_users(RiakcPid, "moss.users", {key, Key});
        {undefined, Name} ->
            print_users(RiakcPid, "moss.users", {name, Name})
    end.

list_accesses(RiakcPid, ListFun) ->
    heading("~-40..=s ~-8..=s ~-16..=s ~-16..=s ~-32..=s~n",
            ["Key ", "Sibl. ", "StartTime ", "EndTime ", "MossNode "]),
    PrintFun = fun(Keys) ->
                       [[io:format("~-40s ~8B ~-16s ~-16s ~-32s~n",
                                   [Key, SiblingNo, Start, End, Node])
                         || {SiblingNo, {MD, V}} <- get_riak_object(RiakcPid, "moss.access", Key),
                            {Start, End, Node, _Stats} <-
                                [case MD of
                                     tombstone -> tombstone;
                                     _ -> stats_sample_from_binary(V)
                                 end]]
                        || Key <- Keys]
               end,
    ListFun(RiakcPid, "moss.access", PrintFun).

show_access(_RiakcPid, undefined)->
    usage(access, show),
    halt(1);
show_access(RiakcPid, Key)->
    [print_access_stats(Key, SiblingNo, StatsBin)
     || {SiblingNo, {_RiakMD, StatsBin}}
            <- get_riak_object(RiakcPid, "moss.access", Key)].

list_gc(RiakcPid, undefined, ListFun)->
    PrintFun = fun(ManifestKeys) ->
                       list_gc_from_keys(RiakcPid, ManifestKeys)
               end,
    print_gc_manifest_summary(pid, bucket, sibling_no, header),
    ListFun(RiakcPid, "riak-cs-gc", PrintFun);
list_gc(RiakcPid, InputFileName, _)->
    {ok, Bin} = file:read_file(InputFileName),
    ManifestKeys = binary:split(Bin, <<"\n">>, [global, trim]),
    print_gc_manifest_summary(pid, bucket, sibling_no, header),
    list_gc_from_keys(RiakcPid, ManifestKeys).

list_gc_from_keys(RiakcPid, ManifestKeys)->
    [print_gc_manifest_summary(RiakcPid, Key, SiblingNo, M)
     || Key <- ManifestKeys,
        {SiblingNo, _UUID, M} <- get_gc_manifest(RiakcPid, "riak-cs-gc", Key)].

show_gc(_RiakcPid, undefined)->
    usage(gc, show),
    halt(1);
show_gc(RiakcPid, Key)->
    Manifests = get_gc_manifest(RiakcPid, "riak-cs-gc", Key),
    io:format("----- ~B instance(s) -----~n", [length(Manifests)]),
    [ print_manifest(Manifest) || Manifest <- Manifests].

list_storage(RiakcPid, ListFun) ->
    heading("~-40..=s ~-8..=s ~-16..=s ~-16..=s~n",
            ["Key ", "Sibl. ", "StartTime ", "EndTime "]),
    PrintFun = fun(Keys) ->
                       [[io:format("~-40s ~8B ~-16s ~-16s~n",
                                   [Key, SiblingNo, Start, End])
                         || {SiblingNo, {MD, V}} <- get_riak_object(RiakcPid, "moss.storage", Key),
                            {Start, End, _Node, _Stats} <- [case MD of
                                          tombstone -> tombstone;
                                          _ -> stats_sample_from_binary(V)
                                      end]]
                        || Key <- Keys]
               end,
    ListFun(RiakcPid, "moss.storage", PrintFun).

show_storage(_RiakcPid, undefined, _)->
    usage(storage, show),
    halt(1);
show_storage(RiakcPid, Key, PrintZeros)->
    [print_storage_stats(Key, SiblingNo, StatsBin, PrintZeros)
     || {SiblingNo, {_RiakMD, StatsBin}}
            <- get_riak_object(RiakcPid, "moss.storage", Key)].

print_gc_manifest_summary(_RiakcPid, _Key, _SiblingNo, header) ->
    heading("~-32..=s ~-8..=s ~-16..=s ~-32..=s ~-16..=s ~-32..=s~n",
            ["Key ", "Sibl. ", "State ", "UUID ", "Content-Length", "CS Key "]);
print_gc_manifest_summary(_RiakcPid, Key, SiblingNo, {tombstone, {_Bucket, Key}}) ->
    io:format("~-32s ~-8B ~-16s ~-32s ~16B ~-32s~n",
              [Key, SiblingNo, tombstone, tombstone, 0, tombstone]);
print_gc_manifest_summary(_RiakcPid, Key, SiblingNo, empty_twop_set) ->
    io:format("~-32s ~-8B ~-16s ~-32s ~16B ~-32s~n",
              [Key, SiblingNo, empty_twop_set, empty_twop_set, 0, empty_twop_set]);
print_gc_manifest_summary(_RiakcPid, Key, SiblingNo, M) ->
    {_, CSKey} = m_attr(bkey, M),
    io:format("~-32s ~-8B ~-16s ~-32s ~16B ~-32s~n",
              [Key, SiblingNo, m_attr(state, M), uuid_hex(M),
               m_attr(content_length, M), CSKey]).

print_manifest_summary(_RiakcPid, _Bucket, _SiblingNo, header) ->
    heading("~-32..=s ~-8..=s ~-16..=s ~-32..=s ~-16..=s ~-9..=s ~-16..=s~n",
            ["Key ", "Sibl. ", "State ", "UUID ", "Content-Length",
             "Type ", "First Block "]);
print_manifest_summary(_RiakcPid, _Bucket, SiblingNo, {tombstone, {_Bucket, Key}}) ->
    io:format("~-32s ~-8B ~-16s ~-32s ~16B ~-9s ~-16s~n",
              [Key, SiblingNo, tombstone, tombstone, 0, "N/A", tombstone]);
print_manifest_summary(RiakcPid, Bucket, SiblingNo, M) ->
    {_, Key} = m_attr(bkey, M),
    {BlockUUID, Type} = first_block_uuid(M, m_attr(props, M)),
    FirstBlockStatus =
        case BlockUUID of
            no_first_block -> "**No Entry**";
            _ ->
                FirstBlockId = 0,
                {RiakBucket, RiakKey} = full_bkey(Bucket, Key, BlockUUID, FirstBlockId),
                case riakc_pb_socket:get(RiakcPid, RiakBucket, RiakKey) of
                    {ok, _RiakObject} ->
                        "Found";
                    {error, notfound} ->
                        "**Not Found**"
                end
        end,
    io:format("~-32s ~-8B ~-16s ~-32s ~16B ~-9s ~-16s~n",
              [Key, SiblingNo, m_attr(state, M), uuid_hex(M),
               m_attr(content_length, M), Type, FirstBlockStatus]).

first_block_uuid(M, undefined) ->
    {m_attr(uuid, M), "normal"};
first_block_uuid(M, Props) ->
    case proplists:get_value(multipart, Props) of
        undefined -> {m_attr(uuid, M), "normal"};
        MpM ->
            case ordsets:to_list(MpM?MULTIPART_MANIFEST.parts) of
                [] -> {no_first_block, "multipart"};
                [FirstPart|_] ->
                    {FirstPart#part_manifest_v1.part_id, "multipart"}
            end
    end.

-spec print_object(pid(), string(), string()) -> any().
print_object(RiakcPid, Bucket, "key=" ++ Key)->
    Manifests = get_manifest(RiakcPid, Bucket, Key),
    io:format("----- ~B instance(s) -----~n", [length(Manifests)]),
    [ print_manifest(Manifest) || Manifest <- Manifests].

show_bucket(RiakcPid, Bucket) ->
    [print_cs_bucket(Bucket, SiblingNo, UserKey, RiakMD)
     || {SiblingNo, {RiakMD, UserKey}}
            <- get_riak_object(RiakcPid, "moss.buckets", Bucket)].

-spec show_manifest(pid(), string(), string()) -> any().
show_manifest(_RiakcPid, undefined, _Key) ->
    usage(object, show),
    halt(1);
show_manifest(_RiakcPid, _Bucket, undefined) ->
    usage(object, show),
    halt(1);
show_manifest(RiakcPid, Bucket, Key) ->
    Manifests = get_manifest(RiakcPid, Bucket, Key),
    io:format("----- ~B instance(s) -----~n", [length(Manifests)]),
    [ print_manifest(Manifest) || Manifest <- Manifests].

-spec count_manifest(pid(), string()) -> any().
count_manifest(_RiakcPid, undefined) ->
    usage(object, count),
    halt(1);
count_manifest(RiakcPid, Bucket) ->
    ManifestBucketBin = riak_cs_utils:to_bucket_name(objects, Bucket),
    count_riak_bucket(RiakcPid, ManifestBucketBin, Bucket ++ ":manifests", 100*1000).

stats_sample_from_binary(Bin) ->
    {struct, Sample} = mochijson2:decode(binary_to_list(Bin)),
    stats_sample_from_binary(Sample, {undefined, undefined, undefined, []}).

stats_sample_from_binary([], {Start, End, Node, Ops}) ->
    {Start, End, Node, Ops};
stats_sample_from_binary([{<<"StartTime">>, Start} | Rest],
                         {_Start, End, Node, Ops}) ->
    stats_sample_from_binary(Rest, {Start, End, Node, Ops});
stats_sample_from_binary([{<<"EndTime">>, End} | Rest],
                         {Start, _End, Node, Ops}) ->
    stats_sample_from_binary(Rest, {Start, End, Node, Ops});
stats_sample_from_binary([{<<"MossNode">>, Node} | Rest],
                         {Start, End, _Node, Ops}) ->
    stats_sample_from_binary(Rest, {Start, End, Node, Ops});
stats_sample_from_binary([{OpName, {struct, Stats}} | Rest],
                         {Start, End, Node, Ops}) ->
    stats_sample_from_binary(Rest, {Start, End, Node, [{OpName, Stats} | Ops]});
stats_sample_from_binary([{OpName, Other} | Rest],
                         {Start, End, Node, Ops}) ->
    stats_sample_from_binary(Rest, {Start, End, Node, [{OpName, Other} | Ops]}).

count_riak_bucket(RiakcPid, Bucket, BucketToDisplay, Timeout) ->
    case riakc_pb_socket:stream_list_keys(RiakcPid, Bucket) of
        {ok, ReqId} ->
            case wait_for_count_riak_bucket(ReqId, Timeout, 0) of
                {ok, Count} ->
                    io:format("count(~s): ~B~n", [BucketToDisplay, Count]);
                Error ->
                    error(Error)
            end;
        Error ->
            error(Error)
    end.

wait_for_count_riak_bucket(ReqId, Timeout, Acc) ->
    receive
        {ReqId, done} ->
            {ok, Acc};
        {ReqId, {keys,Res}} ->
            wait_for_count_riak_bucket(ReqId, Timeout, length(Res) + Acc);
        {ReqId, {error, Reason}} ->
            {error, Reason}
    after Timeout ->
            {error, {timeout, Acc}}
    end.

print_cs_bucket(Bucket, SiblingNo, UserKey, MD) ->
    {BagId, Acl, Policy} =
        case dict:find(<<"X-Riak-Meta">>, MD) of
            {ok, CsMeta} ->
                {term_from_meta(<<"X-Rcs-Bag">>, CsMeta),
                 term_from_meta(<<"X-Moss-Acl">>, CsMeta),
                 term_from_meta(<<"X-Rcs-Policy">>, CsMeta)};
            error ->
                {undefined, undefined}
        end,
    io:nl(),
    io:format("Bucket    : ~s~n", [Bucket]),
    io:format("SiblingNo : ~B~n", [SiblingNo]),
    io:format("BagId     : ~s~n", [BagId]),
    io:format("Owner     : ~s~n", [UserKey]),
    io:format("Acl:~n"),
    print_record(Acl),
    io:format("Policy:~n~s~n", [Policy]).

print_access_stats(Key, SiblingNo, StatsBin) ->
    {Start, End, Node, Ops} = stats_sample_from_binary(StatsBin),
    io:nl(),
    io:format("Key       : ~s~n", [Key]),
    io:format("SiblingNo : ~B~n", [SiblingNo]),
    io:format("StartTime : ~s~n", [Start]),
    io:format("EndTime   : ~s~n", [End]),
    io:format("MossNode  : ~s~n", [Node]),
    io:format("Ops :~n"),
    [io:format("    ~-12s: ~12s = ~15B~n", [Op, StatsKey, StatsValue]) ||
        {Op, OpStats} <- Ops,
        {StatsKey, StatsValue} <- OpStats].

print_storage_stats(Key, SiblingNo, StatsBin, PrintZeros) ->
    {Start, End, _Node, Buckets} = stats_sample_from_binary(StatsBin),
    io:nl(),
    io:format("Key       : ~s~n", [Key]),
    io:format("SiblingNo : ~B~n", [SiblingNo]),
    io:format("StartTime : ~s~n", [Start]),
    io:format("EndTime   : ~s~n", [End]),
    io:format("~-36..=s: ~-22..=s ~-22..=s ~-22..=s~n",
              ["Bucket ", "Objects ", "Bytes ", "Blocks "]),
    [case StatItems of
         ErrorMessage when is_binary(ErrorMessage) ->
             io:format("~-36s: ~s~n", [Bucket, ErrorMessage]);
         _ ->
             Objects = get_storage_stats_number(<<"Objects">>, StatItems),
             Bytes = get_storage_stats_number(<<"Bytes">>, StatItems),
             Blocks = get_storage_stats_number(<<"Blocks">>, StatItems),
             io:format("~-36s: ~22B ~22B ~22B~n", [Bucket, Objects, Bytes, Blocks]),
             case length(StatItems) =< 2 of
                 true -> ok;
                 false ->
                     [begin
                          Ob = get_storage_stats_number(<<Prefix/binary, "Objects">>, StatItems),
                          By = get_storage_stats_number(<<Prefix/binary, "Bytes">>, StatItems),
                          Bl = get_storage_stats_number(<<Prefix/binary, "Blocks">>, StatItems),
                          case {PrintZeros, Ob, By, Bl} of
                              {false, 0, 0, 0} -> ok;
                              _ ->
                                  io:format("  +---~-30s: ~22B ~22B ~22B~n",
                                            [Prefix, Ob, By, Bl])
                          end
                      end || Prefix <- [<<"Active">>,
                                        <<"WritingMultipart">>,
                                        <<"ActiveInvisible">>,
                                        <<"WritingNew">>,
                                        <<"WritingOld">>,
                                        <<"ScheduledDeleteNew">>,
                                        <<"ScheduledDeleteOld">>,
                                        <<"PendingDeleteNew">>,
                                        <<"PendingDeleteOld">>]]
             end
     end || {Bucket, StatItems} <- Buckets].

get_storage_stats_number(Key, StatItems) when is_binary(Key) ->
    case lists:keyfind(Key, 1, StatItems) of
        {Key, Num}  -> Num;
        false -> -1
    end;
get_storage_stats_number(Keys, StatItems) when is_list(Keys) ->
    get_storage_stats_number(list_to_binary(Keys), StatItems).

print_users(RiakcPid, Bucket, Options) ->
    {ok, Keys} = riakc_pb_socket:list_keys(RiakcPid, Bucket),
    [[maybe_print_user(U, SiblingNo, Options)
      || {SiblingNo, {RiakMD, ValueBin}}
             <- get_riak_object(RiakcPid, Bucket, UserKey),
         U <- [case RiakMD of
                   tombstone -> {tombstone, UserKey};
                   _ -> {RiakMD, binary_to_term(ValueBin)}
               end]]
     || UserKey <- Keys].

maybe_print_user({tombstone, UserKey}, SiblingNo, {key, UserKey}) ->
    io:nl(),
    io:format("User Key : ~s~n", [UserKey]),
    io:format("SiblingNo: ~B~n", [SiblingNo]),
    io:format("Record   : **tombstone**");
maybe_print_user({tombstone, _}, _SiblingNo, _Options) ->
    ok;
maybe_print_user({MD, User}, SiblingNo, {name, Name}) ->
    case user_attr(name, User) of
        Name ->
            print_user(MD, User, SiblingNo);
        _ ->
            ok
    end;
maybe_print_user({MD, User}, SiblingNo, {key, KeyPrefix}) ->
    case lists:prefix(KeyPrefix, user_attr(key_id, User)) of
        true ->
            print_user(MD, User, SiblingNo);
        _ ->
            ok
    end.

print_user(MD, User, SiblingNo) ->
    io:nl(),
    io:format("User Key : ~s~n", [user_attr(name, User)]),
    io:format("SiblingNo: ~B~n", [SiblingNo]),
    io:format("MD       : ~p~n", [MD]),
    io:format("Record   :~n"),
    print_record(User),
    print_user_verbose(User).

print_user_verbose(User) ->
    VerboseMode = os:getenv("CS_INSPECTOR_VERBOSE"),
    case user_verbose_format(VerboseMode) of
        undefined ->
            ok;
        Format ->
            io:format("For ~s ------8<------8<------8<------8<------8<------~n",
                      [VerboseMode]),
            io:format(standard_error,
                      Format, [user_attr(key_id, User), user_attr(key_secret, User)])
    end.

user_verbose_format("s3cfg") ->
    "access_key = ~s~n"
    "secret_key = ~s~n";
user_verbose_format("s3curl") ->
    "%awsSecretAccessKeys = (~n"
    "    rcs => {~n"
    "        id  => '~s',~n"
    "        key => '~s',~n"
    "    },~n"
    ");~n";
user_verbose_format("cs") ->
    "              {admin_key, \"~s\"},~n"
    "              {admin_secret, \"~s\"},~n";
user_verbose_format("stanchion") ->
    "              {admin_key, \"~s\"},~n"
    "              {admin_secret, \"~s\"}~n";
user_verbose_format("cs_control") ->
    "       {cs_admin_key, \"~s\"},~n"
    "       {cs_admin_secret, \"~s\"},~n";
user_verbose_format(_) ->
    undefined.

list_blocks(_, undefined, _, _) ->
    usage(block, list),
    halt(1);
list_blocks(_, _,undefined, _) ->
    usage(block, list),
    halt(1);
list_blocks(_, _, _, undefined) ->
    usage(block, list),
    halt(1);
list_blocks(RiakcPid, Bucket, Key, UUIDHexPrefix) ->
    %% Blocks::[{UUID, BlockId}]
    {UUID, Blocks} =
        case [{GotUUID, M}
              || {_SiblingNo, GotUUID, M} <- get_manifest(RiakcPid, Bucket, Key),
                 GotUUID =/= tombstone,
                 lists:prefix(UUIDHexPrefix, mochihex:to_hex(GotUUID))] of
            [] ->
                throw({uuid_not_found, UUIDHexPrefix});
            [{GotUUID, Manifest} | _] ->
                %% TODO: lfs_manifest_v2
                {GotUUID, riak_cs_lfs_utils:block_sequences_for_manifest(Manifest)}
        end,
    UUIDName =
        case lists:nth(1, Blocks) of
            {UUID, _Seq} ->
                "UUID ";
            {_PartUUID, _Seq} ->
                "Part UUID "
        end,
    io:format("Blocks in object [~s/~s]:~n", [Key, mochihex:to_hex(UUID)]),
    io:format("~-32..=s ~-8..=s ~-10..=s ~-64..=s~n",
              [UUIDName, "Seq ", "Size ", "Value(first 8 or 32 bytes) "]),
    [print_block_summary(RiakcPid, Bucket, Key, UUID, B) || B <- Blocks ].

count_blocks(_, undefined) ->
    usage(block, count),
    halt(1);
count_blocks(RiakcPid, Bucket) ->
    BlockBucketBin = riak_cs_utils:to_bucket_name(blocks, Bucket),
    count_riak_bucket(RiakcPid, BlockBucketBin, Bucket ++ ":blocks", 100*1000).

print_block_summary(RiakcPid, Bucket, Key, _ManifestUUID, {UUID, SeqNo}) ->
    case get_block(RiakcPid, Bucket, Key, UUID, SeqNo) of
        notfound ->
            io:format("~-32s ~8B ~10s ~64s~n",
                      [mochihex:to_hex(UUID), SeqNo, "*********", "**Not Found**"]);
        Value ->
            ByteSize = byte_size(Value),
            FirstChars = binary:part(Value, 0, min(32, ByteSize)),
            %% http://gambasdoc.org/help/doc/pcre
            %% - :graph: printing excluding space
            %% - \A:     start of subject
            %% - \z:     end of subject
            case re:run(FirstChars, "\\A[[:graph:]]*\\z", []) of
                nomatch ->
                    io:format("~-32s ~8B ~10B ~64w~n",
                              [mochihex:to_hex(UUID), SeqNo,
                               ByteSize, binary:part(Value, 0, min(8, ByteSize))]);
                _ ->
                    io:format("~-32s ~8B ~10B ~s~n",
                              [mochihex:to_hex(UUID), SeqNo,
                               ByteSize, FirstChars])
            end
    end.

show_block(_, undefined, _, _) ->
    usage(block, show),
    halt(1);
show_block(_, _, undefined, _) ->
    usage(block, show),
    halt(1);
show_block(_, _, _, undefined) ->
    usage(block, show),
    halt(1);
show_block(RiakcPid, Bucket, UUIDHexFull, SeqStr) ->
    Seq = list_to_integer(SeqStr),
    Key = undefined, % The key of riak-cs object. Not needed currently.
    Value = get_block(RiakcPid, Bucket, Key, mochihex:to_bin(UUIDHexFull), Seq),
    io:format("~s~n", [Value]).

get_block(RiakcPid, Bucket, Key, UUID, Seq) ->
    {RiakBucket, RiakKey} = full_bkey(Bucket, Key, UUID, Seq),
    case riakc_pb_socket:get(RiakcPid, RiakBucket, RiakKey) of
        {ok, RiakObject} ->
            riakc_obj:get_value(RiakObject);
        {error, notfound} ->
            notfound
    end.

%% Commandline Utilities

init_specs() ->
    spec_register(?bucket_cmd_spec),
    spec_register(?object_cmd_spec),
    spec_register(?block_cmd_spec),
    spec_register(?gc_cmd_spec),
    spec_register(?access_cmd_spec),
    spec_register(?storage_cmd_spec),
    spec_register(?user_cmd_spec),
    ok.

spec_register({Name, Spec}) ->
    CmdSpec = [{SubCmd, ?global_opt_spec++OptSpec}||{SubCmd, OptSpec}<-Spec],
    put(cmd_spec, dict:store(Name, {Name, CmdSpec}, get_spec_registry())).

get_spec_registry() ->
    case get(cmd_spec) of
        undefined ->
            dict:new();
        D -> D
    end.

get_merged_spec() ->
    [Spec ||
        {_Cmd, {_Cmd, SubSpecs}} <- dict:to_list(get_spec_registry()),
        {_SubCmd, OptSpec} <- SubSpecs,
        {_, Short, Long, _, _} = Spec<- OptSpec,
        Short =/= undefined orelse Long =/= undefined].

get_spec(Cmd) ->
    dict:fetch(Cmd, get_spec_registry()).

is_valid_cmd(Cmd) when is_list(Cmd) ->
    is_valid_cmd(list_to_atom(Cmd));
is_valid_cmd(Cmd) ->
    dict:is_key(Cmd, get_spec_registry()).

is_valid_cmd(Cmd, SubCmd) when is_list(Cmd) ->
    is_valid_cmd(list_to_atom(Cmd), list_to_atom(SubCmd));
is_valid_cmd(Cmd, SubCmd) ->
    is_valid_cmd(Cmd) andalso is_valid_subcmd(Cmd, SubCmd).

is_valid_subcmd(Cmd, SubCmd) ->
    {_Cmd, Spec} = get_spec(Cmd),
    proplists:is_defined(SubCmd, Spec).

get_opt_spec(Cmd, SubCmd) when is_list(Cmd) ->
    get_opt_spec(list_to_atom(Cmd), list_to_atom(SubCmd));
get_opt_spec(Cmd, SubCmd) ->
    {_Cmd, Spec} = get_spec(Cmd),
    proplists:get_value(SubCmd, Spec).

get_cmd_names() ->
    dict:fetch_keys(get_spec_registry()).

cmd_list() ->
    lists:flatten([cmd_list(Cmd)||Cmd <- get_cmd_names()]).

cmd_list(Cmd) when is_list(Cmd) ->
    cmd_list(list_to_atom(Cmd));
cmd_list(Cmd) ->
    {Cmd, SubCmdSpecs} = get_spec(Cmd),
    lists:foldl(fun({SubName, _}, Acc) ->
                  Acc++[{Cmd, SubName}]
          end, [],SubCmdSpecs).

usage(Cmd, SubCmd) ->
    getopt:usage(get_opt_spec(Cmd, SubCmd),
                 io_lib:format("~s ~s ~s", [?MODULE, Cmd, SubCmd])).

usage(Cmd) ->
    getopt:usage(?global_opt_spec, "riak_cs_inspector command "),
    io:format("Available commands:~n"),
    print_cmd_list(cmd_list(Cmd)).

usage() ->
    getopt:usage(?global_opt_spec, "riak_cs_inspector command "),
    io:format("Available commands:~n"),
    print_cmd_list(cmd_list()).

print_cmd_list(CmdList) ->
    [io:format("   ~9.s ~.5s [option]~n", [Cmd, SubCmd])||{Cmd, SubCmd} <- CmdList].

exit_with_usage(Reason) ->
    io:format("Error: ~p~n~n", [Reason]),
    usage(),
    halt(1).

maybe_exit_with_usage(Cmd) ->
    case is_valid_cmd(Cmd) of
        false ->
            usage(),
            halt(1);
        _ ->
            ok
    end.

maybe_exit_with_usage(Cmd, SubCmd) ->
    case is_valid_cmd(Cmd, SubCmd) of
        false ->
            usage(),
            halt(1);
        _ ->
            ok
    end.

maybe_exit_with_usage(Cmd, SubCmd, Opts) ->
    case proplists:get_value(help, Opts) of
        true ->
            usage(Cmd, SubCmd),
            halt(0);
        _ ->
            ok
    end.

%% CS Utilities

term_from_meta(Key, CsMeta) ->
    case lists:keyfind(Key, 1, CsMeta) of
        false ->
            undefined;
        {Key, Value} ->
            binary_to_term(Value)
    end.

-spec get_manifest(pid(), string(), string()) ->
                          [{integer(), UUID::(tombstone | binary()), Manifest::term()}].
get_manifest(RiakcPid, Bucket, Key)->
    ManifestBucketBin = riak_cs_utils:to_bucket_name(objects, Bucket),
    KeyBin = iolist_to_binary(Key), % todo: think about unicode
    lists:sort(
      [{SiblingNo, UUID, M}
       || {SiblingNo, {MD, Value}} <-
              get_riak_object(RiakcPid, ManifestBucketBin, KeyBin),
          {UUID, M} <- case MD of
                           tombstone ->
                               [{tombstone, {tombstone, {Bucket, Key}}}];
                           _V ->
                               case dict:find(<<"X-Riak-Deleted">>, MD) of
                                   {ok, true} ->
                                       [{tombstone, {tombstone, {Bucket, Key}}}];
                                   _ ->
                                       binary_to_term(Value)
                               end
                       end]).

get_gc_manifest(RiakcPid, Bucket, Key)->
    lists:sort(
      [{SiblingNo, UUID, M}
       || {SiblingNo, {MD, Value}} <-
              get_riak_object(RiakcPid, Bucket, Key),
          {UUID, M} <- case MD of
                           tombstone ->
                               [{tombstone, {tombstone, {Bucket, Key}}}];
                           _ ->
                               case twop_set:to_list(binary_to_term(Value)) of
                                   [] ->
                                       [{empty_twop_set, empty_twop_set}];
                                   Manifests ->
                                       Manifests
                               end
                       end]).

full_bkey(Bucket, Key, UUID, Seq) ->
    PrefixedBucket = riak_cs_utils:to_bucket_name(blocks, Bucket),
    FullKey = riak_cs_lfs_utils:block_name(Key, UUID, Seq),
    {PrefixedBucket, FullKey}.

?user_attr(key_id);
?user_attr(key_secret);
?user_attr(name).

?m_attr(bkey);
?m_attr(state);
?m_attr(uuid);
?m_attr(content_md5);
?m_attr(content_length);
?m_attr(props).

uuid_hex(M) ->
    mochihex:to_hex(m_attr(uuid, M)).

content_md5_hex(M) ->
    case m_attr(content_md5, M) of
        undefined ->
            undefined;
        %% Multipart has 2-tuple coment_md5, {UUID, "-" ++ length(Parts)}}
        %% At least at the time I'm writing this code
        {UUID, Suffix} ->
            mochihex:to_hex(binary_to_list(UUID) ++ Suffix);
        MD5 ->
            mochihex:to_hex(MD5)
    end.

%% Rather cutting-corners way. Export rec_pp_fun/2 if used.
print_record(Record) ->
    io:format(io_lib_pretty:print(Record, fun ?MODULE:rec_pp_fun/2)),
    io:nl().

?rec_pp_fun(moss_user);
?rec_pp_fun(moss_user_v1);
?rec_pp_fun(moss_bucket);
?rec_pp_fun(moss_bucket_v1);
?rec_pp_fun(rcs_user_v2);
?rec_pp_fun(acl_v1);
?rec_pp_fun(acl_v2);
?rec_pp_fun(lfs_manifest_v2);
?rec_pp_fun(lfs_manifest_v3);
?rec_pp_fun(part_manifest_v1);
?rec_pp_fun(multipart_manifest_v1);
?rec_pp_fun(access_v1);
rec_pp_fun(_, _) -> no.

print_manifest({SiblingNo, _, {tombstone, {B,K}}}) ->
    io:nl(),
    io:format("Sibling=~B: ~s/~s [~s]~n", [SiblingNo, B, K, "**tombstone**"]);
print_manifest({SiblingNo, _, M = #lfs_manifest_v3{}}) ->
    {B,K} = M#lfs_manifest_v3.bkey,
    io:nl(),
    io:format("Sibling=~B: ~s/~s [~s]~n", [SiblingNo, B, K, lfs_manifest_v3]),
    pp(uuid,                    uuid_hex(M)),
    pp(block_size,              M#lfs_manifest_v3.block_size),
    pp(metadata,                M#lfs_manifest_v3.metadata),
    pp(created,                 M#lfs_manifest_v3.created),
    pp(content_length,          M#lfs_manifest_v3.content_length),
    pp(content_type,            M#lfs_manifest_v3.content_type),
    pp(content_md5,             content_md5_hex(M)),
    pp(state,                   M#lfs_manifest_v3.state),
    pp(write_start_time,        M#lfs_manifest_v3.write_start_time),
    pp(last_block_written_time, M#lfs_manifest_v3.last_block_written_time),
    pp(delete_marked_time,      M#lfs_manifest_v3.delete_marked_time),
    pp(last_block_deleted_time, M#lfs_manifest_v3.last_block_deleted_time),
    pp(delete_blocks_remaining, M#lfs_manifest_v3.delete_blocks_remaining),
    pp(scheduled_delete_time,   M#lfs_manifest_v3.scheduled_delete_time),
    pp(acl,                     M#lfs_manifest_v3.acl),
    pp(props,                   M#lfs_manifest_v3.props),
    pp(cluster_id,              M#lfs_manifest_v3.cluster_id);

print_manifest({SiblingNo, _, M = #lfs_manifest_v2{}}) ->
    {B,K} = M#lfs_manifest_v2.bkey,
    io:nl(),
    io:format("SiblingNo=~B: ~s/~s [~s]~n", [SiblingNo, B, K, lfs_manifest_v2]),
    pp(uuid,       uuid_hex(M)),
    pp(block_size,              M#lfs_manifest_v2.block_size),
    pp(metadata,                M#lfs_manifest_v2.metadata),
    pp(created,                 M#lfs_manifest_v2.created),
    pp(content_length,          M#lfs_manifest_v2.content_length),
    pp(content_type,            M#lfs_manifest_v2.content_type),
    pp(content_md5,             content_md5_hex(M)),
    pp(state,                   M#lfs_manifest_v2.state),
    pp(write_start_time,        M#lfs_manifest_v2.write_start_time),
    pp(last_block_written_time, M#lfs_manifest_v2.last_block_written_time),
    pp(delete_marked_time,      M#lfs_manifest_v2.delete_marked_time),
    pp(last_block_deleted_time, M#lfs_manifest_v2.last_block_deleted_time),
    pp(delete_blocks_remaining, M#lfs_manifest_v2.delete_blocks_remaining),
    pp(acl,                     M#lfs_manifest_v2.acl),
    pp(props,                   M#lfs_manifest_v2.props),
    pp(cluster_id,              M#lfs_manifest_v2.cluster_id);

print_manifest(V) ->
    io:nl(),
    pp(unknown_lfs, V).

%% Riak Utilities

-spec get_riak_object(pid(), binary(), binary()) ->
                             [{SiblingNo::integer(),
                               {tombstone, tombstone} |
                               {Metadata::dict(), Value::binary()}}].
get_riak_object(RiakcPid, RiakBucket, RiakKey) ->
    %% With option deletedvclock, tombstone is represented as Object with no contents
    case riakc_pb_socket:get(RiakcPid, RiakBucket, RiakKey, [deletedvclock]) of
        {ok, Object} ->
            case riakc_obj:get_contents(Object) of
                [] ->
                    [{1, {tombstone, tombstone}}];
                MDAndValues ->
                    lists:zip(lists:seq(1, length(MDAndValues)), MDAndValues)
            end;
        {error, notfound} ->
            [];
        {error, notfound, _VC} ->
            [{1, {tombstone, tombstone}}]
    end.

lookup_listbucket_fun(Opts) ->
    case proplists:get_value(sort, Opts) of
        true ->
            fun pb_list_buckets/2;
        _ ->
            fun pb_stream_list_buckets/2
    end.

lookup_listkey_fun(Opts) ->
    case proplists:get_value(sort, Opts) of
        true ->
            fun pb_list_keys/3;
        _ ->
            fun pb_stream_list_keys/3
    end.

pb_list_buckets(RiakcPid, PrintFun) ->
        case riakc_pb_socket:list_buckets(RiakcPid) of
            {ok, Keys} ->
                PrintFun(lists:sort(Keys));
            Error ->
                Error
        end.

pb_stream_list_buckets(RiakcPid, PrintFun) ->
        case riakc_pb_socket:stream_list_buckets(RiakcPid) of
            {ok, ReqId} ->
                print_stream_list(ReqId, PrintFun);
            Error ->
                error(Error)
        end.

pb_list_keys(RiakcPid, BucketName, PrintFun) ->

        case riakc_pb_socket:list_keys(RiakcPid, BucketName) of
            {ok, Keys} ->
                PrintFun(lists:sort(Keys));
            Error ->
                Error
        end.

pb_stream_list_keys(RiakcPid, BucketName, PrintFun) ->
        case riakc_pb_socket:stream_list_keys(RiakcPid, BucketName) of
            {ok, ReqId} ->
                print_stream_list(ReqId, PrintFun);
            Error ->
                error(Error)
        end.

print_stream_list(ReqId, PrintFun) ->
    receive
        {ReqId, done} ->
            ok;
        {ReqId, {error, Reason}} ->
            error(Reason);
        {ReqId, {_, Res}} ->
            PrintFun(Res),
            print_stream_list(ReqId, PrintFun)
    end.

%% Other utilities

pp(Atom, Value) ->
    io:format("~30s: ~p~n", [Atom, Value]).
