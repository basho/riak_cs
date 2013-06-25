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
%% - MP manifest support
%% - moss.access, moss.storage bucket inspection
%% - key streaming instead of key listing, or use 2i streaming to keep result sorted

-export([main/1]).
-export([list_buckets/1, list_objects/2, print_object/3, list_blocks/4]).
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

usage() ->
    e("List buckets     : $ riak_cs_inspector.erl PB-IP PB-Port~n"),
    e("List CS keys     : $ riak_cs_inspector.erl PB-IP PB-Port CS-Bucket~n"),
    e("Count manifests  : $ riak_cs_inspector.erl PB-IP PB-Port CS-Bucket    "
      "count-manifests~n"),
    e("Count blocks     : $ riak_cs_inspector.erl PB-IP PB-Port CS-Bucket    "
      "count-blocks~n"),
    e("Show manifest    : $ riak_cs_inspector.erl PB-IP PB-Port CS-Bucket    "
      "key=Key~n"),
    e("List blocks      : $ riak_cs_inspector.erl PB-IP PB-Port CS-Bucket    "
      "key=Key uuid=UUIDPrefix~n"),
    e("List GC bucket   : $ riak_cs_inspector.erl PB-IP PB-Port riak-cs-gc~n"),
    e("Count GC bucket  : $ riak_cs_inspector.erl PB-IP PB-Port riak-cs-gc   "
      "count~n"),
    e("Show GC manifest : $ riak_cs_inspector.erl PB-IP PB-Port riak-cs-gc   "
      "key=Key~n"),
    e("List CS buckets  : $ riak_cs_inspector.erl PB-IP PB-Port moss.buckets~n"),
    e("Show CS bucket   : $ riak_cs_inspector.erl PB-IP PB-Port moss.buckets "
      "key=CsBucket~n"),
    e("List CS users    : $ riak_cs_inspector.erl PB-IP PB-Port moss.users~n"),
    e("Show CS user     : $ riak_cs_inspector.erl PB-IP PB-Port moss.users   "
      "name=Name~n"),
    e("Show CS user     : $ riak_cs_inspector.erl PB-IP PB-Port moss.users   "
      "key=KeyPrefix~n"),
    e("~n"),
    e("NOTE1: Make sure riak_cs is in ERL_LIBS or code paths.~n"),
    e("  e.g. $ ERL_LIBS=/path/to/riak-cs/lib riak_cs_inspector.erl ...~n"),
    e("NOTE2: PB-Port aliases, rel=8087, dev1=10017, etc..~n"),
    e("  e.g. $ riak_cs_inspector.erl 127.0.0.1 dev1 ...~n"),
    halt(1).

main([IP, PortStr | Rest]) ->
    Port = case PortStr of
               "rel"  ->  8087;
               "dev" ++ Index -> 10007 + list_to_integer(Index) * 10;
               _      -> list_to_integer(PortStr)
           end,
    io:format("Connecting to ~s:~B...~n", [IP, Port]),
    {ok, RiakcPid} = riakc_pb_socket:start_link(IP, Port),
    case Rest of
        [] ->
            list_buckets(RiakcPid);
        [Bucket] ->
            list_objects(RiakcPid, Bucket);
        [Bucket, Key] ->
            print_object(RiakcPid, Bucket, Key);
        [Bucket, Key, UUID] ->
            list_blocks(RiakcPid, Bucket, Key, UUID);
        _ ->
            usage()
    end;
main(_) ->
    usage().

e(Line) ->
    io:format(standard_error, Line, []).

-spec list_buckets(pid()) -> any().
list_buckets(RiakcPid) ->
    %% TODO(shino): Can include all names of [a-z][0-9a-zA-Z-]{2..4} ?
    KnownNames = [{crypto:md5(list_to_binary(B)), B}
                  || B <- ?KNOWN_BUCKETS],
    {ok, RiakBuckets} = riakc_pb_socket:list_buckets(RiakcPid),
    io:format("All buckets:~n"),
    io:format("[~-7s] ~-32..=s = ~-32..=s~n",
              [type, "cs-bucket-name ", "riak-bucket-name "]),
    [io:format("[~-7s] ~-32s = ~w~n", cs_bucket_info(RiakBucket, KnownNames))
     || RiakBucket <- lists:sort(RiakBuckets) ].

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

-spec list_objects(pid(), string()) -> any().
list_objects(RiakcPid, "moss.buckets" = Bucket)->
    io:format("~-64..=s ~-8..=s: ~-40..=s~n",
              ["CS Bucket Name ", "Sibl. ", "Owner Key "]),
    {ok, Keys} = riakc_pb_socket:list_keys(RiakcPid, Bucket),
    lists:foreach(fun(Key) ->
                          [io:format("~-64s ~-8B: ~-40s~n", [Key, SiblingNo, V])
                           || {SiblingNo, {_MD, V}}
                                  <- get_riak_object(RiakcPid, Bucket, Key)]
                  end, lists:sort(Keys));
list_objects(RiakcPid, "moss.users" = Bucket)->
    io:format("~-40..=s ~-8..=s: ~-40..=s ~-40..=s~n",
              ["Key ID ", "Sibl. ", "Name ", "Secret "]),
    {ok, Keys} = riakc_pb_socket:list_keys(RiakcPid, Bucket),
    %% TODO: only support #rcs_user_v2{}
    [[io:format("~-40s ~8B: ~-40s ~-40s~n",
                [user_attr(key_id, User),
                 SiblingNo,
                 user_attr(name, User),
                 user_attr(key_secret, User)])
      || {SiblingNo, {MD, V}} <- get_riak_object(RiakcPid, Bucket, Key),
         User <- [case MD of
                      tombstone -> tombstone;
                      _ -> binary_to_term(V)
                  end]]
     || Key <- lists:sort(Keys)];
list_objects(_RiakcPid, "moss.access")->
    throw(not_yet_implemented);
list_objects(_RiakcPid, "moss.storage")->
    throw(not_yet_implemented);
list_objects(RiakcPid, "riak-cs-gc" = Bucket)->
    ManifestKeys =
        case os:getenv("CS_INSPECTOR_INPUT") of
            false ->
                {ok, Keys} = riakc_pb_socket:list_keys(RiakcPid, Bucket),
                Keys;
            InputFileName ->
                {ok, Bin} = file:read_file(InputFileName),
                binary:split(Bin, <<"\n">>, [global, trim])
        end,
    print_gc_manifest_summary(pid, bucket, sibling_no, header),
    [print_gc_manifest_summary(RiakcPid, Key, SiblingNo, M)
     || Key <- lists:sort(ManifestKeys),
        {SiblingNo, _UUID, M} <- get_gc_manifest(RiakcPid, Bucket, Key)];
list_objects(RiakcPid, Bucket)->
    print_manifest_summary(pid, bucket, sibling_no, header),
    ManifestBucketName = riak_cs_utils:to_bucket_name(objects, Bucket),
    {ok, ManifestKeys} = riakc_pb_socket:list_keys(RiakcPid, ManifestBucketName),
    [print_manifest_summary(RiakcPid, Bucket, SiblingNo, M)
     || Key <- lists:sort(ManifestKeys),
        {SiblingNo, _UUID, M} <- get_manifest(RiakcPid, Bucket, Key)].

print_gc_manifest_summary(_RiakcPid, _Key, _SiblingNo, header) ->
    io:format("~-32..=s: ~-8..=s ~-16..=s ~-32..=s ~-16..=s ~-32..=s~n",
              ["Key ", "Sibl. ", "State ", "UUID ", "Content-Length", "CS Key "]);
print_gc_manifest_summary(_RiakcPid, Key, SiblingNo, {tombstone, {_Bucket, Key}}) ->
    io:format("~-32s: ~-8B ~-16s ~-32s ~16B ~-32s~n",
              [Key, SiblingNo, tombstone, tombstone, 0, tombstone]);
print_gc_manifest_summary(_RiakcPid, Key, SiblingNo, M) ->
    {_, CSKey} = m_attr(bkey, M),
    io:format("~-32s: ~-8B ~-16s ~-32s ~16B ~-32s~n",
              [Key, SiblingNo, m_attr(state, M), uuid_hex(M),
               m_attr(content_length, M), CSKey]).

print_manifest_summary(_RiakcPid, _Bucket, _SiblingNo, header) ->
    io:format("~-32..=s: ~-8..=s ~-16..=s ~-32..=s ~-16..=s ~-16..=s~n",
              ["Key ", "Sibl. ", "State ", "UUID ", "Content-Length", "First Block "]);
print_manifest_summary(_RiakcPid, _Bucket, SiblingNo, {tombstone, {_Bucket, Key}}) ->
    io:format("~-32s: ~-8B ~-16s ~-32s ~16B ~-16s~n",
              [Key, SiblingNo, tombstone, tombstone, 0, tombstone]);
print_manifest_summary(RiakcPid, Bucket, SiblingNo, M) ->
    {_, Key} = m_attr(bkey, M),
    UUID = m_attr(uuid, M),
    FirstBlockId = 0,
    {RiakBucket, RiakKey} = full_bkey(Bucket, Key, UUID, FirstBlockId),
    FirstBlockStatus = case riakc_pb_socket:get(RiakcPid, RiakBucket, RiakKey) of
                           {ok, _RiakObject} ->
                               "Found";
                           {error, notfound} ->
                               "**Not Found**"
                       end,
    io:format("~-32s: ~-8B ~-16s ~-32s ~16B ~-16s~n",
              [Key, SiblingNo, m_attr(state, M), uuid_hex(M),
               m_attr(content_length, M), FirstBlockStatus]).
    
-spec print_object(pid(), string(), string()) -> any().
print_object(RiakcPid, "moss.buckets" = RiakBucket, Condition) ->
    "key=" ++ CSBucket = Condition,
    [print_cs_bucket(CSBucket, SiblingNo, UserKey, RiakMD)
     || {SiblingNo, {RiakMD, UserKey}}
            <- get_riak_object(RiakcPid, RiakBucket, CSBucket)];
print_object(RiakcPid, "moss.users" = Bucket, Condition) ->
    case Condition of
        "name=" ++ Name ->
            print_users(RiakcPid, Bucket, {name, Name});
        "key=" ++ KeyPrefix ->
            print_users(RiakcPid, Bucket, {key, KeyPrefix})
    end;
print_object(RiakcPid, Bucket, "count" = _Key)->
    count_riak_bucket(RiakcPid, Bucket, Bucket, 100*1000);
print_object(RiakcPid, Bucket, "count-manifests" = _Key)->
    ManifestBucketBin = riak_cs_utils:to_bucket_name(objects, Bucket),
    count_riak_bucket(RiakcPid, ManifestBucketBin, Bucket ++ ":manifests", 100*1000);
print_object(RiakcPid, Bucket, "count-blocks" = _Key)->
    BlockBucketBin = riak_cs_utils:to_bucket_name(blocks, Bucket),
    count_riak_bucket(RiakcPid, BlockBucketBin, Bucket ++ ":blocks", 100*1000);
print_object(RiakcPid, "riak-cs-gc" = Bucket, "key=" ++ Key)->
    Manifests = get_gc_manifest(RiakcPid, Bucket, Key),
    io:format("----- ~B instance(s) -----~n", [length(Manifests)]),
    [ print_manifest(Manifest) || Manifest <- Manifests];
print_object(RiakcPid, Bucket, "key=" ++ Key)->
    Manifests = get_manifest(RiakcPid, Bucket, Key),
    io:format("----- ~B instance(s) -----~n", [length(Manifests)]),
    [ print_manifest(Manifest) || Manifest <- Manifests].

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
    {Acl, Policy} = case dict:find(<<"X-Riak-Meta">>, MD) of
                        {ok, CsMeta} ->
                            {term_from_meta(<<"X-Moss-Acl">>, CsMeta),
                             term_from_meta(<<"X-Rcs-Policy">>, CsMeta)};
                        error ->
                            {undefined, undefined}
                    end,
    io:nl(),
    io:format("Bucket   : ~s~n", [Bucket]),
    io:format("SiblingNo: ~B~n", [SiblingNo]),
    io:format("Owner    : ~s~n", [UserKey]),
    io:format("Acl:~n"),
    print_record(Acl),
    io:format("Policy:~n~s~n", [Policy]).

print_users(RiakcPid, Bucket, Options) ->
    {ok, Keys} = riakc_pb_socket:list_keys(RiakcPid, Bucket),
    [[maybe_print_user(U, SiblingNo, Options)
      || {SiblingNo, {RiakMD, ValueBin}}
             <- get_riak_object(RiakcPid, Bucket, UserKey),
         U <- [case RiakMD of
                   tombstone -> {tombstone, UserKey};
                   _ -> binary_to_term(ValueBin)
               end]]
     || UserKey <- Keys].
    %% [
    %% lists:foreach(
    %%   fun(UserKey) ->
    %%           {ok, RiakObj} = riakc_pb_socket:get(RiakcPid, Bucket, UserKey),
    %%           MDAndValues = riakc_obj:get_contents(RiakObj),
    %%           [print_user(MD, Value, Options)
    %%            || {MD, Value} <- MDAndValues]
    %%   end, Keys).

maybe_print_user({tombstone, UserKey}, SiblingNo, {key, UserKey}) ->
    io:nl(),
    io:format("User Key : ~s~n", [UserKey]),
    io:format("SiblingNo: ~B~n", [SiblingNo]),
    io:format("Record   : **tombstone**");
maybe_print_user({tombstone, _}, _SiblingNo, _Options) ->
    ok;
maybe_print_user(User, SiblingNo, {name, Name}) ->
    case user_attr(name, User) of
        Name ->
            print_user(User, SiblingNo);
        _ ->
            ok
    end;
maybe_print_user(User, SiblingNo, {key, KeyPrefix}) ->
    case lists:prefix(KeyPrefix, user_attr(key_id, User)) of
        true ->
            print_user(User, SiblingNo);
        _ ->
            ok
    end.

print_user(User, SiblingNo) ->
    io:nl(),
    io:format("User Key : ~s~n", [user_attr(name, User)]),
    io:format("SiblingNo: ~B~n", [SiblingNo]),
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

list_blocks(RiakcPid, Bucket, "key=" ++ Key, "uuid=" ++ UUIDHexPrefix) ->
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
                %% TODO: multipart upload
                {GotUUID, riak_cs_lfs_utils:block_sequences_for_manifest(Manifest)}
        end,
    io:format("Blocks in object [~s/~s]:~n", [Key, mochihex:to_hex(UUID)]),
    io:format("~-32..=s ~-8..=s: ~-10..=s ~-64..=s~n",
              ["UUID ", "Block ", "Size ", "Value(first 8 or 32 bytes) "]),
    [print_block(RiakcPid, Bucket, Key, UUID, B) || B <- Blocks ].

print_block(RiakcPid, Bucket, Key, UUID, Block) ->
    BlockId = case Block of
                  {_UUID, Id} ->
                      Id;
                  Id ->
                      Id
              end,
    {RiakBucket, RiakKey} = full_bkey(Bucket, Key, UUID, BlockId),
    case riakc_pb_socket:get(RiakcPid, RiakBucket, RiakKey) of
        {ok, RiakObject} ->
            Value = riakc_obj:get_value(RiakObject),
            ByteSize = byte_size(Value),
            FirstChars = binary:part(Value, 0, min(32, ByteSize)),
            %% http://gambasdoc.org/help/doc/pcre
            %% - :graph: printing excluding space
            %% - \A:     start of subject 
            %% - \z:     end of subject 
            case re:run(FirstChars, "\\A[[:graph:]]*\\z", []) of
                nomatch ->
                    io:format("~-32s ~8B: ~10B ~64w~n",
                              [mochihex:to_hex(UUID), BlockId,
                               ByteSize, binary:part(Value, 0, min(8, ByteSize))]);
                _ ->
                    io:format("~-32s ~8B: ~10B ~s~n",
                              [mochihex:to_hex(UUID), BlockId,
                               ByteSize, FirstChars])
            end;
        {error, notfound} ->
            io:format("~-32B: ~-32s ~-32s~n",
                      [BlockId, "****************", "**Not Found**"])
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
                           _ ->
                               binary_to_term(Value)
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
                               twop_set:to_list(binary_to_term(Value))
                       end]).

full_bkey(Bucket, Key, UUID, BlockId) ->
    PrefixedBucket = riak_cs_utils:to_bucket_name(blocks, Bucket),
    FullKey = riak_cs_lfs_utils:block_name(Key, UUID, BlockId),
    {PrefixedBucket, FullKey}.

?user_attr(key_id);
?user_attr(key_secret);
?user_attr(name).

?m_attr(bkey);
?m_attr(state);
?m_attr(uuid);
?m_attr(content_md5);
?m_attr(content_length).

uuid_hex(M) ->
    mochihex:to_hex(m_attr(uuid, M)).

content_md5_hex(M) ->
    case m_attr(content_md5, M) of
        undefined ->
            undefined;
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
            []
    end.

%% Other utilities

pp(Atom, Value) ->
    io:format("~30s: ~p~n", [Atom, Value]).
