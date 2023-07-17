%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved,
%%               2021, 2022 TI Tokyo    All Rights Reserved.
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
         version/1,
         status/1,
         supps/1,
         test/1,
         cluster_info/1,
         audit_bucket_ownership/1,
         cleanup_orphan_multipart/0,
         cleanup_orphan_multipart/1
        ]).

-export([resolve_siblings/2, resolve_siblings/3, resolve_siblings/7]).

-include("riak_cs.hrl").
-include_lib("riakc/include/riakc.hrl").
-include_lib("kernel/include/logger.hrl").

%%%===================================================================
%%% Public API
%%%===================================================================

version([]) ->
    {ok, Vsn} = application:get_env(riak_cs, cs_version),
    io:format("version : ~p~n", [Vsn]),
    ok.

status([]) ->
    Stats = riak_cs_stats:get_stats() ++ stanchion_stats:get_stats(),
    _ = [io:format("~p : ~p~n", [Name, Value])
         || {Name, Value} <- Stats],
    ok.

supps(Opts) ->
    supps:p(Opts).


test([]) ->
    UserName = <<"ADMINTESTUSER">>,
    Bucket = riak_cs_aws_utils:make_id(10),
    try
        {ok, RcPid} = riak_cs_riak_client:start_link([]),
        {User, UserObj} = get_test_user(UserName, RcPid),

        BagId = riak_cs_mb_helper:choose_bag_id(manifest, Bucket),

        Acl = riak_cs_acl_utils:default_acl(
                       User?RCS_USER.display_name,
                       User?RCS_USER.canonical_id,
                       User?RCS_USER.key_id),

        io:format("creating bucket\n", []),
        ok = riak_cs_bucket:create_bucket(
               User, UserObj, Bucket, BagId, Acl),

        Key = <<"testkey">>,
        Value = <<"testvalue">>,
        io:format("putting object\n", []),
        ok = put_object(Bucket, Key, Value, Acl, RcPid),
        io:format("reading object back\n", []),
        ok = verify_object(Bucket, Key, Value, RcPid),
        io:format("deleting object\n", []),
        ok = delete_object(Bucket, Key, RcPid),

        io:format("deleting bucket\n", []),
        ok = riak_cs_bucket:delete_bucket(
               User, UserObj, Bucket, RcPid),
        ok = riak_cs_riak_client:stop(RcPid),
        ok
    catch
        E:R:T ->
            logger:error("self-test failed: ~p:~p", [E, R]),
            io:format("self-test failed: ~p:~p\nStacktrace: ~p\n", [E, R, T])
    end.

get_test_user(Name, RcPid) ->
    Email = <<Name/binary, "@testusers.com">>,
    case riak_cs_user:create_user(Name, Email) of
        {ok, ?RCS_USER{key_id = KeyId}} ->
            {ok, UU} =
                riak_cs_user:get_user(KeyId, RcPid),
            UU;
        {error, unknown} ->
            %% this is because velvet sees {error,insufficient_vnodes_available}. Just retry.
            timer:sleep(200),
            get_test_user(Name, RcPid);
        {error, user_already_exists} ->
            {ok, UU} =
                riak_cs_iam:find_user(#{email => Email}, RcPid),
            UU
    end.


put_object(Bucket, Key, Value, Acl, RcPid) ->
    Args = [{Bucket, Key, ?LFS_DEFAULT_OBJECT_VERSION,
             size(Value), <<"application/octet-stream">>,
             [], riak_cs_lfs_utils:block_size(), Acl, timer:seconds(60), self(), RcPid}],
    {ok, Pid} = riak_cs_put_fsm_sup:start_put_fsm(node(), Args),
    ok = riak_cs_put_fsm:augment_data(Pid, Value),
    {ok, _Mfst} = riak_cs_put_fsm:finalize(Pid, base64:encode(crypto:hash(md5, Value))),
    ok.

verify_object(Bucket, Key, Value, RcPid) ->
    FetchConcurrency = riak_cs_lfs_utils:fetch_concurrency(),
    BufferFactor = riak_cs_lfs_utils:get_fsm_buffer_size_factor(),
    {ok, Pid} =
        riak_cs_get_fsm_sup:start_get_fsm(
          node(), Bucket, Key, ?LFS_DEFAULT_OBJECT_VERSION, self(), RcPid,
          FetchConcurrency, BufferFactor),

    _Manifest = riak_cs_get_fsm:get_manifest(Pid),
    riak_cs_get_fsm:continue(Pid, {0, size(Value) - 1}),
    Value = read_object(Pid, <<>>),

    ok = riak_cs_get_fsm:stop(Pid),
    ok.

read_object(Pid, Q) ->
    case riak_cs_get_fsm:get_next_chunk(Pid) of
        {done, Chunk} ->
            <<Q/binary, Chunk/binary>>;
        {chunk, Chunk} ->
            read_object(Pid, <<Q/binary, Chunk/binary>>)
    end.

delete_object(Bucket, Key, RcPid) ->
    {ok, _UUIDsMarkedforDelete} =
        riak_cs_utils:delete_object(Bucket, Key, ?LFS_DEFAULT_OBJECT_VERSION, RcPid),
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
            logger:error("Cluster_info failed ~p:~p", [Exception, Reason]),
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
    ?LOG_DEBUG("FromUsers: ~p", [lists:usort(gb_sets:to_list(FromUsers))]),
    ?LOG_DEBUG("FromBuckets: ~p", [lists:usort(gb_sets:to_list(FromBuckets))]),
    ?LOG_DEBUG("OwnedBy: ~p", [lists:usort(gb_trees:to_list(OwnedBy))]),
    Inconsistencies0 =
        gb_sets:fold(
          fun ({U, B}, Acc) ->
                  logger:info("Bucket is not tracked by user: {User, Bucket} = {~s, ~s}", [U, B]),
                  [{not_tracked, {U, B}} | Acc]
          end, [], gb_sets:subtract(FromBuckets, FromUsers)),
    gb_sets:fold(
      fun({U,B}, Acc) ->
              case gb_trees:lookup(B, OwnedBy) of
                  none ->
                      logger:info("Bucket does not exist: {User, Bucket} = {~s, ~s}", [U, B]),
                      [{no_bucket_object, {U, B}} | Acc];
                  {value, DifferentUser} ->
                      logger:info("Bucket is owned by different user: {User, Bucket, DifferentUser} = "
                                  "{~s, ~s, ~s}", [U, B, DifferentUser]),
                      [{different_user, {U, B, DifferentUser}} | Acc]
              end
      end, Inconsistencies0, gb_sets:subtract(FromUsers, FromBuckets)).

ownership_from_users(RcPid) ->
    {ok, UserKeys} = riak_cs_user:fetch_user_keys(RcPid),
    lists:foldl(
      fun(UserKey, Ownership) ->
              {ok, {RCSUser, _Obj}} = riak_cs_user:get_user(UserKey, RcPid),
              ?RCS_USER{buckets = Buckets} = RCSUser,
              lists:foldl(
                fun(?RCS_BUCKET{name = Bucket}, InnerOwnership) ->
                        gb_sets:add_element({UserKey, Bucket},
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
    logger:info("cleaning up with timestamp ~s", [Timestamp]),
    io:format("cleaning up with timestamp ~s", [Timestamp]),
    Fun = fun(RcPidForOneBucket, BucketName, GetResult, Acc0) ->
                  ok = maybe_cleanup_csbucket(RcPidForOneBucket, BucketName,
                                              GetResult, Timestamp),
                  Acc0
          end,
    _ = riak_cs_bucket:fold_all_buckets(Fun, [], RcPid),

    ok = riak_cs_riak_client:stop(RcPid),
    logger:info("All old unaborted orphan multipart uploads have been deleted.", []),
    io:format("~nAll old unaborted orphan multipart uploads have been deleted.~n", []).


-spec resolve_siblings(binary(), binary()) -> any().
resolve_siblings(RawBucket, RawKey) ->
    case riak_cs_config:is_multibag_enabled() of
        true ->
            {error, not_supported};
        false ->
            {ok, RcPid} = riak_cs_riak_client:start_link([]),
            try
                {ok, Pid} = riak_cs_riak_client:master_pbc(RcPid),
                resolve_siblings(Pid, RawBucket, RawKey)
            after
                riak_cs_riak_client:stop(RcPid)
            end
    end.

%% @doc Resolve siblings as quick operation. Assuming login via
%% `riak-cs attach` RawKey and RawBucket should be provided via Riak
%% log. Example usage is:
%%
%% > {ok, Pid} = riakc_pb_socket:start_link(Host, Port).
%% > riak_cs_console:resolve_siblings(Pid, <<...>>, <<...>>).
%% > riakc_pb_socket:stop(Pid).
%%
%% or, in case of manifests, for CS Bucket and Key name:
%%
%% > {ok, Pid} = riakc_pb_socket:start_link(Host, Port).
%% > RawBucket = riak_cs_utils:to_bucket_name(objects, CSBucketName),
%% > riak_cs_console:resolve_siblings(Pid, RawBucket, CSKey).
%% > riakc_pb_socket:stop(Pid).
%%
-spec resolve_siblings(pid(), RawBucket::binary(), RawKey::binary()) -> ok | {error, term()}.
resolve_siblings(Pid, RawBucket, RawKey) ->
    GetOptions = [{r, all}],
    PutOptions = [{w, all}],
    resolve_siblings(Pid, RawBucket, RawKey,
                     GetOptions, ?DEFAULT_RIAK_TIMEOUT,
                     PutOptions, ?DEFAULT_RIAK_TIMEOUT).

resolve_siblings(Pid, RawBucket, RawKey, GetOptions, GetTimeout, PutOptions, PutTimeout)
  when is_integer(GetTimeout) andalso is_integer(PutTimeout) ->
    case riakc_pb_socket:get(Pid, RawBucket, RawKey, GetOptions, GetTimeout) of
        {ok, RiakObj} ->
            logger:info("Trying to resolve ~p sibling(s) of ~p:~p",
                        [riakc_obj:value_count(RiakObj), RawBucket, RawKey]),
            case resolve_ro_siblings(RiakObj, RawBucket, RawKey) of
                {ok, RiakObj2} ->
                    R = riakc_pb_socket:put(Pid, RiakObj2, PutOptions, PutTimeout),
                    logger:info("Resolution result: ~p", [R]),
                    R;
                ok ->
                    logger:info("No siblings in ~p:~p", [RawBucket, RawKey]);
                {error, _} = E ->
                    logger:info("Not updating ~p:~p: ~p", [RawBucket, RawKey, E]),
                    E
            end;
        {error, Reason} = E ->
            logger:info("Failed to get an object before resolution: ~p", [Reason]),
            E
    end.

%%%===================================================================
%%% Internal functions
%%%===================================================================

-spec resolve_ro_siblings(riakc_obj:riakc_obj(), binary(), binary()) ->
                                 {ok, riakc_obj:riakc_obj()} | %% Needs update to resolve siblings
                                 ok | %% No siblings, no need to update
                                 {error, term()}. %% Some other reason that prohibits update
resolve_ro_siblings(_, ?USER_BUCKET, _) ->
    %% This is not supposed to happen unless something is messed up
    %% with Stanchion. Resolve logic is not obvious and needs operator decision.
    {error, {not_supported, ?USER_BUCKET}};
resolve_ro_siblings(_, ?ACCESS_BUCKET, _) ->
    %% To support access bucket, JSON'ized data should change its data
    %% structure to G-set, or map(register -> P-counter)
    {error, {not_supported, ?ACCESS_BUCKET}};
resolve_ro_siblings(_, ?STORAGE_BUCKET, _) ->
    %% Storage bucket access conflict resolution is not obvious; maybe
    %% adopt max usage on each buckets.
    {error, {not_supported, ?STORAGE_BUCKET}};
resolve_ro_siblings(_, ?BUCKETS_BUCKET, _) ->
    %% Bucket conflict resolution obvious, referring to all users
    %% described in value and find out who's true user. If multiple
    %% users are owner (that cannot happen unless Stanchion is messed
    %% up), operator must choose who is true owner.
    {error, {not_supported, ?BUCKETS_BUCKET}};
resolve_ro_siblings(RO, ?GC_BUCKET, _) ->
    Resolved = riak_cs_gc:decode_and_merge_siblings(RO, twop_set:new()),
    Obj = riak_cs_utils:update_obj_value(RO,
                                         riak_cs_utils:encode_term(Resolved)),
    {ok, Obj};
resolve_ro_siblings(RO, <<"0b:", _/binary>>, _) ->
    case riak_cs_utils:resolve_robj_siblings(riakc_obj:get_contents(RO)) of
        {{MD, Value}, true} when is_binary(Value) ->
            RO1 = riakc_obj:update_metadata(RO, MD),
            {ok, riakc_obj:update_value(RO1, Value)};
        {E, true} ->
            logger:info("Cannot resolve: ~p", [E]),
            {error, E};
        {_, false} ->
            ok
    end;
resolve_ro_siblings(RiakObject, <<"0o:", _/binary>>, _RawKey) ->
    [{_, Manifest}|_] = Manifests =
        riak_cs_manifest:manifests_from_riak_object(RiakObject),
    logger:info("Number of histories after sibling resolution: ~p.", [length(Manifests)]),
    ObjectToWrite0 = riak_cs_utils:update_obj_value(
                       RiakObject, riak_cs_utils:encode_term(Manifests)),

    {B, _K} = Manifest?MANIFEST.bkey,
    RO = riak_cs_manifest_fsm:update_md_with_multipart_2i(ObjectToWrite0, Manifests, B),
    {ok, RO}.

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
                    logger:warning("Error in deleting old uploads: ~p", [Error]),
                    io:format("Error in deleting old uploads: ~p <<< \n", [Error]),
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
    logger:error("~p on processing ~s", [Error, BucketName]),
    io:format("Error: ~p on processing ~s\n", [Error, BucketName]),
    Error.
