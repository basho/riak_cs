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

%% @doc riak_cs bucket utility functions, but I dare not use a module
%% name with '_utils'.

-module(riak_cs_bucket).

%% Public API
-export([
         fetch_bucket_object/2,
         create_bucket/6,
         delete_bucket/4,
         get_buckets/1,
         set_bucket_acl/5,
         set_bucket_policy/5,
         delete_bucket_policy/4,
         get_bucket_acl_policy/3,
         maybe_log_bucket_owner_error/2,
         resolve_buckets/3,
         update_bucket_record/1,
         delete_all_uploads/2,
         delete_old_uploads/3,
         fold_all_buckets/3,
         fetch_bucket_keys/1
        ]).

-include("riak_cs.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").
-include_lib("riakc/include/riakc.hrl").

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Create a bucket in the global namespace or return
%% an error if it already exists.
-spec create_bucket(rcs_user(), term(), binary(), bag_id(), acl(), riak_client()) ->
                           ok |
                           {error, term()}.
create_bucket(User, UserObj, Bucket, BagId, ACL, RcPid) ->
    CurrentBuckets = get_buckets(User),

    %% Do not attempt to create bucket if the user already owns it
    AttemptCreate = riak_cs_config:disable_local_bucket_check() orelse
        not bucket_exists(CurrentBuckets, binary_to_list(Bucket)),
    case AttemptCreate of
        true ->
            BucketLimit = riak_cs_config:max_buckets_per_user(),
            case valid_bucket_name(Bucket) of
                true when length(CurrentBuckets) >= BucketLimit ->
                    {error, {toomanybuckets, length(CurrentBuckets), BucketLimit}};
                true ->
                    serialized_bucket_op(Bucket,
                                         BagId,
                                         ACL,
                                         User,
                                         UserObj,
                                         create,
                                         [velvet, create_bucket],
                                         RcPid);
                false ->
                    {error, invalid_bucket_name}
            end;
        false ->
            ok
    end.

-spec valid_bucket_name(binary()) -> boolean().
valid_bucket_name(Bucket) when byte_size(Bucket) < 3 orelse
                               byte_size(Bucket) > 63 ->
    false;
valid_bucket_name(Bucket) ->
    lists:all(fun(X) -> X end, [valid_bucket_label(Label) ||
                                   Label <- binary:split(Bucket,
                                                         <<".">>,
                                                         [global])])
        andalso not is_bucket_ip_addr(binary_to_list(Bucket)).

-spec valid_bucket_label(binary()) -> boolean().
valid_bucket_label(<<>>) ->
    %% this clause gets called when we either have a `.' as the first or
    %% last byte. Or if it appears twice in a row. Examples are:
    %% `<<".myawsbucket">>'
    %% `<<"myawsbucket.">>'
    %% `<<"my..examplebucket">>'
    false;
valid_bucket_label(Label) ->
    valid_bookend_char(binary:first(Label)) andalso
        valid_bookend_char(binary:last(Label)) andalso
        lists:all(fun(X) -> X end,
                  [valid_bucket_char(C) || C <- middle_chars(Label)]).

-spec middle_chars(binary()) -> list().
middle_chars(B) when byte_size(B) < 3 ->
    [];
middle_chars(B) ->
    %% `binary:at/2' is zero based
    ByteSize = byte_size(B),
    [binary:at(B, Position) || Position <- lists:seq(1, ByteSize - 2)].

-spec is_bucket_ip_addr(string()) -> boolean().
is_bucket_ip_addr(Bucket) ->
    case inet_parse:ipv4strict_address(Bucket) of
        {ok, _} ->
            true;
        {error, _} ->
            false
    end.

-spec valid_bookend_char(integer()) -> boolean().
valid_bookend_char(Char) ->
    numeric_char(Char) orelse lower_case_char(Char).

-spec valid_bucket_char(integer()) -> boolean().
valid_bucket_char(Char) ->
    numeric_char(Char) orelse
        lower_case_char(Char) orelse
        dash_char(Char).

-spec numeric_char(integer()) -> boolean().
numeric_char(Char) ->
    Char >= $0 andalso Char =< $9.

-spec lower_case_char(integer()) -> boolean().
lower_case_char(Char) ->
    Char >= $a andalso Char =< $z.

-spec dash_char(integer()) -> boolean().
dash_char(Char) ->
    Char =:= $-.

%% @doc Delete a bucket
-spec delete_bucket(rcs_user(), riakc_obj:riakc_obj(), binary(), riak_client()) ->
                           ok |
                           {error, remaining_multipart_upload}.
delete_bucket(User, UserObj, Bucket, RcPid) ->
    CurrentBuckets = get_buckets(User),

    %% Buckets can only be deleted if they exist
    {AttemptDelete, LocalError} =
        case bucket_exists(CurrentBuckets, binary_to_list(Bucket)) of
            true ->
                case bucket_empty(Bucket, RcPid) of
                    {ok, true}  -> {true, ok};
                    {ok, false} -> {false, {error, bucket_not_empty}}
                end;
            false -> {true, ok}
        end,
    case AttemptDelete of
        true ->
            %% TODO: output log if failed in cleaning up existing uploads.
            %% The number of retry is hardcoded.
            {ok, Count} = delete_all_uploads(Bucket, RcPid),
            _ = lager:debug("deleted ~p multiparts before bucket deletion.", [Count]),
            %% This call still may return {error, remaining_multipart_upload}
            %% even if all uploads cleaned up above, because concurrent
            %% multiple deletion may happen. Then Riak CS returns 409 confliction
            %% which is not in S3 specification....
            serialized_bucket_op(Bucket,
                                 ?ACL{},
                                 User,
                                 UserObj,
                                 delete,
                                 [velvet, delete_bucket],
                                 RcPid);
        false ->
            LocalError
    end.

%% @doc TODO: this function is to be moved to riak_cs_multipart_utils or else?
-spec delete_all_uploads(binary(), riak_client()) -> {ok, non_neg_integer()} | {error, term()}.
delete_all_uploads(Bucket, RcPid) ->
    delete_old_uploads(Bucket, RcPid, <<255>>).

%% @doc deletes all multipart uploads older than Timestamp.
%% input binary format of iso8068
-spec delete_old_uploads(binary(), riak_client(), binary()) ->
                                {ok, non_neg_integer()} | {error, term()}.
delete_old_uploads(Bucket, RcPid, Timestamp) when is_binary(Timestamp) ->
    Opts = [{delimiter, undefined}, {max_uploads, undefined},
            {prefix, undefined}, {key_marker, <<>>},
            {upload_id_marker, <<>>}],
    {ok, {Ds, _Commons}} = riak_cs_mp_utils:list_all_multipart_uploads(Bucket, Opts, RcPid),
    fold_delete_uploads(Bucket, RcPid, Ds, Timestamp, 0).

fold_delete_uploads(_Bucket, _RcPid, [], _Timestamp, Count) -> {ok, Count};
fold_delete_uploads(Bucket, RcPid, [D|Ds], Timestamp, Count)->
    Key = D?MULTIPART_DESCR.key,

    %% cannot fail here
    {ok, Obj, Manifests} = riak_cs_manifest:get_manifests(RcPid, Bucket, Key),

    UploadId = D?MULTIPART_DESCR.upload_id,

    %% find_manifest_with_uploadid
    case lists:keyfind(UploadId, 1, Manifests) of

        {UploadId, M} when M?MANIFEST.state == writing
                           %% comparing timestamp here, like
                           %% <<"2012-02-17T18:22:50.000Z">> < <<"2014-05-11-....">> => true
                           andalso M?MANIFEST.created < Timestamp ->
            case riak_cs_gc:gc_specific_manifests(
                   [M?MANIFEST.uuid], Obj, Bucket, Key, RcPid) of
                {ok, _NewObj} ->
                    fold_delete_uploads(Bucket, RcPid, Ds, Timestamp, Count+1);
                E ->
                    lager:debug("cannot delete multipart manifest: ~p ~p (~p)",
                                [{Bucket, Key}, M?MANIFEST.uuid, E]),
                    E
            end;
        _E ->
            lager:debug("skipping multipart manifest: ~p ~p (~p)",
                        [{Bucket, Key}, UploadId, _E]),
            fold_delete_uploads(Bucket, RcPid, Ds, Timestamp, Count)
    end.

-spec fold_all_buckets(fun(), term(), riak_client()) -> {ok, term()} | {error, any()}.
fold_all_buckets(Fun, Acc0, RcPid) when is_function(Fun) ->
    iterate_csbuckets(RcPid, Acc0, Fun, undefined).

-spec iterate_csbuckets(riak_client(), term(), fun(), binary()|undefined) ->
                               {ok, term()} | {error, any()}.
iterate_csbuckets(RcPid, Acc0, Fun, Cont0) ->

    Options = case Cont0 of
                  undefined -> [];
                  _ ->         [{continuation, Cont0}]
              end ++ [{max_results, 1024}],

    {ok, MasterPbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riak_cs_pbc:get_index_range(MasterPbc, ?BUCKETS_BUCKET,
                                     <<"$key">>, <<0>>, <<255>>,
                                     Options, [riakc, get_cs_buckets_by_index]) of
        {ok, ?INDEX_RESULTS{keys=BucketNames, continuation=Cont}} ->
            Foldfun = iterate_csbuckets_fold_fun(Fun),
            Acc2 = lists:foldl(Foldfun, Acc0, BucketNames),
            case Cont of
                undefined ->
                    {ok, Acc2};
                _ ->
                    iterate_csbuckets(RcPid, Acc2, Fun, Cont)
            end;

        Error ->
            _ = lager:error("iterating CS buckets: ~p", [Error]),
            {error, {Error, Acc0}}
    end.

iterate_csbuckets_fold_fun(FunForOneBucket) ->
    fun(BucketName, Acc) ->
            {ok, RcPidForOneBucket} = riak_cs_riak_client:start_link([]),
            try
                BucketRes = riak_cs_riak_client:get_bucket(RcPidForOneBucket, BucketName),
                FunForOneBucket(RcPidForOneBucket, BucketName, BucketRes, Acc)
            after
                riak_cs_riak_client:stop(RcPidForOneBucket)
            end
    end.

%% @doc Return a user's buckets.
-spec get_buckets(rcs_user()) -> [cs_bucket()].
get_buckets(?RCS_USER{buckets=Buckets}) ->
    [Bucket || Bucket <- Buckets, Bucket?RCS_BUCKET.last_action /= deleted].

%% @doc Set the ACL for a bucket. Existing ACLs are only
%% replaced, they cannot be updated.
-spec set_bucket_acl(rcs_user(), riakc_obj:riakc_obj(), binary(), acl(), riak_client()) ->
                            ok | {error, term()}.
set_bucket_acl(User, UserObj, Bucket, ACL, RcPid) ->
    serialized_bucket_op(Bucket,
                         ACL,
                         User,
                         UserObj,
                         update_acl,
                         [velvet, set_bucket_acl],
                         RcPid).

%% @doc Set the policy for a bucket. Existing policy is only overwritten.
-spec set_bucket_policy(rcs_user(), riakc_obj:riakc_obj(), binary(), []|policy()|acl(), riak_client()) ->
                               ok | {error, term()}.
set_bucket_policy(User, UserObj, Bucket, PolicyJson, RcPid) ->
    serialized_bucket_op(Bucket,
                         PolicyJson,
                         User,
                         UserObj,
                         update_policy,
                         [velvet, set_bucket_policy],
                         RcPid).

%% @doc Set the policy for a bucket. Existing policy is only overwritten.
-spec delete_bucket_policy(rcs_user(), riakc_obj:riakc_obj(), binary(), riak_client()) ->
                                  ok | {error, term()}.
delete_bucket_policy(User, UserObj, Bucket, RcPid) ->
    serialized_bucket_op(Bucket,
                         [],
                         User,
                         UserObj,
                         delete_policy,
                         [velvet, delete_bucket_policy],
                         RcPid).

%% @doc fetch moss.bucket and return acl and policy
-spec get_bucket_acl_policy(binary(), atom(), riak_client()) ->
                                   {acl(), policy()} | {error, term()}.
get_bucket_acl_policy(Bucket, PolicyMod, RcPid) ->
    case fetch_bucket_object(Bucket, RcPid) of
        {ok, Obj} ->
            %% For buckets there should not be siblings, but in rare
            %% cases it may happen so check for them and attempt to
            %% resolve if possible.
            Contents = riakc_obj:get_contents(Obj),
            Acl = riak_cs_acl:bucket_acl_from_contents(Bucket, Contents),
            Policy = PolicyMod:bucket_policy_from_contents(Bucket, Contents),
            format_acl_policy_response(Acl, Policy);
        {error, _}=Error ->
            Error
    end.

-type policy_from_meta_result() :: {'ok', policy()} | {'error', 'policy_undefined'}.
-type bucket_policy_result() :: policy_from_meta_result() | {'error', 'multiple_bucket_owners'}.
-type acl_from_meta_result() :: {'ok', acl()} | {'error', 'acl_undefined'}.
-type bucket_acl_result() :: acl_from_meta_result() | {'error', 'multiple_bucket_owners'}.
-spec format_acl_policy_response(bucket_acl_result(), bucket_policy_result()) ->
                                        {error, atom()} | {acl(), 'undefined' | policy()}.
format_acl_policy_response({error, _}=Error, _) ->
    Error;
format_acl_policy_response(_, {error, multiple_bucket_owners}=Error) ->
    Error;
format_acl_policy_response({ok, Acl}, {error, policy_undefined}) ->
    {Acl, undefined};
format_acl_policy_response({ok, Acl}, {ok, Policy}) ->
    {Acl, Policy}.


%% ===================================================================
%% Internal functions
%% ===================================================================

%% @doc Generate a JSON document to use for a bucket
%% ACL request.
-spec bucket_acl_json(acl(), string()) -> string().
bucket_acl_json(ACL, KeyId)  ->
    binary_to_list(
      iolist_to_binary(
        mochijson2:encode({struct, [{<<"requester">>, list_to_binary(KeyId)},
                                    riak_cs_acl_utils:acl_to_json_term(ACL)]}))).

%% @doc Generate a JSON document to use for a bucket
-spec bucket_policy_json(binary(), string()) -> string().
bucket_policy_json(PolicyJson, KeyId)  ->
    binary_to_list(
      iolist_to_binary(
        mochijson2:encode({struct, [{<<"requester">>, list_to_binary(KeyId)},
                                    {<<"policy">>, PolicyJson}]
                          }))).

%% @doc Check if a bucket is empty
-spec bucket_empty(binary(), riak_client()) -> {ok, boolean()} | {error, term()}.
bucket_empty(Bucket, RcPid) ->
    ManifestBucket = riak_cs_utils:to_bucket_name(objects, Bucket),
    %% @TODO Use `stream_list_keys' instead
    {ok, ManifestPbc} = riak_cs_riak_client:manifest_pbc(RcPid),
    Timeout = riak_cs_config:list_keys_list_objects_timeout(),
    ListKeysResult = riak_cs_pbc:list_keys(ManifestPbc, ManifestBucket, Timeout,
                                           [riakc, list_all_manifest_keys]),
    {ok, bucket_empty_handle_list_keys(RcPid, Bucket, ListKeysResult)}.

-spec bucket_empty_handle_list_keys(riak_client(), binary(),
                                    {ok, list()} |
                                    {error, term()}) ->
                                           boolean().
bucket_empty_handle_list_keys(RcPid, Bucket, {ok, Keys}) ->
    AnyPred = bucket_empty_any_pred(RcPid, Bucket),
    %% `lists:any/2' will break out early as soon
    %% as something returns `true'
    not lists:any(AnyPred, Keys);
bucket_empty_handle_list_keys(_RcPid, _Bucket, _Error) ->
    false.

-spec bucket_empty_any_pred(riak_client(), Bucket :: binary()) ->
                                   fun((Key :: binary()) -> boolean()).
bucket_empty_any_pred(RcPid, Bucket) ->
    fun (Key) ->
            riak_cs_utils:key_exists(RcPid, Bucket, Key)
    end.

%% @doc Fetches the bucket object and verify its status.
-spec fetch_bucket_object(binary(), riak_client()) ->
                                 {ok, riakc_obj:riakc_obj()} | {error, term()}.
fetch_bucket_object(BucketName, RcPid) ->
    case fetch_bucket_object_raw(BucketName, RcPid) of
        {ok, Obj} ->
            [Value | _] = riakc_obj:get_values(Obj),
            case Value of
                ?FREE_BUCKET_MARKER ->
                    {error, no_such_bucket};
                _ ->
                    {ok, Obj}
            end;
        {error, _}=Error ->
            Error
    end.

%% @doc Fetches the bucket object, even it is marked as free
-spec fetch_bucket_object_raw(binary(), riak_client()) ->
                                     {ok, riakc_obj:riakc_obj()} | {error, term()}.
fetch_bucket_object_raw(BucketName, RcPid) ->
    case riak_cs_riak_client:get_bucket(RcPid, BucketName) of
        {ok, Obj} ->
            Values = riakc_obj:get_values(Obj),
            maybe_log_sibling_warning(BucketName, Values),
            {ok, Obj};
        {error, _}=Error ->
            Error
    end.

-spec maybe_log_sibling_warning(binary(), list(riakc_obj:value())) -> ok.
maybe_log_sibling_warning(Bucket, Values) when length(Values) > 1 ->
    _ = lager:warning("The bucket ~s has ~b siblings that may need resolution.",
                      [binary_to_list(Bucket), length(Values)]),
    ok;
maybe_log_sibling_warning(_, _) ->
    ok.

-spec maybe_log_bucket_owner_error(binary(), list(riakc_obj:value())) -> ok.
maybe_log_bucket_owner_error(Bucket, Values) when length(Values) > 1 ->
    _ = lager:error("The bucket ~s has ~b owners."
                    " This situation requires administrator intervention.",
                    [binary_to_list(Bucket), length(Values)]),
    ok;
maybe_log_bucket_owner_error(_, _) ->
    ok.

%% @doc Check if a bucket exists in a list of the user's buckets.
%% @TODO This will need to change once globally unique buckets
%% are enforced.
-spec bucket_exists([cs_bucket()], string()) -> boolean().
bucket_exists(Buckets, CheckBucket) ->
    SearchResults = [Bucket || Bucket <- Buckets,
                               Bucket?RCS_BUCKET.name =:= CheckBucket andalso
                                   Bucket?RCS_BUCKET.last_action =:= created],
    case SearchResults of
        [] ->
            false;
        _ ->
            true
    end.

%% @doc Return a closure over a specific function
%% call to the stanchion client module for either
%% bucket creation or deletion.
-spec bucket_fun(bucket_operation(),
                 binary(),
                 bag_id(),
                 [] | policy() | acl(),
                 string(),
                 {string(), string()},
                 {string(), pos_integer(), boolean()}) -> function().
bucket_fun(create, Bucket, BagId, ACL, KeyId, AdminCreds, StanchionData) ->
    {StanchionIp, StanchionPort, StanchionSSL} = StanchionData,
    %% Generate the bucket JSON document
    BucketDoc = bucket_json(Bucket, BagId, ACL, KeyId),
    fun() ->
            velvet:create_bucket(StanchionIp,
                                 StanchionPort,
                                 "application/json",
                                 BucketDoc,
                                 [{ssl, StanchionSSL},
                                  {auth_creds, AdminCreds}])
    end;
bucket_fun(update_acl, Bucket, _BagId, ACL, KeyId, AdminCreds, StanchionData) ->
    {StanchionIp, StanchionPort, StanchionSSL} = StanchionData,
    %% Generate the bucket JSON document for the ACL request
    AclDoc = bucket_acl_json(ACL, KeyId),
    fun() ->
            velvet:set_bucket_acl(StanchionIp,
                                  StanchionPort,
                                  Bucket,
                                  "application/json",
                                  AclDoc,
                                  [{ssl, StanchionSSL},
                                   {auth_creds, AdminCreds}])
    end;
bucket_fun(update_policy, Bucket, _BagId, PolicyJson, KeyId, AdminCreds, StanchionData) ->
    {StanchionIp, StanchionPort, StanchionSSL} = StanchionData,
    %% Generate the bucket JSON document for the ACL request
    PolicyDoc = bucket_policy_json(PolicyJson, KeyId),
    fun() ->
            velvet:set_bucket_policy(StanchionIp,
                                     StanchionPort,
                                     Bucket,
                                     "application/json",
                                     PolicyDoc,
                                     [{ssl, StanchionSSL},
                                      {auth_creds, AdminCreds}])
    end;
bucket_fun(delete_policy, Bucket, _BagId, _, KeyId, AdminCreds, StanchionData) ->
    {StanchionIp, StanchionPort, StanchionSSL} = StanchionData,
    %% Generate the bucket JSON document for the ACL request
    fun() ->
            velvet:delete_bucket_policy(StanchionIp,
                                        StanchionPort,
                                        Bucket,
                                        KeyId,
                                        [{ssl, StanchionSSL},
                                         {auth_creds, AdminCreds}])
    end;
bucket_fun(delete, Bucket, _BagId, _ACL, KeyId, AdminCreds, StanchionData) ->
    {StanchionIp, StanchionPort, StanchionSSL} = StanchionData,
    fun() ->
            velvet:delete_bucket(StanchionIp,
                                 StanchionPort,
                                 Bucket,
                                 KeyId,
                                 [{ssl, StanchionSSL},
                                  {auth_creds, AdminCreds}])
    end.

%% @doc Generate a JSON document to use for a bucket
%% creation request.
-spec bucket_json(binary(), bag_id(), acl(), string()) -> string().
bucket_json(Bucket, BagId, ACL, KeyId)  ->
    BagElement = case BagId of
                     undefined -> [];
                     _ -> [{<<"bag">>, BagId}]
                 end,
    binary_to_list(
      iolist_to_binary(
        mochijson2:encode({struct, [{<<"bucket">>, Bucket},
                                    {<<"requester">>, list_to_binary(KeyId)},
                                    riak_cs_acl_utils:acl_to_json_term(ACL)] ++
                               BagElement}))).

%% @doc Return a bucket record for the specified bucket name.
-spec bucket_record(binary(), bucket_operation()) -> cs_bucket().
bucket_record(Name, Operation) ->
    Action = case Operation of
                 create -> created;
                 delete -> deleted;
                 _ -> undefined
             end,
    ?RCS_BUCKET{name=binary_to_list(Name),
                last_action=Action,
                creation_date=riak_cs_wm_utils:iso_8601_datetime(),
                modification_time=os:timestamp()}.

%% @doc Check for and resolve any conflict between
%% a bucket record from a user record sibling and
%% a list of resolved bucket records.
-spec bucket_resolver(cs_bucket(), [cs_bucket()]) -> [cs_bucket()].
bucket_resolver(Bucket, ResolvedBuckets) ->
    case lists:member(Bucket, ResolvedBuckets) of
        true ->
            ResolvedBuckets;
        false ->
            case [RB || RB <- ResolvedBuckets,
                        RB?RCS_BUCKET.name =:=
                            Bucket?RCS_BUCKET.name] of
                [] ->
                    [Bucket | ResolvedBuckets];
                [ExistingBucket] ->
                    case keep_existing_bucket(ExistingBucket,
                                              Bucket) of
                        true ->
                            ResolvedBuckets;
                        false ->
                            [Bucket | lists:delete(ExistingBucket,
                                                   ResolvedBuckets)]
                    end
            end
    end.

%% @doc Ordering function for sorting a list of bucket records
%% according to bucket name.
-spec bucket_sorter(cs_bucket(), cs_bucket()) -> boolean().
bucket_sorter(?RCS_BUCKET{name=Bucket1},
              ?RCS_BUCKET{name=Bucket2}) ->
    Bucket1 =< Bucket2.

%% @doc Return true if the last action for the bucket
%% is deleted and the action occurred over the configurable
%% maximum prune-time.
-spec cleanup_bucket(cs_bucket()) -> boolean().
cleanup_bucket(?RCS_BUCKET{last_action=created}) ->
    false;
cleanup_bucket(?RCS_BUCKET{last_action=deleted,
                           modification_time=ModTime}) ->
    %% the prune-time is specified in seconds, so we must
    %% convert Erlang timestamps to seconds first
    NowSeconds = riak_cs_utils:second_resolution_timestamp(os:timestamp()),
    ModTimeSeconds = riak_cs_utils:second_resolution_timestamp(ModTime),
    (NowSeconds - ModTimeSeconds) >
        riak_cs_config:user_buckets_prune_time().

%% @doc Determine if an existing bucket from the resolution list
%% should be kept or replaced when a conflict occurs.
-spec keep_existing_bucket(cs_bucket(), cs_bucket()) -> boolean().
keep_existing_bucket(?RCS_BUCKET{last_action=LastAction1,
                                 modification_time=ModTime1},
                     ?RCS_BUCKET{last_action=LastAction2,
                                 modification_time=ModTime2}) ->
    if
        LastAction1 == LastAction2
        andalso
        ModTime1 =< ModTime2 ->
            true;
        LastAction1 == LastAction2 ->
            false;
        ModTime1 > ModTime2 ->
            true;
        true ->
            false
    end.

%% @doc Resolve the set of buckets for a user when
%% siblings are encountered on a read of a user record.
-spec resolve_buckets([rcs_user()], [cs_bucket()], boolean()) ->
                             [cs_bucket()].
resolve_buckets([], Buckets, true) ->
    lists:sort(fun bucket_sorter/2, Buckets);
resolve_buckets([], Buckets, false) ->
    lists:sort(fun bucket_sorter/2, [Bucket || Bucket <- Buckets, not cleanup_bucket(Bucket)]);
resolve_buckets([HeadUserRec | RestUserRecs], [], KeepDeletedBuckets) ->
    %% We can assume there are no bucket duplication under a single
    %% user record.  It's already resolved. This function works
    %% without this head, but this head makes it very effecient in
    %% case of thousands of bucket records under single user.
    resolve_buckets(RestUserRecs, HeadUserRec?RCS_USER.buckets, KeepDeletedBuckets);
resolve_buckets([HeadUserRec | RestUserRecs], Buckets, _KeepDeleted) ->
    HeadBuckets = HeadUserRec?RCS_USER.buckets,
    UpdBuckets = lists:foldl(fun bucket_resolver/2, Buckets, HeadBuckets),
    resolve_buckets(RestUserRecs, UpdBuckets, _KeepDeleted).

%% @doc Shared code used when doing a bucket creation or deletion.
-spec serialized_bucket_op(binary(),
                           [] | acl() | policy(),
                           rcs_user(),
                           riakc_obj:riakc_obj(),
                           bucket_operation(),
                           riak_cs_stats:key(),
                           riak_client()) ->
                                  ok |
                                  {error, term()}.
serialized_bucket_op(Bucket, ACL, User, UserObj, BucketOp, StatKey, RcPid) ->
    serialized_bucket_op(Bucket, undefined, ACL, User, UserObj,
                         BucketOp, StatKey, RcPid).

%% @doc Shared code used when doing a bucket creation or deletion.
-spec serialized_bucket_op(binary(),
                           bag_id(),
                           [] | acl() | policy(),
                           rcs_user(),
                           riakc_obj:riakc_obj(),
                           bucket_operation(),
                           riak_cs_stats:key(),
                           riak_client()) ->
                                  ok |
                                  {error, term()}.
serialized_bucket_op(Bucket, BagId, ACL, User, UserObj, BucketOp, StatsKey, RcPid) ->
    StartTime = os:timestamp(),
    _ = riak_cs_stats:inflow(StatsKey),
    {ok, AdminCreds} = riak_cs_config:admin_creds(),

    BucketFun = bucket_fun(BucketOp,
                           Bucket,
                           BagId,
                           ACL,
                           User?RCS_USER.key_id,
                           AdminCreds,
                           riak_cs_utils:stanchion_data()),
    %% Make a call to the request serialization service.
    OpResult = BucketFun(),
    _ = riak_cs_stats:update_with_start(StatsKey, StartTime, OpResult),
    case OpResult of
        ok ->
            BucketRecord = bucket_record(Bucket, BucketOp),
            case update_user_buckets(User, BucketRecord) of
                {ok, ignore} when BucketOp == update_acl ->
                    OpResult;
                {ok, ignore} ->
                    OpResult;
                {ok, UpdUser} ->
                    riak_cs_user:save_user(UpdUser, UserObj, RcPid)
            end;
        {error, {error_status, Status, _, ErrorDoc}} ->
            handle_stanchion_response(Status, ErrorDoc, BucketOp, Bucket);
        {error, _} ->
            OpResult
    end.

%% @doc needs retry for delete op.  409 assumes
%% MultipartUploadRemaining for now if a new feature that needs retry
%% could come up, add branch here. See tests in
%% tests/riak_cs_bucket_test.erl
-spec handle_stanchion_response(200..503, string(), delete|create, binary()) ->
                                       {error, remaining_multipart_upload} |
                                       {error, atom()}.
handle_stanchion_response(409, ErrorDoc, Op, Bucket)
  when Op =:= delete orelse Op =:= create ->

    Value = riak_cs_s3_response:xml_error_code(ErrorDoc),
    case {lists:flatten(Value), Op} of
        {"MultipartUploadRemaining", delete} ->
            _ = lager:error("Concurrent multipart upload might have"
                            " happened on deleting bucket '~s'.", [Bucket]),
            {error, remaining_multipart_upload};
        {"MultipartUploadRemaining", create} ->
            %% might be security issue
            _ = lager:critical("Multipart upload remains in deleted bucket (~s)"
                               " Clean up the deleted buckets now.", [Bucket]),
            %% Broken, returns 500
            throw({remaining_multipart_upload_on_deleted_bucket, Bucket});
        Other ->
            _ = lager:debug("errordoc: ~p => ~s", [Other, ErrorDoc]),
            riak_cs_s3_response:error_response(ErrorDoc)
    end;
handle_stanchion_response(_C, ErrorDoc, _M, _) ->
    %% _ = lager:error("unexpected errordoc: (~p, ~p) ~s", [_C, _M, ErrorDoc]),
    riak_cs_s3_response:error_response(ErrorDoc).

%% @doc Update a bucket record to convert the name from binary
%% to string if necessary.
-spec update_bucket_record(term()) -> cs_bucket().
update_bucket_record(Bucket=?RCS_BUCKET{name=Name}) when is_binary(Name) ->
    Bucket?RCS_BUCKET{name=binary_to_list(Name)};
update_bucket_record(Bucket) ->
    Bucket.

%% @doc Check if a user already has an ownership of
%% a bucket and update the bucket list if needed.
-spec update_user_buckets(rcs_user(), cs_bucket()) ->
                                 {ok, ignore} | {ok, rcs_user()}.
update_user_buckets(User, Bucket) ->
    Buckets = User?RCS_USER.buckets,
    %% At this point any siblings from the read of the
    %% user record have been resolved so the user bucket
    %% list should have 0 or 1 buckets that share a name
    %% with `Bucket'.
    case [B || B <- Buckets, B?RCS_BUCKET.name =:= Bucket?RCS_BUCKET.name] of
        [] ->
            {ok, User?RCS_USER{buckets=[Bucket | Buckets]}};
        [ExistingBucket] ->
            case
                (Bucket?RCS_BUCKET.last_action == deleted andalso
                 ExistingBucket?RCS_BUCKET.last_action == created)
                orelse
                (Bucket?RCS_BUCKET.last_action == created andalso
                 ExistingBucket?RCS_BUCKET.last_action == deleted) of
                true ->
                    UpdBuckets = [Bucket | lists:delete(ExistingBucket, Buckets)],
                    {ok, User?RCS_USER{buckets=UpdBuckets}};
                false ->
                    {ok, ignore}
            end
    end.

%% @doc Grab the whole list of Riak CS bucket keys.
-spec fetch_bucket_keys(riak_client()) -> {ok, [binary()]} | {error, term()}.
fetch_bucket_keys(RcPid) ->
    {ok, MasterPbc} = riak_cs_riak_client:master_pbc(RcPid),
    Timeout = riak_cs_config:list_keys_list_buckets_timeout(),
    riak_cs_pbc:list_keys(MasterPbc, ?BUCKETS_BUCKET, Timeout,
                          [riakc, list_all_bucket_keys]).
