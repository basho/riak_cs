%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
%%               2021-2023 TI Tokyo    All Rights Reserved.
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

%% @doc stanchion utility functions

-module(stanchion_utils).

%% Public API
-export([create_bucket/1,
         create_role/1,
         create_user/1,
         delete_bucket/2,
         delete_role/1,
         get_admin_creds/0,
         get_manifests_raw/4,
         get_pbc/0,
         has_tombstone/1,
         make_pbc/0,
         set_bucket_acl/2,
         set_bucket_policy/2,
         set_bucket_versioning/2,
         delete_bucket_policy/2,
         to_bucket_name/2,
         update_user/2,
         get_role/2,
         sha_mac/2
        ]).

-include("riak_cs.hrl").
-include("stanchion.hrl").
-include("manifest.hrl").
-include("moss.hrl").
-include_lib("riakc/include/riakc.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").
-include_lib("kernel/include/logger.hrl").


-define(ROLE_ID_LENGTH, 21).  %% length("AROAJQABLZS4A3QDU576Q").
-define(ROLE_ID_CHARSET, "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789").


-type riak_connect_failed() :: {riak_connect_failed, tuple()}.

-spec make_pbc() -> pid().
make_pbc() ->
    {Host, Port} =
        case riak_cs_config:tussle_voss_riak_host() of
            auto ->
                {H,P} = riak_cs_config:riak_host_port(),
                logger:info("using main riak cluster for voss data at ~s:~b", [H, P]),
                {H,P};
            Configured ->
                Configured
        end,
    Timeout = application:get_env(riak_cs, riakc_connect_timeout, 10000),
    StartOptions = [{connect_timeout, Timeout},
                    {auto_reconnect, true}],
    {ok, Pid} = riakc_pb_socket:start_link(Host, Port, StartOptions),
    ets:insert(?STANCHION_OWN_PBC_TABLE, {pid, Pid}),
    Pid.

get_pbc() ->
    [{pid, Pid}] = ets:lookup(?STANCHION_OWN_PBC_TABLE, pid),
    case is_process_alive(Pid) of
        true ->
            Pid;
        false ->
            ?LOG_WARNING("voss riakc process ~p exited; spawning a new one."
                         " Check riak is reachable as configured (~p)",
                         [Pid, riak_cs_config:tussle_voss_riak_host()]),
            make_pbc(),
            timer:sleep(1000),
            get_pbc()
    end.

%% @doc Create a bucket in the global namespace or return
%% an error if it already exists.
-spec create_bucket(maps:map()) -> ok | {error, term()}.
create_bucket(#{bucket := Bucket,
                requester := OwnerId,
                acl := Acl_} = FF) ->
    Acl = riak_cs_acl:exprec_detailed(Acl_),
    BagId = maps:get(bag, FF, undefined),
    case riak_connection() of
        {ok, Pbc} ->
            OpResult1 = do_bucket_op(Bucket, OwnerId, [{acl, Acl}, {bag, BagId}], create, Pbc),
            OpResult2 =
                case OpResult1 of
                    ok ->
                        BucketRecord = bucket_record(Bucket, create),
                        {ok, {UserObj, KeepDeletedBuckets}} = riak_cs_riak_client:get_user_with_pbc(Pbc, OwnerId),
                        User = riak_cs_user:from_riakc_obj(UserObj, KeepDeletedBuckets),
                        UpdUser = update_user_buckets(add, User, BucketRecord),
                        save_user(false, UpdUser, UserObj, Pbc);
                    {error, _} ->
                        OpResult1
                end,
            _ = close_riak_connection(Pbc),
            OpResult2;
        Error ->
            Error
    end.


bucket_record(Name, Operation) ->
    Action = case Operation of
                 create -> created;
                 delete -> deleted
             end,
    ?RCS_BUCKET{name = Name,
                last_action = Action,
                creation_date = riak_cs_wm_utils:iso_8601_datetime(),
                modification_time = os:system_time(millisecond)}.


%% @doc Attempt to create a new user
-spec create_user(maps:map()) -> ok | {error, riak_connect_failed() | term()}.
create_user(FF = #{email := Email, key_id := KeyId}) ->
    case riak_connection() of
        {ok, RiakPid} ->
            try
                case email_available(Email, RiakPid) of
                    true ->
                        User = exprec:frommap_rcs_user_v2(FF#{status => enabled, buckets => []}),
                        save_user(User, RiakPid);
                    {false, _} ->
                        logger:info("Refusing to create an existing user with email ~s", [Email]),
                        {error, user_already_exists}
                end
            catch T:E ->
                    logger:error("Error on creating user ~s (key_id: ~s): ~p",
                                 [Email, KeyId, {T, E}]),
                    {error, {T, E}}
            after
                close_riak_connection(RiakPid)
            end;
        {error, _} = Else ->
            Else
    end.

%% @doc Delete a bucket
-spec delete_bucket(binary(), binary()) -> ok | {error, term()}.
delete_bucket(Bucket, OwnerId) ->
    case riak_connection() of
        {ok, Pbc} ->
            OpResult1 = do_bucket_op(Bucket, OwnerId, [{acl, ?ACL{}}], delete, Pbc),
            OpResult2 =
                case OpResult1 of
                    ok ->
                        BucketRecord = bucket_record(Bucket, delete),
                        {ok, {UserObj, KDB}} = riak_cs_riak_client:get_user_with_pbc(Pbc, OwnerId),
                        User = riak_cs_user:from_riakc_obj(UserObj, KDB),
                        UpdUser = update_user_buckets(delete, User, BucketRecord),
                        save_user(false, UpdUser, UserObj, Pbc);
                    {error, _} ->
                        OpResult1
                end,
            _ = close_riak_connection(Pbc),
            OpResult2;
        Error ->
            Error
    end.

-spec create_role(proplists:proplist()) -> {ok, string()} | {error, riak_connect_failed() | term()}.
create_role(Fields) ->
    Role_ = ?IAM_ROLE{assume_role_policy_document = A} =
                riak_cs_roles:exprec_detailed(
                  riak_cs_roles:fix_permissions_boundary(Fields)),
    Role = Role_?IAM_ROLE{assume_role_policy_document = base64:decode(A)},
    case riak_connection() of
        {ok, RiakPid} ->
            try
                save_role(Role, RiakPid)
            after
                close_riak_connection(RiakPid)
            end;
        {error, _} = Else ->
            Else
    end.


%% @doc Return the credentials of the admin user
-spec get_admin_creds() -> {ok, {string(), string()}} | {error, term()}.
get_admin_creds() ->
    case application:get_env(riak_cs, admin_key) of
        {ok, KeyId} ->
            case application:get_env(riak_cs, admin_secret) of
                {ok, Secret} ->
                    {ok, {KeyId, Secret}};
                undefined ->
                    logger:warning("The admin user's secret has not been defined."),
                    {error, secret_undefined}
            end;
        undefined ->
            logger:warning("The admin user's key id has not been defined."),
            {error, key_id_undefined}
    end.

%% @doc
-spec get_manifests(pid(), binary(), binary(), binary()) ->
    {ok, term(), term()} | {error, notfound}.
get_manifests(RiakcPid, Bucket, Key, Vsn) ->
    case get_manifests_raw(RiakcPid, Bucket, Key, Vsn) of
        {ok, Object} ->
            DecodedSiblings = [binary_to_term(V) ||
                                  {_, V}=Content <- riakc_obj:get_contents(Object),
                                  not has_tombstone(Content)],

            %% Upgrade the manifests to be the latest erlang
            %% record version
            Upgraded = rcs_common_manifest_utils:upgrade_wrapped_manifests(DecodedSiblings),

            %% resolve the siblings
            Resolved = rcs_common_manifest_resolution:resolve(Upgraded),

            %% prune old scheduled_delete manifests

            %% commented out because we don't have the
            %% riak_cs_gc module
            Pruned = rcs_common_manifest_utils:prune(
                       Resolved, erlang:timestamp(),
                       50,  %% riak_cs defaults for max_scheduled_delete_manifests and
                       86400),  %% leeway_seconds
            {ok, Object, Pruned};
        {error, notfound} = NotFound ->
            NotFound
    end.

%% @doc Determine if a set of contents of a riak object has a tombstone.
-spec has_tombstone({dict:dict(), binary()}) -> boolean().
has_tombstone({_, <<>>}) ->
    true;
has_tombstone({MD, _V}) ->
    dict:is_key(<<"X-Riak-Deleted">>, MD) =:= true.

%% @doc List the keys from a bucket
list_keys(BucketName, RiakPid) ->
    case ?TURNAROUND_TIME(riakc_pb_socket:list_keys(RiakPid, BucketName)) of
        {{ok, Keys}, TAT} ->
            stanchion_stats:update([riakc, list_all_manifest_keys], TAT),
            {ok, lists:sort(Keys)};
        {{error, _} = ER, TAT} ->
            stanchion_stats:update([riakc, list_all_manifest_keys], TAT),
            ER
    end.

%% @doc Get a protobufs connection to the riak cluster
%% using information from the application environment.
riak_connection() ->
    {ok, {Host, Port}} = application:get_env(riak_cs, riak_host),
    riak_connection(Host, Port).

%% @doc Get a protobufs connection to the riak cluster.
riak_connection(Host, Port) ->
    %% We use start() here instead of start_link() because if we can't
    %% connect to Host & Port for whatever reason (e.g. service down,
    %% host down, host unreachable, ...), then we'll be crashed by the
    %% newly-spawned-gen_server-proc's link to us.
    %%
    %% There is still a race condition if the PB socket proc's init()
    %% is successful but then dies immediately *before* we call the
    %% link() BIF.  That's life in the big city.
    case riakc_pb_socket:start(Host, Port) of
        {ok, Pid} = Good ->
            true = link(Pid),
            Good;
        {error, Else} ->
            {error, {riak_connect_failed, {Else, Host, Port}}}
    end.

close_riak_connection(Pid) ->
    riakc_pb_socket:stop(Pid).

%% @doc Set the ACL for a bucket
-spec set_bucket_acl(binary(), term()) -> ok | {error, term()}.
set_bucket_acl(Bucket, #{requester := OwnerId,
                         acl := Acl}) ->
    do_bucket_op(Bucket, OwnerId, [{acl, Acl}], update_acl).

%% @doc add bucket policy in the global namespace
%% FieldList.policy has JSON-encoded policy from user
-spec set_bucket_policy(binary(), term()) -> ok | {error, term()}.
set_bucket_policy(Bucket, FieldList) ->
    OwnerId = proplists:get_value(requester, FieldList, <<>>),
    PolicyJson = proplists:get_value(policy, FieldList, []),

    % @TODO: Already Checked at Riak CS, so store as it is JSON here
    % if overhead of parsing JSON were expensive, need to import
    % code of JSON parse from riak_cs_s3_policy
    do_bucket_op(Bucket, OwnerId, [{policy, PolicyJson}], update_policy).

%% @doc set bucket versioning option
-spec set_bucket_versioning(binary(), term()) -> ok | {error, term()}.
set_bucket_versioning(Bucket, FieldList) ->
    OwnerId = proplists:get_value(<<"requester">>, FieldList, <<>>),
    Json = proplists:get_value(<<"versioning">>, FieldList, []),
    do_bucket_op(Bucket, OwnerId, [{versioning, Json}], update_versioning).


%% @doc Delete a bucket
-spec delete_bucket_policy(binary(), binary()) -> ok | {error, term()}.
delete_bucket_policy(Bucket, OwnerId) ->
    do_bucket_op(Bucket, OwnerId, [delete_policy], delete_policy).

%% Get the proper bucket name for either the MOSS object
%% bucket or the data block bucket.
-spec to_bucket_name(objects | blocks, binary()) -> binary().
to_bucket_name(Type, Bucket) ->
    case Type of
        objects ->
            Prefix = ?OBJECT_BUCKET_PREFIX;
        blocks ->
            Prefix = ?BLOCK_BUCKET_PREFIX
    end,
    BucketHash = md5(Bucket),
    <<Prefix/binary, BucketHash/binary>>.

%% @doc Attmpt to create a new user
-spec update_user(string(), [{term(), term()}]) ->
                         ok | {error, riak_connect_failed() | term()}.
update_user(KeyId, UserFields) ->
    case riak_connection() of
        {ok, RiakPid} ->
            try
                case riak_cs_riak_client:get_user_with_pbc(RiakPid, KeyId) of
                    {ok, {UserObj, KDB}} ->
                        User = riak_cs_user:from_riakc_obj(UserObj, KDB),
                        {UpdUser, EmailUpdated} =
                            update_user_record(UserFields, User, false),
                        save_user(EmailUpdated,
                                  UpdUser,
                                  UserObj,
                                  RiakPid);
                    {error, _}=Error ->
                        Error
                end
            catch T:E:_ST ->
                    logger:error("Error on updating user ~s: ~p", [KeyId, {T, E}]),
                    {error, {T, E}}
            after
                close_riak_connection(RiakPid)
            end;
        {error, _} = Else ->
            Else
    end.


sha_mac(KeyData, STS) ->
    crypto:mac(hmac, sha, KeyData, STS).

md5(Bin) ->
    crypto:hash(md5, Bin).

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @doc Check if a bucket is empty
-spec bucket_empty(binary(), pid()) -> boolean().
bucket_empty(Bucket, RiakcPid) ->
    ManifestBucket = to_bucket_name(objects, Bucket),
    %% @TODO Use `stream_list_keys' instead and
    ListKeysResult = list_keys(ManifestBucket, RiakcPid),
    bucket_empty_handle_list_keys(RiakcPid,
                                  Bucket,
                                  ListKeysResult).

bucket_empty_handle_list_keys(RiakcPid, Bucket, {ok, Keys}) ->
    AnyPred = bucket_empty_any_pred(RiakcPid, Bucket),
    %% `lists:any/2' will break out early as soon
    %% as something returns `true'
    not lists:any(AnyPred, Keys);
bucket_empty_handle_list_keys(_RiakcPid, _Bucket, _Error) ->
    false.

bucket_empty_any_pred(RiakcPid, Bucket) ->
    fun (Key) ->
            key_exists(RiakcPid, Bucket, Key)
    end.

key_exists(RiakcPid, Bucket, Key) ->
    key_exists_handle_get_manifests(
      get_manifests(RiakcPid, Bucket, Key, ?LFS_DEFAULT_OBJECT_VERSION)).

key_exists_handle_get_manifests({ok, _Object, Manifests}) ->
    active_to_bool(active_manifest_from_response({ok, Manifests}));
key_exists_handle_get_manifests(Error) ->
    active_to_bool(active_manifest_from_response(Error)).

active_to_bool({ok, _Active}) ->
    true;
active_to_bool({error, notfound}) ->
    false.

active_manifest_from_response({ok, Manifests}) ->
    handle_active_manifests(
      rcs_common_manifest_utils:active_manifest(Manifests));
active_manifest_from_response({error, notfound}=NotFound) ->
    NotFound.

handle_active_manifests({ok, _Active}=ActiveReply) ->
    ActiveReply;
handle_active_manifests({error, no_active_manifest}) ->
    {error, notfound}.

bucket_available(Bucket, RequesterId, BucketOp, RiakPid) ->
    GetOptions = [{pr, all}],
    case ?TURNAROUND_TIME(riakc_pb_socket:get(RiakPid, ?BUCKETS_BUCKET, Bucket, GetOptions)) of
        {{ok, BucketObj}, TAT} ->
            stanchion_stats:update([riakc, get_cs_bucket], TAT),
            OwnerId = riakc_obj:get_value(BucketObj),
            case {OwnerId, BucketOp} of
                {?FREE_BUCKET_MARKER, create} ->
                    is_bucket_ready_to_create(Bucket, RiakPid, BucketObj);
                {?FREE_BUCKET_MARKER, _} ->
                    {false, no_such_bucket};

                {RequesterId, delete} ->
                    is_bucket_ready_to_delete(Bucket, RiakPid, BucketObj);
                {RequesterId, _} ->
                    {true, BucketObj};
                _ ->
                    {false, bucket_already_exists}
            end;

        {{error, notfound}, TAT} ->
            stanchion_stats:update([riakc, get_cs_bucket], TAT),
            case BucketOp of
                create ->
                    BucketObj = riakc_obj:new(?BUCKETS_BUCKET, Bucket, RequesterId),
                    {true, BucketObj};
                update_acl ->
                    {false, no_such_bucket};
                update_policy ->
                    {false, no_such_bucket};
                update_versioning ->
                    {false, no_such_bucket};
                delete ->
                    {false, no_such_bucket}
            end;
        {{error, Reason}, TAT} ->
            stanchion_stats:update([riakc, get_cs_bucket], TAT),
            %% @TODO Maybe bubble up this error info
            logger:warning("Error occurred trying to check if the bucket ~p exists. Reason: ~p",
                           [Bucket, Reason]),
            {false, Reason}
    end.


do_bucket_op(Bucket, OwnerId, Opts, BucketOp) ->
    do_bucket_op(Bucket, OwnerId, Opts, BucketOp, undefined).
do_bucket_op(<<"riak-cs">>, _OwnerId, _Opts, _BucketOp, _) ->
    {error, access_denied};
do_bucket_op(Bucket, OwnerId, Opts, BucketOp, undefined) ->
    case riak_connection() of
        {ok, Pbc} ->
            %% Buckets operations can only be completed if the bucket exists
            %% and the requesting party owns the bucket.
            try
                do_bucket_op2(Bucket, OwnerId, Opts, BucketOp, Pbc)
            catch T:E:ST ->
                    logger:error("Error on updating bucket ~s: ~p. Stacktrace: ~p",
                                 [Bucket, {T, E}, ST]),
                    {error, {T, E}}
            after
                close_riak_connection(Pbc)
            end;
        {error, _} = Else ->
            Else
    end;
do_bucket_op(Bucket, OwnerId, Opts, BucketOp, Pbc) ->
    try
        do_bucket_op2(Bucket, OwnerId, Opts, BucketOp, Pbc)
    catch
        T:E:ST ->
            logger:error("Error on updating bucket ~s: ~p. Stacktrace: ~p",
                         [Bucket, {T, E}, ST]),
            {error, {T, E}}
    end.

do_bucket_op2(Bucket, OwnerId, Opts, BucketOp, Pbc) ->
    case bucket_available(Bucket, OwnerId, BucketOp, Pbc) of
        {true, BucketObj} ->
            Value = case BucketOp of
                        create ->            OwnerId;
                        update_acl ->        OwnerId;
                        update_policy ->     OwnerId;
                        delete_policy ->     OwnerId;
                        update_versioning -> OwnerId;
                        delete ->            ?FREE_BUCKET_MARKER
                    end,
            put_bucket(BucketObj, Value, Opts, Pbc);
        {false, Reason1} ->
            {error, Reason1}
    end.

%% @doc Store a new bucket in Riak
%% though whole metadata itself is a dict, a metadata of ?MD_USERMETA is
%% proplists of {?MD_ACL, ACL::binary()}|{?MD_POLICY, PolicyBin::binary()}|
%%  {?MD_BAG, BagId::binary()}, {?MD_VERSIONING, bucket_versioning_option()}}.
%% should preserve other metadata. ACL and Policy can be overwritten.
put_bucket(BucketObj, OwnerId, Opts, RiakPid) ->
    PutOptions = [{w, all}, {pw, all}],
    UpdBucketObj0 = riakc_obj:update_value(BucketObj, OwnerId),
    MD = case riakc_obj:get_metadatas(UpdBucketObj0) of
             [] -> % create
                 dict:from_list([{?MD_USERMETA, []}]);
             [MD0] -> MD0;
             _E ->
                 MsgData = {siblings, riakc_obj:key(BucketObj)},
                 logger:error("bucket has siblings: ~p", [MsgData]),
                 throw(MsgData) % @TODO: data broken; handle this
           end,
    MetaData = make_new_metadata(MD, Opts),
    UpdBucketObj = riakc_obj:update_metadata(UpdBucketObj0, MetaData),
    {Result, TAT} = ?TURNAROUND_TIME(riakc_pb_socket:put(RiakPid, UpdBucketObj, PutOptions)),
    stanchion_stats:update([riakc, put_cs_bucket], TAT),
    Result.

make_new_metadata(MD, Opts) ->
    MetaVals = dict:fetch(?MD_USERMETA, MD),
    UserMetaData = make_new_user_metadata(MetaVals, Opts),
    dict:store(?MD_USERMETA, UserMetaData, dict:erase(?MD_USERMETA, MD)).

make_new_user_metadata(MetaVals, [])->
    MetaVals;
make_new_user_metadata(MetaVals, [{acl, Acl} | Opts]) ->
    make_new_user_metadata(replace_meta(?MD_ACL, Acl, MetaVals), Opts);
make_new_user_metadata(MetaVals, [{policy, Policy} | Opts]) ->
    make_new_user_metadata(replace_meta(?MD_POLICY, Policy, MetaVals), Opts);
make_new_user_metadata(MetaVals, [{bag, undefined} | Opts]) ->
    make_new_user_metadata(MetaVals, Opts);
make_new_user_metadata(MetaVals, [{bag, BagId} | Opts]) ->
    make_new_user_metadata(replace_meta(?MD_BAG, BagId, MetaVals), Opts);
make_new_user_metadata(MetaVals, [delete_policy | Opts]) ->
    make_new_user_metadata(proplists:delete(?MD_POLICY, MetaVals), Opts);
make_new_user_metadata(MetaVals, [{versioning, VsnOpt} | Opts]) ->
    make_new_user_metadata(replace_meta(?MD_VERSIONING, VsnOpt, MetaVals), Opts).

replace_meta(Key, NewValue, MetaVals) ->
    [{Key, term_to_binary(NewValue)} | proplists:delete(Key, MetaVals)].


%% @doc bucket is ok to delete when bucket is empty. Ongoing multipart
%% uploads are all supposed to be automatically aborted by Riak CS.
%% If the bucket still has active objects, just fail. Else if the
%% bucket still has ongoing multipart, Stanchion returns error and
%% Riak CS retries some times, in case of concurrent multipart
%% initiation occuring.  After a few retry Riak CS will eventually
%% returns error to the client (maybe 500?)  Or fallback to heavy
%% abort-all-multipart and then deletes bucket?  This will be a big
%% TODO.
-spec is_bucket_ready_to_delete(binary(), pid(), riakc_obj()) ->
                                       {false, multipart_upload_remains|bucket_not_empty} |
                                       {true, riakc_obj()}.
is_bucket_ready_to_delete(Bucket, RiakPid, BucketObj) ->
    is_bucket_clean(Bucket, RiakPid, BucketObj).

%% @doc ensure there are no multipart uploads in creation time because
%% multipart uploads remains in deleted buckets in former versions
%% before 1.5.0 (or 1.4.6) where the bug (identified in riak_cs/#475).
-spec is_bucket_ready_to_create(binary(), pid(), riakc_obj()) ->
                                       {false, multipart_upload_remains|bucket_not_empty} |
                                       {true, riakc_obj()}.
is_bucket_ready_to_create(Bucket, RiakPid, BucketObj) ->
    is_bucket_clean(Bucket, RiakPid, BucketObj).

%% @doc here runs two list_keys, one in bucket_empty/2, another in
%% stanchion_multipart:check_no_multipart_uploads/2. If there are
%% bunch of pending_delete manifests this may slow (twice as before
%% #475 fix). If there are bunch of scheduled_delete manifests, this
%% may also slow, but wait for Garbage Collection to collect those
%% trashes may improve the speed. => TODO.
-spec is_bucket_clean(binary(), pid(), riakc_obj()) ->
                                       {false, multipart_upload_remains|bucket_not_empty} |
                                       {true, riakc_obj()}.
is_bucket_clean(Bucket, RiakPid, BucketObj) ->
    {ok, ManifestRiakPid} = manifest_connection(RiakPid, BucketObj),
    try
        case bucket_empty(Bucket, ManifestRiakPid) of
            false ->
                {false, bucket_not_empty};
            true ->
                case stanchion_multipart:check_no_multipart_uploads(Bucket, ManifestRiakPid) of
                    false ->
                        {false, multipart_upload_remains};
                    true ->
                        {true, BucketObj}
                end
        end
    catch T:E:ST ->
            logger:error("Could not check whether bucket was empty. Reason: ~p:~p - ~p",
                         [T, E, ST]),
            error({T, E})
    after
        close_manifest_connection(RiakPid, ManifestRiakPid)
    end.

-spec manifest_connection(pid(), riakc_obj:riakc_obj()) -> {ok, pid()} | {error, term()}.
manifest_connection(RiakPid, BucketObj) ->
    case bag_id_from_bucket(BucketObj) of
        undefined -> {ok, RiakPid};
        BagId ->
            case conn_info_from_bag(BagId, application:get_env(riak_cs, bags)) of
                %% No connection information for the bag. Mis-configuration. Stop processing.
                undefined -> {error, {no_bag, BagId}};
                {Address, Port} -> riak_connection(Address, Port)
            end
    end.

conn_info_from_bag(_BagId, undefined) ->
    undefined;
conn_info_from_bag(BagId, {ok, Bags}) ->
    BagIdStr = binary_to_list(BagId),
    case lists:keyfind(BagIdStr, 1, Bags) of
        false ->
            {error, no_bag};
        {BagIdStr, Address, Port} ->
            {Address, Port}
    end.

bag_id_from_bucket(BucketObj) ->
    Contents = riakc_obj:get_contents(BucketObj),
    bag_id_from_contents(Contents).

bag_id_from_contents([]) ->
    undefined;
bag_id_from_contents([{MD, _} | Contents]) ->
    case bag_id_from_meta(dict:fetch(?MD_USERMETA, MD)) of
        undefined ->
            bag_id_from_contents(Contents);
        BagId ->
            BagId
    end.

bag_id_from_meta([]) ->
    undefined;
bag_id_from_meta([{?MD_BAG, Value} | _]) ->
    binary_to_term(Value);
bag_id_from_meta([_MD | MDs]) ->
    bag_id_from_meta(MDs).

close_manifest_connection(RiakPid, RiakPid) ->
    ok;
close_manifest_connection(_RiakPid, ManifestRiakPid) ->
    close_riak_connection(ManifestRiakPid).

%% @doc Determine if a user with the specified email
%% address already exists. There could be consistency
%% issues here since secondary index queries use
%% coverage and only consult a single vnode
%% for a particular key.
%% @TODO Consider other options that would give more
%% assurance that a particular email address is available.
-spec email_available(string() | binary(), pid()) -> true | {false, user_already_exists | term()}.
email_available(Email_, RiakPid) ->

    %% this is to pacify dialyzer, which makes an issue of
    %% Email::string() (coming from #rcs_user_v2.email type) as
    %% conflicting with binary() as the type appropriate for 4th arg
    %% in get_index_eq
    Email = iolist_to_binary([Email_]),

    {Res, TAT} = ?TURNAROUND_TIME(riakc_pb_socket:get_index_eq(RiakPid, ?USER_BUCKET, ?EMAIL_INDEX, Email)),
    riak_cs_stats:update([riakc, get_user_by_index], TAT),
    case Res of
        {ok, ?INDEX_RESULTS{keys=[]}} ->
            true;
        {ok, _} ->
            {false, user_already_exists};
        {error, Reason} ->
            %% @TODO Maybe bubble up this error info
            logger:warning("Error occurred trying to check if the address ~p has been registered. Reason: ~p",
                           [Email, Reason]),
            {false, Reason}
    end.

%% internal fun to retrieve the riak object
%% at a bucket/key
-spec get_manifests_raw(pid(), binary(), binary(), binary()) ->
    {ok, riakc_obj:riakc_obj()} | {error, notfound}.
get_manifests_raw(RiakcPid, Bucket, Key, Vsn) ->
    ManifestBucket = to_bucket_name(objects, Bucket),
    {Res, TAT} = ?TURNAROUND_TIME(
                    riakc_pb_socket:get(RiakcPid, ManifestBucket,
                                        rcs_common_manifest:make_versioned_key(Key, Vsn))),
    stanchion_stats:update([riakc, get_manifest], TAT),
    Res.

save_user(User, RiakPid) ->
    Indexes = [{?EMAIL_INDEX, User?MOSS_USER.email},
               {?ID_INDEX, User?MOSS_USER.canonical_id}],
    Meta = dict:store(?MD_INDEX, Indexes, dict:new()),
    Obj = riakc_obj:new(?USER_BUCKET, iolist_to_binary(User?MOSS_USER.key_id), term_to_binary(User)),
    UserObj = riakc_obj:update_metadata(Obj, Meta),
    {Res, TAT} = ?TURNAROUND_TIME(riakc_pb_socket:put(RiakPid, UserObj)),
    case Res of
        ok ->
            stanchion_stats:update([riakc, put_cs_user], TAT);
        {error, Reason} ->
            logger:error("Failed to save user: Reason", [Reason]),
            Res
    end.

save_user(true, User=?RCS_USER{email=Email}, UserObj, RiakPid) ->
    case email_available(Email, RiakPid) of
        true ->
            Indexes = [{?EMAIL_INDEX, Email},
                       {?ID_INDEX, User?RCS_USER.canonical_id}],
            MD = dict:store(?MD_INDEX, Indexes, dict:new()),
            UpdUserObj = riakc_obj:update_metadata(
                           riakc_obj:update_value(UserObj,
                                                  term_to_binary(User)),
                           MD),
            {Res, TAT} = ?TURNAROUND_TIME(riakc_pb_socket:put(RiakPid, UpdUserObj)),
            stanchion_stats:update([riakc, put_cs_user], TAT),
            Res;
        {false, Reason} ->
            {error, Reason}
    end;
save_user(false, User, UserObj, RiakPid) ->
    Indexes = [{?EMAIL_INDEX, User?RCS_USER.email},
               {?ID_INDEX, User?RCS_USER.canonical_id}],
    MD = dict:store(?MD_INDEX, Indexes, dict:new()),
    UpdUserObj = riakc_obj:update_metadata(
                   riakc_obj:update_value(UserObj,
                                          term_to_binary(User)),
                   MD),
    {Res, TAT} = ?TURNAROUND_TIME(riakc_pb_socket:put(RiakPid, UpdUserObj)),
    stanchion_stats:update([riakc, put_cs_user], TAT),
    Res.


update_user_record([], User, EmailUpdated) ->
    {User, EmailUpdated};
update_user_record([{<<"name">>, Name} | RestUserFields], User, EmailUpdated) ->
    update_user_record(RestUserFields,
                       User?RCS_USER{name=binary_to_list(Name)}, EmailUpdated);
update_user_record([{<<"email">>, Email} | RestUserFields],
                   User, _) ->
    UpdEmail = binary_to_list(Email),
    EmailUpdated =  not (User?RCS_USER.email =:= UpdEmail),
    update_user_record(RestUserFields,
                       User?RCS_USER{email=UpdEmail}, EmailUpdated);
update_user_record([{<<"display_name">>, Name} | RestUserFields], User, EmailUpdated) ->
    update_user_record(RestUserFields,
                       User?RCS_USER{display_name=binary_to_list(Name)}, EmailUpdated);
update_user_record([{<<"key_secret">>, KeySecret} | RestUserFields], User, EmailUpdated) ->
    update_user_record(RestUserFields,
                       User?RCS_USER{key_secret=binary_to_list(KeySecret)}, EmailUpdated);
update_user_record([{<<"status">>, Status} | RestUserFields], User, EmailUpdated) ->
    case Status of
        <<"enabled">> ->
            update_user_record(RestUserFields,
                               User?RCS_USER{status=enabled}, EmailUpdated);
        <<"disabled">> ->
            update_user_record(RestUserFields,
                               User?RCS_USER{status=disabled}, EmailUpdated);
        _ ->
            update_user_record(RestUserFields, User, EmailUpdated)
    end;
update_user_record([_ | RestUserFields], User, EmailUpdated) ->
    update_user_record(RestUserFields, User, EmailUpdated).


update_user_buckets(add, User, Bucket) ->
    Buckets = User?RCS_USER.buckets,
    %% At this point any siblings from the read of the
    %% user record have been resolved so the user bucket
    %% list should have 0 or 1 buckets that share a name
    %% with `Bucket'.
    case [B || B <- Buckets, B?RCS_BUCKET.name =:= Bucket?RCS_BUCKET.name] of
        [] ->
            User?RCS_USER{buckets = [Bucket | Buckets]};
        [ExistingBucket] ->
            UpdBuckets = [Bucket | lists:delete(ExistingBucket, Buckets)],
            User?RCS_USER{buckets = UpdBuckets}
    end;
update_user_buckets(delete, User, Bucket) ->
    Buckets = User?RCS_USER.buckets,
    case [B || B <- Buckets, B?RCS_BUCKET.name =:= Bucket?RCS_BUCKET.name] of
        [] ->
            logger:error("attempt to remove bucket ~s from user ~s who does not own it",
                         [Bucket?RCS_BUCKET.name, User?RCS_USER.name]),
            User;
        [ExistingBucket] ->
            UpdBuckets = lists:delete(ExistingBucket, Buckets),
            User?RCS_USER{buckets=UpdBuckets}
    end.

-spec get_role(string(), pid()) -> {ok, role()} | {error, no_value}.
get_role(Id, RiakPid) ->
    %% Check for and resolve siblings to get a
    %% coherent view of the bucket ownership.
    BinKey = iolist_to_binary(Id),
    case fetch_object(?IAM_BUCKET, BinKey, RiakPid) of
        {ok, {Obj, _KeepDeletedBuckets}} ->
            role_from_riakc_obj(Obj);
        Error ->
            Error
    end.

role_from_riakc_obj(Obj) ->
    case riakc_obj:value_count(Obj) of
        1 ->
            Role = binary_to_term(riakc_obj:get_value(Obj)),
            {ok, Role};
        0 ->
            {error, no_value};
        _ ->
            Values = [binary_to_term(Value) ||
                         Value <- riakc_obj:get_values(Obj),
                         Value /= <<>>  % tombstone
                     ],
            Role = hd(Values),
            {ok, Role}
    end.


-spec save_role(role(), pid()) -> {ok, string()} | {error, term()}.
save_role(Role0 = ?IAM_ROLE{role_name = RoleName,
                            path = Path}, RiakPid) ->
    RoleId = ensure_unique_role_id(RiakPid),

    ?LOG_INFO("Saving new role \"~s\" with id ~s", [RoleName, RoleId]),
    Role1 = Role0?IAM_ROLE{role_id = RoleId},

    Indexes = [{?ROLE_NAME_INDEX, RoleName},
               {?ROLE_ID_INDEX, RoleId},
               {?ROLE_PATH_INDEX, Path}
              ],
    Meta = dict:store(?MD_INDEX, Indexes, dict:new()),
    Obj = riakc_obj:update_metadata(
            riakc_obj:new(?IAM_BUCKET, iolist_to_binary(RoleName), term_to_binary(Role1)),
            Meta),
    {Res, TAT} = ?TURNAROUND_TIME(riakc_pb_socket:put(RiakPid, Obj)),
    case Res of
        ok ->
            ok = stanchion_stats:update([riakc, put_cs_role], TAT),
            {ok, RoleId};
        {error, Reason} ->
            logger:error("Failed to save role \"~s\": ~p", [Reason]),
            Res
    end.

ensure_unique_role_id(RcPid) ->
    Id = make_role_id(),
    case fetch_object(?IAM_BUCKET, Id, RcPid) of
        {ok, _} ->
            ensure_unique_role_id(RcPid);
        _ ->
            Id
    end.

make_role_id() ->
    fill(?ROLE_ID_LENGTH - 4, "AROA").
fill(0, Q) ->
    Q;
fill(N, Q) ->
    fill(N-1, Q ++ [lists:nth(rand:uniform(length(?ROLE_ID_CHARSET)), ?ROLE_ID_CHARSET)]).


-spec delete_role(string()) -> ok.
delete_role(RoleName) ->
    case riak_connection() of
        {ok, RiakPid} ->
            try
                Obj = riakc_obj:new(?IAM_BUCKET, iolist_to_binary(RoleName), ?FREE_ROLE_MARKER),
                {Res, TAT} = ?TURNAROUND_TIME(riakc_pb_socket:put(RiakPid, Obj)),
                case Res of
                    ok ->
                        stanchion_stats:update([riakc, put_cs_role], TAT);
                    {error, Reason} ->
                        logger:error("Failed to save deleted role object \"~s\": ~p", [Reason]),
                        Res
                end
            after
                close_riak_connection(RiakPid)
            end;
        {error, _} = Else ->
            Else
    end.



%% @doc Perform an initial read attempt with R=PR=N.
%% If the initial read fails retry using
%% R=quorum and PR=1, but indicate that bucket deletion
%% indicators should not be cleaned up.
fetch_object(Bucket, Key, RiakPid) ->
    StrongOptions = [{r, all}, {pr, all}, {notfound_ok, false}],
    {Res, TAT} = ?TURNAROUND_TIME(riakc_pb_socket:get(RiakPid, Bucket, Key, StrongOptions)),
    stanchion_stats:update([riakc, metric_for(Bucket, strong)], TAT),
    case Res of
        {ok, Obj} ->
            {ok, {Obj, true}};
        {error, _} ->
            weak_fetch_object(Bucket, Key, RiakPid)
    end.

weak_fetch_object(Bucket, Key, RiakPid) ->
    WeakOptions = [{r, quorum}, {pr, one}, {notfound_ok, false}],
    {Res, TAT} = ?TURNAROUND_TIME(riakc_pb_socket:get(RiakPid, Bucket, Key, WeakOptions)),
    stanchion_stats:update([riakc, metric_for(Bucket, weak)], TAT),
    case Res of
        {ok, Obj} ->
            {ok, {Obj, false}};
        {error, Reason} ->
            {error, Reason}
    end.

metric_for(?IAM_BUCKET, strong) ->
    get_cs_role_strong;
metric_for(?USER_BUCKET, strong) ->
    get_cs_user_strong;
metric_for(?IAM_BUCKET, weak) ->
    get_cs_role_strong;
metric_for(?USER_BUCKET, weak) ->
    get_cs_user.

