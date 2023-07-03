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
-export([create_bucket/2,
         delete_bucket/3,
         create_user/2,
         delete_user/2,
         update_user/2,
         create_role/2,
         update_role/2,
         delete_role/2,
         create_policy/2,
         update_policy/2,
         delete_policy/2,
         create_saml_provider/2,
         delete_saml_provider/2,
         set_bucket_acl/3,
         set_bucket_policy/3,
         set_bucket_versioning/3,
         delete_bucket_policy/3
        ]).
-export([get_manifests_raw/4,
         get_pbc/0,
         has_tombstone/1,
         make_pbc/0,
         to_bucket_name/2,
         sha_mac/2
        ]).

-include("riak_cs.hrl").
-include("stanchion.hrl").
-include("manifest.hrl").
-include("moss.hrl").
-include_lib("riakc/include/riakc.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").
-include_lib("kernel/include/logger.hrl").


%% this riak connection is separate, potentially to a different riak
%% endpoint, from the standard one obtained via riak_cs_utils
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
-spec create_bucket(maps:map(), pid()) -> ok | {error, term()}.
create_bucket(#{bucket := Bucket,
                requester := OwnerId,
                acl := Acl_} = FF, Pbc) ->
    Acl = riak_cs_acl:exprec_acl(Acl_),
    BagId = maps:get(bag, FF, undefined),
    OpResult1 = do_bucket_op(Bucket, OwnerId, [{acl, Acl}, {bag, BagId}], create, Pbc),
    case OpResult1 of
        ok ->
            BucketRecord = bucket_record(Bucket, create),
            {ok, RcPid} = riak_cs_riak_client:checkout(),
            try
                case riak_cs_user:get_user(OwnerId, RcPid) of
                    {ok, {_FedereatedUser, undefined}} ->
                        logger:info("Refusing to create bucket ~s for a temp user (key_id: ~s) with assumed role",
                                    [Bucket, OwnerId]),
                        {error, temp_users_create_bucket_restriction};
                    {ok, {User, UserObj}} ->
                        UpdUser = update_user_buckets(add, User, BucketRecord),
                        save_user(UpdUser, UserObj, Pbc)
                end
            after
                riak_cs_riak_client:checkin(RcPid)
            end;
        {error, _} ->
            OpResult1
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


%% @doc Delete a bucket
-spec delete_bucket(binary(), binary(), pid()) -> ok | {error, term()}.
delete_bucket(Bucket, OwnerId, Pbc) ->
    OpResult1 = do_bucket_op(Bucket, OwnerId, [{acl, ?ACL{}}], delete, Pbc),
    case OpResult1 of
        ok ->
            {ok, RcPid} = riak_cs_riak_client:checkout(),
            try
                BucketRecord = bucket_record(Bucket, delete),
                {ok, {User, UserObj}} = riak_cs_iam:find_user(#{key_id => OwnerId}, RcPid),
                UpdUser = update_user_buckets(delete, User, BucketRecord),
                save_user(UpdUser, UserObj, Pbc)
            after
                ok = riak_cs_riak_client:checkin(RcPid)
            end;
        {error, _} ->
            OpResult1
    end.


%% @doc Attempt to create a new user
-spec create_user(maps:map(), pid()) -> ok | {error, term()}.
create_user(FF = #{email := Email}, Pbc) ->
    case email_available(Email, Pbc) of
        true ->
            User = riak_cs_iam:unarm(
                     riak_cs_iam:exprec_user(FF)),
            save_user(User, Pbc);
        {false, Reason} ->
            logger:info("Refusing to create a user with email ~s: ~s", [Email, Reason]),
            {error, user_already_exists}
    end.

-spec delete_user(flat_arn(), pid()) -> ok | {error, term()}.
delete_user(Arn, Pbc) ->
    case ?TURNAROUND_TIME(
            riakc_pb_socket:delete(Pbc, ?USER_BUCKET, Arn, ?CONSISTENT_DELETE_OPTIONS)) of
        {ok, TAT} ->
            stanchion_stats:update([riakc, delete_cs_user], TAT);
        {error, Reason} = ER ->
            logger:error("Failed to delete role object ~s: ~p", [Arn, Reason]),
            ER
    end.

-spec update_user(maps:map(), pid()) -> ok | {error, term()}.
update_user(FF, Pbc) ->
    User = ?IAM_USER{arn = Arn,
                     email = Email} =
        riak_cs_iam:exprec_user(FF),
    {ok, RcPid} = riak_cs_riak_client:checkout(),
    try
        {ok, {_OldUser, Obj}} = riak_cs_iam:get_user(Arn, RcPid),
        CanProceed =
            case riak_cs_iam:find_user(#{email => Email}, RcPid) of
                {ok, {?IAM_USER{arn = Arn}, _}} ->
                    true;  %% found self
                {ok, {?IAM_USER{email = Email}, _}} ->
                    false; %% found some other user with this email
                {error, notfound} ->
                    true
            end,
        if CanProceed ->
                save_user(User, Obj, Pbc);
           el/=se ->
                {error, user_already_exists}
        end
    after
        riak_cs_riak_client:checkin(RcPid)
    end.


%% @doc Determine if a user with the specified email
%% address already exists. There could be consistency
%% issues here since secondary index queries use
%% coverage and only consult a single vnode
%% for a particular key.
%% @TODO Consider other options that would give more
%% assurance that a particular email address is available.
email_available(Email, Pbc) ->
    case riakc_pb_socket:get_index_eq(Pbc, ?USER_BUCKET, ?USER_EMAIL_INDEX, Email) of
        {ok, ?INDEX_RESULTS{keys = []}} ->
            true;
        {ok, ?INDEX_RESULTS{keys = [KK]}} ->
            {false, user_already_exists};
        {error, Reason} ->
            %% @TODO Maybe bubble up this error info
            logger:warning("Error occurred trying to check if the address ~p has been registered. Reason: ~p",
                           [Email, Reason]),
            {false, Reason}
    end.


-spec create_role(maps:map(), pid()) -> {ok, role()} | {error, already_exists|term()}.
create_role(Fields, Pbc) ->
    R = ?IAM_ROLE{role_name = Name} =
        riak_cs_iam:unarm(
          riak_cs_iam:exprec_role(
            riak_cs_iam:fix_permissions_boundary(Fields))),
    case role_name_available(Name) of
        true ->
            save_role(R, Pbc);
        false ->
            {error, role_already_exists}
    end.

-spec update_role(maps:map(), pid()) -> ok | {error, already_exists|term()}.
update_role(Fields, Pbc) ->
    Role = riak_cs_iam:exprec_role(Fields),
    save_role_directly(Role, Pbc).

role_name_available(Name) ->
    {ok, Pbc} = riak_cs_riak_client:checkout(),
    Res = (riak_cs_iam:find_role(#{name => Name}, Pbc) == {error, notfound}),
    riak_cs_riak_client:checkin(Pbc),
    Res.

save_role(Role0 = ?IAM_ROLE{role_name = Name,
                            path = Path}, Pbc) ->
    Id = riak_cs_aws_utils:make_unique_index_id(role, Pbc),
    Arn = riak_cs_aws_utils:make_role_arn(Name, Path),
    Role1 = Role0?IAM_ROLE{arn = Arn,
                           role_id = Id},

    Meta = dict:store(?MD_INDEX, riak_cs_utils:object_indices(Role1), dict:new()),
    Obj = riakc_obj:update_metadata(
            riakc_obj:new(?IAM_ROLE_BUCKET, Arn, term_to_binary(Role1)),
            Meta),
    {Res, TAT} = ?TURNAROUND_TIME(riakc_pb_socket:put(Pbc, Obj, ?CONSISTENT_WRITE_OPTIONS)),
    case Res of
        ok ->
            ok = stanchion_stats:update([riakc, put_cs_role], TAT),
            {ok, Role1};
        {error, Reason} ->
            logger:error("Failed to save role \"~s\": ~p", [Name, Reason]),
            Res
    end.

save_role_directly(Role = ?IAM_ROLE{arn = Arn,
                                    role_name = Name}, Pbc) ->
    Meta = dict:store(?MD_INDEX, riak_cs_utils:object_indices(Role), dict:new()),
    Obj = riakc_obj:update_metadata(
            riakc_obj:new(?IAM_ROLE_BUCKET, Arn, term_to_binary(Role)),
            Meta),
    {Res, TAT} = ?TURNAROUND_TIME(riakc_pb_socket:put(Pbc, Obj, ?CONSISTENT_WRITE_OPTIONS)),
    case Res of
        ok ->
            ok = stanchion_stats:update([riakc, put_cs_role], TAT),
            ok;
        {error, Reason} ->
            logger:error("Failed to save role \"~s\": ~p", [Name, Reason]),
            Res
    end.

-spec delete_role(binary(), pid()) -> ok | {error, term()}.
delete_role(Arn, Pbc) ->
    case ?TURNAROUND_TIME(
            riakc_pb_socket:delete(Pbc, ?IAM_ROLE_BUCKET, Arn, ?CONSISTENT_DELETE_OPTIONS)) of
        {ok, TAT} ->
            stanchion_stats:update([riakc, delete_cs_role], TAT);
        {error, Reason} = ER ->
            logger:error("Failed to delete role object ~s: ~p", [Arn, Reason]),
            ER
    end.


-spec create_policy(maps:map(), pid()) -> {ok, policy()} | {error, term()}.
create_policy(Fields, Pbc) ->
    P = ?IAM_POLICY{policy_name = Name} =
        riak_cs_iam:unarm(
          riak_cs_iam:exprec_policy(Fields)),
    case policy_name_available(Name) of
        true ->
            save_policy(P, Pbc);
        false ->
            {error, policy_already_exists}
    end.

-spec update_policy(maps:map(), pid()) -> ok | {error, term()}.
update_policy(Fields, Pbc) ->
    Policy = riak_cs_iam:exprec_policy(Fields),
    save_policy_directly(Policy, Pbc).

policy_name_available(Name) ->
    {ok, RcPid} = riak_cs_riak_client:checkout(),
    try
        {error, notfound} == riak_cs_iam:find_policy(#{name => Name}, RcPid)
    after
        riak_cs_riak_client:checkin(RcPid)
    end.

save_policy(Policy0 = ?IAM_POLICY{policy_name = Name,
                                  path = Path}, Pbc) ->
    Id = riak_cs_aws_utils:make_unique_index_id(policy, Pbc),
    Arn = riak_cs_aws_utils:make_policy_arn(Name, Path),
    Policy1 = Policy0?IAM_POLICY{arn = Arn,
                                 policy_id = Id},

    Meta = dict:store(?MD_INDEX, riak_cs_utils:object_indices(Policy1), dict:new()),
    Obj = riakc_obj:update_metadata(
            riakc_obj:new(?IAM_POLICY_BUCKET, Arn, term_to_binary(Policy1)),
            Meta),
    {Res, TAT} = ?TURNAROUND_TIME(riakc_pb_socket:put(Pbc, Obj, ?CONSISTENT_WRITE_OPTIONS)),
    case Res of
        ok ->
            ok = stanchion_stats:update([riakc, put_cs_policy], TAT),
            {ok, Policy1};
        {error, Reason} ->
            logger:error("Failed to save managed policy \"~s\": ~p", [Name, Reason]),
            Res
    end.

save_policy_directly(Policy = ?IAM_POLICY{arn = Arn,
                                          policy_name = Name}, Pbc) ->
    Meta = dict:store(?MD_INDEX, riak_cs_utils:object_indices(Policy), dict:new()),
    Obj = riakc_obj:update_metadata(
            riakc_obj:new(?IAM_POLICY_BUCKET, Arn, term_to_binary(Policy)),
            Meta),
    {Res, TAT} = ?TURNAROUND_TIME(riakc_pb_socket:put(Pbc, Obj, ?CONSISTENT_WRITE_OPTIONS)),
    case Res of
        ok ->
            ok = stanchion_stats:update([riakc, put_cs_policy], TAT),
            ok;
        {error, Reason} ->
            logger:error("Failed to save managed policy \"~s\": ~p", [Name, Reason]),
            Res
    end.

-spec delete_policy(binary(), pid()) -> ok | {error, term()}.
delete_policy(Arn, Pbc) ->
    case ?TURNAROUND_TIME(
            riakc_pb_socket:delete(Pbc, ?IAM_POLICY_BUCKET, Arn, ?CONSISTENT_DELETE_OPTIONS)) of
        {ok, TAT} ->
            stanchion_stats:update([riakc, delete_cs_policy], TAT);
        {error, Reason} = ER ->
            logger:error("Failed to delete managed policy object ~s: ~p", [Arn, Reason]),
            ER
    end.


-spec create_saml_provider(maps:map(), pid()) -> {ok, {string(), [tag()]}} | {error, term()}.
create_saml_provider(Fields, Pbc) ->
    P0 = ?IAM_SAML_PROVIDER{name = Name} =
        riak_cs_iam:unarm(
          riak_cs_iam:exprec_saml_provider(Fields)),
    case riak_cs_iam:parse_saml_provider_idp_metadata(P0) of
        {ok, P9} ->
            case saml_provider_name_available(Name) of
                true ->
                    save_saml_provider(P9, Pbc);
                false ->
                    {error, saml_provider_already_exists}
            end;
        ER ->
            ER
    end.

saml_provider_name_available(Name) ->
    {ok, Pbc} = riak_cs_riak_client:checkout(),
    try
        {error, notfound} == riak_cs_iam:find_saml_provider(#{name => Name}, Pbc)
    after
        riak_cs_riak_client:checkin(Pbc)
    end.

save_saml_provider(P0 = ?IAM_SAML_PROVIDER{name = Name,
                                           tags = Tags}, Pbc) ->
    Arn = riak_cs_aws_utils:make_provider_arn(Name),
    P1 = P0?IAM_SAML_PROVIDER{arn = Arn},

    Meta = dict:store(?MD_INDEX, riak_cs_utils:object_indices(P1), dict:new()),
    Obj = riakc_obj:update_metadata(
            riakc_obj:new(?IAM_SAMLPROVIDER_BUCKET, Arn, term_to_binary(P1)),
            Meta),
    {Res, TAT} = ?TURNAROUND_TIME(riakc_pb_socket:put(Pbc, Obj, ?CONSISTENT_WRITE_OPTIONS)),
    case Res of
        ok ->
            ok = stanchion_stats:update([riakc, put_cs_samlprovider], TAT),
            {ok, {Arn, Tags}};
        {error, Reason} ->
            logger:error("Failed to save SAML provider \"~s\": ~p", [Reason]),
            Res
    end.


-spec delete_saml_provider(binary(), pid()) -> ok | {error, term()}.
delete_saml_provider(Arn, Pbc) ->
    case ?TURNAROUND_TIME(
            riakc_pb_socket:delete(Pbc, ?IAM_SAMLPROVIDER_BUCKET, Arn, ?CONSISTENT_DELETE_OPTIONS)) of
        {ok, TAT} ->
            stanchion_stats:update([riakc, put_cs_samlprovider], TAT);
        {error, Reason} = ER ->
            logger:error("Failed to delete saml_provider object ~s: ~p", [Arn, Reason]),
            ER
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
list_keys(BucketName, Pbc) ->
    case ?TURNAROUND_TIME(riakc_pb_socket:list_keys(Pbc, BucketName)) of
        {{ok, Keys}, TAT} ->
            stanchion_stats:update([riakc, list_all_manifest_keys], TAT),
            {ok, lists:sort(Keys)};
        {{error, _} = ER, TAT} ->
            stanchion_stats:update([riakc, list_all_manifest_keys], TAT),
            ER
    end.

%% @doc Set the ACL for a bucket
-spec set_bucket_acl(binary(), maps:map(), pid()) -> ok | {error, term()}.
set_bucket_acl(Bucket, #{requester := OwnerId,
                         acl := Acl}, Pbc) ->
    do_bucket_op(Bucket, OwnerId, [{acl, Acl}], update_acl, Pbc).

%% @doc add bucket policy in the global namespace
%% FieldList.policy has JSON-encoded policy from user
-spec set_bucket_policy(binary(), maps:map(), pid()) -> ok | {error, term()}.
set_bucket_policy(Bucket, #{requester := Requester,
                            policy := PolicyJson}, Pbc) ->
    do_bucket_op(Bucket, Requester, [{policy, base64:decode(PolicyJson)}], update_policy, Pbc).

%% @doc set bucket versioning option
-spec set_bucket_versioning(binary(), maps:map(), pid()) -> ok | {error, term()}.
set_bucket_versioning(Bucket, #{requester := Requester,
                                versioning := Versioning}, Pbc) ->
    do_bucket_op(Bucket, Requester, [{versioning, Versioning}], update_versioning, Pbc).


%% @doc Delete a bucket
-spec delete_bucket_policy(binary(), binary(), pid()) -> ok | {error, term()}.
delete_bucket_policy(Bucket, OwnerId, Pbc) ->
    do_bucket_op(Bucket, OwnerId, [delete_policy], delete_policy, Pbc).

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


sha_mac(KeyData, STS) ->
    crypto:mac(hmac, sha, KeyData, STS).

md5(Bin) ->
    crypto:hash(md5, Bin).

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @doc Check if a bucket is empty
-spec bucket_empty(binary(), pid()) -> boolean().
bucket_empty(Bucket, Pbc) ->
    ManifestBucket = to_bucket_name(objects, Bucket),
    %% @TODO Use `stream_list_keys' instead and
    ListKeysResult = list_keys(ManifestBucket, Pbc),
    bucket_empty_handle_list_keys(Pbc,
                                  Bucket,
                                  ListKeysResult).

bucket_empty_handle_list_keys(Pbc, Bucket, {ok, Keys}) ->
    %% `lists:any/2' will break out early as soon
    %% as something returns `true'
    not lists:any(
         fun (Key) -> key_exists(Pbc, Bucket, Key) end,
          Keys);
bucket_empty_handle_list_keys(_RiakcPid, _Bucket, _Error) ->
    false.

key_exists(Pbc, Bucket, Key) ->
    key_exists_handle_get_manifests(
      get_manifests(Pbc, Bucket, Key, ?LFS_DEFAULT_OBJECT_VERSION)).

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

bucket_available(Bucket, RequesterId, BucketOp, Pbc) ->
    GetOptions = [{pr, all}],
    case ?TURNAROUND_TIME(riakc_pb_socket:get(Pbc, ?BUCKETS_BUCKET, Bucket, GetOptions)) of
        {{ok, BucketObj}, TAT} ->
            stanchion_stats:update([riakc, get_cs_bucket], TAT),
            OwnerId = riakc_obj:get_value(BucketObj),
            case {OwnerId, BucketOp} of
                {?FREE_BUCKET_MARKER, create} ->
                    is_bucket_ready_to_create(Bucket, Pbc, BucketObj);
                {?FREE_BUCKET_MARKER, _} ->
                    {false, no_such_bucket};

                {RequesterId, delete} ->
                    is_bucket_ready_to_delete(Bucket, Pbc, BucketObj);
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


do_bucket_op(<<"riak-cs">>, _OwnerId, _Opts, _BucketOp, _Pbc) ->
    {error, access_denied};
do_bucket_op(Bucket, OwnerId, Opts, BucketOp, Pbc) ->
    %% Buckets operations can only be completed if the bucket exists
    %% and the requesting party owns the bucket.
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
put_bucket(BucketObj, OwnerId, Opts, Pbc) ->
    PutOptions = [{w, all}, {pw, all}],
    UpdBucketObj0 = riakc_obj:update_value(BucketObj, OwnerId),
    MD = case riakc_obj:get_metadatas(UpdBucketObj0) of
             [] -> % create
                 dict:from_list([{?MD_USERMETA, []}]);
             [MD0] -> MD0;
             _E ->
                 MsgData = {siblings, riakc_obj:key(BucketObj)},
                 logger:warning("bucket has siblings: ~p", [MsgData]),
                 throw(MsgData) % @TODO: data broken; handle this
           end,
    MetaData = make_new_metadata(MD, Opts),
    UpdBucketObj = riakc_obj:update_metadata(UpdBucketObj0, MetaData),
    {Result, TAT} = ?TURNAROUND_TIME(riakc_pb_socket:put(Pbc, UpdBucketObj, PutOptions)),
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
                                       {false, remaining_multipart_upload|bucket_not_empty} |
                                       {true, riakc_obj()}.
is_bucket_ready_to_delete(Bucket, Pbc, BucketObj) ->
    is_bucket_clean(Bucket, Pbc, BucketObj).

%% @doc ensure there are no multipart uploads in creation time because
%% multipart uploads remains in deleted buckets in former versions
%% before 1.5.0 (or 1.4.6) where the bug (identified in riak_cs/#475).
-spec is_bucket_ready_to_create(binary(), pid(), riakc_obj()) ->
                                       {false, remaining_multipart_upload|bucket_not_empty} |
                                       {true, riakc_obj()}.
is_bucket_ready_to_create(Bucket, Pbc, BucketObj) ->
    is_bucket_clean(Bucket, Pbc, BucketObj).

%% @doc here runs two list_keys, one in bucket_empty/2, another in
%% stanchion_multipart:check_no_multipart_uploads/2. If there are
%% bunch of pending_delete manifests this may slow (twice as before
%% #475 fix). If there are bunch of scheduled_delete manifests, this
%% may also slow, but wait for Garbage Collection to collect those
%% trashes may improve the speed. => TODO.
is_bucket_clean(Bucket, Pbc, BucketObj) ->
    {ok, ManifestPbc} = manifest_connection(Pbc, BucketObj),
    try
        case bucket_empty(Bucket, ManifestPbc) of
            false ->
                {false, bucket_not_empty};
            true ->
                case stanchion_multipart:check_no_multipart_uploads(Bucket, ManifestPbc) of
                    false ->
                        {false, remaining_multipart_upload};
                    true ->
                        {true, BucketObj}
                end
        end
    catch T:E:ST ->
            logger:error("Could not check whether bucket was empty. Reason: ~p:~p - ~p",
                         [T, E, ST]),
            error({T, E})
    after
        close_manifest_connection(Pbc, ManifestPbc)
    end.

manifest_connection(Pbc, BucketObj) ->
    case bag_id_from_bucket(BucketObj) of
        undefined ->
            {ok, Pbc};
        BagId ->
            case conn_info_from_bag(BagId, application:get_env(riak_cs, bags)) of
                %% No connection information for the bag. Mis-configuration. Stop processing.
                undefined ->
                    {error, {no_bag, BagId}};
                {Address, Port} ->
                    riak_connection(Address, Port)
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

close_manifest_connection(Pbc, Pbc) ->
    ok;
close_manifest_connection(_Pbc, ManifestPbc) ->
    close_riak_connection(ManifestPbc).

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



%% internal fun to retrieve the riak object
%% at a bucket/key
-spec get_manifests_raw(pid(), binary(), binary(), binary()) ->
    {ok, riakc_obj:riakc_obj()} | {error, notfound}.
get_manifests_raw(Pbc, Bucket, Key, Vsn) ->
    ManifestBucket = to_bucket_name(objects, Bucket),
    {Res, TAT} = ?TURNAROUND_TIME(
                    riakc_pb_socket:get(Pbc, ManifestBucket,
                                        rcs_common_manifest:make_versioned_key(Key, Vsn))),
    stanchion_stats:update([riakc, get_manifest], TAT),
    Res.

save_user(?IAM_USER{arn = Arn} = User, Pbc) ->
    save_user(User, riakc_obj:new(?USER_BUCKET, Arn, term_to_binary(User)), Pbc).

save_user(User, Obj0, Pbc) ->
    Meta = dict:store(?MD_INDEX, riak_cs_utils:object_indices(User), dict:new()),
    Obj = riakc_obj:update_metadata(
            riakc_obj:update_value(Obj0, term_to_binary(User)),
            Meta),
    {Res, TAT} = ?TURNAROUND_TIME(riakc_pb_socket:put(Pbc, Obj, ?CONSISTENT_WRITE_OPTIONS)),
    stanchion_stats:update([riakc, put_cs_user], TAT),
    Res.


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
