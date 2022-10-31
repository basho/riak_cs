%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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
-export([binary_to_hexlist/1,
         close_riak_connection/1,
         create_bucket/1,
         create_user/1,
         get_user/2,
         from_riakc_obj/2,
         delete_bucket/2,
         from_bucket_name/1,
         get_admin_creds/0,
         get_keys_and_values/1,
         get_manifests/4,
         get_manifests_raw/4,
         has_tombstone/1,
         pow/2,
         pow/3,
         riak_connection/0,
         riak_connection/2,
         set_bucket_acl/2,
         set_bucket_policy/2,
         set_bucket_versioning/2,
         delete_bucket_policy/2,
         timestamp/1,
         to_bucket_name/2,
         update_user/2,
         sha_mac/2,
         md5/1
        ]).

-include("stanchion.hrl").
-include_lib("riak_cs/include/manifest.hrl").
-include_lib("riak_cs/include/moss.hrl").
-include_lib("riakc/include/riakc.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").

-define(EMAIL_INDEX, <<"email_bin">>).
-define(ID_INDEX, <<"c_id_bin">>).
-define(OBJECT_BUCKET_PREFIX, <<"0o:">>).       % Version # = 0
-define(BLOCK_BUCKET_PREFIX, <<"0b:">>).        % Version # = 0

-type bucket_op() :: create | update_acl | delete | update_policy | delete_policy | update_versioning.
-type bucket_op_option() :: {acl, acl()}
                          | {policy, binary()}
                          | delete_policy
                          | {bag, binary()}
                          | {versioning, binary()}.
-type bucket_op_options() :: [bucket_op_option()].

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Convert the passed binary into a string where the numbers are represented in hexadecimal (lowercase and 0 prefilled).
-spec binary_to_hexlist(binary()) -> string().
binary_to_hexlist(Bin) ->
    XBin =
        [ begin
              Hex = erlang:integer_to_list(X, 16),
              if
                  X < 16 ->
                      lists:flatten(["0" | Hex]);
                  true ->
                      Hex
              end
          end || X <- binary_to_list(Bin)],
    string:to_lower(lists:flatten(XBin)).

%% @doc Close a protobufs connection to the riak cluster.
-spec close_riak_connection(pid()) -> ok.
close_riak_connection(Pid) ->
    riakc_pb_socket:stop(Pid).

%% @doc Create a bucket in the global namespace or return
%% an error if it already exists.
-spec create_bucket([{term(), term()}]) -> ok | {error, term()}.
create_bucket(BucketFields) ->
    %% @TODO Check for missing fields
    Bucket = proplists:get_value(<<"bucket">>, BucketFields, <<>>),
    BagId = proplists:get_value(<<"bag">>, BucketFields, undefined),
    OwnerId = proplists:get_value(<<"requester">>, BucketFields, <<>>),
    AclJson = proplists:get_value(<<"acl">>, BucketFields, []),
    Acl = stanchion_acl_utils:acl_from_json(AclJson),
    do_bucket_op(Bucket, OwnerId, [{acl, Acl}, {bag, BagId}], create).

%% @doc Attmpt to create a new user
-spec create_user([{term(), term()}]) -> ok | {error, riak_connect_failed() | term()}.
create_user(UserFields) ->
    %% @TODO Check for missing fields
    UserName = binary_to_list(proplists:get_value(<<"name">>, UserFields, <<>>)),
    DisplayName = binary_to_list(proplists:get_value(<<"display_name">>, UserFields, <<>>)),
    Email = proplists:get_value(<<"email">>, UserFields, <<>>),
    KeyId = binary_to_list(proplists:get_value(<<"key_id">>, UserFields, <<>>)),
    KeySecret = binary_to_list(proplists:get_value(<<"key_secret">>, UserFields, <<>>)),
    CanonicalId = binary_to_list(proplists:get_value(<<"id">>, UserFields, <<>>)),
    case riak_connection() of
        {ok, RiakPid} ->
            try
                case email_available(Email, RiakPid) of
                    true ->
                        User = ?MOSS_USER{name=UserName,
                                          display_name=DisplayName,
                                          email=binary_to_list(Email),
                                          key_id=KeyId,
                                          key_secret=KeySecret,
                                          canonical_id=CanonicalId},
                        save_user(User, RiakPid);
                    {false, Reason1} ->
                        {error, Reason1}
                end
            catch T:E ->
                    logger:error("Error on creating user ~s: ~p", [KeyId, {T, E}]),
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
    do_bucket_op(Bucket, OwnerId, [{acl, ?ACL{}}], delete).

%% Get the root bucket name for either a MOSS object
%% bucket or the data block bucket name.
-spec from_bucket_name(binary()) -> {'blocks' | 'objects', binary()}.
from_bucket_name(BucketNameWithPrefix) ->
    BlocksName = ?BLOCK_BUCKET_PREFIX,
    ObjectsName = ?OBJECT_BUCKET_PREFIX,
    BlockByteSize = byte_size(BlocksName),
    ObjectsByteSize = byte_size(ObjectsName),

    case BucketNameWithPrefix of
        <<BlocksName:BlockByteSize/binary, BucketName/binary>> ->
            {blocks, BucketName};
        <<ObjectsName:ObjectsByteSize/binary, BucketName/binary>> ->
            {objects, BucketName}
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

%% @doc Integer version of the standard pow() function; call the recursive accumulator to calculate.
-spec pow(integer(), integer()) -> integer().
pow(Base, Power) ->
    pow(Base, Power, 1).

%% @doc Integer version of the standard pow() function.
-spec pow(integer(), integer(), integer()) -> integer().
pow(Base, Power, Acc) ->
    case Power of
        0 ->
            Acc;
        _ ->
            pow(Base, Power - 1, Acc * Base)
    end.

%% @doc Store a new bucket in Riak
%% though whole metadata itself is a dict, a metadata of ?MD_USERMETA is
%% proplists of {?MD_ACL, ACL::binary()}|{?MD_POLICY, PolicyBin::binary()}|
%%  {?MD_BAG, BagId::binary()}, {?MD_VERSIONING, bucket_versioning_option()}}.
%% should preserve other metadata. ACL and Policy can be overwritten.
-spec put_bucket(term(), binary(), bucket_op_options(), pid()) ->
                        ok | {error, term()}.
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

-spec make_new_metadata(dict:dict(), bucket_op_options()) -> dict:dict().
make_new_metadata(MD, Opts) ->
    MetaVals = dict:fetch(?MD_USERMETA, MD),
    UserMetaData = make_new_user_metadata(MetaVals, Opts),
    dict:store(?MD_USERMETA, UserMetaData, dict:erase(?MD_USERMETA, MD)).

-spec make_new_user_metadata(proplists:proplist(), bucket_op_options()) -> proplists:proplist().
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

%% @doc Get a protobufs connection to the riak cluster
%% using information from the application environment.
-type riak_connect_failed() :: {riak_connect_failed, tuple()}.
-spec riak_connection() -> {ok, pid()} | {error, riak_connect_failed()}.
riak_connection() ->
    {Host, Port} = case application:get_env(riak_cs, riak_host) of
                       {ok, {_, _} = HostPort} -> HostPort;
                       undefined -> {"127.0.0.1",  8087}
                   end,
    riak_connection(Host, Port).

%% @doc Get a protobufs connection to the riak cluster.
-spec riak_connection(string(), pos_integer()) -> {ok, pid()} | {error, riak_connect_failed()}.
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

%% @doc Set the ACL for a bucket
-spec set_bucket_acl(binary(), term()) -> ok | {error, term()}.
set_bucket_acl(Bucket, FieldList) ->
    %% @TODO Check for missing fields
    OwnerId = proplists:get_value(<<"requester">>, FieldList, <<>>),
    AclJson = proplists:get_value(<<"acl">>, FieldList, []),
    Acl = stanchion_acl_utils:acl_from_json(AclJson),
    do_bucket_op(Bucket, OwnerId, [{acl, Acl}], update_acl).

%% @doc add bucket policy in the global namespace
%% FieldList.policy has JSON-encoded policy from user
-spec set_bucket_policy(binary(), term()) -> ok | {error, term()}.
set_bucket_policy(Bucket, FieldList) ->
    OwnerId = proplists:get_value(<<"requester">>, FieldList, <<>>),
    PolicyJson = proplists:get_value(<<"policy">>, FieldList, []),

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

%% @doc Generate a key for storing a set of manifests for deletion.
-spec timestamp(erlang:timestamp()) -> non_neg_integer().
timestamp({MegaSecs, Secs, _MicroSecs}) ->
    (MegaSecs * 1000000) + Secs.

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
                case get_user(KeyId, RiakPid) of
                    {ok, {User, UserObj}} ->

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

-spec bucket_empty_handle_list_keys(pid(), binary(),
                                    {ok, list()} |
                                    {error, term()}) ->
    boolean().
bucket_empty_handle_list_keys(RiakcPid, Bucket, {ok, Keys}) ->
    AnyPred = bucket_empty_any_pred(RiakcPid, Bucket),
    %% `lists:any/2' will break out early as soon
    %% as something returns `true'
    not lists:any(AnyPred, Keys);
bucket_empty_handle_list_keys(_RiakcPid, _Bucket, _Error) ->
    false.

-spec bucket_empty_any_pred(RiakcPid :: pid(), Bucket :: binary()) ->
    fun((Key :: binary()) -> boolean()).
bucket_empty_any_pred(RiakcPid, Bucket) ->
    fun (Key) ->
            key_exists(RiakcPid, Bucket, Key)
    end.

%% @private
%% `Bucket' should be the raw bucket name,
%% we'll take care of calling `to_bucket_name'
-spec key_exists(pid(), binary(), binary()) -> boolean().
key_exists(RiakcPid, Bucket, Key) ->
    key_exists_handle_get_manifests(
      get_manifests(RiakcPid, Bucket, Key, ?LFS_DEFAULT_OBJECT_VERSION)).

%% @private
-spec key_exists_handle_get_manifests({ok, riakc_obj:riakc_obj(), list()} |
                                      {error, term()}) ->
    boolean().
key_exists_handle_get_manifests({ok, _Object, Manifests}) ->
    active_to_bool(active_manifest_from_response({ok, Manifests}));
key_exists_handle_get_manifests(Error) ->
    active_to_bool(active_manifest_from_response(Error)).

%% @private
-spec active_to_bool({ok, term()} | {error, notfound}) -> boolean().
active_to_bool({ok, _Active}) ->
    true;
active_to_bool({error, notfound}) ->
    false.

-spec active_manifest_from_response({ok, orddict:orddict()} |
                                    {error, notfound}) ->
    {ok, term()} | {error, notfound}.
active_manifest_from_response({ok, Manifests}) ->
    handle_active_manifests(rcs_common_manifest_utils:active_manifest(Manifests));
active_manifest_from_response({error, notfound}=NotFound) ->
    NotFound.

%% @private
-spec handle_active_manifests({ok, term()} |
                              {error, no_active_manifest}) ->
    {ok, term()} | {error, notfound}.
handle_active_manifests({ok, _Active}=ActiveReply) ->
    ActiveReply;
handle_active_manifests({error, no_active_manifest}) ->
    {error, notfound}.

%% @doc Determine if a bucket is exists and is available
%% for creation or deletion by the inquiring user.
-spec bucket_available(binary(), fun(), bucket_op(), pid()) -> {true, term()} | {false, atom()}.
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

%% @doc Perform an operation on a bucket.
-spec do_bucket_op(binary(), binary(), bucket_op_options(), bucket_op()) ->
                          ok | {error, term()}.
do_bucket_op(<<"riak-cs">>, _OwnerId, _Opts, _BucketOp) ->
    {error, access_denied};
do_bucket_op(Bucket, OwnerId, Opts, BucketOp) ->
    case riak_connection() of
        {ok, RiakPid} ->
            %% Buckets operations can only be completed if the bucket exists
            %% and the requesting party owns the bucket.
            try
                case bucket_available(Bucket, OwnerId, BucketOp, RiakPid) of
                    {true, BucketObj} ->
                        Value = case BucketOp of
                                    create ->            OwnerId;
                                    update_acl ->        OwnerId;
                                    update_policy ->     OwnerId;
                                    delete_policy ->     OwnerId;
                                    update_versioning -> OwnerId;
                                    delete ->            ?FREE_BUCKET_MARKER
                                end,
                        put_bucket(BucketObj, Value, Opts, RiakPid);
                    {false, Reason1} ->
                        {error, Reason1}
                end
            catch T:E:ST ->
                    logger:error("Error on updating bucket ~s: ~p. Stacktrace: ~p",
                                 [Bucket, {T, E}, ST]),
                    {error, {T, E}}
            after
                close_riak_connection(RiakPid)
            end;
        {error, _} = Else ->
            Else
    end.

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
            case conn_info_from_bag(BagId, application:get_env(stanchion, bags)) of
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
    stanchion_stats:update([riakc, get_user_by_index], TAT),
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

%% @doc Return a list of keys for a bucket along
%% with their associated values
-spec get_keys_and_values(binary()) -> {ok, [{binary(), binary()}]} | {error, term()}.
get_keys_and_values(BucketName) ->
    case riak_connection() of
        {ok, RiakPid} ->
            Res =
                case list_keys(BucketName, RiakPid) of
                    {ok, Keys} ->
                        KeyValuePairs =
                            [{Key, get_value(BucketName, Key, RiakPid)}
                             || Key <- Keys],
                        {ok, KeyValuePairs};
                    {error, Reason1} ->
                        {error, Reason1}
                end,
            close_riak_connection(RiakPid),
            Res;
        {error, _} = Else ->
            Else
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

%% @doc Extract the value from a Riak object.
-spec get_value(binary(), binary(), pid()) ->
                       binary().
get_value(BucketName, Key, RiakPid) ->
    {Res, TAT} = ?TURNAROUND_TIME(riakc_pb_socket:get(RiakPid, BucketName, Key)),
    stanchion_stats:update([riakc, get_manifest], TAT),
    case Res of
    %% case get_object(BucketName, Key, RiakPid) of
        {ok, RiakObj} ->
            riakc_obj:get_value(RiakObj);
        {error, Reason} ->
            logger:warning("Failed to retrieve value for ~p. Reason: ~p", [Key, Reason]),
            <<"unknown">>
    end.

%% @doc Save information about a user
-spec save_user(moss_user(), pid()) -> ok.
save_user(User, RiakPid) ->
    Indexes = [{?EMAIL_INDEX, User?MOSS_USER.email},
               {?ID_INDEX, User?MOSS_USER.canonical_id}],
    Meta = dict:store(?MD_INDEX, Indexes, dict:new()),
    Obj = riakc_obj:new(?USER_BUCKET, iolist_to_binary(User?MOSS_USER.key_id), term_to_binary(User)),
    UserObj = riakc_obj:update_metadata(Obj, Meta),
    %% @TODO Error handling
    {Res, TAT} = ?TURNAROUND_TIME(riakc_pb_socket:put(RiakPid, UserObj)),
    stanchion_stats:update([riakc, put_cs_user], TAT),
    Res.

%% @doc Save information about a Riak CS user
-spec save_user(boolean(), rcs_user(), riakc_obj:riakc_obj(), pid()) ->

                       ok | {error, term()}.
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

%% @doc Retrieve a Riak CS user's information based on their id string.
-spec get_user('undefined' | list(), pid()) -> {ok, {rcs_user(), riakc_obj:riakc_obj()}} | {error, term()}.
get_user(undefined, _RiakPid) ->
    {error, no_user_key};
get_user(KeyId, RiakPid) ->
    %% Check for and resolve siblings to get a
    %% coherent view of the bucket ownership.
    BinKey = list_to_binary(KeyId),
    case fetch_user(BinKey, RiakPid) of
        {ok, {Obj, KeepDeletedBuckets}} ->
            from_riakc_obj(Obj, KeepDeletedBuckets);
        Error ->
            Error
    end.

from_riakc_obj(Obj, KeepDeletedBuckets) ->
    case riakc_obj:value_count(Obj) of
        1 ->
            User = binary_to_term(riakc_obj:get_value(Obj)),
            {ok, {User, Obj}};
        0 ->
            {error, no_value};
        _ ->
            Values = [binary_to_term(Value) ||
                         Value <- riakc_obj:get_values(Obj),
                         Value /= <<>>  % tombstone
                     ],
            User = hd(Values),
            Buckets = resolve_buckets(Values, [], KeepDeletedBuckets),
            {ok, {User?RCS_USER{buckets=Buckets}, Obj}}
    end.

%% @doc Perform an initial read attempt with R=PR=N.
%% If the initial read fails retry using
%% R=quorum and PR=1, but indicate that bucket deletion
%% indicators should not be cleaned up.
-spec fetch_user(binary(), pid()) ->
                        {ok, {term(), boolean()}} | {error, term()}.
fetch_user(Key, RiakPid) ->
    StrongOptions = [{r, all}, {pr, all}, {notfound_ok, false}],
    {Res0, TAT0} = ?TURNAROUND_TIME(riakc_pb_socket:get(RiakPid, ?USER_BUCKET, Key, StrongOptions)),
    stanchion_stats:update([riakc, get_cs_user_strong], TAT0),
    case Res0 of
        {ok, Obj} ->
            {ok, {Obj, true}};
        {error, _} ->
            weak_fetch_user(Key, RiakPid)
    end.

weak_fetch_user(Key, RiakPid) ->
    WeakOptions = [{r, quorum}, {pr, one}, {notfound_ok, false}],
    {Res, TAT} = ?TURNAROUND_TIME(riakc_pb_socket:get(RiakPid, ?USER_BUCKET, Key, WeakOptions)),
    stanchion_stats:update([riakc, get_user], TAT),
    case Res of
        {ok, Obj} ->
            {ok, {Obj, false}};
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Resolve the set of buckets for a user when
%% siblings are encountered on a read of a user record.
-spec resolve_buckets([rcs_user()], [cs_bucket()], boolean()) ->
                             [cs_bucket()].
resolve_buckets([], Buckets, true) ->
    lists:sort(fun bucket_sorter/2, Buckets);
resolve_buckets([], Buckets, false) ->
    lists:sort(fun bucket_sorter/2, [Bucket || Bucket <- Buckets, not cleanup_bucket(Bucket)]);
resolve_buckets([HeadUserRec | RestUserRecs], Buckets, _KeepDeleted) ->
    HeadBuckets = HeadUserRec?RCS_USER.buckets,
    UpdBuckets = lists:foldl(fun bucket_resolver/2, Buckets, HeadBuckets),
    resolve_buckets(RestUserRecs, UpdBuckets, _KeepDeleted).

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

%% @doc Return true if the last action for the bucket
%% is deleted and the action occurred over 24 hours ago.
-spec cleanup_bucket(cs_bucket()) -> boolean().
cleanup_bucket(?RCS_BUCKET{last_action=created}) ->
    false;
cleanup_bucket(?RCS_BUCKET{last_action=deleted,
                            modification_time=ModTime}) ->
    timer:now_diff(os:timestamp(), ModTime) > 86400.

%% @doc Ordering function for sorting a list of bucket records
%% according to bucket name.
-spec bucket_sorter(cs_bucket(), cs_bucket()) -> boolean().
bucket_sorter(?RCS_BUCKET{name=Bucket1},
              ?RCS_BUCKET{name=Bucket2}) ->
    Bucket1 =< Bucket2.

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
