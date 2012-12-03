%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc riak_cs utility functions

-module(riak_cs_utils).

%% Public API
-export([anonymous_user_creation/0,
         binary_to_hexlist/1,
         check_bucket_exists/2,
         close_riak_connection/1,
         close_riak_connection/2,
         create_bucket/5,
         create_user/2,
         delete_bucket/4,
         delete_object/3,
         from_bucket_name/1,
         get_admin_creds/0,
         get_buckets/1,
         get_env/3,
         get_keys_and_manifests/3,
         has_tombstone/1,
         is_admin/1,
         map_keys_and_manifests/3,
         reduce_keys_and_manifests/2,
         get_object/3,
         get_manifests/3,
         manifests_from_riak_object/1,
         active_manifest_from_response/1,
         get_user/2,
         get_user_by_index/3,
         get_user_index/3,
         json_pp_print/1,
         list_keys/2,
         pow/2,
         pow/3,
         put_object/5,
         put_with_no_meta/2,
         put_with_no_meta/3,
         riak_connection/0,
         riak_connection/1,
         save_user/3,
         set_bucket_acl/5,
         set_object_acl/5,
         timestamp/1,
         timestamp_to_seconds/1,
         to_bucket_name/2,
         update_key_secret/1,
         get_cluster_id/1,
         proxy_get_active/0]).

-include("riak_cs.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-ifdef(TEST).
-compile(export_all).
-endif.

-define(OBJECT_BUCKET_PREFIX, <<"0o:">>).       % Version # = 0
-define(BLOCK_BUCKET_PREFIX, <<"0b:">>).        % Version # = 0

%% Definitions for json_pp_print, from riak_core's json_pp.erl
-define(SPACE, 32).
-define(is_quote(C), (C == $\") orelse (C == $\')).
-define(is_indent(C), (C == 91) orelse (C == 123)). % [, {
-define(is_undent(C), (C == 93) orelse (C == 125)). % ], }

-type xmlElement() :: #xmlElement{}.

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Return the value of the `anonymous_user_creation' application
%% environment variable.
-spec anonymous_user_creation() -> boolean().
anonymous_user_creation() ->
    EnvResponse = application:get_env(riak_cs, anonymous_user_creation),
    handle_env_response(EnvResponse, false).

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

%% @doc Release a protobufs connection from the specified
%% connection pool.
-spec close_riak_connection(pid()) -> ok.
close_riak_connection(Pid) ->
    close_riak_connection(request_pool, Pid).

%% @doc Release a protobufs connection from the specified
%% connection pool.
-spec close_riak_connection(atom(), pid()) -> ok.
close_riak_connection(Pool, Pid) ->
    poolboy:checkin(Pool, Pid).

%% @doc Create a bucket in the global namespace or return
%% an error if it already exists.
-spec create_bucket(rcs_user(), term(), binary(), acl(), pid()) ->
                           ok |
                           {error, term()}.
create_bucket(User, UserObj, Bucket, ACL, RiakPid) ->
    serialized_bucket_op(Bucket,
                         ACL,
                         User,
                         UserObj,
                         create,
                         bucket_create,
                         RiakPid).

%% @doc Create a new Riak CS user
-spec create_user(string(), string()) -> {ok, rcs_user()} | {error, term()}.
create_user(Name, Email) ->
    %% Validate the email address
    case validate_email(Email) of
        ok ->
            {StanchionIp, StanchionPort, StanchionSSL} =
                stanchion_data(),
            User = user_record(Name, Email),
            case get_admin_creds() of
                {ok, AdminCreds} ->
                    %% Generate the user JSON document
                    UserDoc = user_json(User),

                    %% Make a call to the user request
                    %% serialization service.
                    CreateResult =
                        velvet:create_user(StanchionIp,
                                           StanchionPort,
                                           "application/json",
                                           UserDoc,
                                           [{ssl, StanchionSSL},
                                            {auth_creds, AdminCreds}]),
                    case CreateResult of
                        ok ->
                            {ok, User};
                        {error, {error_status, _, _, ErrorDoc}} ->
                            ErrorCode = xml_error_code(ErrorDoc),
                            {error, riak_cs_s3_response:error_code_to_atom(ErrorCode)};
                        {error, _} ->
                            CreateResult
                    end;
                {error, _Reason1}=Error1 ->
                    Error1
            end;
        {error, _Reason}=Error ->
            Error
    end.

%% @doc Delete a bucket
-spec delete_bucket(rcs_user(), riakc_obj:riakc_obj(), binary(), pid()) ->
                           ok |
                           {error, term()}.
delete_bucket(User, UserObj, Bucket, RiakPid) ->
    CurrentBuckets = get_buckets(User),

    %% Buckets can only be deleted if they exist
    case bucket_exists(CurrentBuckets, binary_to_list(Bucket)) of
        true ->
            case bucket_empty(Bucket, RiakPid) of
                true ->
                    AttemptDelete = true,
                    LocalError = ok;
                false ->
                    AttemptDelete = false,
                    LocalError = {error, bucket_not_empty}
            end;
        false ->
            AttemptDelete = true,
            LocalError = ok
    end,
    case AttemptDelete of
        true ->
            serialized_bucket_op(Bucket,
                                 ?ACL{},
                                 User,
                                 UserObj,
                                 delete,
                                 bucket_delete,
                                 RiakPid);
        false ->
            LocalError
    end.

%% @doc Mark all active manifests as pending_delete.
%% If successful, returns a list of the UUIDs that were marked for
%% Garbage collection. Otherwise returns an error. Note,
%% {error, notfound} counts as success in this case,
%% with the list of UUIDs being [].
-spec delete_object(binary(), binary(), pid()) ->
    {ok, [binary()]} | {error, term()}.
delete_object(Bucket, Key, RiakcPid) ->
    StartTime = os:timestamp(),
    maybe_gc_active_manifests(get_manifests(RiakcPid, Bucket, Key),
                         StartTime, RiakcPid).

%% @private
maybe_gc_active_manifests({ok, RiakObject, Manifests}, StartTime, RiakcPid) ->
    R = riak_cs_gc:gc_active_manifests(Manifests, RiakObject, RiakcPid),
    ok = riak_cs_stats:update_with_start(object_delete, StartTime),
    R;
maybe_gc_active_manifests({error, notfound}, _StartTime, _RiakcPid) ->
    {ok, []};
maybe_gc_active_manifests({error, _Reason}=Error, _StartTime, _RiakcPid) ->
    Error.


%% Get the root bucket name for either a Riak CS object
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

%% @doc Return a user's buckets.
-spec get_buckets(rcs_user()) -> [cs_bucket()].
get_buckets(?RCS_USER{buckets=Buckets}) ->
    [Bucket || Bucket <- Buckets, Bucket?RCS_BUCKET.last_action /= deleted].

%% @doc Return `stanchion' configuration data.
-spec stanchion_data() -> {string(), pos_integer(), boolean()}.
stanchion_data() ->
    case application:get_env(riak_cs, stanchion_ip) of
        {ok, IP} ->
            ok;
        undefined ->
            _ = lager:warning("No IP address or host name for stanchion access defined. Using default."),
            IP = ?DEFAULT_STANCHION_IP
    end,
    case application:get_env(riak_cs, stanchion_port) of
        {ok, Port} ->
            ok;
        undefined ->
            _ = lager:warning("No port for stanchion access defined. Using default."),
            Port = ?DEFAULT_STANCHION_PORT
    end,
    case application:get_env(riak_cs, stanchion_ssl) of
        {ok, SSL} ->
            ok;
        undefined ->
            _ = lager:warning("No ssl flag for stanchion access defined. Using default."),
            SSL = ?DEFAULT_STANCHION_SSL
    end,
    {IP, Port, SSL}.

%% @doc Get an application environment variable or return a default term.
-spec get_env(atom(), atom(), term()) -> term().
get_env(App, Key, Default) ->
    case application:get_env(App, Key) of
        {ok, Value} ->
            Value;
        _ ->
            Default
    end.

%% @doc Return a list of keys for a bucket along
%% with their associated objects.
-spec get_keys_and_manifests(binary(), binary(), pid()) -> {ok, [lfs_manifest()]} | {error, term()}.
get_keys_and_manifests(BucketName, Prefix, RiakPid) ->
    ManifestBucket = to_bucket_name(objects, BucketName),
    case active_manifests(ManifestBucket, Prefix, RiakPid) of
        {ok, KeyManifests} ->
            {ok, lists:keysort(1, KeyManifests)};
        {error, Reason} ->
            {error, Reason}
    end.

active_manifests(ManifestBucket, Prefix, RiakPid) ->
    Input = case Prefix of
                <<>> -> ManifestBucket;
                _ ->
                    %% using filtered listkeys instead of 2i here
                    %% because 2i seems no more than a 10% performance
                    %% increase, and it requires extra finagling to
                    %% deal with its range query being inclusive
                    %% instead of exclusive
                    {ManifestBucket, [[<<"starts_with">>, Prefix]]}
            end,
    Query = [{map, {modfun, riak_cs_utils, map_keys_and_manifests},
              undefined, false},
             {reduce, {modfun, riak_cs_utils, reduce_keys_and_manifests},
              undefined, true}],
    {ok, ReqId} = riakc_pb_socket:mapred_stream(RiakPid, Input, Query, self()),
    receive_keys_and_manifests(ReqId, []).

%% Stream keys to avoid riakc_pb_socket:wait_for_mapred/2's use of
%% orddict:append_list/3, because it's mega-inefficient, to the point
%% of unusability for large buckets (memory allocation exit of the
%% erlang vm for a 100k-object bucket observed in testing).
receive_keys_and_manifests(ReqId, Acc) ->
    receive
        {ReqId, done} ->
            {ok, Acc};
        {ReqId, {mapred, _Phase, Res}} ->
            %% The use of ++ here shouldn't be *too* bad, especially
            %% since Res is always a single list element in Riak 1.1
            receive_keys_and_manifests(ReqId, Res++Acc);
        {ReqId, {error, Reason}} ->
            {error, Reason}
    after 60000 ->
            %% timing out after complete inactivity for 1min
            %% TODO: would shorter be better? should there be an
            %% overall timeout?
            {error, timeout}
    end.

%% MapReduce function, runs on the Riak nodes, should therefore use
%% riak_object, not riakc_obj.
map_keys_and_manifests({error, notfound}, _, _) ->
    [];
map_keys_and_manifests(Object, _, _) ->
    try
        AllManifests = [ binary_to_term(V)
                         || V <- riak_object:get_values(Object) ],
        Upgraded = riak_cs_manifest_utils:upgrade_wrapped_manifests(AllManifests),
        Resolved = riak_cs_manifest_resolution:resolve(Upgraded),
        case riak_cs_manifest_utils:active_manifest(Resolved) of
            {ok, Manifest} ->
                [{riak_object:key(Object), {ok, Manifest}}];
            _ ->
                []
        end
    catch Type:Reason ->
            _ = lager:warning("Riak CS object list map failed: ~p:~p",
                              [Type, Reason]),
            []
    end.

%% Pipe all the bucket listing results through a passthrough reduce
%% phase.  This is just a temporary kludge until the sink backpressure
%% work is done.
reduce_keys_and_manifests(Acc, _) ->
    Acc.

%% @doc Return the credentials of the admin user
-spec get_admin_creds() -> {ok, {string(), string()}} | {error, term()}.
get_admin_creds() ->
    KeyEnvResult = application:get_env(riak_cs, admin_key),
    SecretEnvResult = application:get_env(riak_cs, admin_secret),
    admin_creds_response(
      handle_env_response(KeyEnvResult, undefined),
      handle_env_response(SecretEnvResult, undefined)).

%% @doc Get an object from Riak
-spec get_object(binary(), binary(), pid()) ->
                        {ok, riakc_obj:riakc_obj()} | {error, term()}.
get_object(BucketName, Key, RiakPid) ->
    riakc_pb_socket:get(RiakPid, BucketName, Key).

%% internal fun to retrieve the riak object
%% at a bucket/key
-spec get_manifests_raw(pid(), binary(), binary()) ->
    {ok, riakc_obj:riakc_obj()} | {error, term()}.
get_manifests_raw(RiakcPid, Bucket, Key) ->
    ManifestBucket = to_bucket_name(objects, Bucket),
    riakc_pb_socket:get(RiakcPid, ManifestBucket, Key).

%% @doc
-spec get_manifests(pid(), binary(), binary()) ->
    {ok, term(), term()} | {error, term()}.
get_manifests(RiakcPid, Bucket, Key) ->
    case get_manifests_raw(RiakcPid, Bucket, Key) of
        {ok, Object} ->
            Manifests = manifests_from_riak_object(Object),
            {ok, Object, Manifests};
        {error, _Reason}=Error ->
            Error
    end.

-spec manifests_from_riak_object(riakc_obj:riak_object()) -> orddict:orddict().
manifests_from_riak_object(RiakObject) ->
    DecodedSiblings = [binary_to_term(V) ||
                          {_, V}=Content <- riakc_obj:get_contents(RiakObject),
                          not has_tombstone(Content)],

    %% Upgrade the manifests to be the latest erlang
    %% record version
    Upgraded = riak_cs_manifest_utils:upgrade_wrapped_manifests(DecodedSiblings),

    %% resolve the siblings
    Resolved = riak_cs_manifest_resolution:resolve(Upgraded),

    %% prune old scheduled_delete manifests
    riak_cs_manifest_utils:prune(Resolved).

-spec active_manifest_from_response({ok, orddict:orddict()} |
                                    {error, notfound}) ->
    {ok, lfs_manifest()} | {error, notfound}.
active_manifest_from_response({ok, Manifests}) ->
    handle_active_manifests(riak_cs_manifest_utils:active_manifest(Manifests));
active_manifest_from_response({error, notfound}=NotFound) ->
    NotFound.

%% @private
-spec handle_active_manifests({ok, lfs_manifest()} |
                              {error, no_active_manifest}) ->
    {ok, lfs_manifest()} | {error, notfound}.
handle_active_manifests({ok, _Active}=ActiveReply) ->
    ActiveReply;
handle_active_manifests({error, no_active_manifest}) ->
    {error, notfound}.

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
            case riakc_obj:value_count(Obj) of
                1 ->
                    Value = binary_to_term(riakc_obj:get_value(Obj)),
                    User = update_user_record(Value),
                    {ok, {User, Obj}};
                0 ->
                    {error, no_value};
                _ ->
                    Values = [binary_to_term(Value) ||
                                 Value <- riakc_obj:get_values(Obj)],
                    User = update_user_record(hd(Values)),
                    Buckets = resolve_buckets(Values, [], KeepDeletedBuckets),
                    {ok, {User?RCS_USER{buckets=Buckets}, Obj}}
            end;
        Error ->
            Error
    end.

%% @doc Retrieve a Riak CS user's information based on their
%% canonical id string.
%% @TODO May want to use mapreduce job for this.
-spec get_user_by_index(binary(), binary(), pid()) ->
                               {ok, {rcs_user(), term()}} |
                               {error, term()}.
get_user_by_index(Index, Value, RiakPid) ->
    case get_user_index(Index, Value, RiakPid) of
        {ok, KeyId} ->
            get_user(KeyId, RiakPid);
        {error, _}=Error1 ->
            Error1
    end.

%% @doc Query `Index' for `Value' in the users bucket.
-spec get_user_index(binary(), binary(), pid()) -> {ok, string()} | {error, term()}.
get_user_index(Index, Value, RiakPid) ->
    case riakc_pb_socket:get_index(RiakPid, ?USER_BUCKET, Index, Value) of
        {ok, []} ->
            {error, notfound};
        {ok, [Key | _]} ->
            {ok, binary_to_list(Key)};
        {error, Reason}=Error ->
            _ = lager:warning("Error occurred trying to query ~p in user index ~p. Reason: ~p", [Value,
                                                                                                 Index,
                                                                                                 Reason]),
            Error
    end.

%% @doc Determine if a set of contents of a riak object has a tombstone.
-spec has_tombstone({dict(), binary()}) -> boolean().
has_tombstone({_, <<>>}) ->
    true;
has_tombstone({MD, _V}) ->
    dict:is_key(<<"X-Riak-Deleted">>, MD) =:= true.

%% @doc Determine if the specified user account is a system admin.
-spec is_admin(rcs_user()) -> boolean().
is_admin(User) ->
    is_admin(User, get_admin_creds()).

%% @doc Pretty-print a JSON string ... from riak_core's json_pp.erl
json_pp_print(Str) when is_list(Str) ->
    json_pp_print(Str, 0, undefined, []).

json_pp_print([$\\, C| Rest], I, C, Acc) -> % in quote
    json_pp_print(Rest, I, C, [C, $\\| Acc]);
json_pp_print([C| Rest], I, undefined, Acc) when ?is_quote(C) ->
    json_pp_print(Rest, I, C, [C| Acc]);
json_pp_print([C| Rest], I, C, Acc) -> % in quote
    json_pp_print(Rest, I, undefined, [C| Acc]);
json_pp_print([C| Rest], I, undefined, Acc) when ?is_indent(C) ->
    json_pp_print(Rest, I+1, undefined, [json_pp_indent(I+1), $\n, C| Acc]);
json_pp_print([C| Rest], I, undefined, Acc) when ?is_undent(C) ->
    json_pp_print(Rest, I-1, undefined, [C, json_pp_indent(I-1), $\n| Acc]);
json_pp_print([$,| Rest], I, undefined, Acc) ->
    json_pp_print(Rest, I, undefined, [json_pp_indent(I), $\n, $,| Acc]);
json_pp_print([$:| Rest], I, undefined, Acc) ->
    json_pp_print(Rest, I, undefined, [?SPACE, $:| Acc]);
json_pp_print([C|Rest], I, Q, Acc) ->
    json_pp_print(Rest, I, Q, [C| Acc]);
json_pp_print([], _I, _Q, Acc) -> % done
    lists:reverse(Acc).

json_pp_indent(I) -> lists:duplicate(I*4, ?SPACE).

%% @doc List the keys from a bucket
-spec list_keys(binary(), pid()) -> {ok, [binary()]} | {error, term()}.
list_keys(BucketName, RiakPid) ->
    case riakc_pb_socket:list_keys(RiakPid, BucketName) of
        {ok, Keys} ->
            %% TODO:
            %% This is a naive implementation,
            %% the longer-term solution is likely
            %% going to involve 2i and merging the
            %% results from each of the vnodes.
            {ok, lists:sort(Keys)};
        {error, _}=Error ->
            Error
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

%% @doc Store an object in Riak
-spec put_object(binary(), undefined | binary(), binary(), [term()], pid()) -> ok | {error, term()}.
put_object(BucketName, undefined, Value, Metadata, _RiakPid) ->
    error_logger:warning_msg("Attempt to put object into ~p with undefined key "
                             "and value ~P and dict ~p\n",
                             [BucketName, Value, 30, Metadata]),
    {error, bad_key};
put_object(BucketName, Key, Value, Metadata, RiakPid) ->
    RiakObject = riakc_obj:new(BucketName, Key, Value),
    NewObj = riakc_obj:update_metadata(RiakObject, Metadata),
    riakc_pb_socket:put(RiakPid, NewObj).

put_with_no_meta(RiakcPid, RiakcObj) ->
    put_with_no_meta(RiakcPid, RiakcObj, []).
%% @doc Put an object in Riak with empty
%% metadata. This is likely used when because
%% you want to avoid manually setting the metadata
%% to an empty dict. You'd want to do this because
%% if the previous object had metadata siblings,
%% not explicitly setting the metadata will
%% cause a siblings exception to be raised.
-spec put_with_no_meta(pid(), riakc_obj:riakc_obj(), term()) ->
    ok | {ok, riakc_obj:riak_object()} | {ok, binary()} | {error, term()}.
put_with_no_meta(RiakcPid, RiakcObject, Options) ->
    WithMeta = riakc_obj:update_metadata(RiakcObject, dict:new()),
    riakc_pb_socket:put(RiakcPid, WithMeta, Options).

%% @doc Get a protobufs connection to the riak cluster
%% from the default connection pool.
-spec riak_connection() -> {ok, pid()} | {error, term()}.
riak_connection() ->
    riak_connection(request_pool).

%% @doc Get a protobufs connection to the riak cluster
%% from the specified connection pool.
-spec riak_connection(atom()) -> {ok, pid()} | {error, term()}.
riak_connection(Pool) ->
    case catch poolboy:checkout(Pool, false) of
        full ->
            {error, all_workers_busy};
        {'EXIT', _Error} ->
            {error, poolboy_error};
        Worker ->
            {ok, Worker}
    end.

%% @doc Save information about a Riak CS user
-spec save_user(rcs_user(), riakc_obj:riakc_obj(), pid()) -> ok.
save_user(User, UserObj, RiakPid) ->
    %% Metadata is currently never updated so if there
    %% are siblings all copies should be the same
    [MD | _] = riakc_obj:get_metadatas(UserObj),
    UpdUserObj = riakc_obj:update_metadata(
                   riakc_obj:update_value(UserObj, term_to_binary(User)),
                   MD),
    %% @TODO Error handling
    riakc_pb_socket:put(RiakPid, UpdUserObj).

%% @doc Set the ACL for a bucket. Existing ACLs are only
%% replaced, they cannot be updated.
-spec set_bucket_acl(rcs_user(), riakc_obj:riakc_obj(), binary(), acl(), pid()) -> ok | {error, term()}.
set_bucket_acl(User, UserObj, Bucket, ACL, RiakPid) ->
    serialized_bucket_op(Bucket,
                         ACL,
                         User,
                         UserObj,
                         update_acl,
                         bucket_put_acl,
                         RiakPid).

%% @doc Set the ACL for an object. Existing ACLs are only
%% replaced, they cannot be updated.
-spec set_object_acl(binary(), binary(), lfs_manifest(), acl(), pid()) ->
            ok | {error, term()}.
set_object_acl(Bucket, Key, Manifest, Acl, RiakPid) ->
    StartTime = os:timestamp(),
    {ok, ManiPid} = riak_cs_manifest_fsm:start_link(Bucket, Key, RiakPid),
    _ActiveMfst = riak_cs_manifest_fsm:get_active_manifest(ManiPid),
    UpdManifest = Manifest?MANIFEST{acl=Acl},
    Res = riak_cs_manifest_fsm:update_manifest_with_confirmation(ManiPid, UpdManifest),
    riak_cs_manifest_fsm:stop(ManiPid),
    if Res == ok ->
            ok = riak_cs_stats:update_with_start(object_put_acl, StartTime);
       true ->
            ok
    end,
    Res.

%% @doc Generate a key for storing a set of manifests for deletion.
-spec timestamp(erlang:timestamp()) -> non_neg_integer().
timestamp({MegaSecs, Secs, _MicroSecs}) ->
    (MegaSecs * 1000000) + Secs.

-spec timestamp_to_seconds(erlang:timestamp()) -> float().
timestamp_to_seconds({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000) + Secs + (MicroSecs / 1000000).

%% Get the proper bucket name for either the Riak CS object
%% bucket or the data block bucket.
-spec to_bucket_name(objects | blocks, binary()) -> binary().
to_bucket_name(Type, Bucket) ->
    case Type of
        objects ->
            Prefix = ?OBJECT_BUCKET_PREFIX;
        blocks ->
            Prefix = ?BLOCK_BUCKET_PREFIX
    end,
    BucketHash = crypto:md5(Bucket),
    <<Prefix/binary, BucketHash/binary>>.


%% @doc Generate a new `key_secret' for a user record.
-spec update_key_secret(rcs_user()) -> rcs_user().
update_key_secret(User=?RCS_USER{email=Email,
                                 key_id=KeyId}) ->
    EmailBin = list_to_binary(Email),
    User?RCS_USER{key_secret=generate_secret(EmailBin, KeyId)}.

%% ===================================================================
%% Internal functions
%% ===================================================================

-spec admin_creds_response(term(), term()) -> {ok, {term(), term()}} |
                                              {error, atom()}.
admin_creds_response(undefined, _) ->
    _ = lager:warning("The admin user's key id"
                      "has not been specified."),
    {error, admin_key_undefined};
admin_creds_response([], _) ->
    _ = lager:warning("The admin user's key id"
                      "has not been specified."),
    {error, admin_key_undefined};
admin_creds_response(_, undefined) ->
    _ = lager:warning("The admin user's secret"
                      "has not been specified."),
    {error, admin_secret_undefined};
admin_creds_response(_, []) ->
    _ = lager:warning("The admin user's secret"
                      "has not been specified."),
    {error, admin_secret_undefined};
admin_creds_response(Key, Secret) ->
    {ok, {Key, Secret}}.

%% @doc Generate a JSON document to use for a bucket
%% ACL request.
-spec bucket_acl_json(acl(), string()) -> string().
bucket_acl_json(ACL, KeyId)  ->
    binary_to_list(
      iolist_to_binary(
        mochijson2:encode({struct, [{<<"requester">>, list_to_binary(KeyId)},
                                    stanchion_acl_utils:acl_to_json_term(ACL)]}))).

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
    key_exists_handle_get_manifests(get_manifests(RiakcPid, Bucket, Key)).

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

%% @doc Check if a bucket exists in the `buckets' bucket and verify
%% that it has an owner assigned. If true return the object;
%% otherwise, return an error.
%%
%% @TODO Rename current `bucket_exists' function to
%% `bucket_exists_for_user' and rename this function
%% `bucket_exists'.
-spec check_bucket_exists(binary(), pid()) ->
                                 {ok, riak_obj:riak_obj()} | {error, term()}.
check_bucket_exists(Bucket, RiakPid) ->
    case riak_cs_utils:get_object(?BUCKETS_BUCKET, Bucket, RiakPid) of
        {ok, Obj} ->
            %% Make sure the bucket has an owner
            [Value | _] = riakc_obj:get_values(Obj),
            case Value of
                <<"0">> ->
                    {error, no_such_bucket};
                _ ->
                    {ok, Obj}
            end;
        {error, _}=Error ->
            Error
    end.

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
                 acl(),
                 string(),
                 {string(), string()},
                 {string(), pos_integer(), boolean()}) -> function().
bucket_fun(create, Bucket, ACL, KeyId, AdminCreds, StanchionData) ->
    {StanchionIp, StanchionPort, StanchionSSL} = StanchionData,
    %% Generate the bucket JSON document
    BucketDoc = bucket_json(Bucket, ACL, KeyId),
    fun() ->
            velvet:create_bucket(StanchionIp,
                                 StanchionPort,
                                 "application/json",
                                 BucketDoc,
                                 [{ssl, StanchionSSL},
                                  {auth_creds, AdminCreds}])
    end;
bucket_fun(update_acl, Bucket, ACL, KeyId, AdminCreds, StanchionData) ->
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
bucket_fun(delete, Bucket, _ACL, KeyId, AdminCreds, StanchionData) ->
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
-spec bucket_json(binary(), acl(), string()) -> string().
bucket_json(Bucket, ACL, KeyId)  ->
    binary_to_list(
      iolist_to_binary(
        mochijson2:encode({struct, [{<<"bucket">>, Bucket},
                                    {<<"requester">>, list_to_binary(KeyId)},
                                    stanchion_acl_utils:acl_to_json_term(ACL)]}))).

%% @doc Return a bucket record for the specified bucket name.
-spec bucket_record(binary(), bucket_operation()) -> cs_bucket().
bucket_record(Name, Operation) ->
    case Operation of
        create ->
            Action = created;
        delete ->
            Action = deleted;
        _ ->
            Action = undefined
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
%% is deleted and the action occurred over 24 hours ago.
-spec cleanup_bucket(cs_bucket()) -> boolean().
cleanup_bucket(?RCS_BUCKET{last_action=created}) ->
    false;
cleanup_bucket(?RCS_BUCKET{last_action=deleted,
                            modification_time=ModTime}) ->
    timer:now_diff(os:timestamp(), ModTime) > 86400.

%% @doc Strip off the user name portion of an email address
-spec display_name(string()) -> string().
display_name(Email) ->
    Index = string:chr(Email, $@),
    string:sub_string(Email, 1, Index-1).

%% @doc Perform an initial read attempt with R=PR=N.
%% If the initial read fails retry using
%% R=quorum and PR=1, but indicate that bucket deletion
%% indicators should not be cleaned up.
-spec fetch_user(binary(), pid()) ->
                        {ok, {term(), boolean()}} | {error, term()}.
fetch_user(Key, RiakPid) ->
    StrongOptions = [{r, all}, {pr, all}, {notfound_ok, false}],
    case riakc_pb_socket:get(RiakPid, ?USER_BUCKET, Key, StrongOptions) of
        {ok, Obj} ->
            {ok, {Obj, true}};
        {error, _} ->
            WeakOptions = [{r, quorum}, {pr, one}, {notfound_ok, false}],
            case riakc_pb_socket:get(RiakPid, ?USER_BUCKET, Key, WeakOptions) of
                {ok, Obj} ->
                    {ok, {Obj, false}};
                {error, Reason} ->
                    {error, Reason}
            end
    end.

%% @doc Generate a new set of access credentials for user.
-spec generate_access_creds(string()) -> {iodata(), iodata()}.
generate_access_creds(UserId) ->
    UserBin = list_to_binary(UserId),
    KeyId = generate_key(UserBin),
    Secret = generate_secret(UserBin, KeyId),
    {KeyId, Secret}.

%% @doc Generate the canonical id for a user.
-spec generate_canonical_id(string(), string()) -> string().
generate_canonical_id(KeyID, Secret) ->
    Bytes = 16,
    Id1 = crypto:md5(KeyID),
    Id2 = crypto:md5(Secret),
    binary_to_hexlist(
      iolist_to_binary(<< Id1:Bytes/binary,
                          Id2:Bytes/binary >>)).

%% @doc Generate an access key for a user
-spec generate_key(binary()) -> [iodata()].
generate_key(UserName) ->
    Ctx = crypto:hmac_init(sha, UserName),
    Ctx1 = crypto:hmac_update(Ctx, druuid:v4()),
    Key = crypto:hmac_final_n(Ctx1, 15),
    string:to_upper(base64url:encode_to_string(Key)).

%% @doc Generate a secret access token for a user
-spec generate_secret(binary(), string()) -> iodata().
generate_secret(UserName, Key) ->
    Bytes = 14,
    Ctx = crypto:hmac_init(sha, UserName),
    Ctx1 = crypto:hmac_update(Ctx, list_to_binary(Key)),
    SecretPart1 = crypto:hmac_final_n(Ctx1, Bytes),
    Ctx2 = crypto:hmac_init(sha, UserName),
    Ctx3 = crypto:hmac_update(Ctx2, druuid:v4()),
    SecretPart2 = crypto:hmac_final_n(Ctx3, Bytes),
    base64url:encode_to_string(
      iolist_to_binary(<< SecretPart1:Bytes/binary,
                          SecretPart2:Bytes/binary >>)).

-spec handle_env_response(undefined | {ok, term()}, term()) -> term().
handle_env_response(undefined, Default) ->
    Default;
handle_env_response({ok, Value}, _) ->
    Value.

%% @doc Determine if the specified user account is a system admin.
-spec is_admin(rcs_user(), {ok, {string(), string()}} |
               {error, term()}) -> boolean().
is_admin(?RCS_USER{key_id=KeyId, key_secret=KeySecret},
         {ok, {KeyId, KeySecret}}) ->
    true;
is_admin(_, _) ->
    false.

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

%% @doc Process the top-level elements of the
-spec process_xml_error([xmlElement()]) -> string().
process_xml_error([]) ->
    [];
process_xml_error([HeadElement | RestElements]) ->
    _ = lager:debug("Element name: ~p", [HeadElement#xmlElement.name]),
    ElementName = HeadElement#xmlElement.name,
    case ElementName of
        'Code' ->
            [Content] = HeadElement#xmlElement.content,
            Content#xmlText.value;
        _ ->
            process_xml_error(RestElements)
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

%% @doc Shared code used when doing a bucket creation or deletion.
-spec serialized_bucket_op(binary(),
                           acl(),
                           rcs_user(),
                           riakc_obj:riakc_obj(),
                           bucket_operation(),
                           atom(),
                           pid()) ->
                                  ok |
                                  {error, term()}.
serialized_bucket_op(Bucket, ACL, User, UserObj, BucketOp, StatName, RiakPid) ->
    StartTime = os:timestamp(),
    case get_admin_creds() of
        {ok, AdminCreds} ->
            BucketFun = bucket_fun(BucketOp,
                                   Bucket,
                                   ACL,
                                   User?RCS_USER.key_id,
                                   AdminCreds,
                                   stanchion_data()),
            %% Make a call to the bucket request
            %% serialization service.
            OpResult = BucketFun(),
            case OpResult of
                ok ->
                    BucketRecord = bucket_record(Bucket, BucketOp),
                    case update_user_buckets(User, BucketRecord) of
                        {ok, ignore} when BucketOp == update_acl ->
                            ok = riak_cs_stats:update_with_start(StatName,
                                                                 StartTime),
                            OpResult;
                        {ok, ignore} ->
                            OpResult;
                        {ok, UpdUser} ->
                            X = save_user(UpdUser, UserObj, RiakPid),
                            ok = riak_cs_stats:update_with_start(StatName,
                                                                 StartTime),
                            X
                    end;
                {error, {error_status, _, _, ErrorDoc}} ->
                    ErrorCode = xml_error_code(ErrorDoc),
                    {error, riak_cs_s3_response:error_code_to_atom(ErrorCode)};
                {error, _} ->
                    OpResult
            end;
        {error, Reason1} ->
            {error, Reason1}
    end.

%% @doc Generate a JSON document to use for a user
%% creation request.
-spec user_json(rcs_user()) -> string().
user_json(User) ->
    ?RCS_USER{name=UserName,
               display_name=DisplayName,
               email=Email,
               key_id=KeyId,
               key_secret=Secret,
               canonical_id=CanonicalId} = User,
    binary_to_list(
      iolist_to_binary(
        mochijson2:encode({struct, [{<<"email">>, list_to_binary(Email)},
                                    {<<"display_name">>, list_to_binary(DisplayName)},
                                    {<<"name">>, list_to_binary(UserName)},
                                    {<<"key_id">>, list_to_binary(KeyId)},
                                    {<<"key_secret">>, list_to_binary(Secret)},
                                    {<<"canonical_id">>, list_to_binary(CanonicalId)}
                                   ]}))).

%% @doc Validate an email address.
-spec validate_email(string()) -> ok | {error, term()}.
validate_email(EmailAddr) ->
    %% @TODO More robust email address validation
    case string:chr(EmailAddr, $@) of
        0 ->
            {error, invalid_email_address};
        _ ->
            ok
    end.

%% @doc Get the value of the `Code' element from
%% and XML document.
-spec xml_error_code(string()) -> string().
xml_error_code(Xml) ->
    {ParsedData, _Rest} = xmerl_scan:string(Xml, []),
    process_xml_error(ParsedData#xmlElement.content).

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

%% @doc Update a user record from a previous version if necessary.
-spec update_user_record(rcs_user()) -> rcs_user().
update_user_record(User=?RCS_USER{}) ->
    User;
update_user_record(User=#moss_user_v1{}) ->
    ?RCS_USER{name=User#moss_user_v1.name,
              display_name=User#moss_user_v1.display_name,
              email=User#moss_user_v1.email,
              key_id=User#moss_user_v1.key_id,
              key_secret=User#moss_user_v1.key_secret,
              canonical_id=User#moss_user_v1.canonical_id,
              buckets=[update_bucket_record(Bucket) ||
                          Bucket <- User#moss_user_v1.buckets]}.

%% @doc Return a user record for the specified user name and
%% email address.
-spec user_record(string(), string()) -> rcs_user().
user_record(Name, Email) ->
    user_record(Name, Email, []).

%% @doc Return a user record for the specified user name and
%% email address.
-spec user_record(string(), string(), [cs_bucket()]) -> rcs_user().
user_record(Name, Email, Buckets) ->
    {KeyId, Secret} = generate_access_creds(Email),
    CanonicalId = generate_canonical_id(KeyId, Secret),
    DisplayName = display_name(Email),
    ?RCS_USER{name=Name,
               display_name=DisplayName,
               email=Email,
               key_id=KeyId,
               key_secret=Secret,
               canonical_id=CanonicalId,
               buckets=Buckets}.

%% doc Return the current cluster ID. Used for repl
%% After obtaining the clusterid the first time,
%% store the value in app:set_env
-spec get_cluster_id(pid()) -> binary().
get_cluster_id(Pid) ->
    case application:get_env(riak_cs, cluster_id) of
        {ok, ClusterID} ->
            ClusterID;
        undefined ->
            Timeout = case application:get_env(riak_cs,cluster_id_timeout) of
                          {ok, Value} -> Value;
                          undefined   -> ?DEFAULT_CLUSTER_ID_TIMEOUT
                      end,
            case riak_repl_pb_api:get_clusterid(Pid, Timeout) of
                {ok, ClusterID} ->
                    application:set_env(riak_cs, cluster_id, ClusterID),
                    ClusterID;
                _ ->
                    lager:debug("Unable to obtain cluster ID"),
                    undefined
            end
    end.

%% doc Check app.config to see if repl proxy_get is enabled
%% Defaults to false.
proxy_get_active() ->
    case application:get_env(riak_cs, proxy_get) of
        {ok, enabled} ->
            true;
        {ok, disabled} ->
            false;
        {ok, _} ->
            lager:warning("proxy_get value in app.config is invalid"),
            false;
        undefined -> false
    end.
