%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2011 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc riak_moss utility functions

-module(riak_moss_utils).

%% Public API
-export([binary_to_hexlist/1,
         close_riak_connection/1,
         create_bucket/4,
         create_user/2,
         delete_bucket/3,
         delete_object/2,
         from_bucket_name/1,
         get_admin_creds/0,
         get_buckets/1,
         get_keys_and_manifests/2,
         get_object/2,
         get_object/3,
         get_user/1,
         get_user_by_index/2,
         get_user_index/3,
         list_keys/1,
         list_keys/2,
         pow/2,
         pow/3,
         put_object/4,
         riak_connection/0,
         riak_connection/2,
         set_bucket_acl/4,
         to_bucket_name/2]).

-include("riak_moss.hrl").
-include_lib("riakc/include/riakc_obj.hrl").
-include_lib("xmerl/include/xmerl.hrl").

-ifdef(TEST).
-compile(export_all).
-endif.

-define(OBJECT_BUCKET_PREFIX, <<"objects:">>).
-define(BLOCK_BUCKET_PREFIX, <<"blocks:">>).

-type xmlElement() :: #xmlElement{}.

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
-spec create_bucket(moss_user(), term(), binary(), acl_v1()) ->
                           {ok, moss_user()} |
                           {ok, ignore} |
                           {error, term()}.
create_bucket(User, VClock, Bucket, ACL) ->
    serialized_bucket_op(Bucket,
                         ACL,
                         User,
                         VClock,
                         create).

%% @doc Create a new MOSS user
-spec create_user(string(), string()) -> {ok, moss_user()}.
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
                            {error, riak_moss_s3_response:error_code_to_atom(ErrorCode)};
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
-spec delete_bucket(moss_user(), term(), binary()) ->
                           {ok, moss_user()} |
                           {ok, ignore} |
                           {error, term()}.
delete_bucket(User, VClock, Bucket) ->
    CurrentBuckets = get_buckets(User),

    %% Buckets can only be deleted if they exist
    case bucket_exists(CurrentBuckets, Bucket) of
        true ->
            case bucket_empty(Bucket) of
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
                                 VClock,
                                 delete);
        false ->
            LocalError
    end.

%% @doc Delete an object from Riak
-spec delete_object(binary(), binary()) -> ok.
delete_object(BucketName, Key) ->
    case riak_connection() of
        {ok, RiakPid} ->
            Res = riakc_pb_socket:delete(RiakPid, BucketName, Key),
            close_riak_connection(RiakPid),
            Res;
        {error, Reason} ->
            {error, {riak_connect_failed, Reason}}
    end.

%% Get the root bucket name for either a MOSS object
%% bucket or the data block bucket name.
-spec from_bucket_name(binary()) -> binary().
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
-spec get_buckets(moss_user()) -> [moss_bucket()].
get_buckets(?MOSS_USER{buckets=Buckets}) ->
    [Bucket || Bucket <- Buckets, Bucket?MOSS_BUCKET.last_action /= deleted].

%% @doc Return `stanchion' configuration data.
-spec stanchion_data() -> {string(), pos_integer(), boolean()} | {error, term()}.
stanchion_data() ->
    case application:get_env(riak_moss, stanchion_ip) of
        {ok, IP} ->
            ok;
        undefined ->
            lager:warning("No IP address or host name for stanchion access defined. Using default."),
            IP = ?DEFAULT_STANCHION_IP
    end,
    case application:get_env(riak_moss, stanchion_port) of
        {ok, Port} ->
            ok;
        undefined ->
            lager:warning("No port for stanchion access defined. Using default."),
            Port = ?DEFAULT_STANCHION_PORT
    end,
    case application:get_env(riak_moss, stanchion_ssl) of
        {ok, SSL} ->
            ok;
        undefined ->
            lager:warning("No ssl flag for stanchion access defined. Using default."),
            SSL = ?DEFAULT_STANCHION_SSL
    end,
    {IP, Port, SSL}.

%% @doc Return a list of keys for a bucket along
%% with their associated objects.
-spec get_keys_and_manifests(binary(), binary()) -> {ok, [lfs_manifest()]}.
get_keys_and_manifests(BucketName, Prefix) ->
    case riak_connection() of
        {ok, RiakPid} ->
            ManifestBucket = riak_moss_utils:to_bucket_name(objects, BucketName),
            case list_keys(ManifestBucket, RiakPid) of
                {ok, Keys} ->
                    KeyObjPairs =
                        [begin
                             {ok, ManiPid} = riak_moss_manifest_fsm:start_link(BucketName, Key),
                             Manifest = riak_moss_manifest_fsm:get_active_manifest(ManiPid),
                             {Key, Manifest}
                         end
                         || Key <- prefix_filter(Keys, Prefix)],
                    Res = {ok, KeyObjPairs};
                {error, Reason1} ->
                    Res = {error, Reason1}
            end,
            close_riak_connection(RiakPid),
            Res;
        {error, Reason} ->
            {error, Reason}
    end.

-spec prefix_filter(list(), binary()) -> list().
prefix_filter(Keys, <<>>) ->
    Keys;
prefix_filter(Keys, Prefix) ->
    PL = size(Prefix),
    lists:filter(
      fun(<<P:PL/binary,_/binary>>) when P =:= Prefix -> true;
         (_) -> false
      end, Keys).


%% @doc Return the credentials of the admin user
-spec get_admin_creds() -> {ok, {string(), string()}} | {error, term()}.
get_admin_creds() ->
    case application:get_env(riak_moss, admin_key) of
        {ok, []} ->
            lager:warning("The admin user's key id has not been specified."),
            {error, admin_key_undefined};
        {ok, KeyId} ->
            case application:get_env(riak_moss, admin_secret) of
                {ok, []} ->
                    lager:warning("The admin user's secret has not been specified."),
                    {error, admin_secret_undefined};
                {ok, Secret} ->
                    {ok, {KeyId, Secret}};
                undefined ->
                    lager:warning("The admin user's secret is not defined."),
                    {error, admin_secret_undefined}
            end;
        undefined ->
            lager:warning("The admin user's key id is not defined."),
            {error, admin_key_undefined}
    end.

%% @doc Get an object from Riak
-spec get_object(binary(), binary()) ->
                        {ok, binary()} | {error, term()}.
get_object(BucketName, Key) ->
    case riak_connection() of
        {ok, RiakPid} ->
            Res = get_object(BucketName, Key, RiakPid),
            close_riak_connection(RiakPid),
            Res;
        {error, Reason} ->
            {error, {riak_connect_failed, Reason}}
    end.

%% @doc Get an object from Riak
-spec get_object(binary(), binary(), pid()) ->
                        {ok, binary()} | {error, term()}.
get_object(BucketName, Key, RiakPid) ->
    riakc_pb_socket:get(RiakPid, BucketName, Key).

%% @doc Retrieve a MOSS user's information based on their id string.
-spec get_user(string() | undefined) -> {ok, term()} | {error, term()}.
get_user(undefined) ->
    {error, no_user_key};
get_user(KeyID) ->
    case riak_connection() of
        {ok, RiakPid} ->
            Res = get_user(KeyID, RiakPid),
            close_riak_connection(RiakPid),
            Res;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Retrieve a MOSS user's information based on their
%% canonical id string.
%% @TODO May want to use mapreduce job for this.
-spec get_user_by_index(binary(), binary()) -> {ok, {moss_user(), term()}} |
                                               {error, term()}.
get_user_by_index(Index, Value) ->
    case riak_connection() of
        {ok, RiakPid} ->
            case get_user_index(Index, Value, RiakPid) of
                {ok, KeyId} ->
                    Res = get_user(KeyId, RiakPid);
                {error, _}=Error1 ->
                    Res = Error1
            end,
            close_riak_connection(RiakPid),
            Res;
        {error, _}=Error ->
            Error
    end.

%% @doc Query `Index' for `Value' in the users bucket.
-spec get_user_index(binary(), binary(), pid()) -> {ok, binary()} | {error, term()}.
get_user_index(Index, Value, RiakPid) ->
    case riakc_pb_socket:get_index(RiakPid, ?USER_BUCKET, Index, Value) of
        {ok, []} ->
            {error, notfound};
        {ok, [[_, Key]]} ->
            {ok, binary_to_list(Key)};
        {error, Reason}=Error ->
            lager:warning("Error occurred trying to query ~p in user index ~p. Reason: ~p", [Value,
                                                                                             Index,
                                                                                             Reason]),
            Error
    end.

%% @doc List the keys from a bucket
-spec list_keys(binary()) -> {ok, [binary()]}.
list_keys(BucketName) ->
    case riak_connection() of
        {ok, RiakPid} ->
            Res = list_keys(BucketName, RiakPid),
            close_riak_connection(RiakPid),
            Res;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc List the keys from a bucket
-spec list_keys(binary(), pid()) -> {ok, [binary()]}.
list_keys(BucketName, RiakPid) ->
    case riakc_pb_socket:list_keys(RiakPid, BucketName) of
        {ok, Keys} ->
            %% TODO:
            %% This is a naive implementation,
            %% the longer-term solution is likely
            %% going to involve 2i and merging the
            %% results from each of the vnodes.
            {ok, lists:sort(Keys)};
        {error, Reason} ->
            {error, Reason}
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
-spec put_object(binary(), binary(), binary(), [term()]) -> ok.
put_object(BucketName, Key, Value, Metadata) ->
    case riak_connection() of
        {ok, RiakPid} ->
            RiakObject = riakc_obj:new(BucketName, Key, Value),
            NewObj = riakc_obj:update_metadata(RiakObject, Metadata),
            Res = riakc_pb_socket:put(RiakPid, NewObj),
            close_riak_connection(RiakPid),
            Res;
        {error, Reason} ->
            {error, Reason}
    end.

%% @doc Get a protobufs connection to the riak cluster
%% using information from the application environment.
-spec riak_connection() -> {ok, pid()} | {error, term()}.
riak_connection() ->
    case application:get_env(riak_moss, riak_ip) of
        {ok, Host} ->
            ok;
        undefined ->
            Host = "127.0.0.1"
    end,
    case application:get_env(riak_moss, riak_pb_port) of
        {ok, Port} ->
            ok;
        undefined ->
            Port = 8087
    end,
    riak_connection(Host, Port).

%% @doc Get a protobufs connection to the riak cluster.
-spec riak_connection(string(), pos_integer()) -> {ok, pid()} | {error, term()}.
riak_connection(Host, Port) ->
    riakc_pb_socket:start_link(Host, Port).

%% @doc Set the ACL for a bucket. Existing ACLs are only
%% replaced, they cannot be updated.
-spec set_bucket_acl(moss_user(), term(), binary(), acl_v1()) -> ok.
set_bucket_acl(User, VClock, Bucket, ACL) ->
    serialized_bucket_op(Bucket,
                         ACL,
                         User,
                         VClock,
                         update_acl).

%% Get the proper bucket name for either the MOSS object
%% bucket or the data block bucket.
-spec to_bucket_name([objects | blocks], binary()) -> binary().
to_bucket_name(objects, Name) ->
    <<?OBJECT_BUCKET_PREFIX/binary, Name/binary>>;
to_bucket_name(blocks, Name) ->
    <<?BLOCK_BUCKET_PREFIX/binary, Name/binary>>.

%% ===================================================================
%% Internal functions
%% ===================================================================

%% @doc Generate a JSON document to use for a bucket
%% ACL request.
-spec bucket_acl_json(acl_v1(), string()) -> string().
bucket_acl_json(ACL, KeyId)  ->
    binary_to_list(
      iolist_to_binary(
        mochijson2:encode([stanchion_acl_utils:acl_to_json_term(ACL),
                           {struct, [{<<"requester">>, list_to_binary(KeyId)}]}]))).

%% @doc Check if a bucket is empty
-spec bucket_empty(binary()) -> boolean().
bucket_empty(Bucket) ->

    case riak_connection() of
        {ok, RiakPid} ->
            ObjBucket = to_bucket_name(objects, Bucket),
            case list_keys(ObjBucket, RiakPid) of
                {ok, []} ->
                    Res = true;
                _ ->
                    Res = false
            end,
            close_riak_connection(RiakPid),
            Res;
        {error, Reason} ->
            {error, {riak_connect_failed, Reason}}
    end.

%% @doc Check if a bucket exists in a list of the user's buckets.
%% @TODO This will need to change once globally unique buckets
%% are enforced.
-spec bucket_exists([moss_bucket()], string()) -> boolean().
bucket_exists(Buckets, CheckBucket) ->
    SearchResults = [Bucket || Bucket <- Buckets,
                               Bucket?MOSS_BUCKET.name =:= CheckBucket andalso
                                   Bucket?MOSS_BUCKET.last_action =:= created],
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
                 acl_v1(),
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
-spec bucket_json(binary(), acl_v1(), string()) -> string().
bucket_json(Bucket, ACL, KeyId)  ->
    binary_to_list(
      iolist_to_binary(
        mochijson2:encode([{struct, [{<<"bucket">>, Bucket}]},
                           stanchion_acl_utils:acl_to_json_term(ACL),
                           {struct, [{<<"requester">>, list_to_binary(KeyId)}]}]))).


%% @doc Return a bucket record for the specified bucket name.
-spec bucket_record(binary(), bucket_operation()) -> moss_bucket().
bucket_record(Name, Operation) ->
    case Operation of
        create ->
            Action = created;
        delete ->
            Action = deleted;
        _ ->
            Action = undefined
    end,
    ?MOSS_BUCKET{name=Name,
                 last_action=Action,
                 creation_date=riak_moss_wm_utils:iso_8601_datetime(),
                 modification_time=erlang:now()}.

%% @doc Check for and resolve any conflict between
%% a bucket record from a user record sibling and
%% a list of resolved bucket records.
-spec bucket_resolver(moss_bucket(), [moss_bucket()]) -> [moss_bucket()].
bucket_resolver(Bucket, ResolvedBuckets) ->
    case lists:member(Bucket, ResolvedBuckets) of
        true ->
            ResolvedBuckets;
        false ->
            case [RB || RB <- ResolvedBuckets,
                        RB?MOSS_BUCKET.name =:=
                            Bucket?MOSS_BUCKET.name] of
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
-spec bucket_sorter(moss_bucket(), moss_bucket()) -> boolean().
bucket_sorter(?MOSS_BUCKET{name=Bucket1},
              ?MOSS_BUCKET{name=Bucket2}) ->
    Bucket1 =< Bucket2.

%% @doc Return true if the last action for the bucket
%% is deleted and the action occurred over 24 hours ago.
-spec cleanup_bucket(moss_bucket()) -> boolean().
cleanup_bucket(?MOSS_BUCKET{last_action=created}) ->
    false;
cleanup_bucket(?MOSS_BUCKET{last_action=deleted,
                            modification_time=ModTime}) ->
    timer:now_diff(erlang:now(), ModTime) > 86400.

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
-spec generate_access_creds(string()) -> {binary(), binary()}.
generate_access_creds(UserId) ->
    UserBin = list_to_binary(UserId),
    KeyId = generate_key(UserBin),
    Secret = generate_secret(UserBin, KeyId),
    {KeyId, Secret}.

%% @doc Retrieve a MOSS user's information based on their id string.
-spec get_user(string(), pid()) -> {ok, {term(), term()}} | {error, term()}.
get_user(KeyId, RiakPid) ->
    %% @TODO Check for an resolve siblings to get a
    %% coherent view of the bucket ownership.
    BinKey = list_to_binary(KeyId),
    case fetch_user(BinKey, RiakPid) of
        {ok, {Obj, KeepDeletedBuckets}} ->
            case riakc_obj:value_count(Obj) of
                1 ->
                    {ok, {binary_to_term(riakc_obj:get_value(Obj)),
                          riakc_obj:vclock(Obj)}};
                0 ->
                    {error, no_value};
                _ ->
                    Values = [binary_to_term(Value) ||
                                 Value <- riakc_obj:get_values(Obj)],
                    User = hd(Values),
                    Buckets = resolve_buckets(Values, [], KeepDeletedBuckets),
                    {ok, {User?MOSS_USER{buckets=Buckets},
                          riakc_obj:vclock(Obj)}}
            end;
        Error ->
            Error
    end.

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
-spec generate_key(binary()) -> string().
generate_key(UserName) ->
    Ctx = crypto:hmac_init(sha, UserName),
    Ctx1 = crypto:hmac_update(Ctx, druuid:v4()),
    Key = crypto:hmac_final_n(Ctx1, 15),
    string:to_upper(base64url:encode_to_string(Key)).

%% @doc Generate a secret access token for a user
-spec generate_secret(binary(), string()) -> string().
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

%% @doc Determine if an existing bucket from the resolution list
%% should be kept or replaced when a conflict occurs.
-spec keep_existing_bucket(moss_bucket(), moss_bucket()) -> boolean().
keep_existing_bucket(?MOSS_BUCKET{last_action=LastAction1,
                                  modification_time=ModTime1},
                     ?MOSS_BUCKET{last_action=LastAction2,
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
    lager:debug("Element name: ~p", [HeadElement#xmlElement.name]),
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
-spec resolve_buckets([moss_user()], [moss_bucket()], boolean()) ->
                             [moss_bucket()].
resolve_buckets([], Buckets, true) ->
    lists:sort(fun bucket_sorter/2, Buckets);
resolve_buckets([], Buckets, false) ->
    lists:sort(fun bucket_sorter/2, [Bucket || Bucket <- Buckets, not cleanup_bucket(Bucket)]);
resolve_buckets([HeadUserRec | RestUserRecs], Buckets, _KeepDeleted) ->
    HeadBuckets = HeadUserRec?MOSS_USER.buckets,
    UpdBuckets = lists:foldl(fun bucket_resolver/2, Buckets, HeadBuckets),
    resolve_buckets(RestUserRecs, UpdBuckets, _KeepDeleted).

%% @doc Save information about a MOSS user
-spec save_user(moss_user(), term()) -> ok.
save_user(User, VClock) ->
    case riak_connection() of
        {ok, RiakPid} ->
            Res = save_user(User, VClock, RiakPid),
            close_riak_connection(RiakPid),
            Res;
        {error, Reason} ->
            {error, {riak_connect_failed, Reason}}
    end.

%% @doc Save information about a MOSS user
-spec save_user(moss_user(), term(), pid()) -> ok.
save_user(User, VClock, RiakPid) ->
    UserObj0 = riakc_obj:new(?USER_BUCKET,
                             list_to_binary(User?MOSS_USER.key_id),
                             User),
    case VClock of
        undefined ->
            UserObj1 = UserObj0;
        _ ->
            UserObj1 = riakc_obj:set_vclock(UserObj0, VClock)
    end,
    Indexes = [{?EMAIL_INDEX, User?MOSS_USER.email},
               {?ID_INDEX, User?MOSS_USER.canonical_id}],
    Meta = dict:store(?MD_INDEX, Indexes, dict:new()),
    UserObj = riakc_obj:update_metadata(UserObj1, Meta),

    %% @TODO Error handling
    riakc_pb_socket:put(RiakPid, UserObj).

%% @doc Shared code used when doing a bucket creation or deletion.
-spec serialized_bucket_op(binary(),
                           acl_v1(),
                           moss_user(),
                           term(),
                           bucket_action()) ->
                                  {ok, moss_user()} |
                                  {ok, ignore} |
                                  {error, term()}.
serialized_bucket_op(Bucket, ACL, User, VClock, BucketOp) ->
    case get_admin_creds() of
        {ok, AdminCreds} ->
            BucketFun = bucket_fun(BucketOp,
                                   Bucket,
                                   ACL,
                                   User?MOSS_USER.key_id,
                                   AdminCreds,
                                   stanchion_data()),
            %% Make a call to the bucket request
            %% serialization service.
            OpResult = BucketFun(),
            case OpResult of
                ok ->
                    BucketRecord = bucket_record(Bucket, BucketOp),
                    case update_user_buckets(User, BucketRecord) of
                        {ok, ignore} ->
                            OpResult;
                        {ok, UpdUser} ->
                            save_user(UpdUser, VClock)
                    end;
                {error, {error_status, _, _, ErrorDoc}} ->
                    ErrorCode = xml_error_code(ErrorDoc),
                    {error, riak_moss_s3_response:error_code_to_atom(ErrorCode)};
                {error, _} ->
                    OpResult
            end;
        {error, Reason1} ->
            {error, Reason1}
    end.

%% @doc Generate a JSON document to use for a user
%% creation request.
-spec user_json(moss_user()) -> string().
user_json(User) ->
    ?MOSS_USER{name=UserName,
               display_name=DisplayName,
               email=Email,
               key_id=KeyId,
               key_secret=Secret,
               canonical_id=CanonicalId} = User,
    binary_to_list(
      iolist_to_binary(
        mochijson2:encode([{struct, [{<<"email">>, list_to_binary(Email)}]},
                           {struct, [{<<"display_name">>, list_to_binary(DisplayName)}]},
                           {struct, [{<<"name">>, list_to_binary(UserName)}]},
                           {struct, [{<<"key_id">>, list_to_binary(KeyId)}]},
                           {struct, [{<<"key_secret">>, list_to_binary(Secret)}]},
                           {struct, [{<<"canonical_id">>, list_to_binary(CanonicalId)}]}
                          ]))).

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

%% @doc Check if a user already has an ownership of
%% a bucket and update the bucket list if needed.
-spec update_user_buckets(moss_user(), moss_bucket()) ->
                                 {ok, ignore} | {ok, moss_user()}.
update_user_buckets(User, Bucket) ->
    Buckets = User?MOSS_USER.buckets,
    %% At this point any siblings from the read of the
    %% user record have been resolved so the user bucket
    %% list should have 0 or 1 buckets that share a name
    %% with `Bucket'.
    case [B || B <- Buckets, B?MOSS_BUCKET.name =:= Bucket?MOSS_BUCKET.name] of
        [] ->
            {ok, User?MOSS_USER{buckets=[Bucket | Buckets]}};
        [ExistingBucket] ->
            case
                (Bucket?MOSS_BUCKET.last_action == deleted andalso
                 ExistingBucket?MOSS_BUCKET.last_action == created)
                orelse
                (Bucket?MOSS_BUCKET.last_action == created andalso
                 ExistingBucket?MOSS_BUCKET.last_action == deleted) of
                true ->
                    UpdBuckets = [Bucket | lists:delete(ExistingBucket, Buckets)],
                    {ok, User?MOSS_USER{buckets=UpdBuckets}};
                false ->
                    {ok, ignore}
            end
    end.

%% @doc Return a user record for the specified user name and
%% email address.
-spec user_record(string(), string()) -> moss_user().
user_record(Name, Email) ->
    user_record(Name, Email, []).

%% @doc Return a user record for the specified user name and
%% email address.
-spec user_record(string(), string(), [moss_bucket()]) -> moss_user().
user_record(Name, Email, Buckets) ->
    {KeyId, Secret} = generate_access_creds(Name),
    CanonicalId = generate_canonical_id(KeyId, Secret),
    DisplayName = display_name(Email),
    ?MOSS_USER{name=Name,
               display_name=DisplayName,
               email=Email,
               key_id=KeyId,
               key_secret=Secret,
               canonical_id=CanonicalId,
               buckets=Buckets}.
