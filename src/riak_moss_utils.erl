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
         create_bucket/3,
         create_user/2,
         delete_bucket/2,
         delete_object/2,
         from_bucket_name/1,
         get_admin_creds/0,
         get_buckets/1,
         get_keys_and_objects/2,
         get_object/2,
         get_object/3,
         get_user/1,
         list_keys/1,
         list_keys/2,
         pow/2,
         pow/3,
         put_object/4,
         riak_connection/0,
         riak_connection/2,
         to_bucket_name/2]).

-include("riak_moss.hrl").
-include_lib("xmerl/include/xmerl.hrl").

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
%% TODO:
%% We need to be checking that
%% this bucket doesn't already
%% exist anywhere, since everyone
%% shares a global bucket namespace
-spec create_bucket(string(), binary(), acl_v1()) -> ok.
create_bucket(KeyId, Bucket, _ACL) ->
    case riak_connection() of
        {ok, RiakPid} ->
            %% TODO:
            %% We don't do anything about
            %% {error, Reason} here
            {ok, {User, VClock}} = get_user(KeyId, RiakPid),
            {StanchionIp, StanchionPort, StanchionSSL} = get_stanchion_data(),
            case get_admin_creds() of
                {ok, AdminCreds} ->
                    %% Generate the bucket JSON document
                    BucketDoc = bucket_json(Bucket, KeyId),
                    %% Make a call to the bucket request
                    %% serialization service.
                    CreateResult = velvet:create_bucket(StanchionIp,
                                                        StanchionPort,
                                                        "application/json",
                                                        BucketDoc,
                                                        [{ssl, StanchionSSL},
                                                         {auth_creds, AdminCreds}]),
                    case CreateResult of
                        ok ->
                            BucketRecord = bucket_record(Bucket, created),
                            case update_user_buckets(User, BucketRecord) of
                                {ok, ignore} ->
                                    Res = CreateResult;
                                {ok, UpdUser} ->
                                    Res = save_user(UpdUser, VClock, RiakPid)
                            end;
                        {error, {error_status, _, _, ErrorDoc}} ->
                            ErrorCode = xml_error_code(ErrorDoc),
                            Res = {error, riak_moss_s3_response:error_code_to_atom(ErrorCode)};
                        {error, _} ->
                            Res = CreateResult
                    end;
                {error, Reason1} ->
                    Res = {error, Reason1}
            end,
            close_riak_connection(RiakPid),
            Res;
        {error, Reason} ->
            {error, {riak_connect_failed, Reason}}
    end.

%% @doc Create a new MOSS user
-spec create_user(string(), string()) -> {ok, moss_user()}.
create_user(UserName, Email) ->
    %% Validate the email address
    case validate_email(Email) of
        ok ->
            case riak_connection() of
                {ok, RiakPid} ->
                    {KeyID, Secret} = generate_access_creds(UserName),
                    CanonicalID = generate_canonical_id(KeyID, Secret),
                    DisplayName = display_name(Email),
                    User = ?MOSS_USER{name=UserName,
                                      display_name=DisplayName,
                                      email=Email,
                                      key_id=KeyID,
                                      key_secret=Secret,
                                      canonical_id=CanonicalID},
                    save_user(User, undefined, RiakPid),
                    close_riak_connection(RiakPid),
                    {ok, User};
                {error, Reason} ->
                    {error, {riak_connect_failed, Reason}}
            end;
        {error, Reason1} ->
            {error, Reason1}
    end.

%% @doc Delete a bucket
-spec delete_bucket(string(), binary()) -> ok.
delete_bucket(KeyId, BucketName) ->
    case riak_connection() of
        {ok, RiakPid} ->
            %% @TODO This will need to be updated once globally
            %% unique buckets are enforced.
            {ok, {User, VClock}} = get_user(KeyId, RiakPid),
            CurrentBuckets = get_buckets(User),

            {StanchionIp, StanchionPort, StanchionSSL} = get_stanchion_data(),
            %% Buckets can only be deleted if they exist
            case bucket_exists(CurrentBuckets, BucketName) of
                true ->
                    case bucket_empty(BucketName, RiakPid) of
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
                    case get_admin_creds() of
                        {ok, AdminCreds} ->
                            %% Make a call to the bucket request
                            %% serialization service.
                            DeleteRes = velvet:delete_bucket(StanchionIp,
                                                             StanchionPort,
                                                             BucketName,
                                                             KeyId,
                                                             [{ssl, StanchionSSL},
                                                              {auth_creds, AdminCreds}]),
                            case DeleteRes of
                                ok ->
                                    UpdatedBuckets =
                                        remove_bucket(CurrentBuckets, BucketName),
                                    UpdatedUser =
                                        User#moss_user{buckets=UpdatedBuckets},
                                    Res = save_user(UpdatedUser, VClock, RiakPid);
                                {error, {error_status, _, _, ErrorDoc}} ->
                                    ErrorCode = xml_error_code(ErrorDoc),
                                    Res = {error, riak_moss_s3_response:error_code_to_atom(ErrorCode)};
                                {error, _} ->
                                    Res = DeleteRes
                            end;
                        {error, Reason1} ->
                            Res = {error, Reason1}
                    end;
                false ->
                    Res = LocalError
            end,
            close_riak_connection(RiakPid),
            Res;
        {error, Reason} ->
            {error, {riak_connect_failed, Reason}}
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

%% @doc Return `stanchion' configuration data.
-spec get_stanchion_data() -> {string(), pos_integer(), boolean()} | {error, term()}.
get_stanchion_data() ->
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

%% @doc Return a user's buckets.
-spec get_buckets(moss_user()) -> [binary()].
get_buckets(#moss_user{buckets=Buckets}) ->
    lists:sort(Buckets).

%% @doc Return a list of keys for a bucket along
%% with their associated objects.
-spec get_keys_and_objects(binary(), binary()) -> {ok, [binary()]}.
get_keys_and_objects(BucketName, Prefix) ->
    case riak_connection() of
        {ok, RiakPid} ->
            case list_keys(BucketName, RiakPid) of
                {ok, Keys} ->
                    KeyObjPairs =
                        [{Key, get_object(BucketName, Key, RiakPid)}
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
-spec get_user(string()) -> {ok, term()} | {error, term()}.
get_user(KeyID) ->
    case riak_connection() of
        {ok, RiakPid} ->
            Res = get_user(KeyID, RiakPid),
            close_riak_connection(RiakPid),
            Res;
        {error, Reason} ->
            {error, Reason}
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

%% @doc Check if a bucket is empty
-spec bucket_empty(string(), pid()) -> boolean().
bucket_empty(Bucket, RiakPid) ->
    ObjBucket = to_bucket_name(objects, list_to_binary(Bucket)),
    case list_keys(ObjBucket, RiakPid) of
        {ok, []} ->
            true;
        _ ->
            false
    end.

%% @doc Check if a bucket exists in a list of the user's buckets.
%% @TODO This will need to change once globally unique buckets
%% are enforced.
-spec bucket_exists([moss_bucket()], string()) -> boolean().
bucket_exists(Buckets, CheckBucket) ->
    SearchResults = [Bucket || Bucket <- Buckets,
                               Bucket#moss_bucket.name =:= CheckBucket],
    case SearchResults of
        [] ->
            false;
        _ ->
            true
    end.

%% @doc Generate a JSON document to use for a bucket
%% creation request.
-spec bucket_json(binary(), string()) -> string().
bucket_json(Bucket, KeyId)  ->
    binary_to_list(
      iolist_to_binary(
        mochijson2:encode([{struct, [{<<"bucket">>, Bucket}]},
                           {struct, [{<<"requester">>, list_to_binary(KeyId)}]}]))).

%% @doc Return a bucket record for the specified bucket name.
-spec bucket_record(binary(), created | deleted) -> moss_bucket().
bucket_record(Name, Action) ->
    ?MOSS_BUCKET{name=Name,
                 last_action=Action,
                 modification_time=erlang:now()}.

%% @doc Strip off the user name portion of an email address
-spec display_name(string()) -> string().
display_name(Email) ->
    Index = string:chr(Email, $@),
    string:sub_string(Email, 1, Index-1).

%% @doc Generate a new set of access credentials for user.
-spec generate_access_creds(string()) -> {binary(), binary()}.
generate_access_creds(UserName) ->
    BinUser = list_to_binary(UserName),
    KeyID = generate_key(BinUser),
    Secret = generate_secret(BinUser, KeyID),
    {KeyID, Secret}.

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
                    Values = riakc_obj:get_values(Obj),
                    User = binary_to_term(hd(Values)),
                    Buckets = resolve_buckets(Values, [], KeepDeletedBuckets),
                    {ok, {User?MOSS_USER{buckets=Buckets},
                          riakc_obj:vclock(Obj)}}
            end;
        Error ->
            Error
    end.

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

%% @doc Return true if the last action for the bucket
%% is deleted and the action occurred over 24 hours ago.
-spec cleanup_bucket(moss_bucket()) -> boolean().
cleanup_bucket(?MOSS_BUCKET{last_action=created}) ->
    false;
cleanup_bucket(?MOSS_BUCKET{last_action=deleted,
                            modification_time=ModTime}) ->
    timer:now_diff(erlang:now(), ModTime) > 86400.

%% @doc Resolve the set of buckets for a user when
%% siblings are encountered on a read of a user record.
-spec resolve_buckets([moss_user()], [moss_bucket()], boolean()) ->
                             [moss_bucket()].
resolve_buckets([], Buckets, true) ->
    Buckets;
resolve_buckets([], Buckets, false) ->
    [Bucket || Bucket <- Buckets, not cleanup_bucket(Bucket)];
resolve_buckets([HeadUserVal | RestUserVals], Buckets, _KeepDeleted) ->
    UserRecord = binary_to_term(HeadUserVal),
    HeadBuckets = UserRecord?MOSS_USER.buckets,
    UpdBuckets = lists:foldl(fun bucket_resolver/2, Buckets, HeadBuckets),
    resolve_buckets(RestUserVals, UpdBuckets, _KeepDeleted).

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
    string:to_upper(base64:encode_to_string(Key)).

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
    base64:encode_to_string(
      iolist_to_binary(<< SecretPart1:Bytes/binary,
                          SecretPart2:Bytes/binary >>)).

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

%% @doc Remove a bucket from a user's list of buckets.
%% @TODO This may need to change once globally unique buckets
%% are enforced.
-spec remove_bucket([moss_bucket()], string()) -> boolean().
remove_bucket(Buckets, RemovalBucket) ->
    FilterFun =
        fun(Element) ->
                Element#moss_bucket.name =/= RemovalBucket
        end,
    lists:filter(FilterFun, Buckets).

%% @doc Save information about a MOSS user
-spec save_user(moss_user(), term(), pid()) -> ok.
save_user(User, _VClock, RiakPid) ->
    UserObj = riakc_obj:new(?USER_BUCKET, list_to_binary(User?MOSS_USER.key_id), User),
    %% @TODO Error handling
    riakc_pb_socket:put(RiakPid, UserObj).

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
    case [B || B <- Buckets, B?MOSS_BUCKET.name =:= Bucket] of
        [] ->
            {ok, User?MOSS_USER{buckets=[Bucket | Buckets]}};
        [ExistingBucket] ->
            case ExistingBucket?MOSS_BUCKET.last_action of
                created ->
                    {ok, ignore};
                deleted ->
                    {ok, [Bucket | lists:delete(ExistingBucket, Buckets)]}
            end
    end.
