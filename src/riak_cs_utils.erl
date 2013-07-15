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

%% @doc riak_cs utility functions

-module(riak_cs_utils).

%% Public API
-export([etag_from_binary/1,
         etag_from_binary/2,
         etag_from_binary_no_quotes/1,
         check_bucket_exists/2,
         close_riak_connection/1,
         close_riak_connection/2,
         create_bucket/5,
         create_user/2,
         create_user/4,
         delete_bucket/4,
         delete_object/3,
         display_name/1,
         encode_term/1,
         from_bucket_name/1,
         get_buckets/1,
         get_keys_and_manifests/3,
         has_tombstone/1,
         is_admin/1,
         map_keys_and_manifests/3,
         md5/1,
         md5_init/0,
         md5_update/2,
         md5_final/1,
         reduce_keys_and_manifests/2,
         get_object/3,
         get_manifests/3,
         manifests_from_riak_object/1,
         active_manifest_from_response/1,
         get_user/2,
         get_user_by_index/3,
         get_user_index/3,
         hexlist_to_binary/1,
         binary_to_hexlist/1,
         json_pp_print/1,
         list_keys/2,
         maybe_log_bucket_owner_error/2,
         pow/2,
         pow/3,
         put_object/5,
         put/2,
         put/3,
         put_with_no_meta/2,
         put_with_no_meta/3,
         resolve_robj_siblings/1,
         riak_connection/0,
         riak_connection/1,
         safe_base64_decode/1,
         safe_base64url_decode/1,
         safe_list_to_integer/1,
         save_user/3,
         set_bucket_acl/5,
         set_object_acl/5,
         set_bucket_policy/5,
         delete_bucket_policy/4,
         get_bucket_acl_policy/3,
         second_resolution_timestamp/1,
         timestamp_to_seconds/1,
         timestamp_to_milliseconds/1,
         to_bucket_name/2,
         update_key_secret/1,
         update_obj_value/2,
         pid_to_binary/1,
         update_user/3]).

-include("riak_cs.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").
-include_lib("riakc/include/riakc.hrl").

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

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Convert the passed binary into a string where the numbers are represented in hexadecimal (lowercase and 0 prefilled).
-spec binary_to_hexlist(binary()) -> string().
binary_to_hexlist(<<>>) ->
    [];
binary_to_hexlist(<<A:4, B:4, T/binary>>) ->
    [num2hexchar(A), num2hexchar(B)|binary_to_hexlist(T)].

num2hexchar(N) when N < 10 ->
    N + $0;
num2hexchar(N) when N < 16 ->
    (N - 10) + $a.

%% @doc Convert the passed binary into a string where the numbers are represented in hexadecimal (lowercase and 0 prefilled).
-spec hexlist_to_binary(string()) -> binary().
hexlist_to_binary(HS) ->
    list_to_binary(hexlist_to_binary_2(HS)).

hexlist_to_binary_2([]) ->
    [];
hexlist_to_binary_2([A,B|T]) ->
    [hex2byte(A, B)|hexlist_to_binary_2(T)].

hex2byte(A, B) ->
    An = hexchar2num(A),
    Bn = hexchar2num(B),
    <<An:4, Bn:4>>.

hexchar2num(C) when $0 =< C, C =< $9 ->
    C - $0;
hexchar2num(C) when $a =< C, C =< $f ->
    (C - $a) + 10.

%% @doc Return a hexadecimal string of `Binary', with double quotes
%% around it.
-spec etag_from_binary(binary()) -> string().
etag_from_binary(Binary) ->
    etag_from_binary(Binary, []).

%% @doc Return a hexadecimal string of `Binary', with double quotes
%% around it.
-spec etag_from_binary(binary(), string()) -> string().
etag_from_binary(Binary, []) ->
    "\"" ++ etag_from_binary_no_quotes(Binary) ++ "\"";
etag_from_binary(Binary, Suffix) ->
    "\"" ++ etag_from_binary_no_quotes(Binary) ++ Suffix ++ "\"".

%% @doc Return a hexadecimal string of `Binary', without double quotes
%% around it.
-spec etag_from_binary_no_quotes(binary() | {binary(), string()}) -> string().
etag_from_binary_no_quotes({Binary, Suffix}) ->
    binary_to_hexlist(Binary) ++ Suffix;
etag_from_binary_no_quotes(Binary) ->
    binary_to_hexlist(Binary).

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
    CurrentBuckets = get_buckets(User),

    %% Do not attempt to create bucket if the user already owns it
    AttemptCreate = not bucket_exists(CurrentBuckets, binary_to_list(Bucket)),
    case AttemptCreate of
        true ->
            case valid_bucket_name(Bucket) of
                true ->
                    serialized_bucket_op(Bucket,
                                         ACL,
                                         User,
                                         UserObj,
                                         create,
                                         bucket_create,
                                         RiakPid);
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

%% @doc Create a new Riak CS user
-spec create_user(string(), string()) -> {ok, rcs_user()} | {error, term()}.
create_user(Name, Email) ->
    {KeyId, Secret} = generate_access_creds(Email),
    create_user(Name, Email, KeyId, Secret).

%% @doc Create a new Riak CS user
-spec create_user(string(), string(), string(), string()) -> {ok, rcs_user()} | {error, term()}.
create_user(Name, Email, KeyId, Secret) ->
    case validate_email(Email) of
        ok ->
            User = user_record(Name, Email, KeyId, Secret),
            create_credentialed_user(riak_cs_config:admin_creds(), User);
        {error, _Reason}=Error ->
            Error
    end.

-spec create_credentialed_user({error, term()}, rcs_user()) ->
                                  {error, term()};
                              ({ok, {term(), term()}}, rcs_user()) ->
                                  {ok, rcs_user()} | {error, term()}.
create_credentialed_user({error, _}=Error, _User) ->
    Error;
create_credentialed_user({ok, AdminCreds}, User) ->
    {StIp, StPort, StSSL} = stanchion_data(),
    %% Make a call to the user request serialization service.
    Result = velvet:create_user(StIp,
                                StPort,
                                "application/json",
                                binary_to_list(riak_cs_json:to_json(User)),
                                [{ssl, StSSL}, {auth_creds, AdminCreds}]),
    handle_create_user(Result, User).

handle_create_user(ok, User) ->
    {ok, User};
handle_create_user({error, {error_status, _, _, ErrorDoc}}, _User) ->
    case riak_cs_config:api() of
        s3 ->
            riak_cs_s3_response:error_response(ErrorDoc);
        oos ->
            {error, ErrorDoc}
    end;
handle_create_user({error, _}=Error, _User) ->
    Error.

handle_update_user(ok, User, UserObj, RiakPid) ->
    _ = save_user(User, UserObj, RiakPid),
    {ok, User};
handle_update_user({error, {error_status, _, _, ErrorDoc}}, _User, _, _) ->
    case riak_cs_config:api() of
        s3 ->
            riak_cs_s3_response:error_response(ErrorDoc);
        oos ->
            {error, ErrorDoc}
    end;
handle_update_user({error, _}=Error, _User, _, _) ->
    Error.

%% @doc Update a Riak CS user record
-spec update_user(rcs_user(), riakc_obj:riakc_obj(), pid()) ->
                         {ok, rcs_user()} | {error, term()}.
update_user(User, UserObj, RiakPid) ->
    {StIp, StPort, StSSL} = stanchion_data(),
    case riak_cs_config:admin_creds() of
        {ok, AdminCreds} ->
            Options = [{ssl, StSSL}, {auth_creds, AdminCreds}],
            %% Make a call to the user request serialization service.
            Result = velvet:update_user(StIp,
                                        StPort,
                                        "application/json",
                                        User?RCS_USER.key_id,
                                        binary_to_list(riak_cs_json:to_json(User)),
                                        Options),
            handle_update_user(Result, User, UserObj, RiakPid);
        {error, _}=Error ->
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
    ok = riak_cs_stats:update_with_start(object_delete, os:timestamp()),
    riak_cs_gc:gc_active_manifests(Bucket, Key, RiakcPid).

-spec encode_term(term()) -> binary().
encode_term(Term) ->
    case riak_cs_config:use_t2b_compression() of
        true ->
            term_to_binary(Term, [compressed]);
        false ->
            term_to_binary(Term)
    end.

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

-type context() :: binary().
-type digest() :: binary().

-spec md5(string() | binary()) -> digest().
md5(Bin) when is_binary(Bin) ->
    md5_final(md5_update(md5_init(), Bin));
md5(List) when is_list(List) ->
    md5(list_to_binary(List)).

-spec md5_init() -> context().
md5_init() ->
    crypto:md5_init().

-define(MAX_UPDATE_SIZE, (32*1024)).

-spec md5_update(context(), binary()) -> context().
md5_update(Ctx, Bin) when size(Bin) =< ?MAX_UPDATE_SIZE ->
    crypto:md5_update(Ctx, Bin);
md5_update(Ctx, <<Part:?MAX_UPDATE_SIZE/binary, Rest/binary>>) ->
    md5_update(crypto:md5_update(Ctx, Part), Rest).

-spec md5_final(context()) -> digest().
md5_final(Ctx) ->
    crypto:md5_final(Ctx).

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
            _  = gc_deleted_while_writing_manifests(Object, Manifests, Bucket, Key, RiakcPid),
            {ok, Object, Manifests};
        {error, _Reason}=Error ->
            Error
    end.

gc_deleted_while_writing_manifests(Object, Manifests, Bucket, Key, RiakcPid) ->
    UUIDs = riak_cs_manifest_utils:deleted_while_writing(Manifests),
    riak_cs_gc:gc_specific_manifests(UUIDs, Object, Bucket, Key, RiakcPid).

-spec manifests_from_riak_object(riakc_obj:riakc_obj()) -> orddict:orddict().
manifests_from_riak_object(RiakObject) ->
    %% For example, riak_cs_manifest_fsm:get_and_update/4 may wish to
    %% update the #riakc_obj without a roundtrip to Riak first.  So we
    %% need to see what the latest
    Contents = try
                   %% get_update_value will return the updatevalue or
                   %% a single old original value.
                   [{riakc_obj:get_update_metadata(RiakObject),
                     riakc_obj:get_update_value(RiakObject)}]
               catch throw:_ ->
                       %% Original value had many contents
                       riakc_obj:get_contents(RiakObject)
               end,
    DecodedSiblings = [binary_to_term(V) ||
                          {_, V}=Content <- Contents,
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
                                 Value <- riakc_obj:get_values(Obj),
                                 Value /= <<>>  % tombstone
                             ],
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
        {ok, ?INDEX_RESULTS{keys=[]}} ->
            {error, notfound};
        {ok, ?INDEX_RESULTS{keys=[Key | _]}} ->
            {ok, binary_to_list(Key)};
        {error, Reason}=Error ->
            _ = lager:warning("Error occurred trying to query ~p in user"
                              "index ~p. Reason: ~p",
                              [Value, Index, Reason]),
            Error
    end.

%% @doc Determine if a set of contents of a riak object has a tombstone.
-spec has_tombstone({dict(), binary()}) -> boolean().
has_tombstone({_, <<>>}) ->
    true;
has_tombstone({MD, _V}) ->
    dict:is_key(?MD_DELETED, MD) =:= true.

%% @doc Determine if the specified user account is a system admin.
-spec is_admin(rcs_user()) -> boolean().
is_admin(User) ->
    is_admin(User, riak_cs_config:admin_creds()).

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

put(RiakcPid, RiakcObj) ->
    put(RiakcPid, RiakcObj, []).

put(RiakcPid, RiakcObj, Options) ->
    riakc_pb_socket:put(RiakcPid, RiakcObj, Options).

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
    ok | {ok, riakc_obj:riakc_obj()} | {ok, binary()} | {error, term()}.
put_with_no_meta(RiakcPid, RiakcObject, Options) ->
    WithMeta = riakc_obj:update_metadata(RiakcObject, dict:new()),
    io:format("put_NO_META LINE ~p contents ~p bucket ~p key ~p\n", [?LINE, length(riakc_obj:get_contents(WithMeta)), riakc_obj:bucket(WithMeta), riakc_obj:key(WithMeta)]),
    catch exit(yoyo),
    io:format("put_NO_META LINE ~p ~p\n", [?LINE, erlang:get_stacktrace()]),
    riakc_pb_socket:put(RiakcPid, WithMeta, Options).


-type resolve_ok() :: {term(), binary()}.
-type resolve_error() :: {atom(), atom()}.
-spec resolve_robj_siblings(RObj::term()) ->
                      {resolve_ok() | resolve_error(), NeedsRepair::boolean()}.

resolve_robj_siblings(Cs) ->
    [{BestRating, BestMDV}|Rest] = lists:sort([{rate_a_dict(MD, V), MDV} ||
                                                  {MD, V} = MDV <- Cs]),
    if BestRating =< 0 ->
            {BestMDV, length(Rest) > 0};
       true ->
            %% The best has a failing checksum
            {{no_dict_available, bad_checksum}, true}
    end.

%% Corruption simulation:
%% rate_a_dict(_MD, _V) -> case find_rcs_bcsum(_MD) of _ -> 666777888 end.

rate_a_dict(MD, V) ->
    %% The lower the score, the better.
    case dict:find(?MD_DELETED, MD) of
        {ok, true} ->
            -10;                                % Trump everything
        error ->
            case find_rcs_bcsum(MD) of
                CorrectBCSum when is_binary(CorrectBCSum) ->
                    case riak_cs_utils:md5(V) of
                        X when X =:= CorrectBCSum ->
                            -1;                 % Hooray correctness
                        _Bad ->
                            666                 % Boooo
                    end;
                _ ->
                    0                           % OK for legacy data
            end
    end.

find_rcs_bcsum(MD) ->
    case find_md_usermeta(MD) of
        {ok, Ps} ->
            proplists:get_value(<<?USERMETA_BCSUM>>, Ps);
        error ->
            undefined
    end.

find_md_usermeta(MD) ->
    dict:find(?MD_USERMETA, MD).

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
-spec save_user(rcs_user(), riakc_obj:riakc_obj(), pid()) -> ok | {error, term()}.
save_user(User, UserObj, RiakPid) ->
    Indexes = [{?EMAIL_INDEX, User?RCS_USER.email},
               {?ID_INDEX, User?RCS_USER.canonical_id}],
    MD = dict:store(?MD_INDEX, Indexes, dict:new()),
    UpdUserObj = riakc_obj:update_metadata(
                   riakc_obj:update_value(UserObj,
                                          riak_cs_utils:encode_term(User)),
                   MD),
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

%% @doc Set the policy for a bucket. Existing policy is only overwritten.
-spec set_bucket_policy(rcs_user(), riakc_obj:riakc_obj(), binary(), []|policy()|acl(), pid()) -> ok | {error, term()}.
set_bucket_policy(User, UserObj, Bucket, PolicyJson, RiakPid) ->
    serialized_bucket_op(Bucket,
                         PolicyJson,
                         User,
                         UserObj,
                         update_policy,
                         bucket_put_policy,
                         RiakPid).

%% @doc Set the policy for a bucket. Existing policy is only overwritten.
-spec delete_bucket_policy(rcs_user(), riakc_obj:riakc_obj(), binary(), pid()) -> ok | {error, term()}.
delete_bucket_policy(User, UserObj, Bucket, RiakPid) ->
    serialized_bucket_op(Bucket,
                         [],
                         User,
                         UserObj,
                         delete_policy,
                         bucket_put_policy,
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

% @doc fetch moss.bucket and return acl and policy
-spec get_bucket_acl_policy(binary(), atom(), pid()) -> {acl(), policy()} | {error, term()}.
get_bucket_acl_policy(Bucket, PolicyMod, RiakPid) ->
    case check_bucket_exists(Bucket, RiakPid) of
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

-spec second_resolution_timestamp(erlang:timestamp()) -> non_neg_integer().
%% @doc Return the number of seconds this timestamp represents. Truncated to
%% seconds, as an integer.
second_resolution_timestamp({MegaSecs, Secs, _MicroSecs}) ->
    (MegaSecs * 1000000) + Secs.

-spec timestamp_to_seconds(erlang:timestamp()) -> float().
timestamp_to_seconds({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000) + Secs + (MicroSecs / 1000000).

-spec timestamp_to_milliseconds(erlang:timestamp()) -> float().
timestamp_to_milliseconds(Timestamp) ->
    timestamp_to_seconds(Timestamp) * 1000.

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
    BucketHash = md5(Bucket),
    <<Prefix/binary, BucketHash/binary>>.


%% @doc Generate a new `key_secret' for a user record.
-spec update_key_secret(rcs_user()) -> rcs_user().
update_key_secret(User=?RCS_USER{email=Email,
                                 key_id=KeyId}) ->
    EmailBin = list_to_binary(Email),
    User?RCS_USER{key_secret=generate_secret(EmailBin, KeyId)}.

%% @doc Update the object's value blob, and take the first metadata
%%      dictionary because we don't care about trying to merge them.
-spec update_obj_value(riakc_obj:riakc_obj(), binary()) -> riakc_obj:riakc_obj().
update_obj_value(Obj, Value) when is_binary(Value) ->
    [MD | _] = riakc_obj:get_metadatas(Obj),
    riakc_obj:update_metadata(riakc_obj:update_value(Obj, Value),
                              MD).

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
                                    stanchion_acl_utils:acl_to_json_term(ACL)]}))).

%% @doc Generate a JSON document to use for a bucket
-spec bucket_policy_json(binary(), string()) -> string().
bucket_policy_json(PolicyJson, KeyId)  ->
    binary_to_list(
      iolist_to_binary(
        mochijson2:encode({struct, [{<<"requester">>, list_to_binary(KeyId)},
                                    {<<"policy">>, PolicyJson}]
                          }))).

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
                                 {ok, riakc_obj:riakc_obj()} | {error, term()}.
check_bucket_exists(Bucket, RiakPid) ->
    case riak_cs_utils:get_object(?BUCKETS_BUCKET, Bucket, RiakPid) of
        {ok, Obj} ->
            %% Make sure the bucket has an owner
            [Value | _] = Values = riakc_obj:get_values(Obj),
            maybe_log_sibling_warning(Bucket, Values),
            case Value of
                <<"0">> ->
                    {error, no_such_bucket};
                _ ->
                    {ok, Obj}
            end;
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
bucket_fun(update_policy, Bucket, PolicyJson, KeyId, AdminCreds, StanchionData) ->
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
bucket_fun(delete_policy, Bucket, _, KeyId, AdminCreds, StanchionData) ->
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
-spec generate_canonical_id(string(), undefined | string()) -> string().
generate_canonical_id(_KeyID, undefined) ->
    [];
generate_canonical_id(KeyID, Secret) ->
    Bytes = 16,
    Id1 = md5(KeyID),
    Id2 = md5(Secret),
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
                           [] | acl() | policy(),
                           rcs_user(),
                           riakc_obj:riakc_obj(),
                           bucket_operation(),
                           atom(),
                           pid()) ->
                                  ok |
                                  {error, term()}.
serialized_bucket_op(Bucket, ACL, User, UserObj, BucketOp, StatName, RiakPid) ->
    StartTime = os:timestamp(),
    case riak_cs_config:admin_creds() of
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
                    riak_cs_s3_response:error_response(ErrorDoc);
                {error, _} ->
                    OpResult
            end;
        {error, Reason1} ->
            {error, Reason1}
    end.

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
-spec user_record(string(), string(), string(), string()) -> rcs_user().
user_record(Name, Email, KeyId, Secret) ->
    user_record(Name, Email, KeyId, Secret, []).

%% @doc Return a user record for the specified user name and
%% email address.
-spec user_record(string(), string(), string(), string(), [cs_bucket()]) ->
                         rcs_user().
user_record(Name, Email, KeyId, Secret, Buckets) ->
    CanonicalId = generate_canonical_id(KeyId, Secret),
    DisplayName = display_name(Email),
    ?RCS_USER{name=Name,
              display_name=DisplayName,
              email=Email,
              key_id=KeyId,
              key_secret=Secret,
              canonical_id=CanonicalId,
              buckets=Buckets}.

%% @doc Convert a pid to a binary
-spec pid_to_binary(pid()) -> binary().
pid_to_binary(Pid) ->
    list_to_binary(pid_to_list(Pid)).

-spec safe_base64_decode(binary() | string()) -> {ok, binary()} | bad.
safe_base64_decode(Str) ->
    try
        X = base64:decode(Str),
        {ok, X}
    catch _:_ ->
            bad
    end.

-spec safe_base64url_decode(binary() | string()) -> {ok, binary()} | bad.
safe_base64url_decode(Str) ->
    try
        X = base64url:decode(Str),
        {ok, X}
    catch _:_ ->
            bad
    end.

-spec safe_list_to_integer(string()) -> {ok, integer()} | bad.
safe_list_to_integer(Str) ->
    try
        X = list_to_integer(Str),
        {ok, X}
    catch _:_ ->
            bad
    end.
