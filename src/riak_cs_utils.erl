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

%% @doc riak_cs utility functions

-module(riak_cs_utils).

%% Public API
-export([etag_from_binary/1,
         etag_from_binary/2,
         etag_from_binary_no_quotes/1,
         close_riak_connection/1,
         close_riak_connection/2,
         create_user/2,
         create_user/4,
         delete_object/3,
         display_name/1,
         encode_term/1,
         get_keys_and_manifests/3,
         has_tombstone/1,
         is_admin/1,
         map_keys_and_manifests/3,
         maybe_process_resolved/3,
         sha_mac/2,
         sha/1,
         md5/1,
         md5_init/0,
         md5_update/2,
         md5_final/1,
         reduce_keys_and_manifests/2,
         get_manifests/3,
         manifests_from_riak_object/1,
         active_manifest_from_response/1,
         get_user/2,
         get_user_by_index/3,
         get_user_index/3,
         hexlist_to_binary/1,
         binary_to_hexlist/1,
         json_pp_print/1,
         key_exists/3,
         n_val_1_get_requests/0,
         pow/2,
         pow/3,
         resolve_robj_siblings/1,
         riak_connection/0,
         riak_connection/1,
         safe_base64_decode/1,
         safe_base64url_decode/1,
         safe_list_to_integer/1,
         save_user/3,
         set_object_acl/5,
         second_resolution_timestamp/1,
         timestamp_to_seconds/1,
         timestamp_to_milliseconds/1,
         update_key_secret/1,
         update_obj_value/2,
         pid_to_binary/1,
         update_user/3,
         from_bucket_name/1,
         to_bucket_name/2,
         stanchion_data/0
        ]).

-include("riak_cs.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").
-include_lib("riakc/include/riakc.hrl").

-ifdef(TEST).
-compile(export_all).
-endif.

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
    close_riak_connection(riak_cs_riak_client:pbc_pool_name(master), Pid).

%% @doc Release a protobufs connection from the specified
%% connection pool.
-spec close_riak_connection(atom(), pid()) -> ok.
close_riak_connection(Pool, Pid) ->
    poolboy:checkin(Pool, Pid).

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

handle_update_user(ok, User, UserObj, RcPid) ->
    _ = save_user(User, UserObj, RcPid),
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
-spec update_user(rcs_user(), riakc_obj:riakc_obj(), riak_client()) ->
                         {ok, rcs_user()} | {error, term()}.
update_user(User, UserObj, RcPid) ->
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
            handle_update_user(Result, User, UserObj, RcPid);
        {error, _}=Error ->
            Error
    end.


%% @doc Mark all active manifests as pending_delete.
%% If successful, returns a list of the UUIDs that were marked for
%% Garbage collection. Otherwise returns an error. Note,
%% {error, notfound} counts as success in this case,
%% with the list of UUIDs being [].
-spec delete_object(binary(), binary(), riak_client()) ->
    {ok, [binary()]} | {error, term()}.
delete_object(Bucket, Key, RcPid) ->
    ok = riak_cs_stats:update_with_start(object_delete, os:timestamp()),
    riak_cs_gc:gc_active_manifests(Bucket, Key, RcPid).

-spec encode_term(term()) -> binary().
encode_term(Term) ->
    case riak_cs_config:use_t2b_compression() of
        true ->
            term_to_binary(Term, [compressed]);
        false ->
            term_to_binary(Term)
    end.

%% @doc Return a list of keys for a bucket along
%% with their associated objects.
-spec get_keys_and_manifests(binary(), binary(), riak_client()) -> {ok, [lfs_manifest()]} | {error, term()}.
get_keys_and_manifests(BucketName, Prefix, RcPid) ->
    ManifestBucket = to_bucket_name(objects, BucketName),
    case active_manifests(ManifestBucket, Prefix, RcPid) of
        {ok, KeyManifests} ->
            {ok, lists:keysort(1, KeyManifests)};
        {error, Reason} ->
            {error, Reason}
    end.

active_manifests(ManifestBucket, Prefix, RcPid) ->
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
    {ok, ManifestPbc} = riak_cs_riak_client:manifest_pbc(RcPid),
    {ok, ReqId} = riakc_pb_socket:mapred_stream(ManifestPbc, Input, Query, self()),
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
    Handler = fun(Resolved) ->
                      case riak_cs_manifest_utils:active_manifest(Resolved) of
                          {ok, Manifest} ->
                              [{riak_object:key(Object), {ok, Manifest}}];
                          _ ->
                              []
                      end
              end,
    maybe_process_resolved(Object, Handler, []).

maybe_process_resolved(Object, ResolvedManifestsHandler, ErrorReturn) ->
    try
        AllManifests = [ binary_to_term(V)
                         || {_, V} = Content <- riak_object:get_contents(Object),
                            not has_tombstone(Content) ],
        Upgraded = riak_cs_manifest_utils:upgrade_wrapped_manifests(AllManifests),
        Resolved = riak_cs_manifest_resolution:resolve(Upgraded),
        ResolvedManifestsHandler(Resolved)
    catch Type:Reason ->
            StackTrace = erlang:get_stacktrace(),
            _ = lager:log(error,
                          self(),
                          "Riak CS object mapreduce failed for ~p:~p with reason ~p:~p"
                          "at ~p",
                          [riak_object:bucket(Object),
                           riak_object:key(Object),
                           Type,
                           Reason,
                           StackTrace]),
            ErrorReturn
    end.

%% Pipe all the bucket listing results through a passthrough reduce
%% phase.  This is just a temporary kludge until the sink backpressure
%% work is done.
reduce_keys_and_manifests(Acc, _) ->
    Acc.

-spec sha_mac(iolist() | binary(), iolist() | binary()) -> binary().
-spec sha(binary()) -> binary().

-ifdef(new_hash).
sha_mac(Key,STS) -> crypto:hmac(sha, Key,STS).
sha(Bin) -> crypto:hash(sha, Bin).

-else.
sha_mac(Key,STS) -> crypto:sha_mac(Key,STS).
sha(Bin) -> crypto:sha(Bin).

-endif.

-spec md5(string() | binary()) -> digest().
md5(Bin) when is_binary(Bin) ->
    md5_final(md5_update(md5_init(), Bin));
md5(List) when is_list(List) ->
    md5(list_to_binary(List)).

-define(MAX_UPDATE_SIZE, (32*1024)).

-ifdef(new_hash).
-spec md5_init() -> crypto_context().
-spec md5_update(crypto_context(), binary()) -> crypto_context().
-spec md5_final(crypto_context()) -> digest().

md5_init() -> crypto:hash_init(md5).

md5_update(Ctx, Bin) when size(Bin) =< ?MAX_UPDATE_SIZE ->
    crypto:hash_update(Ctx, Bin);
md5_update(Ctx, <<Part:?MAX_UPDATE_SIZE/binary, Rest/binary>>) ->
    md5_update(crypto:hash_update(Ctx, Part), Rest).
md5_final(Ctx) -> crypto:hash_final(Ctx).

-else.
-spec md5_init() -> binary().
-spec md5_update(binary(), binary()) -> binary().
-spec md5_final(binary()) -> digest().

md5_init() -> crypto:md5_init().

md5_update(Ctx, Bin) when size(Bin) =< ?MAX_UPDATE_SIZE ->
    crypto:md5_update(Ctx, Bin);
md5_update(Ctx, <<Part:?MAX_UPDATE_SIZE/binary, Rest/binary>>) ->
    md5_update(crypto:md5_update(Ctx, Part), Rest).

md5_final(Ctx) -> crypto:md5_final(Ctx).
-endif.

%% internal fun to retrieve the riak object
%% at a bucket/key
-spec get_manifests_raw(riak_client(), binary(), binary()) ->
    {ok, riakc_obj:riakc_obj()} | {error, term()}.
get_manifests_raw(RcPid, Bucket, Key) ->
    ManifestBucket = to_bucket_name(objects, Bucket),
    ok = riak_cs_riak_client:set_bucket_name(RcPid, Bucket),
    {ok, ManifestPbc} = riak_cs_riak_client:manifest_pbc(RcPid),
    riakc_pb_socket:get(ManifestPbc, ManifestBucket, Key).

%% @doc
-spec get_manifests(riak_client(), binary(), binary()) ->
    {ok, term(), term()} | {error, term()}.
get_manifests(RcPid, Bucket, Key) ->
    case get_manifests_raw(RcPid, Bucket, Key) of
        {ok, Object} ->
            Manifests = manifests_from_riak_object(Object),
            maybe_log_warning(Bucket, Key, Object, Manifests),
            _  = gc_deleted_while_writing_manifests(Object, Manifests, Bucket, Key, RcPid),
            {ok, Object, Manifests};
        {error, _Reason}=Error ->
            Error
    end.

gc_deleted_while_writing_manifests(Object, Manifests, Bucket, Key, RcPid) ->
    UUIDs = riak_cs_manifest_utils:deleted_while_writing(Manifests),
    riak_cs_gc:gc_specific_manifests(UUIDs, Object, Bucket, Key, RcPid).

-spec maybe_log_warning(binary(), binary(), riakc_obj:riakc_obj(), [term()]) -> ok.
maybe_log_warning(Bucket, Key, Object, Manifests) ->
    maybe_log_warning(
      Bucket, Key,
      riakc_obj:value_count(Object),
      riak_cs_config:get_env(riak_cs, manifest_warn_siblings,
                             ?DEFAULT_MANIFEST_WARN_SIBLINGS),
      "Many manifest siblings for key", "siblings"),
    maybe_log_warning(
      Bucket, Key,
      %% Approximate object size by the sum of only values, ignoring metadata
      lists:sum([byte_size(V) || V <- riakc_obj:get_values(Object)]),
      riak_cs_config:get_env(riak_cs, manifest_warn_bytes,
                             ?DEFAULT_MANIFEST_WARN_BYTES),
      "Large manifest size for key", "bytes"),
    maybe_log_warning(
      Bucket, Key,
      length(Manifests),
      riak_cs_config:get_env(riak_cs, manifest_warn_history,
                             ?DEFAULT_MANIFEST_WARN_HISTORY),
      "Long manifest history for key", "manifests"),
    ok.

-spec maybe_log_warning(binary(), binary(), disabled | non_neg_integer(),
                        non_neg_integer(), string(), string()) -> ok.
maybe_log_warning(Bucket, Key, Actual, Threshold, MessagePrefix, MessageSuffix) ->
    case Threshold of
        disabled -> ok;
        _ when Actual < Threshold -> ok;
        _ -> _ = lager:warning("~s ~s/~s: ~p ~s",
                               [MessagePrefix, Bucket, Key, Actual, MessageSuffix])
    end.


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
-spec get_user('undefined' | list(), riak_client()) -> {ok, {rcs_user(), riakc_obj:riakc_obj()}} | {error, term()}.
get_user(undefined, _RcPid) ->
    {error, no_user_key};
get_user(KeyId, RcPid) ->
    %% Check for and resolve siblings to get a
    %% coherent view of the bucket ownership.
    BinKey = list_to_binary(KeyId),
    case riak_cs_riak_client:get_user(RcPid, BinKey) of
        {ok, {Obj, KeepDeletedBuckets}} ->
            case riakc_obj:value_count(Obj) of
                1 ->
                    Value = binary_to_term(riakc_obj:get_value(Obj)),
                    User = update_user_record(Value),
                    Buckets = riak_cs_bucket:resolve_buckets([Value], [], KeepDeletedBuckets),
                    {ok, {User?RCS_USER{buckets=Buckets}, Obj}};
                0 ->
                    {error, no_value};
                _ ->
                    Values = [binary_to_term(Value) ||
                                 Value <- riakc_obj:get_values(Obj),
                                 Value /= <<>>  % tombstone
                             ],
                    User = update_user_record(hd(Values)),
                    Buckets = riak_cs_bucket:resolve_buckets(Values, [], KeepDeletedBuckets),
                    {ok, {User?RCS_USER{buckets=Buckets}, Obj}}
            end;
        Error ->
            Error
    end.

%% @doc Retrieve a Riak CS user's information based on their
%% canonical id string.
%% @TODO May want to use mapreduce job for this.
-spec get_user_by_index(binary(), binary(), riak_client()) ->
                               {ok, {rcs_user(), term()}} |
                               {error, term()}.
get_user_by_index(Index, Value, RcPid) ->
    case get_user_index(Index, Value, RcPid) of
        {ok, KeyId} ->
            get_user(KeyId, RcPid);
        {error, _}=Error1 ->
            Error1
    end.

%% @doc Query `Index' for `Value' in the users bucket.
-spec get_user_index(binary(), binary(), riak_client()) -> {ok, string()} | {error, term()}.
get_user_index(Index, Value, RcPid) ->
    {ok, MasterPbc} = riak_cs_riak_client:master_pbc(RcPid),
    case riakc_pb_socket:get_index(MasterPbc, ?USER_BUCKET, Index, Value) of
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

-spec n_val_1_get_requests() -> boolean().
n_val_1_get_requests() ->
    riak_cs_config:get_env(riak_cs, n_val_1_get_requests,
                           ?N_VAL_1_GET_REQUESTS).

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
%% from the `request_pool' connection pool of the master bag.
-spec riak_connection() -> {ok, pid()} | {error, term()}.
riak_connection() ->
    riak_connection(riak_cs_riak_client:pbc_pool_name(master)).

%% @doc Get a protobufs connection to the riak cluster
%% from the specified connection pool of the master bag.
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
-spec save_user(rcs_user(), riakc_obj:riakc_obj(), riak_client()) -> ok | {error, term()}.
save_user(User, UserObj, RcPid) ->
    riak_cs_riak_client:save_user(RcPid, User, UserObj).

%% @doc Set the ACL for an object. Existing ACLs are only
%% replaced, they cannot be updated.
-spec set_object_acl(binary(), binary(), lfs_manifest(), acl(), riak_client()) ->
            ok | {error, term()}.
set_object_acl(Bucket, Key, Manifest, Acl, RcPid) ->
    StartTime = os:timestamp(),
    {ok, ManiPid} = riak_cs_manifest_fsm:start_link(Bucket, Key, RcPid),
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

-spec second_resolution_timestamp(erlang:timestamp()) -> non_neg_integer().
%% @doc Return the number of seconds this timestamp represents. Truncated to
%% seconds, as an integer.
second_resolution_timestamp({MegaSecs, Secs, _MicroSecs}) ->
    (MegaSecs * 1000000) + Secs.

%% same as timestamp_to_milliseconds below
-spec timestamp_to_seconds(erlang:timestamp()) -> number().
timestamp_to_seconds({MegaSecs, Secs, MicroSecs}) ->
    (MegaSecs * 1000000) + Secs + (MicroSecs / 1000000).

%% riak_cs_utils.erl:991: Invalid type specification for function riak_cs_utils:timestamp_to_milliseconds/1. The success typing is ({number(),number(),number()}) -> float()
%% this is also a derp that dialyzer shows above message when defined
%% like this, as manpage says it's three-integer tuple :
%% -spec timestamp_to_milliseconds(erlang:timestamp()) -> integer().
-spec timestamp_to_milliseconds(erlang:timestamp()) -> number().
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

%% @private
%% `Bucket' should be the raw bucket name,
%% we'll take care of calling `to_bucket_name'
-spec key_exists(riak_client(), binary(), binary()) -> boolean().
key_exists(RcPid, Bucket, Key) ->
    key_exists_handle_get_manifests(get_manifests(RcPid, Bucket, Key)).

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


%% ===================================================================
%% Internal functions
%% ===================================================================

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

%% @doc Strip off the user name portion of an email address
-spec display_name(string()) -> string().
display_name(Email) ->
    Index = string:chr(Email, $@),
    string:sub_string(Email, 1, Index-1).

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
-spec generate_key(binary()) -> [byte()].
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

%% @doc Update a user record from a previous version if necessary.
-spec update_user_record(rcs_user()) -> rcs_user().
update_user_record(User=?RCS_USER{buckets=Buckets}) ->
    User?RCS_USER{buckets=[riak_cs_bucket:update_bucket_record(Bucket) ||
                              Bucket <- Buckets]};
update_user_record(User=#moss_user_v1{}) ->
    ?RCS_USER{name=User#moss_user_v1.name,
              display_name=User#moss_user_v1.display_name,
              email=User#moss_user_v1.email,
              key_id=User#moss_user_v1.key_id,
              key_secret=User#moss_user_v1.key_secret,
              canonical_id=User#moss_user_v1.canonical_id,
              buckets=[riak_cs_bucket:update_bucket_record(Bucket) ||
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
