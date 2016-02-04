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
         delete_object/3,
         encode_term/1,
         has_tombstone/1,
         map_keys_and_manifests/3,
         maybe_process_resolved/3,
         sha_mac/2,
         sha/1,
         md5/1,
         md5_init/0,
         md5_update/2,
         md5_final/1,
         reduce_keys_and_manifests/2,
         active_manifest_from_response/1,
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
         safe_base64url_decode/1,
         safe_list_to_integer/1,
         set_object_acl/5,
         second_resolution_timestamp/1,
         timestamp_to_seconds/1,
         timestamp_to_milliseconds/1,
         update_obj_value/2,
         pid_to_binary/1,
         from_bucket_name/1,
         to_bucket_name/2,
         big_end_key/1,
         big_end_key/0,
         stanchion_data/0,
         camel_case/1,
         capitalize/1
        ]).

-include("riak_cs.hrl").
-include_lib("riak_pb/include/riak_pb_kv_codec.hrl").
-include_lib("riakc/include/riakc.hrl").

-ifdef(TEST).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").
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

%% @doc Mark all active manifests as pending_delete.
%% If successful, returns a list of the UUIDs that were marked for
%% Garbage collection. Otherwise returns an error. Note,
%% {error, notfound} counts as success in this case,
%% with the list of UUIDs being [].
-spec delete_object(binary(), binary(), riak_client()) ->
                           {ok, [binary()]} | {error, term()}.
delete_object(Bucket, Key, RcPid) ->
    riak_cs_gc:gc_active_manifests(Bucket, Key, RcPid).

-spec encode_term(term()) -> binary().
encode_term(Term) ->
    case riak_cs_config:use_t2b_compression() of
        true ->
            term_to_binary(Term, [compressed]);
        false ->
            term_to_binary(Term)
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
sha_mac(Key,STS) -> crypto:hmac(sha, Key,STS).

-spec sha(binary()) -> binary().
sha(Bin) -> crypto:hash(sha, Bin).

-spec md5(iodata()) -> digest().
md5(IOData) ->
    crypto:hash(md5, IOData).

-define(MAX_UPDATE_SIZE, (32*1024)).

-spec md5_init() -> crypto_context().
md5_init() -> crypto:hash_init(md5).

-spec md5_update(crypto_context(), binary()) -> crypto_context().
md5_update(Ctx, Bin) when size(Bin) =< ?MAX_UPDATE_SIZE ->
    crypto:hash_update(Ctx, Bin);
md5_update(Ctx, <<Part:?MAX_UPDATE_SIZE/binary, Rest/binary>>) ->
    md5_update(crypto:hash_update(Ctx, Part), Rest).

-spec md5_final(crypto_context()) -> digest().
md5_final(Ctx) -> crypto:hash_final(Ctx).

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

%% @doc Determine if a set of contents of a riak object has a tombstone.
-spec has_tombstone({riakc_obj:metadata(), binary()}) -> boolean().
has_tombstone({_, <<>>}) ->
    true;
has_tombstone({MD, _V}) ->
    dict:is_key(?MD_DELETED, MD) =:= true.

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

%% @doc Set the ACL for an object. Existing ACLs are only
%% replaced, they cannot be updated.
-spec set_object_acl(binary(), binary(), lfs_manifest(), acl(), riak_client()) ->
                            ok | {error, term()}.
set_object_acl(Bucket, Key, Manifest, Acl, RcPid) ->
    {ok, ManiPid} = riak_cs_manifest_fsm:start_link(Bucket, Key, RcPid),
    try
        _ActiveMfst = riak_cs_manifest_fsm:get_active_manifest(ManiPid),
        UpdManifest = Manifest?MANIFEST{acl=Acl},
        riak_cs_manifest_fsm:update_manifest_with_confirmation(ManiPid, UpdManifest)
    after
        riak_cs_manifest_fsm:stop(ManiPid)
    end.

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
    Prefix = case Type of
                 objects -> ?OBJECT_BUCKET_PREFIX;
                 blocks ->  ?BLOCK_BUCKET_PREFIX
             end,
    BucketHash = md5(Bucket),
    <<Prefix/binary, BucketHash/binary>>.

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
    key_exists_handle_get_manifests(riak_cs_manifest:get_manifests(RcPid, Bucket, Key)).


-spec big_end_key() -> binary().
big_end_key() ->
    big_end_key(<<>>).

-spec big_end_key(Prefix::binary() | undefined) -> binary().
big_end_key(undefined) ->
    big_end_key(<<>>);
big_end_key(Prefix) ->
    Padding = case riak_cs_config:max_key_length() of
                  unlimited ->
                      <<>>;
                  MaxLen when byte_size(Prefix) > MaxLen ->
                      <<>>;
                  MaxLen ->
                      binary:copy(<<255>>, MaxLen - byte_size(Prefix))
              end,
    <<Prefix/binary, 255, Padding/binary>>.

%% @doc Return `stanchion' configuration data.
-spec stanchion_data() -> {string(), pos_integer(), boolean()}.
stanchion_data() ->
    riak_cs_config:stanchion().

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

%% @doc Convert a pid to a binary
-spec pid_to_binary(pid()) -> binary().
pid_to_binary(Pid) ->
    list_to_binary(pid_to_list(Pid)).

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

-spec camel_case(atom() | string()) -> string().
camel_case(Atom) when is_atom(Atom) ->
    camel_case(atom_to_list(Atom));
camel_case(String) when is_list(String) ->
    string:join([capitalize(Token) ||
                 Token <- string:tokens(String, "_")], "").

-spec capitalize(string()) -> string().
capitalize("") -> "";
capitalize([H|T]) -> string:to_upper([H]) ++ T.

-ifdef(TEST).

camel_case_test() ->
    ?assertEqual("", camel_case("")),
    ?assertEqual("A", camel_case("a")),
    ?assertEqual("A", camel_case(a)),
    ?assertEqual("A", camel_case("A")),
    ?assertEqual("A", camel_case('A')),
    ?assertEqual("Abc", camel_case("abc")),
    ?assertEqual("Abc", camel_case(abc)),
    ?assertEqual("AbcXyz", camel_case("abc_xyz")),
    ?assertEqual("AbcXyz", camel_case(abc_xyz)),
    ?assertEqual("AbcXYZ", camel_case("abc_XYZ")),
    ?assertEqual("AbcXYZ", camel_case(abc_XYZ)),
    ?assertEqual("AbcXyz123", camel_case("abc_xyz_123")),
    ?assertEqual("AbcXyz123", camel_case(abc_xyz_123)).

-endif.
