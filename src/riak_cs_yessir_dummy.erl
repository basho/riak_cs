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

%% @doc Module for riak_kv_yessir_backend callbacks to mimic CS-style storage

-module(riak_cs_yessir_dummy).
-compile(export_all).

default_num_buckets() ->
    1.
default_display_name() ->
    "foobar".
default_public_key() ->
    "J2IP6WGUQ_FNGIAN9AFI".
default_private_key() ->
    "mbB-1VACNsrN0yLAUSpCFmXNNBpAC3X0lPmINA==".
default_canonical_id() ->
    "18983ba0e16e18a2b103ca16b84fad93d12a2fbed1c88048931fb91b0b844ad3".
default_obj_size() ->
    %% 4*1024*1024 + 4.
    80*1024*1024.
default_block_size() ->
    1048576.
default_acl() ->
    {acl_v2,{default_display_name(),
             default_canonical_id(),
             default_public_key()},
     [{{default_display_name(),
        default_canonical_id()},
       ['FULL_CONTROL']}],
     {1370,310148,497003}}.

get_memoized_checksum(Size) ->
    case whereis(?MODULE) of
        undefined ->
            (catch spawn(fun() -> erlang:register(?MODULE, self()),
                                  run_memoized_server(orddict:new())
                         end)),
            timer:sleep(100),
            get_memoized_checksum(Size);
        _Pid ->
            ?MODULE ! {checksum, Size, self()},
            receive
                {checksum_result, X} ->
                    X
            end
    end.

run_memoized_server(D) ->
    receive
        {checksum, Size, Pid} ->
            case orddict:find(Size, D) of
                error ->
                    BlockSize = default_block_size(),
                    WholeBlocks = Size div BlockSize,
                    RemBytes = Size rem BlockSize,
                    X = lists:duplicate(WholeBlocks, <<42:(BlockSize*8)>>) ++
                        [<<42:(RemBytes*8)>>],
                    CSum = my_md5(X),
                    Pid ! {checksum_result, CSum},
                    run_memoized_server(orddict:store(Size, CSum, D));
                {ok, CSum} ->
                    Pid ! {checksum_result, CSum},
                    run_memoized_server(D)
            end
    end.

my_md5(L) ->
    crypto:md5_final(lists:foldl(fun(X, Ctx) -> crypto:md5_update(Ctx, X) end, crypto:md5_init(), L)).

cb_moss_users(_Suffix, Bucket, Key) ->
    V = {rcs_user_v2,"foo bar",default_display_name(),"foobar@example.com",
         Key,
         default_private_key(),
         default_canonical_id(),
         [{moss_bucket_v1,"b"++integer_to_list(X),created,
           "2013-05-02T19:57:03.000Z",{1367,524623,660302}, undefined} ||
             X <- lists:seq(1, default_num_buckets())],
         enabled},
    make_riak_safe_obj(Bucket, Key, riak_object:to_binary(v0, V)).

cb_moss_buckets(_Suffix, Bucket, Key) ->
    V = list_to_binary(default_public_key()),
    Metas = [{<<"X-Moss-Acl">>, term_to_binary(default_acl())}],
    make_riak_safe_obj(Bucket, Key, V, Metas).

cb_object(_Suffix, Bucket, Key) ->
    BlockSize = default_block_size(),
    Size = get_variable_object_size(Key),
    <<UUIDa:5/binary, _/binary>> = erlang:md5([Bucket, Key]),
    UUID = <<UUIDa/binary, BlockSize:(3*8), Size:(8*8)>>,
    V =  [{UUID, % hehheh secret comms channel
           {lfs_manifest_v3,3,default_block_size(),
            {<<"b0">>,Key},              % Don't care about bucket name
            [],"2013-06-04T01:43:28.000Z",
            UUID,
            Size, <<"binary/octet-stream">>,
            get_memoized_checksum(Size),
            active,
            {1370,310148,497212},
            {1370,310148,500525},
            [],undefined,undefined,undefined,undefined,
            default_acl(),
            [],undefined}}],
    make_riak_safe_obj(Bucket, Key, riak_object:to_binary(v0, V)).

%% cb_block(_Suffix, _Bucket, _Key) ->
%%     not_found.
cb_block(_Suffix, Bucket, Key) ->
    <<_Random:5/binary, BlockSize:(3*8), Size:(8*8), BlockNum:32>> = Key,
    EndOfBlock = (BlockNum+1) * BlockSize,
    Bytes = if EndOfBlock > Size ->
                    Size - (BlockNum * BlockSize);
               true ->
                    BlockSize
            end,
    Bin = case get(Bytes) of
              undefined ->
                  io:format("QQQ Bytes ~p\n", [Bytes]),
                  B = <<42:(Bytes*8)>>,
                  put(Bytes, B),
                  B;
              Else ->
                  Else
          end,
    make_riak_safe_obj(Bucket, Key, Bin).

cb_not_found(_, _, _) ->
    not_found.

get_variable_object_size(Key) when is_binary(Key) ->
    get_variable_object_size(binary_to_list(Key));
get_variable_object_size([$g|Rest]) ->
    {Size, _} = string:to_integer(Rest),
    Size * 1024*1024*1024;
get_variable_object_size([$m|Rest]) ->
    {Size, _} = string:to_integer(Rest),
    Size * 1024*1024;
get_variable_object_size([$k|Rest]) ->
    {Size, _} = string:to_integer(Rest),
    Size * 1024;
get_variable_object_size([$b|Rest]) ->
    {Size, _} = string:to_integer(Rest),
    Size;
get_variable_object_size(_) ->
    default_obj_size().

