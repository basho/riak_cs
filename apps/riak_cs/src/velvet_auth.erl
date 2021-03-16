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

-module(velvet_auth).

-include("riak_cs.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-export([request_signature/4]).

%% ===================================================================
%% Public API
%% ===================================================================

%% Calculate a signature for inclusion in a client request.
-type http_verb() :: 'GET' | 'HEAD' | 'PUT' | 'POST' | 'DELETE'.
-spec request_signature(http_verb(),
                        [{string(), string()}],
                        string(),
                        string()) -> string().
request_signature(HttpVerb, RawHeaders, Path, KeyData) ->
    Headers = normalize_headers(RawHeaders),
    BashoHeaders = extract_basho_headers(Headers),
    case proplists:is_defined("x-basho-date", Headers) of
        true ->
            Date = "\n";
        false ->
            Date = [proplists:get_value("date", Headers), "\n"]
    end,
    case proplists:get_value("content-md5", Headers) of
        undefined ->
            CMD5 = [];
        CMD5 ->
            ok
    end,
    case proplists:get_value("content-type", Headers) of
        undefined ->
            ContentType = [];
        ContentType ->
            ok
    end,
    STS = [atom_to_list(HttpVerb),
           "\n",
           CMD5,
           "\n",
           ContentType,
           "\n",
           Date,
           BashoHeaders,
           Path],

    base64:encode_to_string(
      riak_cs_utils:sha_mac(KeyData, STS)).

normalize_headers(Headers) ->
    FilterFun =
        fun({K, V}, Acc) ->
                LowerKey = string:to_lower(any_to_list(K)),
                [{LowerKey, V} | Acc]
        end,
    ordsets:from_list(lists:foldl(FilterFun, [], Headers)).

extract_basho_headers(Headers) ->
    FilterFun =
        fun({K, V}, Acc) ->
                case lists:prefix("x-basho-", K) of
                    true ->
                        [[K, ":", V, "\n"] | Acc];
                    false ->
                        Acc
                end
        end,
    ordsets:from_list(lists:foldl(FilterFun, [], Headers)).

any_to_list(V) when is_list(V) ->
    V;
any_to_list(V) when is_atom(V) ->
    atom_to_list(V);
any_to_list(V) when is_binary(V) ->
    binary_to_list(V);
any_to_list(V) when is_integer(V) ->
    integer_to_list(V).
