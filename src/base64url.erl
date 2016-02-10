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

%% @doc base64url is a wrapper around the base64 module to produce
%%      base64-compatible encodings that are URL safe.
%%      The / character in normal base64 encoding is replaced with
%%      the _ character, + is replaced with -,and = is omitted.
%%      This replacement scheme is named "base64url" by
%%      http://en.wikipedia.org/wiki/Base64

-module(base64url).

-include_lib("eunit/include/eunit.hrl").

-export([decode/1,
         decode_to_string/1,
         encode/1,
         encode_to_string/1]).

decode(Base64url) ->
    base64:decode(amend_equal(urldecode(Base64url))).

decode_to_string(Base64url) ->
    base64:decode_to_string(amend_equal(urldecode(Base64url))).

encode(Data) ->
    urlencode(strip_equal(base64:encode(Data))).

encode_to_string(Data) ->
    urlencode(strip_equal(base64:encode_to_string(Data))).

-spec strip_equal(binary() | string()) -> binary()|string().
strip_equal(Encoded) when is_list(Encoded) ->
    hd(string:tokens(Encoded, "="));
strip_equal(Encoded) when is_binary(Encoded) ->
    LCS = binary:longest_common_suffix([Encoded, <<"===">>]),
    binary:part(Encoded, 0, byte_size(Encoded)-LCS).

%% @doc complements '=' if it doesn't have 4*n length
-spec amend_equal(binary()|string()) -> binary()|string().
amend_equal(Encoded) when is_list(Encoded) ->
    Suffix = case length(Encoded) rem 4 of
                 0 -> "";
                 L -> [$=||_<-lists:seq(1,L)]
             end,
    lists:flatten([Encoded, Suffix]);
amend_equal(Bin) when is_binary(Bin) ->
    case byte_size(Bin) rem 4 of
        0 -> Bin;
        1 -> <<Bin/binary, "===">>;
        2 -> <<Bin/binary, "==">>;
        3 -> <<Bin/binary, "=">>
    end.

urlencode(Base64) when is_list(Base64) ->
    [urlencode_digit(D) || D <- Base64];
urlencode(Base64) when is_binary(Base64) ->
    << << (urlencode_digit(D)) >> || <<D>> <= Base64 >>.

urldecode(Base64url) when is_list(Base64url) ->
    [urldecode_digit(D) || D <- Base64url ];
urldecode(Base64url) when is_binary(Base64url) ->
    << << (urldecode_digit(D)) >> || <<D>> <= Base64url >>.

urlencode_digit($/) -> $_;
urlencode_digit($+) -> $-;
urlencode_digit(D)  -> D.

urldecode_digit($_) -> $/;
urldecode_digit($-) -> $+;
urldecode_digit(D)  -> D.

-ifdef(TEST).
equal_strip_amend_test() ->
    %% TODO: rewrite this with EQC
    _ = [begin
             UUID = druuid:v4(),
             Encoded = base64url:encode(UUID),
             ?assertEqual(nomatch, binary:match(Encoded, [<<"=">>, <<"+">>, <<"/">>])),
             ?assertEqual(UUID, base64url:decode(Encoded))
         end || _<- lists:seq(1, 1024)],
    _ = [begin
             UUID = druuid:v4(),
             Encoded = base64url:encode_to_string(UUID),
             ?assertEqual(nomatch, re:run(Encoded, "(=\\+\\/)")),
             ?assertEqual(UUID, base64url:decode(Encoded))
         end || _<- lists:seq(1, 1024)],
    ok.

-endif.
