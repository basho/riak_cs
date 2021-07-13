%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2016 Basho Technologies, Inc.  All Rights Reserved,
%%               2021 TI Tokyo    All Rights Reserved.
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
%% -------------------------------------------------------------------

%% @doc base64url is a wrapper around the base64 module to produce
%%      base64-compatible encodings that are URL safe.
%%      The / character in normal base64 encoding is replaced with
%%      the _ character, + is replaced with -,and = is omitted.
%%      This replacement scheme is named "base64url" by
%%      http://en.wikipedia.org/wiki/Base64

-module(base64url).

-export([decode/1,
         decode_to_string/1,
         encode/1,
         encode_to_string/1]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

decode(Base64url) ->
    base64:decode(append_equals(urldecode(Base64url))).

decode_to_string(Base64url) ->
    base64:decode_to_string(append_equals(urldecode(Base64url))).

encode(Data) ->
    urlencode(strip_equals(base64:encode(Data))).

encode_to_string(Data) ->
    urlencode(strip_equals(base64:encode_to_string(Data))).

-spec strip_equals(binary() | string()) -> binary()|string().
%% @private Strip off trailing '=' characters.
strip_equals(Str) when is_list(Str) ->
    string:strip(Str, right, $=);
strip_equals(Bin) when is_binary(Bin) ->
    LCS = binary:longest_common_suffix([Bin, <<"===">>]),
    binary:part(Bin, 0, byte_size(Bin)-LCS).

-spec append_equals(binary()|string()) -> binary()|string().
%% @private Append trailing '=' characters to make result legal Base64 length.
%% The most common use case will be a B64-encoded UUID, requiring the addition
%% of 2 characters, so that's the first check. We assume 0 and 3 are equally
%% likely.
%% Because B64 encoding spans all bytes across two characters, the remainder
%% of (length / 4) can never be 1 with a valid encoding, so we throw a badarg
%% with the argument here rather than letting it percolate up from stdlib with
%% no worthwhile information.
append_equals(Str) when is_list(Str) ->
    case length(Str) rem 4 of
        2 ->
            Str ++ "==";
        0 ->
            Str;
        3 ->
            Str ++ "=";
        1 ->
            erlang:error(badarg, [Str])
    end;
append_equals(Bin) when is_binary(Bin) ->
    case byte_size(Bin) rem 4 of
        2 ->
            <<Bin/binary, "==">>;
        0 ->
            Bin;
        3 ->
            <<Bin/binary, "=">>;
        1 ->
            erlang:error(badarg, [Bin])
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

illegal_char_REs() ->
    BinRE = binary:compile_pattern([<<"+">>, <<"/">>, <<"=">>]),
    {ok, StrRE} = re:compile("[\\+/=]"),
    {BinRE, StrRE}.

encode_decode_test() ->
    crypto:start(),
    % Make sure Rand is at least twice as long as the highest count, but
    % because we're depleting the entropy pool don't go overboard!
    RandLen = 2050,
    % crypto:rand:bytes/1 would be fine for us, but it's gone in OTP-19,
    % so use the good stuff rather than bothering with a version check.
    BinRand = crypto:strong_rand_bytes(RandLen),
    % Swap halves so the binary and string tests don't use the same sequences.
    HalfLen = (RandLen div 2),
    RandLo  = binary_part(BinRand, 0, HalfLen),
    RandHi  = binary_part(BinRand, HalfLen, HalfLen),
    StrRand = << RandHi/binary, RandLo/binary >>,
    {BinRE, StrRE} = illegal_char_REs(),
    test_encode_decode_uuid(256,    BinRE, StrRE),
    test_encode_decode_binary(1024, BinRE, StrRE, BinRand),
    test_encode_decode_string(384,  BinRE, StrRE, StrRand).

test_encode_decode_uuid(0, _, _) ->
    ok;
test_encode_decode_uuid(Count, BinRE, StrRE) ->
    test_encode_decode(uuid:get_v4(), BinRE, StrRE),
    test_encode_decode_uuid((Count - 1), BinRE, StrRE).

test_encode_decode_binary(0, _, _, _) ->
    ok;
test_encode_decode_binary(Count, BinRE, StrRE, Rand) ->
    test_encode_decode(binary:part(Rand, Count, Count), BinRE, StrRE),
    test_encode_decode_binary((Count - 1), BinRE, StrRE, Rand).

test_encode_decode_string(0, _, _, _) ->
    ok;
test_encode_decode_string(Count, BinRE, StrRE, Rand) ->
    test_encode_decode(binary:bin_to_list(Rand, Count, Count), BinRE, StrRE),
    test_encode_decode_string((Count - 1), BinRE, StrRE, Rand).

test_encode_decode(Data, BinRE, StrRE) when is_binary(Data) ->
    EncBin = base64url:encode(Data),
    EncStr = base64url:encode_to_string(Data),
    ?assertEqual(EncStr, binary_to_list(EncBin)),
    ?assertEqual(nomatch, binary:match(EncBin, BinRE)),
    ?assertEqual(nomatch, re:run(EncStr, StrRE)),
    ?assertEqual(Data, base64url:decode(EncBin)),
    ?assertEqual(Data, base64url:decode(EncStr));

test_encode_decode(Data, BinRE, StrRE) when is_list(Data) ->
    EncBin = base64url:encode(Data),
    EncStr = base64url:encode_to_string(Data),
    ?assertEqual(EncStr, binary_to_list(EncBin)),
    ?assertEqual(nomatch, binary:match(EncBin, BinRE)),
    ?assertEqual(nomatch, re:run(EncStr, StrRE)),
    ?assertEqual(Data, base64url:decode_to_string(EncBin)),
    ?assertEqual(Data, base64url:decode_to_string(EncStr)).

-endif.
