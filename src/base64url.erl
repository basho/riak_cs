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
%%      the _ character, and + is replaced with -.
%%      This replacement scheme is named "base64url" by
%%      http://en.wikipedia.org/wiki/Base64

-module(base64url).

-export([decode/1,
         decode_to_string/1,
         encode/1,
         encode_to_string/1,
         mime_decode/1,
         mime_decode_to_string/1]).

-spec decode(binary() | [any()]) -> binary().
decode(Base64url) ->
    base64:decode(urldecode(Base64url)).

-spec decode_to_string(binary() | [any()]) -> string().
decode_to_string(Base64url) ->
    base64:decode_to_string(urldecode(Base64url)).

-spec mime_decode(binary() | [any()]) -> binary().
mime_decode(Base64url) ->
    base64:mime_decode(urldecode(Base64url)).

-spec mime_decode_to_string(binary() | [any()]) -> string().
mime_decode_to_string(Base64url) ->
    base64:mime_decode_to_string(urldecode(Base64url)).

-spec encode(binary() | string()) -> <<_:_*1>> | [byte()].
encode(Data) ->
    urlencode(base64:encode(Data)).

-spec encode_to_string(binary() | string()) -> <<_:_*1>> | [byte()].
encode_to_string(Data) ->
    urlencode(base64:encode_to_string(Data)).

-spec urlencode(binary() | [1..255]) -> <<_:_*1>> | [byte()].
urlencode(Base64) when is_list(Base64) ->
    [urlencode_digit(D) || D <- Base64];
urlencode(Base64) when is_binary(Base64) ->
    << << (urlencode_digit(D)) >> || <<D>> <= Base64 >>.

-spec urldecode(binary() | [any()]) -> <<_:_*1>> | [any()].
urldecode(Base64url) when is_list(Base64url) ->
    [urldecode_digit(D) || D <- Base64url ];
urldecode(Base64url) when is_binary(Base64url) ->
    << << (urldecode_digit(D)) >> || <<D>> <= Base64url >>.

-spec urlencode_digit(byte()) -> byte().
urlencode_digit($/) -> $_;
urlencode_digit($+) -> $-;
urlencode_digit(D)  -> D.

-spec urldecode_digit(_) -> any().
urldecode_digit($_) -> $/;
urldecode_digit($-) -> $+;
urldecode_digit(D)  -> D.
