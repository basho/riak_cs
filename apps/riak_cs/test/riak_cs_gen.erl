%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved,
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
%% ---------------------------------------------------------------------

%% @doc Common PropEr generators for Riak CS

-module(riak_cs_gen).

-include_lib("proper/include/proper.hrl").

%% Generators
-export([base64string/0,
         http_verb/0,
         http_version/0,
         http_scheme/0,
         bucket/0,
         bucket_or_blank/0,
         file_name/0,
         vsn/0,
         block_size/0,
         content_length/0,
         bounded_content_length/0,
         content_type/0,
         md5/0,
         uuid/0,
         metadata/0,
         bounded_uuid/0,
         manifest_state/0,
         datetime/0,
         md5_chunk_size/0,
         timestamp/0,
         props/0]).

-export([non_blank_string/0]).

%%====================================================================
%% Generators
%%====================================================================

base64string() ->
    ?LET(Bitstring, bitstring(), base64:encode(Bitstring)).

http_verb() ->
    oneof(['GET', 'PUT', 'HEAD', 'POST', 'DELETE']).

http_version() ->
    oneof([{0,9}, {1,0}, {1,1}]).

http_scheme() ->
    oneof([http, https]).

bucket() ->
    rfc1123_hostname().

bucket_or_blank() ->
    maybe_blank_string().

file_name() ->
    non_blank_string().

vsn() ->
    non_blank_string().

block_size() ->
    elements([bs(El) || El <- [8, 16, 32]]).

content_length() ->
    ?LET(X, large_non_zero_nums(), abs(X)).

bounded_content_length() ->
    ?LET(X, bounded_non_zero_nums(), abs(X)).

content_type() ->
    non_blank_string().

md5() ->
    non_blank_string().

uuid() ->
    non_blank_string().

metadata() ->
    %% TODO: not sure if I could,
    %% just use `dict:new()` as a generator,
    %% but this is more explicit either way
    return(dict:new()).

bounded_uuid() ->
    oneof([<<"uuid-1">>, <<"uuid-2">>, <<"uuid-3">>, <<"uuid-4">>]).

manifest_state() ->
    oneof([writing, active, pending_delete, scheduled_delete]).

datetime() ->
    {{choose(1,5000), choose(1,12), choose(1,28)},
     {choose(0, 23), choose(0, 59), choose(0, 59)}}.

timestamp() ->
    {choose(0, 5000), choose(0, 999999), choose(0, 999999)}.

md5_chunk_size() ->
    oneof([2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048]).

props() ->
    oneof([[], [{deleted, true}]]).

%%====================================================================
%% Helpers
%%====================================================================

large_non_zero_nums() ->
    not_zero(largeint()).

bounded_non_zero_nums() ->
    ?LET(X, not_zero(int()), X * 100000).

non_blank_string() ->
    ?LET(X, not_empty(list(lower_char())), list_to_binary(X)).

maybe_blank_string() ->
    ?LET(X, list(lower_char()), list_to_binary(X)).

%% Generate a lower 7-bit ACSII character that should not cause any problems
%% with utf8 conversion.
lower_char() ->
    choose(16#20, 16#7f).

not_empty(G) ->
    ?SUCHTHAT(X, G, X /= [] andalso X /= <<>>).

not_zero(G) ->
    ?SUCHTHAT(X, G, X /= 0).

bs(Power) ->
    trunc(math:pow(2, Power)).

-define(ALPHABET,
        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ").
-define(DIGIT, "0123456789").

%% @doc This does not support Unicode domain name.
rfc1123_hostname() ->
    ?SUCHTHAT(Hostname, rfc1123_hostname_0(),
              byte_size(Hostname) < 254).

rfc1123_hostname_0() ->
    ?LET(Ds, not_empty(list(label())),
         list_to_binary(string:join(Ds, "."))).

label() ->
    ?SUCHTHAT(Domain, label_0(), length(Domain) < 64).

label_0() ->
    ?LET({S, M, E}, {char_begin(), list(char_mid()), char_end()},
         lists:flatten([S, M, E])).

char_begin() ->
    oneof(?ALPHABET ++ ?DIGIT).

char_mid() ->
    oneof(?ALPHABET ++ "-" ++ ?DIGIT).

char_end() ->
    oneof(?ALPHABET ++ ?DIGIT).
