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

-module(riak_cs_aws_auth_v4_test).

-compile(export_all).
-compile(nowarn_export_all).

-include("riak_cs.hrl").
-include_lib("eunit/include/eunit.hrl").

%% Test cases at http://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
-define(ACCESS_KEY_ID, "AKIAIOSFODNN7EXAMPLE").
-define(SECRET_ACCESS_KEY, "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY").
-define(VERSION, {1, 1}).
-define(BUCKET, "examplebucket").

auth_v4_test_() ->
    {setup,
     fun setup/0,
     fun teardown/1,
     [
      {"GET Object example"           , ?_test(auth_v4_GET_Object())},
      {"GET Object example, with extra spaces in Authorization header",
       ?_test(auth_v4_GET_Object_with_extra_spaces())},
      {"PUT Object example"           , ?_test(auth_v4_PUT_Object())},
      {"GET Object lifecycle example" , ?_test(auth_v4_GET_Object_Lifecycle())},
      {"GET Bucket example"           , ?_test(auth_v4_GET_Bucket())}
     ]}.

setup() ->
    application:set_env(riak_cs, verify_client_clock_skew, false),
    application:set_env(riak_cs, auth_v4_enabled, true).

teardown(_) ->
    application:unset_env(riak_cs, verify_client_clock_skew),
    application:unset_env(riak_cs, auth_v4_enabled).

auth_v4_GET_Object() ->
    Method = 'GET',
    OriginalPath = "/test.txt",
    AuthAttrs = [{"Credential",
                  "AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request"},
                 {"SignedHeaders", "host;range;x-amz-content-sha256;x-amz-date"},
                 {"Signature",
                  "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"}],
    AllHeaders = mochiweb_headers:make(
                   [{"Host", "examplebucket.s3.amazonaws.com"},
                    {"Date", "Fri, 24 May 2013 00:00:00 GMT"},
                    {"Range", "bytes=0-9"},
                    {"x-amz-content-sha256",
                     "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
                    {"x-amz-date", "20130524T000000Z"},
                    {"x-rcs-raw-url", OriginalPath},
                    authorization_header(AuthAttrs)]),
    RD = wrq:create(Method, ?VERSION, "rewritten/path", AllHeaders),
    Context = context_not_used,
    ?assertEqual({?ACCESS_KEY_ID, {v4, AuthAttrs}},
                 riak_cs_aws_auth:identify(RD, Context)),
    ?assertEqual(ok,
                 riak_cs_aws_auth:authenticate(?RCS_USER{key_secret=?SECRET_ACCESS_KEY},
                                               {v4, AuthAttrs}, RD, Context)).

auth_v4_GET_Object_with_extra_spaces() ->
    Method = 'GET',
    OriginalPath = "/test.txt",
    AuthAttrs = [{"Credential",
                  "AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request"},
                 {"SignedHeaders", "host;range;x-amz-content-sha256;x-amz-date"},
                 {"Signature",
                  "f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"}],
    AllHeaders = mochiweb_headers:make(
                   [{"Host", "examplebucket.s3.amazonaws.com"},
                    {"Date", "Fri, 24 May 2013 00:00:00 GMT"},
                    {"Range", "bytes=0-9"},
                    {"x-amz-content-sha256",
                     "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
                    {"x-amz-date", "20130524T000000Z"},
                    {"x-rcs-raw-url", OriginalPath},
                    authorization_header(AuthAttrs, with_space)]),
    RD = wrq:create(Method, ?VERSION, "rewritten/path", AllHeaders),
    Context = context_not_used,
    ?assertEqual({?ACCESS_KEY_ID, {v4, AuthAttrs}},
                 riak_cs_aws_auth:identify(RD, Context)),
    ?assertEqual(ok,
                 riak_cs_aws_auth:authenticate(?RCS_USER{key_secret=?SECRET_ACCESS_KEY},
                                               {v4, AuthAttrs}, RD, Context)).

auth_v4_PUT_Object() ->
    Method = 'PUT',
    OriginalPath = "/test%24file.text", % `%24=$'
    AuthAttrs = [{"Credential", "AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request"},
                 {"SignedHeaders", "date;host;x-amz-content-sha256;x-amz-date;x-amz-storage-class"},
                 {"Signature", "98ad721746da40c64f1a55b78f14c238d841ea1380cd77a1b5971af0ece108bd"}],
    AllHeaders = mochiweb_headers:make(
                   [{"Host", "examplebucket.s3.amazonaws.com"},
                    {"Date", "Fri, 24 May 2013 00:00:00 GMT"},
                    {"x-amz-storage-class", "REDUCED_REDUNDANCY"},
                    {"x-amz-content-sha256",
                     "44ce7dd67c959e0d3524ffac1771dfbba87d2b6b4b4e99e42034a8b803f8b072"},
                    {"x-amz-date", "20130524T000000Z"},
                    {"x-rcs-raw-url", OriginalPath},
                    authorization_header(AuthAttrs)]),
    RD = wrq:create(Method, ?VERSION, "rewritten/path", AllHeaders),
    Context = context_not_used,
    ?assertEqual({?ACCESS_KEY_ID, {v4, AuthAttrs}},
                 riak_cs_aws_auth:identify(RD, Context)),
    ?assertEqual(ok,
                 riak_cs_aws_auth:authenticate(?RCS_USER{key_secret=?SECRET_ACCESS_KEY},
                                               {v4, AuthAttrs}, RD, Context)).

auth_v4_GET_Object_Lifecycle() ->
    Method = 'GET',
    OriginalPath = "/?lifecycle",
    AuthAttrs = [{"Credential", "AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request"},
                 {"SignedHeaders", "host;x-amz-content-sha256;x-amz-date"},
                 {"Signature", "fea454ca298b7da1c68078a5d1bdbfbbe0d65c699e0f91ac7a200a0136783543"}],

    AllHeaders = mochiweb_headers:make(
                   [{"Host", "examplebucket.s3.amazonaws.com"},
                    {"x-amz-content-sha256",
                     "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
                    {"x-amz-date", "20130524T000000Z"},
                    {"x-rcs-raw-url", OriginalPath},
                    authorization_header(AuthAttrs)]),
    RD = wrq:create(Method, ?VERSION, "rewritten/path", AllHeaders),
    Context = context_not_used,
    ?assertEqual({?ACCESS_KEY_ID, {v4, AuthAttrs}},
                 riak_cs_aws_auth:identify(RD, Context)),
    ?assertEqual(ok,
                 riak_cs_aws_auth:authenticate(?RCS_USER{key_secret=?SECRET_ACCESS_KEY},
                                               {v4, AuthAttrs}, RD, Context)).

auth_v4_GET_Bucket() ->
    Method = 'GET',
    OriginalPath = "/?max-keys=2&prefix=J",
    AuthAttrs = [{"Credential", "AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request"},
                 {"SignedHeaders", "host;x-amz-content-sha256;x-amz-date"},
                 {"Signature", "34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"}],
    AllHeaders = mochiweb_headers:make(
                   [{"Host", "examplebucket.s3.amazonaws.com"},
                    {"x-amz-content-sha256",
                     "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"},
                    {"x-amz-date", "20130524T000000Z"},
                    {"x-rcs-raw-url", OriginalPath},
                    authorization_header(AuthAttrs)]),
    RD = wrq:create(Method, ?VERSION, "rewritten/path", AllHeaders),
    Context = context_not_used,
    ?assertEqual({?ACCESS_KEY_ID, {v4, AuthAttrs}},
                 riak_cs_aws_auth:identify(RD, Context)),
    ?assertEqual(ok,
                 riak_cs_aws_auth:authenticate(?RCS_USER{key_secret=?SECRET_ACCESS_KEY},
                                               {v4, AuthAttrs}, RD, Context)).

authorization_header(AuthAttrs) ->
    authorization_header(AuthAttrs, no_space).

authorization_header(AuthAttrs, SpacesAfterComma) ->
    Separator = case SpacesAfterComma of
                    no_space -> ",";
                    with_space -> ", "
                end,
    {"Authorization",
     "AWS4-HMAC-SHA256 " ++
         string:join([lists:flatten([K, $=, V]) || {K, V} <- AuthAttrs], Separator)}.
