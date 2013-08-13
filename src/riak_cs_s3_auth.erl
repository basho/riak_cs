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

-module(riak_cs_s3_auth).

-behavior(riak_cs_auth).

-export([identify/2, authenticate/4]).

-include("riak_cs.hrl").
-include("s3_api.hrl").
-include_lib("webmachine/include/webmachine.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-define(QS_KEYID, "AWSAccessKeyId").
-define(QS_SIGNATURE, "Signature").

%% ===================================================================
%% Public API
%% ===================================================================

-spec identify(term(), term()) -> {string() | undefined , string()}.
identify(RD,_Ctx) ->
    case wrq:get_req_header("authorization", RD) of
        undefined ->
            {wrq:get_qs_value(?QS_KEYID, RD), wrq:get_qs_value(?QS_SIGNATURE, RD)};
        AuthHeader ->
            parse_auth_header(AuthHeader)
    end.

-spec authenticate(rcs_user(), string(), term(), term()) -> ok | {error, atom()}.
authenticate(User, Signature, RD, _Ctx) ->
    CalculatedSignature = calculate_signature(User?RCS_USER.key_secret, RD),
    case check_auth(Signature, CalculatedSignature) of
        true ->
            Expires = wrq:get_qs_value("Expires", RD),
            case Expires of
                undefined ->
                    ok;
                _ ->
                    {MegaSecs, Secs, _} = os:timestamp(),
                    Now = (MegaSecs * 1000000) + Secs,
                    case Now > list_to_integer(Expires) of
                        true ->
                            %% @TODO Not sure if this is the proper error
                            %% to return; will have to check after testing.
                            {error, invalid_authentication};
                        false ->
                            ok
                    end
            end;
        _ ->
            {error, invalid_authentication}
    end.

%% ===================================================================
%% Internal functions
%% ===================================================================

parse_auth_header("AWS " ++ Key) ->
    case string:tokens(Key, ":") of
        [KeyId, KeyData] ->
            {KeyId, KeyData};
        _ -> {undefined, undefined}
    end;
parse_auth_header(_) ->
    {undefined, undefined}.

calculate_signature(KeyData, RD) ->
    Headers = riak_cs_wm_utils:normalize_headers(RD),
    AmazonHeaders = riak_cs_wm_utils:extract_amazon_headers(Headers),
    OriginalResource = riak_cs_s3_rewrite:original_resource(RD),
    Resource = case OriginalResource of
        undefined -> []; %% TODO: get noisy here?
        {Path,QS} -> [Path, canonicalize_qs(QS)]
    end,
    Expires = wrq:get_qs_value("Expires", RD),
    case Expires of
        undefined ->
            case proplists:is_defined("x-amz-date", Headers) of
                true ->
                    Date = "\n";
                false ->
                    Date = [wrq:get_req_header("date", RD), "\n"]
            end;
        _ ->
            Date = Expires ++ "\n"
    end,
    case wrq:get_req_header("content-md5", RD) of
        undefined ->
            CMD5 = [];
        CMD5 ->
            ok
    end,
    case wrq:get_req_header("content-type", RD) of
        undefined ->
            ContentType = [];
        ContentType ->
            ok
    end,
    STS = [atom_to_list(wrq:method(RD)), "\n",
           CMD5,
           "\n",
           ContentType,
           "\n",
           Date,
           AmazonHeaders,
           Resource],
    _ = lager:debug("STS: ~p", [STS]),
    base64:encode_to_string(
      crypto:sha_mac(KeyData, STS)).

check_auth(PresentedSignature, CalculatedSignature) ->
    PresentedSignature == CalculatedSignature.

canonicalize_qs(QS) ->
    %% The QS must be sorted be canonicalized,
    %% and since `canonicalize_qs/2` builds up the
    %% accumulator with cons, it comes back in reverse
    %% order. So we'll sort then reverise, so cons'ing
    %% actually puts it back in the correct order
    ReversedSorted = lists:reverse(lists:sort(QS)),
    canonicalize_qs(ReversedSorted, []).

canonicalize_qs([], []) ->
    [];
canonicalize_qs([], Acc) ->
    lists:flatten(["?", Acc]);
canonicalize_qs([{K, []}|T], Acc) ->
    case lists:member(K, ?SUBRESOURCES) of
        true ->
            Amp = if Acc == [] -> "";
                     true      -> "&"
                  end,
            canonicalize_qs(T, [[K, Amp]|Acc]);
        false ->
            canonicalize_qs(T, Acc)
    end;
canonicalize_qs([{K, V}|T], Acc) ->
    case lists:member(K, ?SUBRESOURCES) of
        true ->
            Amp = if Acc == [] -> "";
                     true      -> "&"
                  end,
            canonicalize_qs(T, [[K, "=", V, Amp]|Acc]);
        false ->
            canonicalize_qs(T, Acc)
    end.

%% ===================================================================
%% Eunit tests
%% ===================================================================

-ifdef(TEST).

%% Test cases for the examples provided by Amazon here:
%% http://docs.amazonwebservices.com/AmazonS3/latest/dev/index.html?RESTAuthentication.html

auth_test_() ->
    {spawn,
     [
      {setup,
       fun setup/0,
       fun teardown/1,
       fun(_X) ->
               [
                example_get_object(),
                example_put_object(),
                example_list(),
                example_fetch(),
                example_delete(),
                example_upload(),
                example_list_all_buckets(),
                example_unicode_keys()
               ]
       end
      }]}.

setup() ->
    application:set_env(riak_cs, cs_root_host, ?ROOT_HOST).

teardown(_) ->
    application:unset_env(riak_cs, cs_root_host).

test_fun(Desc, ExpectedSignature, CalculatedSignature) ->
    {Desc, ?_assert(check_auth(ExpectedSignature,CalculatedSignature))}.

example_get_object() ->
    KeyData = "uV3F3YluFJax1cknvbcGwgjvx4QpvB+leU8dUj2o",
    Method = 'GET',
    Version = {1, 1},
    OrigPath = "/johnsmith/photos/puppy.jpg",
    Path = "/buckets/johnsmith/objects/photos/puppy.jpg",
    Headers =
        mochiweb_headers:make([{"Host", "s3.amazonaws.com"},
                               {"Date", "Tue, 27 Mar 2007 19:36:42 +0000"},
                               {"x-rcs-rewrite-path", OrigPath}]),
    RD = wrq:create(Method, Version, Path, Headers),
    ExpectedSignature = "xXjDGYUmKxnwqr5KXNPGldn5LbA=",
    CalculatedSignature = calculate_signature(KeyData, RD),
    test_fun("example get object test", ExpectedSignature, CalculatedSignature).

example_put_object() ->
    KeyData = "uV3F3YluFJax1cknvbcGwgjvx4QpvB+leU8dUj2o",
    Method = 'PUT',
    Version = {1, 1},
    OrigPath = "/johnsmith/photos/puppy.jpg",
    Path = "/buckets/johnsmith/objects/photos/puppy.jpg",
    Headers =
        mochiweb_headers:make([{"Host", "s3.amazonaws.com"},
                               {"Content-Type", "image/jpeg"},
                               {"x-rcs-rewrite-path", OrigPath},
                               {"Content-Length", 94328},
                               {"Date", "Tue, 27 Mar 2007 21:15:45 +0000"}]),
    RD = wrq:create(Method, Version, Path, Headers),
    ExpectedSignature = "hcicpDDvL9SsO6AkvxqmIWkmOuQ=",
    CalculatedSignature = calculate_signature(KeyData, RD),
    test_fun("example put object test", ExpectedSignature, CalculatedSignature).

example_list() ->
    KeyData = "uV3F3YluFJax1cknvbcGwgjvx4QpvB+leU8dUj2o",
    Method = 'GET',
    Version = {1, 1},
    OrigPath = "/johnsmith/?prefix=photos&max-keys=50&marker=puppy",
    Path = "/buckets/johnsmith/objects?prefix=photos&max-keys=50&marker=puppy",
    Headers =
        mochiweb_headers:make([{"User-Agent", "Mozilla/5.0"},
                               {"Host", "johnsmith.s3.amazonaws.com"},
                               {"x-rcs-rewrite-path", OrigPath},
                               {"Date", "Tue, 27 Mar 2007 19:42:41 +0000"}]),
    RD = wrq:create(Method, Version, Path, Headers),
    ExpectedSignature = "jsRt/rhG+Vtp88HrYL706QhE4w4=",
    CalculatedSignature = calculate_signature(KeyData, RD),
    test_fun("example list test", ExpectedSignature, CalculatedSignature).

example_fetch() ->
    KeyData = "uV3F3YluFJax1cknvbcGwgjvx4QpvB+leU8dUj2o",
    Method = 'GET',
    Version = {1, 1},
    OrigPath = "/johnsmith/?acl",
    Path = "/buckets/johnsmith/acl",
    Headers =
        mochiweb_headers:make([{"Host", "johnsmith.s3.amazonaws.com"},
                               {"x-rcs-rewrite-path", OrigPath},
                               {"Date", "Tue, 27 Mar 2007 19:44:46 +0000"}]),
    RD = wrq:create(Method, Version, Path, Headers),
    ExpectedSignature = "thdUi9VAkzhkniLj96JIrOPGi0g=",
    CalculatedSignature = calculate_signature(KeyData, RD),
    test_fun("example fetch test", ExpectedSignature, CalculatedSignature).

example_delete() ->
    KeyData = "uV3F3YluFJax1cknvbcGwgjvx4QpvB+leU8dUj2o",
    Method = 'DELETE',
    Version = {1, 1},
    OrigPath = "/johnsmith/photos/puppy.jpg",
    Path = "/buckets/johnsmith/objects/photos/puppy.jpg",
    Headers =
        mochiweb_headers:make([{"User-Agent", "dotnet"},
                               {"Host", "s3.amazonaws.com"},
                               {"x-rcs-rewrite-path", OrigPath},
                               {"Date", "Tue, 27 Mar 2007 21:20:27 +0000"},
                               {"x-amz-date", "Tue, 27 Mar 2007 21:20:26 +0000"}]),
    RD = wrq:create(Method, Version, Path, Headers),
    ExpectedSignature = "k3nL7gH3+PadhTEVn5Ip83xlYzk=",
    CalculatedSignature = calculate_signature(KeyData, RD),
    test_fun("example delete test", ExpectedSignature, CalculatedSignature).

%% @TODO This test case should be specified using two separate
%% X-Amz-Meta-ReviewedBy headers, but Amazon strictly interprets
%% section 4.2 of RFC 2616 and forbids anything but commas seperating
%% field values of headers with the same field name whereas webmachine
%% inserts a comma and a space between the field values. This is
%% probably something that can be changed in webmachine without any
%% ill effect, but that needs to be verified. For now, the test case
%% is specified using a singled X-Amz-Meta-ReviewedBy header with
%% multiple field values.
example_upload() ->
    KeyData = "uV3F3YluFJax1cknvbcGwgjvx4QpvB+leU8dUj2o",
    Method = 'PUT',
    Version = {1, 1},
    OrigPath = "/static.johnsmith.net/db-backup.dat.gz",
    Path = "/buckets/static.johnsmith.net/objects/db-backup.dat.gz",
    Headers =
        mochiweb_headers:make([{"User-Agent", "curl/7.15.5"},
                               {"Host", "static.johnsmith.net:8080"},
                               {"Date", "Tue, 27 Mar 2007 21:06:08 +0000"},
                               {"x-rcs-rewrite-path", OrigPath},
                               {"x-amz-acl", "public-read"},
                               {"content-type", "application/x-download"},
                               {"Content-MD5", "4gJE4saaMU4BqNR0kLY+lw=="},
                               {"X-Amz-Meta-ReviewedBy", "joe@johnsmith.net,jane@johnsmith.net"},
                               %% {"X-Amz-Meta-ReviewedBy", "jane@johnsmith.net"},
                               {"X-Amz-Meta-FileChecksum", "0x02661779"},
                               {"X-Amz-Meta-ChecksumAlgorithm", "crc32"},
                               {"Content-Disposition", "attachment; filename=database.dat"},
                               {"Content-Encoding", "gzip"},
                               {"Content-Length", 5913339}]),
    RD = wrq:create(Method, Version, Path, Headers),
    ExpectedSignature = "C0FlOtU8Ylb9KDTpZqYkZPX91iI=",
    CalculatedSignature = calculate_signature(KeyData, RD),
    test_fun("example upload test", ExpectedSignature, CalculatedSignature).

example_list_all_buckets() ->
    KeyData = "uV3F3YluFJax1cknvbcGwgjvx4QpvB+leU8dUj2o",
    Method = 'GET',
    Version = {1, 1},
    Path = "/",
    Headers =
        mochiweb_headers:make([{"Host", "s3.amazonaws.com"},
                               {"x-rcs-rewrite-path", Path},
                               {"Date", "Wed, 28 Mar 2007 01:29:59 +0000"}]),
    RD = wrq:create(Method, Version, Path, Headers),
    ExpectedSignature = "Db+gepJSUbZKwpx1FR0DLtEYoZA=",
    CalculatedSignature = calculate_signature(KeyData, RD),
    test_fun("example list all buckts test", ExpectedSignature, CalculatedSignature).

example_unicode_keys() ->
    KeyData = "uV3F3YluFJax1cknvbcGwgjvx4QpvB+leU8dUj2o",
    Method = 'GET',
    Version = {1, 1},
    OrigPath = "/dictionary/fran%C3%A7ais/pr%c3%a9f%c3%a8re",
    Path = "/buckets/dictionary/objects/fran%C3%A7ais/pr%c3%a9f%c3%a8re",
    Headers =
        mochiweb_headers:make([{"Host", "s3.amazonaws.com"},
                               {"x-rcs-rewrite-path", OrigPath},
                               {"Date", "Wed, 28 Mar 2007 01:49:49 +0000"}]),
    RD = wrq:create(Method, Version, Path, Headers),
    ExpectedSignature = "dxhSBHoI6eVSPcXJqEghlUzZMnY=",
    CalculatedSignature = calculate_signature(KeyData, RD),
    test_fun("example unicode keys test", ExpectedSignature, CalculatedSignature).

-endif.
