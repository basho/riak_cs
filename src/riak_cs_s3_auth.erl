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
            identify_by_query_string(RD);
        AuthHeader ->
            parse_auth_header(AuthHeader)
    end.

-spec authenticate(rcs_user(), term(), term(), term()) -> ok | {error, atom()}.
authenticate(User, {v4, Attributes}, RD, _Ctx) ->
    authenticate_v4(User, Attributes, RD);
authenticate(User, Signature, RD, _Ctx) ->
    CalculatedSignature = calculate_signature_v2(User?RCS_USER.key_secret, RD),
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

%% Idintify user by query string.
%% Currently support signature v2 only, does NOT support signature v4.
identify_by_query_string(RD) ->
    {wrq:get_qs_value(?QS_KEYID, RD), wrq:get_qs_value(?QS_SIGNATURE, RD)}.

parse_auth_header("AWS4-HMAC-SHA256 " ++ String) ->
    parse_auth_v4_header(String, undefined, []);
parse_auth_header("AWS " ++ Key) ->
    case string:tokens(Key, ":") of
        [KeyId, KeyData] ->
            {KeyId, KeyData};
        _ -> {undefined, undefined}
    end;
parse_auth_header(_) ->
    {undefined, undefined}.

-spec parse_auth_v4_header(string(), undefined | string(), [{string(), string()}]) ->
                                  {string(), {v4, [{string(), string()}]}}.
parse_auth_v4_header("", UserId, Acc) ->
    {UserId, {v4, lists:reverse(Acc)}};
parse_auth_v4_header(String, UserId, Acc) ->
    {Key, Rest0} = parse_auth_v4_header_key(String, []),
    {Value, Rest} = parse_auth_v4_header_value(Rest0, []),
    lager:debug("Auth header ~p=~p~n", [Key, Value]),
    case Key of
        "Credential" ->
            [UserIdInCred | _] = string:tokens(Value, [$/]),
            parse_auth_v4_header(Rest, UserIdInCred, [{Key, Value} | Acc]);
        _ ->
            parse_auth_v4_header(Rest, UserId, [{Key, Value} | Acc])
    end.

parse_auth_v4_header_key([$= | Rest], Acc) ->
    {lists:reverse(Acc), Rest};
parse_auth_v4_header_key([C | Rest], Acc) ->
    parse_auth_v4_header_key(Rest, [C | Acc]).

parse_auth_v4_header_value([], Acc) ->
    {lists:reverse(Acc), []};
parse_auth_v4_header_value([$, | Rest], Acc) ->
    {lists:reverse(Acc), Rest};
parse_auth_v4_header_value([C | Rest], Acc) ->
    parse_auth_v4_header_value(Rest, [C | Acc]).

calculate_signature_v2(KeyData, RD) ->
    Headers = riak_cs_wm_utils:normalize_headers(RD),
    AmazonHeaders = riak_cs_wm_utils:extract_amazon_headers(Headers),
    OriginalResource = riak_cs_s3_rewrite:original_resource(RD),
    Resource = case OriginalResource of
        undefined -> []; %% TODO: get noisy here?
        {Path,QS} -> [Path, canonicalize_qs(v2, QS)]
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

    base64:encode_to_string(riak_cs_utils:sha_mac(KeyData, STS)).

authenticate_v4(?RCS_USER{key_secret = SecretAccessKey} = _User, AuthAttrs, RD) ->
    Method = wrq:method(RD),
    {Path, Qs} = riak_cs_s3_rewrite:raw_url(RD),
    AllHeaders = riak_cs_wm_utils:normalize_headers(RD),
    authenticate_v4(SecretAccessKey, AuthAttrs, Method, Path, Qs, AllHeaders).

authenticate_v4(SecretAccessKey, AuthAttrs, Method, Path, Qs, AllHeaders) ->
    CanonicalRequest = canonical_request_v4(AuthAttrs, Method, Path, Qs, AllHeaders),
    _ = lager:debug("CanonicalRequest(v4):~n~s~nEND-OF-CANONICAL-REQUEST", [CanonicalRequest]),
    {StringToSign, Scope} =
        string_to_sign_v4(AuthAttrs, AllHeaders, CanonicalRequest),
    _ = lager:debug("StringToSign(v4):~n~s~nEND-OF-STRING-TO-SIGN", [StringToSign]),
    CalculatedSignature = calculate_signature_v4(SecretAccessKey, Scope, StringToSign),
    _ = lager:debug("CalculatedSignature(v4): ~s", [CalculatedSignature]),
    {"Signature", PresentedSignature} = lists:keyfind("Signature", 1, AuthAttrs),
    _ = lager:debug("PresentedSignature(v4)  : ~s", [PresentedSignature]),
    case CalculatedSignature of
        PresentedSignature -> ok;
        _ -> {error, {unmatched_signature, PresentedSignature, CalculatedSignature}}
    end.

canonical_request_v4(AuthAttrs, Method, Path, Qs, AllHeaders) ->
    CanonicalQs = canonicalize_qs(v4, Qs),
    {"SignedHeaders", SignedHeaders} = lists:keyfind("SignedHeaders", 1, AuthAttrs),
    CanonicalHeaders = canonical_headers_v4(AllHeaders,
                                            string:tokens(SignedHeaders, [$;])),
    {"x-amz-content-sha256", HashedPayload} =
        lists:keyfind("x-amz-content-sha256", 1, AllHeaders),
    CanonicalRequest = [atom_to_list(Method), $\n,
                        strict_url_encode_for_path(Path), $\n,
                        CanonicalQs, $\n,
                        CanonicalHeaders, $\n,
                        SignedHeaders, $\n,
                        HashedPayload],
    CanonicalRequest.

canonical_headers_v4(AllHeaders, HeaderNames) ->
    %% TODO:
    %% - Assert `host' and all `x-amz-*' in AllHeaders are included in `HeaderNames'
    %% - Assert `conetnt-type' is included in `HeaderNames' if it is included in
    %%   `AllHeaders'
    canonical_headers_v4(AllHeaders, HeaderNames, []).

canonical_headers_v4(_AllHeaders, [], Acc) ->
    Acc;
canonical_headers_v4(AllHeaders, [HeaderName | Rest], Acc) ->
    {HeaderName, Value} = lists:keyfind(HeaderName, 1, AllHeaders),
    canonical_headers_v4(AllHeaders, Rest, [Acc | [HeaderName, $:, Value, $\n]]).

string_to_sign_v4(AuthAttrs, AllHeaders, CanonicalRequest) ->
    %% TODO: Expires?
    TimeStamp = case lists:keyfind("x-amz-date", 1, AllHeaders) of
                    false ->
                        {"date", Date} = lists:keyfind("date", 1, AllHeaders),
                        riak_cs_wm_utils:to_iso_8601(Date);
                    {"x-amz-date", XAmzDate} ->
                        XAmzDate
                end,
    {"Credential", Cred} = lists:keyfind("Credential", 1, AuthAttrs),
    [_UserId, CredDate, AwsRegion, "s3" = AwsService, "aws4_request" = AwsRequest] =
        string:tokens(Cred, [$/]),
    %% TODO: Validate `CredDate' be within 7 days
    Scope = [CredDate, $/, AwsRegion, $/, AwsService, $/, AwsRequest],
    {["AWS4-HMAC-SHA256", $\n,
      TimeStamp, $\n,
      Scope, $\n,
      hex_sha256hash(CanonicalRequest)],
     {CredDate, AwsRegion, AwsService, AwsRequest}}.

calculate_signature_v4(SecretAccessKey,
                       {CredDate, AwsRegion, AwsService, AwsRequest}, StringToSign) ->
    DateKey              = hmac_sha256(["AWS4", SecretAccessKey], CredDate),
    DateRegionKey        = hmac_sha256(DateKey, AwsRegion),
    DateRegionServiceKey = hmac_sha256(DateRegionKey, AwsService),
    SigningKey           = hmac_sha256(DateRegionServiceKey, AwsRequest),
    mochihex:to_hex(hmac_sha256(SigningKey, StringToSign)).

hmac_sha256(Key, Data) ->
    %% TODO: R15* compatibility?
    crypto:hmac(sha256, Key, Data).

hex_sha256hash(Data) ->
    mochihex:to_hex(crypto:hash(sha256, Data)).

check_auth(PresentedSignature, CalculatedSignature) ->
    PresentedSignature == CalculatedSignature.

canonicalize_qs(Version, QS) ->
    %% The QS must be sorted be canonicalized,
    %% and since `canonicalize_qs/2` builds up the
    %% accumulator with cons, it comes back in reverse
    %% order. So we'll sort then reverise, so cons'ing
    %% actually puts it back in the correct order
    ReversedSorted = lists:reverse(lists:sort(QS)),
    case Version of
        v2 -> canonicalize_qs_v2(ReversedSorted, []);
        v4 -> canonicalize_qs_v4(ReversedSorted, [])
    end.

canonicalize_qs_v2([], []) ->
    [];
canonicalize_qs_v2([], Acc) ->
    lists:flatten(["?", Acc]);
canonicalize_qs_v2([{K, []}|T], Acc) ->
    case lists:member(K, ?SUBRESOURCES) of
        true ->
            Amp = if Acc == [] -> "";
                     true      -> "&"
                  end,
            canonicalize_qs_v2(T, [[K, Amp]|Acc]);
        false ->
            canonicalize_qs_v2(T, Acc)
    end;
canonicalize_qs_v2([{K, V}|T], Acc) ->
    case lists:member(K, ?SUBRESOURCES) of
        true ->
            Amp = if Acc == [] -> "";
                     true      -> "&"
                  end,
            canonicalize_qs_v2(T, [[K, "=", V, Amp]|Acc]);
        false ->
            canonicalize_qs_v2(T, Acc)
    end.

canonicalize_qs_v4([], []) ->
    [];
canonicalize_qs_v4([], Acc) ->
    Acc;
canonicalize_qs_v4([{K, V}|T], Acc) ->
    Amp = if Acc == [] -> "";
             true      -> "&"
          end,
    canonicalize_qs_v4(T, [[strict_url_encode_for_qs_value(K), "=",
                            strict_url_encode_for_qs_value(V), Amp]|Acc]).

%% Force strict URL encoding for keys and values in query part.
%% For this part, all unsafe characters MUST be encoded including
%% slashes (%2f).
%% Because keys and values from raw query string that is URL-encoded at client
%% side, they should be URL-decoded once and encoded back.
strict_url_encode_for_qs_value(Value) ->
    mochiweb_util:quote_plus(mochiweb_util:unquote(Value)).

%% Force string URL encoding for path part of URL.
%% Contrary to query part, slashes MUST NOT encoded and left as is.
strict_url_encode_for_path(Path) ->
    Tokens = binary:split(list_to_binary(Path), <<"/">>, [global]),
    EncodedTokens = [mochiweb_util:quote_plus(mochiweb_util:unquote(T)) ||
                        T <- Tokens],
    string:join(EncodedTokens, "/").

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
    CalculatedSignature = calculate_signature_v2(KeyData, RD),
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
    CalculatedSignature = calculate_signature_v2(KeyData, RD),
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
    CalculatedSignature = calculate_signature_v2(KeyData, RD),
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
    CalculatedSignature = calculate_signature_v2(KeyData, RD),
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
    CalculatedSignature = calculate_signature_v2(KeyData, RD),
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
    CalculatedSignature = calculate_signature_v2(KeyData, RD),
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
    CalculatedSignature = calculate_signature_v2(KeyData, RD),
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
    CalculatedSignature = calculate_signature_v2(KeyData, RD),
    test_fun("example unicode keys test", ExpectedSignature, CalculatedSignature).

-endif.
