%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,,
%%               2021-2023 TI Tokyo    All Rights Reserved.
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

%% @doc Client module for interacting with `stanchion' application.

-module(velvet).

-export([create_bucket/3,
         create_user/3,
         delete_bucket/3,
         set_bucket_acl/4,
         set_bucket_policy/4,
         set_bucket_versioning/4,
         delete_bucket_policy/3,
         update_user/4,
         create_role/3,
         delete_role/2
        ]).

-include_lib("kernel/include/logger.hrl").

-define(MAX_REQUEST_RETRIES, 3).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Create a bucket for a requesting party.
-spec create_bucket(string(),
                    string(),
                    [{atom(), term()}]) -> ok | {error, term()}.
create_bucket(ContentType, BucketDoc, Options) ->
    AuthCreds = proplists:get_value(auth_creds, Options, no_auth_creds),
    Path = buckets_path(<<>>),
    Headers0 = [{"Content-Md5", content_md5(BucketDoc)},
                {"Date", httpd_util:rfc1123_date()}],
    case AuthCreds of
        {_, _} ->
            Headers =
                [{"Authorization", auth_header('POST',
                                               ContentType,
                                               Headers0,
                                               Path,
                                               AuthCreds)} |
                 Headers0];
        no_auth_creds ->
            Headers = Headers0
    end,
    case request(post, Path, [201], ContentType, Headers, BucketDoc) of
        {ok, {{_, 201, _}, _RespHeaders, _RespBody}} ->
            ok;
        {error, {ok, {{_, StatusCode, Reason}, _RespHeaders, RespBody}}} ->
            {error, {error_status, StatusCode, Reason, RespBody}};
        {error, Error} ->
            {error, Error}
    end.

%% @doc Create a bucket for a requesting party.
-spec create_user(string(),
                  string(),
                  [{atom(), term()}]) -> ok | {error, term()}.
create_user(ContentType, UserDoc, Options) ->
    AuthCreds = proplists:get_value(auth_creds, Options, no_auth_creds),
    Path = users_path([]),
    Headers0 = [{"Content-Md5", content_md5(UserDoc)},
                {"Date", httpd_util:rfc1123_date()}],
    case AuthCreds of
        {_, _} ->
            Headers =
                [{"Authorization", auth_header('POST',
                                               ContentType,
                                               Headers0,
                                               Path,
                                               AuthCreds)} |
                 Headers0];
        no_auth_creds ->
            Headers = Headers0
    end,
    case request(post, Path, [201], ContentType, Headers, UserDoc) of
        {ok, {{_, 201, _}, _RespHeaders, _RespBody}} ->
            ok;
        {error, {ok, {{_, StatusCode, Reason}, _RespHeaders, RespBody}}} ->
            {error, {error_status, StatusCode, Reason, RespBody}};
        {error, Error} ->
            {error, Error}
    end.

%% @doc Delete a bucket. The bucket must be owned by
%% the requesting party.
-spec delete_bucket(binary(),
                    string(),
                    [{atom(), term()}]) -> ok | {error, term()}.
delete_bucket(Bucket, Requester, Options) ->
    AuthCreds = proplists:get_value(auth_creds, Options, no_auth_creds),
    QS = requester_qs(Requester),
    Path = buckets_path(Bucket),
    Headers0 = [{"Date", httpd_util:rfc1123_date()}],
    case AuthCreds of
        {_, _} ->
            Headers =
                [{"Authorization", auth_header('DELETE',
                                               [],
                                               Headers0,
                                               Path,
                                               AuthCreds)} |
                 Headers0];
        no_auth_creds ->
            Headers = Headers0
    end,
    case request(delete, Path ++ QS, [204], Headers) of
        {ok, {{_, 204, _}, _RespHeaders, _}} ->
            ok;
        {error, {ok, {{_, StatusCode, Reason}, _RespHeaders, RespBody}}} ->
            {error, {error_status, StatusCode, Reason, RespBody}};
        {error, Error} ->
            {error, Error}
    end.

%% @doc Create a bucket for a requesting party.
-spec set_bucket_acl(binary(),
                     string(),
                     string(),
                     [{atom(), term()}]) -> ok | {error, term()}.
set_bucket_acl(Bucket, ContentType, AclDoc, Options) ->
    Path = buckets_path(Bucket, acl),
    update_bucket(Path, ContentType, AclDoc, Options, 204).

%% @doc Set bucket policy
-spec set_bucket_policy(binary(),
                        string(),
                        string(),
                        proplists:proplist()) -> ok | {error, term()}.
set_bucket_policy(Bucket, ContentType, PolicyDoc, Options) ->
    Path = buckets_path(Bucket, policy),
    update_bucket(Path, ContentType, PolicyDoc, Options, 204).

%% @doc Set bucket versioning
-spec set_bucket_versioning(binary(),
                            string(),
                            string(),
                            proplists:proplist()) -> ok | {error, term()}.
set_bucket_versioning(Bucket, ContentType, Doc, Options) ->
    Path = buckets_path(Bucket, versioning),
    update_bucket(Path, ContentType, Doc, Options, 204).

%% @doc Delete a bucket. The bucket must be owned by
%% the requesting party.
-spec delete_bucket_policy(binary(),
                           string(),
                           [{atom(), term()}]) -> ok | {error, term()}.
delete_bucket_policy(Bucket, Requester, Options) ->
    AuthCreds = proplists:get_value(auth_creds, Options, no_auth_creds),
    QS = requester_qs(Requester),
    Path = buckets_path(Bucket, policy),
    Headers0 = [{"Date", httpd_util:rfc1123_date()}],
    case AuthCreds of
        {_, _} ->
            Headers =
                [{"Authorization", auth_header('DELETE',
                                               [],
                                               Headers0,
                                               Path,
                                               AuthCreds)} |
                 Headers0];
        no_auth_creds ->
            Headers = Headers0
    end,
    case request(delete, Path ++ QS, [204], Headers) of
        {ok, {{_, 204, _}, _RespHeaders, _}} ->
            ok;
        {error, {ok, {{_, StatusCode, Reason}, _RespHeaders, RespBody}}} ->
            {error, {error_status, StatusCode, Reason, RespBody}};
        {error, Error} ->
            {error, Error}
    end.

%% @doc Update a user record
-spec update_user(string(),
                  string(),
                  string(),
                  [{atom(), term()}]) -> ok | {error, term()}.
update_user(ContentType, KeyId, UserDoc, Options) ->
    AuthCreds = proplists:get_value(auth_creds, Options, no_auth_creds),
    Path = users_path(KeyId),
    Headers0 = [{"Content-Md5", content_md5(UserDoc)},
                {"Date", httpd_util:rfc1123_date()}],
    case AuthCreds of
        {_, _} ->
            Headers =
                [{"Authorization", auth_header('PUT',
                                               ContentType,
                                               Headers0,
                                               Path,
                                               AuthCreds)} |
                 Headers0];
        no_auth_creds ->
            Headers = Headers0
    end,
    case request(put, Path, [204], ContentType, Headers, UserDoc) of
        {ok, {{_, 204, _}, _RespHeaders, _RespBody}} ->
            ok;
        {error, {ok, {{_, StatusCode, Reason}, _RespHeaders, RespBody}}} ->
            {error, {error_status, StatusCode, Reason, RespBody}};
        {error, Error} ->
            {error, Error}
    end.


%% ===================================================================
%% Internal functions
%% ===================================================================

% @doc send request to stanchion server
% @TODO merge with create_bucket, create_user, delete_bucket
-spec update_bucket(string(),
                    string(), string(), proplists:proplist(),
                    pos_integer()) ->
                           ok | {error, term()}.
update_bucket(Path, ContentType, Doc, Options, Expect) ->
    ?LOG_DEBUG("Doc: ~p", [Doc]),
    AuthCreds = proplists:get_value(auth_creds, Options, no_auth_creds),
    Headers0 = [{"Content-Md5", content_md5(Doc)},
                {"Date", httpd_util:rfc1123_date()}],
    case AuthCreds of
        {_, _} ->
            Headers =
                [{"Authorization", auth_header('PUT',
                                               ContentType,
                                               Headers0,
                                               Path,
                                               AuthCreds)} |
                 Headers0];
        no_auth_creds ->
            Headers = Headers0
    end,
    case request(put, Path, [Expect], ContentType, Headers, Doc) of
        {ok, {{_, Expect, _}, _RespHeaders, _RespBody}} ->
            ok;
        {error, {ok, {{_, StatusCode, Reason}, _RespHeaders, RespBody}}} ->
            {error, {error_status, StatusCode, Reason, RespBody}};
        {error, Error} ->
            {error, Error}
    end.


%% @doc Assemble the path for a bucket request
buckets_path(Bucket) ->
    stringy(["/buckets",
             ["/" ++ binary_to_list(Bucket) || Bucket /= <<>>]]).

%% @doc Assemble the path for a bucket request
buckets_path(Bucket, acl) ->
    stringy([buckets_path(Bucket), "/acl"]);
buckets_path(Bucket, policy) ->
    stringy([buckets_path(Bucket), "/policy"]);
buckets_path(Bucket, versioning) ->
    stringy([buckets_path(Bucket), "/versioning"]).



-spec create_role(string(), string(), proplists:proplist()) -> {ok, string()} | {error, term()}.
create_role(ContentType, Doc, Options) ->
    AuthCreds = proplists:get_value(auth_creds, Options, no_auth_creds),
    Path = roles_path([]),
    Headers0 = [{"Content-Md5", content_md5(Doc)},
                {"Date", httpd_util:rfc1123_date()}],
    case AuthCreds of
        {_, _} ->
            Headers =
                [{"Authorization", auth_header('POST',
                                               ContentType,
                                               Headers0,
                                               Path,
                                               AuthCreds)} |
                 Headers0];
        no_auth_creds ->
            Headers = Headers0
    end,
    case request(post, Path, [201], ContentType, Headers, Doc) of
        {ok, {{_, 201, _}, _RespHeaders, RespBody}} ->
            RoleId = RespBody,
            {ok, RoleId};
        {error, {ok, {{_, StatusCode, Reason}, _RespHeaders, RespBody}}} ->
            {error, {error_status, StatusCode, Reason, RespBody}};
        {error, Error} ->
            {error, Error}
    end.

-spec delete_role(string(), proplists:proplist()) -> ok | {error, term()}.
delete_role(Id, Options) ->
    AuthCreds = proplists:get_value(auth_creds, Options, no_auth_creds),
    Path = roles_path(Id),
    Headers0 = [{"Date", httpd_util:rfc1123_date()}],
    case AuthCreds of
        {_, _} ->
            Headers =
                [{"Authorization", auth_header('DELETE',
                                               "",
                                               Headers0,
                                               Path,
                                               AuthCreds)} |
                 Headers0];
        no_auth_creds ->
            Headers = Headers0
    end,
    case request(delete, Path, [204], Headers) of
        {ok, {{_, 204, _}, _RespHeaders, _}} ->
            ok;
        {error, {ok, {{_, StatusCode, Reason}, _RespHeaders, RespBody}}} ->
            {error, {error_status, StatusCode, Reason, RespBody}};
        {error, Error} ->
            {error, Error}
    end.


%% @doc send an HTTP request where `Expect' is a list
%% of expected HTTP status codes.
request(Method, Url, Expect, Headers) ->
    request(Method, Url, Expect, [], Headers, []).

%% @doc send an HTTP request where `Expect' is a list
%% of expected HTTP status codes.
request(Method, Path, Expect, ContentType, Headers, Body) ->
    request(Method, Path, Expect, ContentType, Headers, Body, ?MAX_REQUEST_RETRIES).

request(Method, Path, _Expect, _ContentType, _Headers, _Body, 0) ->
    {Ip, Port, Ssl} = riak_cs_utils:stanchion_data(),
    logger:warning("Giving up trying to send a ~s request to stanchion (~s)",
                   [Method, url(Ip, Port, Ssl, Path)]),
    {error, stanchion_recovery_failure};
request(Method, Path, Expect, ContentType, Headers, Body, Attempt) ->
    stanchion_migration:validate_stanchion(),
    {Ip, Port, Ssl} = riak_cs_utils:stanchion_data(),
    Url = url(Ip, Port, Ssl, Path),

    case Method == put orelse
        Method == post of
        true ->
            Request = {Url, Headers, ContentType, Body};
        false ->
            Request = {Url, Headers}
    end,
    case httpc:request(Method, Request, [], []) of
        Resp={ok, {{_, Status, _}, _RespHeaders, _RespBody}} ->
            case lists:member(Status, Expect) of
                true -> Resp;
                false -> {error, Resp}
            end;
        Error ->
            logger:warning("Error contacting stanchion at ~s: ~p; retrying...", [Url, Error]),
            ok = stanchion_migration:adopt_stanchion(),
            request(Method, Path, Expect, ContentType, Headers, Body, Attempt - 1)
    end.

%% @doc Assemble the root URL for the given client
root_url(Ip, Port, true) ->
    ["https://", Ip, ":", integer_to_list(Port)];
root_url(Ip, Port, false) ->
    ["http://", Ip, ":", integer_to_list(Port)].

url(Ip, Port, Ssl, Path) ->
    lists:flatten(
      [root_url(Ip, Port, Ssl),
       Path
      ]).

%% @doc Calculate an MD5 hash of a request body.
content_md5(Body) ->
    base64:encode_to_string(riak_cs_utils:md5(Body)).

%% @doc Construct a MOSS authentication header
auth_header(HttpVerb, ContentType, Headers, Path, {AuthKey, AuthSecret}) ->
    Signature = velvet_auth:request_signature(HttpVerb,
                                              [{"content-type", ContentType} |
                                               Headers],
                                              Path,
                                              AuthSecret),
    "MOSS " ++ AuthKey ++ ":" ++ Signature.

%% @doc Assemble a requester query string for
%% user in a bucket deletion request.
requester_qs(Requester) ->
    "?requester=" ++
        mochiweb_util:quote_plus(Requester).

users_path(User) ->
    stringy(["/users",
             ["/" ++ User || User /= []]
            ]).

roles_path(Role) ->
    stringy(["/roles",
             ["/" ++ Role || Role /= []]
            ]).

stringy(List) ->
    lists:flatten(List).
