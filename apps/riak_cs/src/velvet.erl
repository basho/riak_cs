%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,,
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

%% @doc Client module for interacting with `stanchion' application.

-module(velvet).

-export([create_bucket/5,
         create_user/5,
         delete_bucket/5,
         ping/3,
         set_bucket_acl/6,
         set_bucket_policy/6,
         set_bucket_versioning/6,
         delete_bucket_policy/5,
         update_user/6
         % @TODO: update_bucket/3
        ]).

%% ===================================================================
%% Public API
%% ===================================================================

%% @doc Create a bucket for a requesting party.
-spec create_bucket(string(),
                    pos_integer(),
                    string(),
                    string(),
                    [{atom(), term()}]) -> ok | {error, term()}.
create_bucket(Ip, Port, ContentType, BucketDoc, Options) ->
    Ssl = proplists:get_value(ssl, Options, true),
    AuthCreds = proplists:get_value(auth_creds, Options, no_auth_creds),
    Path = buckets_path(<<>>),
    Url = url(Ip, Port, Ssl, Path),
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
    case request(post, Url, [201], ContentType, Headers, BucketDoc) of
        {ok, {{_, 201, _}, _RespHeaders, _RespBody}} ->
            ok;
        {error, {ok, {{_, StatusCode, Reason}, _RespHeaders, RespBody}}} ->
            {error, {error_status, StatusCode, Reason, RespBody}};
        {error, Error} ->
            {error, Error}
    end.

%% @doc Create a bucket for a requesting party.
-spec create_user(string(),
                  pos_integer(),
                  string(),
                  string(),
                  [{atom(), term()}]) -> ok | {error, term()}.
create_user(Ip, Port, ContentType, UserDoc, Options) ->
    Ssl = proplists:get_value(ssl, Options, true),
    AuthCreds = proplists:get_value(auth_creds, Options, no_auth_creds),
    Path = users_path([]),
    Url = url(Ip, Port, Ssl, Path),
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
    case request(post, Url, [201], ContentType, Headers, UserDoc) of
        {ok, {{_, 201, _}, _RespHeaders, _RespBody}} ->
            ok;
        {error, {ok, {{_, StatusCode, Reason}, _RespHeaders, RespBody}}} ->
            {error, {error_status, StatusCode, Reason, RespBody}};
        {error, Error} ->
            {error, Error}
    end.

%% @doc Delete a bucket. The bucket must be owned by
%% the requesting party.
-spec delete_bucket(string(),
                    pos_integer(),
                    binary(),
                    string(),
                    [{atom(), term()}]) -> ok | {error, term()}.
delete_bucket(Ip, Port, Bucket, Requester, Options) ->
    Ssl = proplists:get_value(ssl, Options, true),
    AuthCreds = proplists:get_value(auth_creds, Options, no_auth_creds),
    QS = requester_qs(Requester),
    Path = buckets_path(Bucket),
    Url = url(Ip, Port, Ssl, stringy(Path ++ QS)),
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
    case request(delete, Url, [204], Headers) of
        {ok, {{_, 204, _}, _RespHeaders, _}} ->
            ok;
        {error, {ok, {{_, StatusCode, Reason}, _RespHeaders, RespBody}}} ->
            {error, {error_status, StatusCode, Reason, RespBody}};
        {error, Error} ->
            {error, Error}
    end.

%% @doc Ping the server by requesting the "/ping" resource.
-spec ping(string(), pos_integer(), boolean()) -> ok | {error, term()}.
ping(Ip, Port, Ssl) ->
    Url = ping_url(Ip, Port, Ssl),
    case request(get, Url, [200, 204]) of
        {ok, {{_, _Status, _}, _Headers, _Body}} ->
            ok;
        {error, Error} ->
            {error, Error}
    end.

%% @doc Create a bucket for a requesting party.
-spec set_bucket_acl(string(),
                     inet:port_number(),
                     binary(),
                     string(),
                     string(),
                     [{atom(), term()}]) -> ok | {error, term()}.
set_bucket_acl(Ip, Port, Bucket, ContentType, AclDoc, Options) ->
    Path = buckets_path(Bucket, acl),
    update_bucket(Ip, Port, Path, ContentType, AclDoc, Options, 204).

%% @doc Set bucket policy
-spec set_bucket_policy(string(),
                        inet:port_number(),
                        binary(),
                        string(),
                        string(),
                        proplists:proplist()) -> ok | {error, term()}.
set_bucket_policy(Ip, Port, Bucket, ContentType, PolicyDoc, Options) ->
    Path = buckets_path(Bucket, policy),
    update_bucket(Ip, Port, Path, ContentType, PolicyDoc, Options, 204).

%% @doc Set bucket versioning
-spec set_bucket_versioning(string(),
                            inet:port_number(),
                            binary(),
                            string(),
                            string(),
                            proplists:proplist()) -> ok | {error, term()}.
set_bucket_versioning(Ip, Port, Bucket, ContentType, Doc, Options) ->
    Path = buckets_path(Bucket, versioning),
    update_bucket(Ip, Port, Path, ContentType, Doc, Options, 204).

%% @doc Delete a bucket. The bucket must be owned by
%% the requesting party.
-spec delete_bucket_policy(string(),
                           pos_integer(),
                           binary(),
                           string(),
                           [{atom(), term()}]) -> ok | {error, term()}.
delete_bucket_policy(Ip, Port, Bucket, Requester, Options) ->
    Ssl = proplists:get_value(ssl, Options, true),
    AuthCreds = proplists:get_value(auth_creds, Options, no_auth_creds),
    QS = requester_qs(Requester),
    Path = buckets_path(Bucket, policy),
    Url = url(Ip, Port, Ssl, stringy(Path ++ QS)),
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
    case request(delete, Url, [204], Headers) of
        {ok, {{_, 204, _}, _RespHeaders, _}} ->
            ok;
        {error, {ok, {{_, StatusCode, Reason}, _RespHeaders, RespBody}}} ->
            {error, {error_status, StatusCode, Reason, RespBody}};
        {error, Error} ->
            {error, Error}
    end.

%% @doc Update a user record
-spec update_user(string(),
                  pos_integer(),
                  string(),
                  string(),
                  string(),
                  [{atom(), term()}]) -> ok | {error, term()}.
update_user(Ip, Port, ContentType, KeyId, UserDoc, Options) ->
    Ssl = proplists:get_value(ssl, Options, true),
    AuthCreds = proplists:get_value(auth_creds, Options, no_auth_creds),
    Path = users_path(KeyId),
    Url = url(Ip, Port, Ssl, Path),
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
    case request(put, Url, [204], ContentType, Headers, UserDoc) of
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
-spec update_bucket(string(), inet:port_number(), string(),
                    string(), string(), proplists:proplist(),
                    pos_integer()) ->
                           ok | {error, term()}.
update_bucket(Ip, Port, Path, ContentType, Doc, Options, Expect) ->
    AuthCreds = proplists:get_value(auth_creds, Options, no_auth_creds),
    Ssl = proplists:get_value(ssl, Options, true),
    Url = url(Ip, Port, Ssl, Path),
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
    case request(put, Url, [Expect], ContentType, Headers, Doc) of
        {ok, {{_, Expect, _}, _RespHeaders, _RespBody}} ->
            ok;
        {error, {ok, {{_, StatusCode, Reason}, _RespHeaders, RespBody}}} ->
            {error, {error_status, StatusCode, Reason, RespBody}};
        {error, Error} ->
            {error, Error}
    end.

%% @doc Assemble the root URL for the given client
-spec root_url(string(), pos_integer(), boolean()) -> [string()].
root_url(Ip, Port, true) ->
    ["https://", Ip, ":", integer_to_list(Port)];
root_url(Ip, Port, false) ->
    ["http://", Ip, ":", integer_to_list(Port)].

%% @doc Assemble the URL for the ping resource
-spec ping_url(string(), pos_integer(), boolean()) -> string().
ping_url(Ip, Port, Ssl) ->
    lists:flatten([root_url(Ip, Port, Ssl), "ping/"]).

%% @doc Assemble the path for a bucket request
-spec buckets_path(binary()) -> string().
buckets_path(Bucket) ->
    stringy(["/buckets",
             ["/" ++ binary_to_list(Bucket) || Bucket /= <<>>]]).

%% @doc Assemble the path for a bucket request
-spec buckets_path(binary(), acl|policy|versioning) -> string().
buckets_path(Bucket, acl) ->
    stringy([buckets_path(Bucket), "/acl"]);
buckets_path(Bucket, policy) ->
    stringy([buckets_path(Bucket), "/policy"]);
buckets_path(Bucket, versioning) ->
    stringy([buckets_path(Bucket), "/versioning"]).

%% @doc Assemble the URL for a buckets request
-spec url(string(), pos_integer(), boolean(), [string()]) ->
                         string().
url(Ip, Port, Ssl, Path) ->
    lists:flatten(
      [root_url(Ip, Port, Ssl),
       Path
      ]).

%% @doc send an HTTP request where `Expect' is a list
%% of expected HTTP status codes.
-spec request(atom(), string(), [pos_integer()]) ->
                     {ok, {term(), term(), term()}} | {error, term()}.
request(Method, Url, Expect) ->
    request(Method, Url, Expect, [], [], []).

%% @doc send an HTTP request  where `Expect' is a list
%% of expected HTTP status codes.
-spec request(atom(), string(), [pos_integer()], [{string(), string()}]) ->
                     {ok, {term(), term(), term()}} | {error, term()}.
request(Method, Url, Expect, Headers) ->
    request(Method, Url, Expect, [], Headers, []).

%% @doc send an HTTP request where `Expect' is a list
%% of expected HTTP status codes.
-spec request(atom(),
              string(),
              [pos_integer()],
              string(),
              [{string(), string()}],
              string()) -> {ok, {term(), term(), term()}} | {error, term()}.
request(Method, Url, Expect, ContentType, Headers, Body) ->
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
            Error
    end.

%% @doc Calculate an MD5 hash of a request body.
-spec content_md5(string()) -> string().
content_md5(Body) ->
    base64:encode_to_string(riak_cs_utils:md5(list_to_binary(Body))).

%% @doc Construct a MOSS authentication header
-spec auth_header(atom(),
                  string(),
                  [{string() | atom() | binary(), string()}],
                  string(),
                  {string(), iodata()}) -> nonempty_string().
auth_header(HttpVerb, ContentType, Headers, Path, {AuthKey, AuthSecret}) ->
    Signature = velvet_auth:request_signature(HttpVerb,
                                              [{"content-type", ContentType} |
                                               Headers],
                                              Path,
                                              AuthSecret),
    "MOSS " ++ AuthKey ++ ":" ++ Signature.

%% @doc Assemble a requester query string for
%% user in a bucket deletion request.
-spec requester_qs(string()) -> string().
requester_qs(Requester) ->
    "?requester=" ++
        mochiweb_util:quote_plus(Requester).

%% @doc Assemble the path for a users request
-spec users_path(string()) -> string().
users_path(User) ->
    stringy(["/users",
             ["/" ++ User || User /= []]
            ]).

-spec stringy(string() | list(string())) -> string().
stringy(List) ->
    lists:flatten(List).
