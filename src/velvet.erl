%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

%% @doc Client module for interacting with `stanchion' application.

-module(velvet).

-export([create_bucket/5,
         create_user/5,
         delete_bucket/5,
         list_buckets/3,
         list_buckets/4,
         ping/3,
         set_bucket_acl/6
         %% @TODO update_bucket/3
        ]).

%% @TODO Remove after module development is completed
-export([stats_url/3,
         list_buckets_url/4,
         request/4]).

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

%% @doc List all the buckets that currently have owners.
-spec list_buckets(string(), pos_integer(), boolean()) -> {ok, [{binary(), binary()}]}. %% | {error, term()}.
list_buckets(_Ip, _Port, _Ssl) ->
    {ok, []}.

%% @doc List all the buckets owned by a particular user.
-spec list_buckets(string(), pos_integer(), boolean(), binary()) -> {ok, [{binary(), binary()}]}. %% | {error, term()}.
list_buckets(_Ip, _Port, _Ssl, _UserId) ->
    {ok, []}.

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
                     pos_integer(),
                     binary(),
                     string(),
                     string(),
                     [{atom(), term()}]) -> ok | {error, term()}.
set_bucket_acl(Ip, Port, Bucket, ContentType, AclDoc, Options) ->
    Ssl = proplists:get_value(ssl, Options, true),
    AuthCreds = proplists:get_value(auth_creds, Options, no_auth_creds),
    Path = buckets_path(Bucket, true),
    Url = url(Ip, Port, Ssl, Path),
    Headers0 = [{"Content-Md5", content_md5(AclDoc)},
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
    case request(put, Url, [204], ContentType, Headers, AclDoc) of
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

%% @doc Assemble the URL for the stats resource
-spec stats_url(string(), pos_integer(), boolean()) -> string().
stats_url(Ip, Port, Ssl) ->
    lists:flatten([root_url(Ip, Port, Ssl), "stats/"]).

%% @doc Assemble the path for a bucket request
-spec buckets_path(binary()) -> string().
buckets_path(Bucket) ->
    buckets_path(Bucket, false).

%% @doc Assemble the path for a bucket request
-spec buckets_path(binary(), boolean()) -> string().
buckets_path(Bucket, AclRequest) ->
    stringy(["/buckets",
             ["/" ++ binary_to_list(Bucket) || Bucket /= <<>>],
             ["/acl" || AclRequest == true]
            ]).

%% @doc Assemble the URL for a buckets request
-spec url(string(), pos_integer(), boolean(), [string()]) ->
                         string().
url(Ip, Port, Ssl, Path) ->
    lists:flatten(
      [root_url(Ip, Port, Ssl),
       Path
      ]).

%% @doc Assemble the URL for the given bucket and key
-spec list_buckets_url(string(), pos_integer(), boolean(), binary()) -> string().
list_buckets_url(Ip, Port, Ssl, Owner) ->
    Query =
        "owner=" ++
        binary_to_list(Owner),
    lists:flatten(
      [root_url(Ip, Port, Ssl),
       "buckets",
       ["?", mochiweb_util:quote_plus(Query)]
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
    stanchion_utils:binary_to_hexlist(
      list_to_binary(Body)).

%% @doc Construct a MOSS authentication header
-spec auth_header(atom(),
                  string(),
                  [{string() | atom() | binary(), string()}],
                  string(),
                  {string(), iodata()}) -> nonempty_string().
auth_header(HttpVerb, ContentType, Headers, Path, {AuthKey, AuthSecret}) ->
    Signature = stanchion_auth:request_signature(HttpVerb,
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
-spec users_path(string()) -> [string()].
users_path(User) ->
    stringy(["/users",
             ["/" ++ User || User /= []]
            ]).

-spec stringy(string() | list(string())) -> string().
stringy(List) ->
    lists:flatten(List).
