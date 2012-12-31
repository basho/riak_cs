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

-module(riak_cs_oos_rewrite).

-export([rewrite/5, original_resource/1]).

-include("riak_cs.hrl").

-define(RCS_REWRITE_HEADER, "x-rcs-rewrite-path").
-define(OOS_API_VSN_HEADER, "x-oos-api-version").
-define(OOS_ACCOUNT_HEADER, "x-oos-account").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-endif.

%% @doc Function to rewrite headers prior to processing by webmachine.
-spec rewrite(atom(), atom(), {integer(), integer()}, gb_tree(), string()) ->
                     {gb_tree(), string()}.
rewrite(Method, _Scheme, _Vsn, Headers, RawPath) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"rewrite">>),
    {Path, QueryString, _} = mochiweb_util:urlsplit_path(RawPath),
    {ApiVsn, Account, RestPath} = parse_path(Path),
    RewrittenHeaders = rewrite_headers(Headers, RawPath, ApiVsn, Account),
    {RewrittenHeaders, rewrite_path(ApiVsn, Method, RestPath, QueryString)}.

-spec original_resource(term()) -> undefined | {string(), [{term(),term()}]}.
original_resource(RD) ->
    case wrq:get_req_header(?RCS_REWRITE_HEADER, RD) of
        undefined -> undefined;
        RawPath ->
            {Path, QS, _} = mochiweb_util:urlsplit_path(RawPath),
            {Path, mochiweb_util:parse_qs(QS)}
    end.

%% @doc Parse the path string into its component parts: the API version,
%% the account identifier, and the remainder of the path information
%% pertaining to the requested object storage operation.
-spec parse_path(string()) -> {string(), string(), string()}.
parse_path(Path) ->
    [ApiVsn, Account | RestPath] = string:tokens(Path, "/"),
    {ApiVsn, Account, "/" ++ string:join(RestPath, "/")}.

%% @doc Add headers for the raw path, the API version, and the account.
-spec rewrite_headers(gb_tree(), string(), string(), string()) -> gb_tree().
rewrite_headers(Headers, RawPath, ApiVsn, Account) ->
    UpdHdrs0 = mochiweb_headers:default(?RCS_REWRITE_HEADER, RawPath, Headers),
    UpdHdrs1 = mochiweb_headers:enter(?OOS_API_VSN_HEADER, ApiVsn, UpdHdrs0),
    mochiweb_headers:enter(?OOS_ACCOUNT_HEADER, Account, UpdHdrs1).

%% @doc Internal function to handle rewriting the URL. Only `v1' of
%% the API is supported since right now it is the only version that
%% exists, but crash if another version is specfified.
-spec rewrite_path(string(), atom(), string(), string()) -> string().
rewrite_path("v1", Method, Path, QS) ->
    rewrite_v1_path(Method, Path, QS).

%% @doc Internal function to handle rewriting the URL
-spec rewrite_v1_path(atom(),string(), string()) -> string().
rewrite_v1_path(_Method, "/", _QS) ->
    "/buckets";
rewrite_v1_path(Method, Path, QS) ->
    {Bucket, UpdPath} = separate_bucket_from_path(string:tokens(Path, [$/])),
    rewrite_v1_path(Method, UpdPath, QS, Bucket).

rewrite_v1_path(_Method, Path, _QS, "riak-cs") ->
    "/riak-cs" ++ Path;
rewrite_v1_path(_Method, Path, _QS, "usage") ->
    "/usage" ++ Path;
rewrite_v1_path(Method, "/", [], Bucket) when Method =/= 'GET' ->
    lists:flatten(["/buckets/", Bucket]);
rewrite_v1_path(_Method, "/", QS, Bucket) ->
    lists:flatten(["/buckets/", Bucket, format_bucket_qs(QS)]);
rewrite_v1_path(_Method, Path, QS, Bucket) ->
    lists:flatten(["/buckets/",
                   Bucket,
                   "/objects/",
                   mochiweb_util:quote_plus(string:strip(Path, left, $/)),
                   QS
                  ]).

%% @doc Separate the bucket name from the rest of the raw path in the
%% case where the bucket name is included in the path.
-spec separate_bucket_from_path([string()]) -> {string(), term()}.
separate_bucket_from_path([Bucket | []]) ->
    {Bucket, "/"};
separate_bucket_from_path([Bucket | RestPath]) ->
    {Bucket, lists:flatten([["/", PathElement] || PathElement <- RestPath])}.

%% @doc Format a bucket operation query string to conform the the
%% rewrite rules.
-spec format_bucket_qs(string()) -> string().
format_bucket_qs([]) ->
    "/objects";
format_bucket_qs("delete") ->
    "/objects";
format_bucket_qs(QS) ->
    ["/objects?", QS].

%% ===================================================================
%% Eunit tests
%% ===================================================================

-ifdef(TEST).

rewrite_path_test() ->
    %% List Buckets URL
    equal_paths("/buckets", rewrite_with(headers([]), "/v1/CF_xer7_34")),
    %% Bucket Operations
    equal_paths("/buckets/testbucket/objects", rewrite_with('GET', headers([]), "/v1/CF_xer7_34/testbucket")),
    equal_paths("/buckets/testbucket", rewrite_with('HEAD', headers([]), "/v1/CF_xer7_34/testbucket")),
    equal_paths("/buckets/testbucket", rewrite_with('PUT', headers([]), "/v1/CF_xer7_34/testbucket")),
    equal_paths("/buckets/testbucket", rewrite_with('DELETE', headers([]), "/v1/CF_xer7_34/testbucket")),
    %% Object Operations
    equal_paths("/buckets/testbucket/objects/testobject", rewrite_with(headers([]), "/v1/CF_xer7_34/testbucket/testobject")).

rewrite_header_test() ->
    Path = "/v1/CF_xer7_34/testbucket?a=b",
    {Headers, _} = rewrite_with(headers([]), Path),
    ?assertEqual(Path, mochiweb_headers:get_value(?RCS_REWRITE_HEADER, Headers)),
    ?assertEqual("v1", mochiweb_headers:get_value(?OOS_API_VSN_HEADER, Headers)),
    ?assertEqual("CF_xer7_34", mochiweb_headers:get_value(?OOS_ACCOUNT_HEADER, Headers)).

%% Helper function for eunit tests
headers(HeadersList) ->
    mochiweb_headers:make(HeadersList).

equal_paths(EPath, {_RHeaders, RPath}) ->
    ?assertEqual(EPath, RPath).


rewrite_with(Headers, Path) ->
    rewrite_with('GET', Headers, Path).

rewrite_with(Method, Headers, Path) ->
    Scheme = https,
    Version = {1, 1},
    rewrite(Method, Scheme, Version, Headers, Path).

-endif.
