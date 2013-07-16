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

-module(riak_cs_s3_rewrite).

-export([rewrite/5, original_resource/1]).

-include("riak_cs.hrl").
-include("s3_api.hrl").

-define(RCS_REWRITE_HEADER, "x-rcs-rewrite-path").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-endif.

-type subresource() :: {string(), string()}.
-type query_params() :: [{string(), string()}].
-type subresources() :: [subresource()].

%% @doc Function to rewrite headers prior to processing by webmachine.
-spec rewrite(atom(), atom(), {integer(), integer()}, gb_tree(), string()) ->
                     {gb_tree(), string()}.
rewrite(Method, _Scheme, _Vsn, Headers, Url) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"rewrite">>),
    Host = mochiweb_headers:get_value("host", Headers),
    HostBucket = bucket_from_host(Host),
    {Path, QueryString, _} = mochiweb_util:urlsplit_path(Url),
    %% Unquote the path to accomodate some naughty client libs (looking
    %% at you Fog)
    RewrittenPath = rewrite_path(Method,
                                 mochiweb_util:unquote(Path),
                                 QueryString,
                                 HostBucket),
    RewrittenHeaders = mochiweb_headers:default(?RCS_REWRITE_HEADER,
                                                rcs_rewrite_header(Url, HostBucket),
                                                Headers),
    {RewrittenHeaders, RewrittenPath}.

-spec original_resource(term()) -> undefined | {string(), [{term(),term()}]}.
original_resource(RD) ->
    case wrq:get_req_header(?RCS_REWRITE_HEADER, RD) of
        undefined -> undefined;
        RawPath ->
            {Path, QS, _} = mochiweb_util:urlsplit_path(RawPath),
            {Path, mochiweb_util:parse_qs(QS)}
    end.

%% @doc Internal function to handle rewriting the URL
-spec rewrite_path(atom(),string(), string(), undefined | string()) -> string().
rewrite_path(_Method, "/", _QS, undefined) ->
    "/buckets";
rewrite_path(Method, Path, QS, undefined) ->
    {Bucket, UpdPath} = separate_bucket_from_path(Path),
    rewrite_path(Method, UpdPath, QS, Bucket);
rewrite_path(_Method, Path, _QS, "riak-cs") ->
    "/riak-cs" ++ Path;
rewrite_path(_Method, Path, _QS, "usage") ->
    "/usage" ++ Path;
rewrite_path(Method, "/", [], Bucket) when Method =/= 'GET' ->
    lists:flatten(["/buckets/", Bucket]);
rewrite_path(Method, "/", QS, Bucket) ->
    {SubResources, QueryParams} = get_subresources(QS),
    lists:flatten(["/buckets/", Bucket, format_bucket_qs(Method,
                                                         QueryParams,
                                                         SubResources)]);
rewrite_path(_Method, Path, QS, Bucket) ->
    lists:flatten(["/buckets/",
                   Bucket,
                   "/objects/",
                   mochiweb_util:quote_plus(string:strip(Path, left, $/)),
                   format_object_qs(get_subresources(QS))
                  ]).

%% @doc, build the path to be stored as the rewritten path in the request. Bucket name
%% from the host header is added if it exists.
rcs_rewrite_header(RawPath, undefined) ->
    RawPath;
rcs_rewrite_header(RawPath, Bucket) ->
    "/" ++ Bucket ++ RawPath.

%% @doc Extract the bucket name that may have been prepended to the
%% host name in the Host header value.
-spec bucket_from_host(undefined | string()) -> undefined | string().
bucket_from_host(undefined) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"bucket_from_host">>),
    undefined;
bucket_from_host(HostHeader) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"bucket_from_host">>),
    {ok, RootHost} = application:get_env(riak_cs, cs_root_host),
    bucket_from_host(HostHeader, RootHost).

bucket_from_host(HostHeader, RootHost) ->
    HostNoPort = case string:tokens(HostHeader, ":") of
                     []    -> HostHeader;
                     [H|_] -> H
                 end,
    extract_bucket_from_host(HostNoPort,
                             string:rstr(HostNoPort, RootHost)).

%% @doc Extract the bucket name from the `Host' header value if a
%% bucket name is present.
-spec extract_bucket_from_host(string(), non_neg_integer()) -> undefined | string().
extract_bucket_from_host(_Host, 0) ->
    undefined;
extract_bucket_from_host(_Host, 1) ->
    undefined;
extract_bucket_from_host(Host, RootHostIndex) ->
    %% Take the substring of the everything up to
    %% the '.' preceding the root host
    string:sub_string(Host, 1, RootHostIndex-2).

%% @doc Separate the bucket name from the rest of the raw path in the
%% case where the bucket name is included in the path.
-spec separate_bucket_from_path(string()) -> {nonempty_string(), string()}.
separate_bucket_from_path([$/ | Rest]) ->
    separate_bucket_from_path(Rest, []).

separate_bucket_from_path([], Acc) ->
    {lists:reverse(Acc), "/"};
separate_bucket_from_path([$/ | _] = Path, Acc) ->
    {lists:reverse(Acc), Path};
separate_bucket_from_path([C | Rest], Acc) ->
    separate_bucket_from_path(Rest, [C | Acc]).

%% @doc Format a bucket operation query string to conform the the
%% rewrite rules.
-spec format_bucket_qs(atom(), query_params(), subresources()) -> string().
format_bucket_qs('POST', [{"delete", []}], []) ->
    "/objects";
format_bucket_qs(Method, QueryParams, [])
  when Method =:= 'GET'; Method =:= 'POST' ->
    ["/objects",
     format_query_params(QueryParams)];
format_bucket_qs(_Method, QueryParams, SubResources) ->
    [format_subresources(SubResources),
     format_query_params(QueryParams)].

%% @doc Format an object operation query string to conform the the
%% rewrite rules.
-spec format_object_qs({subresources(), query_params()}) -> string().
format_object_qs({SubResources, QueryParams}) ->
    UploadId = proplists:get_value("uploadId", SubResources, []),
    PartNum = proplists:get_value("partNumber", SubResources, []),
    format_object_qs(SubResources, QueryParams, UploadId, PartNum).

%% @doc Format an object operation query string to conform the the
%% rewrite rules.
-spec format_object_qs(subresources(), query_params(), string(), string()) -> string().
format_object_qs(SubResources, QueryParams, [], []) ->
    [format_subresources(SubResources), format_query_params(QueryParams)];
format_object_qs(_SubResources, QueryParams, UploadId, []) ->
    ["/uploads/", UploadId, format_query_params(QueryParams)];
format_object_qs(_SubResources, QueryParams, UploadId, PartNum) ->
    ["/uploads/", UploadId, format_query_params([{"partNumber", PartNum} | QueryParams])].

%% @doc Format a string that expresses the subresource request
%% that can be appended to the URL.
-spec format_subresources(subresources()) -> string().
format_subresources([]) ->
    [];
format_subresources([{Key, []} | _]) ->
    ["/", Key].

%% @doc Format a proplist of query parameters into a string
-spec format_query_params(query_params()) -> string().
format_query_params([]) ->
    [];
format_query_params(QueryParams) ->
    format_query_params(QueryParams, []).

%% @doc Format a proplist of query parameters into a string
-spec format_query_params(query_params(), list()) -> list().
format_query_params([], QS) ->
    ["?", QS];
format_query_params([{Key, []} | RestParams], []) ->
    format_query_params(RestParams, [Key]);
format_query_params([{Key, []} | RestParams], QS) ->
    format_query_params(RestParams, [[Key, "&"] | QS]);
format_query_params([{Key, Value} | RestParams], []) ->
    format_query_params(RestParams, [Key, "=", mochiweb_util:quote_plus(Value)]);
format_query_params([{Key, Value} | RestParams], QS) ->
    format_query_params(RestParams, [[Key, "=", mochiweb_util:quote_plus(Value), "&"] | QS]).

%% @doc Parse the valid subresources from the raw path.
-spec get_subresources(string()) -> {subresources(), query_params()}.
get_subresources(QueryString) ->
    lists:partition(fun valid_subresource/1, mochiweb_util:parse_qs(QueryString)).

%% @doc Determine if a query parameter key is a valid S3 subresource
-spec valid_subresource(subresource()) -> boolean().
valid_subresource({Key, _}) ->
    lists:member(Key, ?SUBRESOURCES).


%% ===================================================================
%% Eunit tests
%% ===================================================================

-ifdef(TEST).

rstr_test() ->
    ?assertEqual("foo." ++ ?ROOT_HOST,
                 bucket_from_host("foo." ++ ?ROOT_HOST ++ "." ++ ?ROOT_HOST,
                                  ?ROOT_HOST)).

rewrite_path_test() ->
    application:set_env(riak_cs, cs_root_host, ?ROOT_HOST),
    %% List Buckets URL
    equal_paths("/buckets",
                rewrite_with(headers([]), "/")),
    %% Bucket Operations
    equal_paths("/buckets/testbucket/objects",
                rewrite_with('GET', headers([]),
                             "/testbucket")),
    equal_paths("/buckets/testbucket/objects",
                rewrite_with('GET', headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/")),
    equal_paths("/buckets/testbucket/objects?max-keys=20&delimiter=%2F&prefix=123",
                rewrite_with('GET', headers([]),
                             "/testbucket?prefix=123&delimiter=/&max-keys=20")),
    equal_paths("/buckets/testbucket/objects?max-keys=20&delimiter=%2F&prefix=123",
                rewrite_with('GET', headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/?prefix=123&delimiter=/&max-keys=20")),
    equal_paths("/buckets/testbucket",
                rewrite_with('HEAD', headers([]), "/testbucket")),
    equal_paths("/buckets/testbucket",
                rewrite_with('HEAD', headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/")),
    equal_paths("/buckets/testbucket",
                rewrite_with('PUT', headers([]),
                             "/testbucket")),
    equal_paths("/buckets/testbucket",
                rewrite_with('PUT', headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/")),
    equal_paths("/buckets/testbucket",
                rewrite_with('DELETE', headers([]),
                             "/testbucket")),
    equal_paths("/buckets/testbucket",
                rewrite_with('DELETE', headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/")),
    equal_paths("/buckets/testbucket/acl",
                rewrite_with(headers([]), "/testbucket?acl")),
    equal_paths("/buckets/testbucket/acl",
                rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/?acl")),
    equal_paths("/buckets/testbucket/location",
                rewrite_with(headers([]), "/testbucket?location")),
    equal_paths("/buckets/testbucket/location",
                rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/?location")),
    equal_paths("/buckets/testbucket/versioning",
                rewrite_with(headers([]), "/testbucket?versioning")),
    equal_paths("/buckets/testbucket/versioning",
                rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/?versioning")),
    equal_paths("/buckets/testbucket/policy",
                rewrite_with(headers([]),
                             "/testbucket?policy")),
    equal_paths("/buckets/testbucket/policy",
                rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/?policy")),
    equal_paths("/buckets/testbucket/uploads",
                rewrite_with(headers([]),
                             "/testbucket?uploads")),
    equal_paths("/buckets/testbucket/uploads",
                rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/?uploads")),
    equal_paths("/buckets/testbucket/uploads?delimiter=D&prefix=ABC&max-uploads=10"
                "&key-marker=bob&upload-id-marker=blah",
                rewrite_with(headers([]),
                             "/testbucket?uploads&upload-id-marker=blah&key-marker=bob"
                             "&max-uploads=10&prefix=ABC&delimiter=D")),
    equal_paths("/buckets/testbucket/uploads?delimiter=D&prefix=ABC&max-uploads=10"
                "&key-marker=bob&upload-id-marker=blah",
                rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/?uploads&upload-id-marker=blah&key-marker=bob"
                             "&max-uploads=10&prefix=ABC&delimiter=D")),
    equal_paths("/buckets/testbucket/objects",
                rewrite_with('POST', headers([]),
                             "/testbucket/?delete")),
    equal_paths("/buckets/testbucket/objects",
                rewrite_with('POST', headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/?delete")),
    %% Object Operations
    equal_paths("/buckets/testbucket/objects/testobject",
                rewrite_with(headers([]),
                             "/testbucket/testobject")),
    equal_paths("/buckets/testbucket/objects/testdir%2F",
                rewrite_with(headers([]),
                             "/testbucket/testdir/")),
    equal_paths("/buckets/testbucket/objects/testdir%2Ftestobject",
                rewrite_with(headers([]),
                             "/testbucket/testdir/testobject")),
    equal_paths("/buckets/testbucket/objects/testobject",
                rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/testobject")),
    equal_paths("/buckets/testbucket/objects/testdir%2F",
                rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/testdir/")),
    equal_paths("/buckets/testbucket/objects/testdir%2Ftestobject",
                rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/testdir/testobject")),
    equal_paths("/buckets/testbucket/objects/testobject/acl",
                rewrite_with(headers([]),
                             "/testbucket/testobject?acl")),
    equal_paths("/buckets/testbucket/objects/testdir%2F/acl",
                rewrite_with(headers([]),
                             "/testbucket/testdir/?acl")),
    equal_paths("/buckets/testbucket/objects/testdir%2Ftestobject/acl",
                rewrite_with(headers([]),
                             "/testbucket/testdir/testobject?acl")),
    equal_paths("/buckets/testbucket/objects/testobject/acl",
                rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/testobject?acl")),
    equal_paths("/buckets/testbucket/objects/testdir%2F/acl",
                rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/testdir/?acl")),
    equal_paths("/buckets/testbucket/objects/testdir%2Ftestobject/acl",
                rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/testdir/testobject?acl")),
    equal_paths("/buckets/testbucket/objects/testobject/uploads",
                rewrite_with(headers([]),
                             "/testbucket/testobject?uploads")),
    equal_paths("/buckets/testbucket/objects/testobject/uploads",
                rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/testobject?uploads")),
    equal_paths("/buckets/testbucket/objects/testobject/uploads/2",
                rewrite_with(headers([]),
                             "/testbucket/testobject?uploadId=2")),
    equal_paths("/buckets/testbucket/objects/testobject/uploads/2",
                rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/testobject?uploadId=2")),
    equal_paths("/buckets/testbucket/objects/testobject/uploads/2?partNumber=1",
                rewrite_with(headers([]),
                             "/testbucket/testobject?partNumber=1&uploadId=2")),
    equal_paths("/buckets/testbucket/objects/testobject/uploads/2?partNumber=1",
                rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/testobject?partNumber=1&uploadId=2")),
    equal_paths("/buckets/testbucket/objects/testobject/uploads/2?AWSAccessKeyId=BF_BI8XYKFJSIW-NNAIR"
                "&Expires=1364406757&Signature=x%2B0vteNN1YillZNw4yDGVQWrT2s%3D",
                rewrite_with(headers([]),
                             "/testbucket/testobject?Signature=x%2B0vteNN1YillZNw4yDGVQWrT2s%3D"
                             "&Expires=1364406757&AWSAccessKeyId=BF_BI8XYKFJSIW-NNAIR&uploadId=2")),
    equal_paths("/buckets/testbucket/objects/testobject/uploads/2?AWSAccessKeyId=BF_BI8XYKFJSIW-NNAIR"
                "&Expires=1364406757&Signature=x%2B0vteNN1YillZNw4yDGVQWrT2s%3D",
                rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/testobject?Signature=x%2B0vteNN1YillZNw4yDGVQWrT2s%3D"
                             "&Expires=1364406757&AWSAccessKeyId=BF_BI8XYKFJSIW-NNAIR&uploadId=2")),
    equal_paths("/buckets/testbucket/objects/testobject/uploads/2?AWSAccessKeyId=BF_BI8XYKFJSIW-NNAIR"
                "&Expires=1364406757&Signature=x%2B0vteNN1YillZNw4yDGVQWrT2s%3D&partNumber=1",
                rewrite_with(headers([]),
                             "/testbucket/testobject?Signature=x%2B0vteNN1YillZNw4yDGVQWrT2s%3D"
                             "&Expires=1364406757&AWSAccessKeyId=BF_BI8XYKFJSIW-NNAIR&partNumber=1&uploadId=2")),
    equal_paths("/buckets/testbucket/objects/testobject/uploads/2?AWSAccessKeyId=BF_BI8XYKFJSIW-NNAIR"
                "&Expires=1364406757&Signature=x%2B0vteNN1YillZNw4yDGVQWrT2s%3D&partNumber=1",
                rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/testobject?Signature=x%2B0vteNN1YillZNw4yDGVQWrT2s%3D"
                             "&Expires=1364406757&AWSAccessKeyId=BF_BI8XYKFJSIW-NNAIR&partNumber=1&uploadId=2")),
    equal_paths("/buckets/testbucket/objects/testobject?AWSAccessKeyId=BF_BI8XYKFJSIW-NNAIR"
                "&Expires=1364406757&Signature=x%2B0vteNN1YillZNw4yDGVQWrT2s%3D",
                rewrite_with(headers([]),
                             "/testbucket/testobject?Signature=x%2B0vteNN1YillZNw4yDGVQWrT2s%3D"
                             "&Expires=1364406757&AWSAccessKeyId=BF_BI8XYKFJSIW-NNAIR")),
    equal_paths("/buckets/testbucket/objects/testobject?AWSAccessKeyId=BF_BI8XYKFJSIW-NNAIR"
                "&Expires=1364406757&Signature=x%2B0vteNN1YillZNw4yDGVQWrT2s%3D",
                rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/testobject?Signature=x%2B0vteNN1YillZNw4yDGVQWrT2s%3D"
                             "&Expires=1364406757&AWSAccessKeyId=BF_BI8XYKFJSIW-NNAIR")).

rewrite_header_test() ->
    Path = "/testbucket?y=z&a=b&m=n",
    {Headers, _} = rewrite_with(headers([]), Path),
    ?assertEqual(Path, mochiweb_headers:get_value(?RCS_REWRITE_HEADER, Headers)).


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
