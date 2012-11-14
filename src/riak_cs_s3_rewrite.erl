%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_s3_rewrite).

-export([rewrite/5, original_resource/1]).

-include("riak_cs.hrl").
-include("s3_api.hrl").

-define(RCS_REWRITE_HEADER, "x-rcs-rewrite-path").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type subresource() :: {string(), string()}.
-type subresources() :: [subresource()].

%% @doc Function to rewrite headers prior to processing by webmachine.
-spec rewrite(atom(), atom(), {integer(), integer()}, gb_tree(), string()) -> 
                     {gb_tree(), string()}.
rewrite(Method, _Scheme, _Vsn, Headers, RawPath) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, [], ?MODULE, <<"rewrite">>, []),
    Host = mochiweb_headers:get_value("host", Headers),
    HostBucket = bucket_from_host(Host),
    {Path, QueryString, _} = mochiweb_util:urlsplit_path(RawPath),    
    RewrittenPath = rewrite_path(Method, Path, QueryString, HostBucket),
    RewrittenHeaders = mochiweb_headers:default(?RCS_REWRITE_HEADER, 
                                                rcs_rewrite_header(RawPath, HostBucket),
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
    {Bucket, UpdPath} = separate_bucket_from_path(string:tokens(Path, [$/])),
    rewrite_path(Method, UpdPath, QS, Bucket);
rewrite_path(_Method, Path, _QS, "riak-cs") ->
    "/riak-cs" ++ Path;
rewrite_path(_Method, Path, _QS, "usage") ->
    "/usage" ++ Path;
rewrite_path(Method, "/", [], Bucket) when Method =/= 'GET' ->
    lists:flatten(["/buckets/", Bucket]);
rewrite_path(_Method, "/", QS, Bucket) ->
    SubResources = get_subresources(QS),
    lists:flatten(["/buckets/", Bucket, format_bucket_qs(QS, SubResources)]);
rewrite_path(_Method, Path, QS, Bucket) ->
    lists:flatten(["/buckets/",
                   Bucket,
                   "/objects/",
                   string:strip(Path, left, $/),
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
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, [], ?MODULE, <<"bucket_from_host">>, []),
    undefined;
bucket_from_host(HostHeader) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, [], ?MODULE, <<"bucket_from_host">>, []),
    HostNoPort = hd(string:tokens(HostHeader, ":")),
    {ok, RootHost} = application:get_env(riak_cs, cs_root_host),
    extract_bucket_from_host(string:tokens(HostNoPort, "."),
                             string:tokens(RootHost, ".")).

%% @doc Extract the bucket name from the `Host' header value if a
%% bucket name is present.
-spec extract_bucket_from_host([string()], [string()]) -> undefined | string().
extract_bucket_from_host(RootHost, RootHost) ->
    undefined;
extract_bucket_from_host([Bucket | RootHost], RootHost) ->
    Bucket;
extract_bucket_from_host(_Host, _RootHost) ->
    undefined.

%% @doc Separate the bucket name from the rest of the raw path in the
%% case where the bucket name is included in the path.
-spec separate_bucket_from_path([string()]) -> string().
separate_bucket_from_path([Bucket | []]) ->
    {Bucket, "/"};
separate_bucket_from_path([Bucket | RestPath]) ->
    {Bucket, lists:flatten([["/", PathElement] || PathElement <- RestPath])}.

%% @doc Format a bucket operation query string to conform the the
%% rewrite rules.
-spec format_bucket_qs(string(), subresources()) -> string().
format_bucket_qs([], []) ->
    "/objects";
format_bucket_qs("delete", []) ->
    "/objects";
format_bucket_qs(QS, []) ->
    ["/objects?", QS];
format_bucket_qs(_QS, SubResources) ->
    format_subresources(SubResources).

%% @doc Format an object operation query string to conform the the
%% rewrite rules.
-spec format_object_qs(subresources()) -> string().
format_object_qs(SubResources) ->
    UploadId = proplists:get_value("uploadId", SubResources, []),
    PartNum = proplists:get_value("partNumber", SubResources, []),
    format_object_qs(SubResources, UploadId, PartNum).

%% @doc Format an object operation query string to conform the the
%% rewrite rules.
-spec format_object_qs(subresources(), string(), string()) -> string().
format_object_qs(SubResources, [], []) ->
    format_subresources(SubResources);
format_object_qs(_SubResources, UploadId, []) ->
    ["/uploads/", UploadId];
format_object_qs(_SubResources, UploadId, PartNum) ->
    ["/uploads/", UploadId, "?partNumber=", PartNum].

%% @doc Format a string that expresses the subresource request
%% that can be appended to the URL.
-spec format_subresources(subresources()) -> string().
format_subresources([]) ->
    [];
format_subresources([{Key, []} | _]) ->
    ["/", Key].

%% @doc Parse the valid subresources from the raw path.
-spec get_subresources(string()) -> subresources().
get_subresources(QueryString) ->
    lists:filter(fun valid_subresource/1, mochiweb_util:parse_qs(QueryString)).

%% @doc Determine if a query parameter key is a valid S3 subresource
-spec valid_subresource(subresource()) -> boolean().
valid_subresource({Key, _}) ->
    lists:member(Key, ?SUBRESOURCES).


%% ===================================================================
%% Eunit tests
%% ===================================================================

-ifdef(TEST).

rewrite_path_test() ->
    application:set_env(riak_cs, cs_root_host, ?ROOT_HOST),
    %% List Buckets URL
    equal_paths("/buckets", rewrite_with(headers([]), "/")),    
    %% Bucket Operations
    equal_paths("/buckets/testbucket/objects", rewrite_with('GET', headers([]), "/testbucket")),
    equal_paths("/buckets/testbucket/objects", rewrite_with('GET', headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/")),
    equal_paths("/buckets/testbucket", rewrite_with('HEAD', headers([]), "/testbucket")),
    equal_paths("/buckets/testbucket", rewrite_with('HEAD', headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/")),
    equal_paths("/buckets/testbucket", rewrite_with('PUT', headers([]), "/testbucket")),
    equal_paths("/buckets/testbucket", rewrite_with('PUT', headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/")),
    equal_paths("/buckets/testbucket", rewrite_with('DELETE', headers([]), "/testbucket")),
    equal_paths("/buckets/testbucket", rewrite_with('DELETE', headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/")),
    equal_paths("/buckets/testbucket/acl", rewrite_with(headers([]), "/testbucket?acl")),
    equal_paths("/buckets/testbucket/acl", rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/?acl")),
    equal_paths("/buckets/testbucket/location", rewrite_with(headers([]), "/testbucket?location")),
    equal_paths("/buckets/testbucket/location", rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/?location")),
    equal_paths("/buckets/testbucket/versioning", rewrite_with(headers([]), "/testbucket?versioning")),
    equal_paths("/buckets/testbucket/versioning", rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/?versioning")),
    equal_paths("/buckets/testbucket/policy", rewrite_with(headers([]), "/testbucket?policy")),
    equal_paths("/buckets/testbucket/policy", rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/?policy")),
    equal_paths("/buckets/testbucket/uploads", rewrite_with(headers([]), "/testbucket?uploads")),
    equal_paths("/buckets/testbucket/uploads", rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/?uploads")),
    %% Object Operations
    equal_paths("/buckets/testbucket/objects/testobject", rewrite_with(headers([]), "/testbucket/testobject")),
    equal_paths("/buckets/testbucket/objects/testobject", rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/testobject")),
    equal_paths("/buckets/testbucket/objects/testobject/acl", rewrite_with(headers([]), "/testbucket/testobject?acl")),
    equal_paths("/buckets/testbucket/objects/testobject/acl", rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/testobject?acl")),
    equal_paths("/buckets/testbucket/objects/testobject/uploads", rewrite_with(headers([]), "/testbucket/testobject?uploads")),
    equal_paths("/buckets/testbucket/objects/testobject/uploads", rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/testobject?uploads")),
    equal_paths("/buckets/testbucket/objects/testobject/uploads/2", rewrite_with(headers([]), "/testbucket/testobject?uploadId=2")),
    equal_paths("/buckets/testbucket/objects/testobject/uploads/2", rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/testobject?uploadId=2")),
    equal_paths("/buckets/testbucket/objects/testobject/uploads/2?partNumber=1", rewrite_with(headers([]), "/testbucket/testobject?partNumber=1&uploadId=2")),
    equal_paths("/buckets/testbucket/objects/testobject/uploads/2?partNumber=1", rewrite_with(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/testobject?partNumber=1&uploadId=2")).

rewrite_header_test() ->
    Path = "/testbucket?a=b",
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
