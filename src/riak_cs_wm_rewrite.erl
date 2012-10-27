%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_wm_rewrite).

-export([rewrite/2]).

-include("riak_cs.hrl").
-include("s3_api.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type subresource() :: {string(), string()}.
-type subresources() :: [subresource()].

%% @doc Function to rewrite headers prior to processing by webmachine.
-spec rewrite(gb_tree(), string()) -> string().
rewrite(Headers, RawPath) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, [], ?MODULE, <<"rewrite">>, []),
    Host = mochiweb_headers:get_value("host", Headers),
    {Path, QueryString, _} = mochiweb_util:urlsplit_path(RawPath),
    do_rewrite(Path, QueryString, bucket_from_host(Host)).

%% @doc Internal function to handle rewriting the URL
-spec do_rewrite(string(), string(), undefined | string()) -> string().
do_rewrite("/", _QS, undefined) ->
    "/buckets";
do_rewrite(Path, QS, undefined) ->
    Bucket = extract_bucket_from_path(string:tokens(Path, [$/])),
    UpdPath = "/" ++ string:substr(Path, length(Bucket)+2),
    do_rewrite(UpdPath, QS, Bucket);
do_rewrite("/", QS, Bucket) ->
    SubResources = get_subresources(QS),
    lists:flatten(["/buckets/", Bucket, format_bucket_qs(QS, SubResources)]);
do_rewrite(Path, QS, Bucket) ->
    lists:flatten(["/buckets/",
                   Bucket,
                   "/objects/",
                   string:strip(Path, left, $/),
                   format_object_qs(get_subresources(QS))
                  ]).

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

%% @doc Extract the bucket name from the raw path in the case where
%% the bucket name is included in the path.
-spec extract_bucket_from_path([string()]) -> string().
extract_bucket_from_path([Bucket | _]) ->
    Bucket.

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

rewrite_test() ->
    application:set_env(riak_cs, cs_root_host, ?ROOT_HOST),
    %% List Buckets URL
    ?assertEqual("/buckets", rewrite(headers([]), "/")),
    %% Bucket Operations
    ?assertEqual("/buckets/testbucket/objects", rewrite(headers([]), "/testbucket")),
    ?assertEqual("/buckets/testbucket/objects", rewrite(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/")),
    ?assertEqual("/buckets/testbucket/acl", rewrite(headers([]), "/testbucket?acl")),
    ?assertEqual("/buckets/testbucket/acl", rewrite(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/?acl")),
    ?assertEqual("/buckets/testbucket/location", rewrite(headers([]), "/testbucket?location")),
    ?assertEqual("/buckets/testbucket/location", rewrite(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/?location")),
    ?assertEqual("/buckets/testbucket/versioning", rewrite(headers([]), "/testbucket?versioning")),
    ?assertEqual("/buckets/testbucket/versioning", rewrite(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/?versioning")),
    ?assertEqual("/buckets/testbucket/policy", rewrite(headers([]), "/testbucket?policy")),
    ?assertEqual("/buckets/testbucket/policy", rewrite(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/?policy")),
    ?assertEqual("/buckets/testbucket/uploads", rewrite(headers([]), "/testbucket?uploads")),
    ?assertEqual("/buckets/testbucket/uploads", rewrite(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/?uploads")),
    %% Object Operations
    ?assertEqual("/buckets/testbucket/objects/testobject", rewrite(headers([]), "/testbucket/testobject")),
    ?assertEqual("/buckets/testbucket/objects/testobject", rewrite(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/testobject")),
    ?assertEqual("/buckets/testbucket/objects/testobject/acl", rewrite(headers([]), "/testbucket/testobject?acl")),
    ?assertEqual("/buckets/testbucket/objects/testobject/acl", rewrite(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/testobject?acl")),
    ?assertEqual("/buckets/testbucket/objects/testobject/uploads", rewrite(headers([]), "/testbucket/testobject?uploads")),
    ?assertEqual("/buckets/testbucket/objects/testobject/uploads", rewrite(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/testobject?uploads")),
    ?assertEqual("/buckets/testbucket/objects/testobject/uploads/2", rewrite(headers([]), "/testbucket/testobject?uploadId=2")),
    ?assertEqual("/buckets/testbucket/objects/testobject/uploads/2", rewrite(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/testobject?uploadId=2")),
    ?assertEqual("/buckets/testbucket/objects/testobject/uploads/2?partNumber=1", rewrite(headers([]), "/testbucket/testobject?partNumber=1&uploadId=2")),
    ?assertEqual("/buckets/testbucket/objects/testobject/uploads/2?partNumber=1", rewrite(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/testobject?partNumber=1&uploadId=2")).

%% Helper function for eunit tests
headers(HeadersList) ->
    mochiweb_headers:make(HeadersList).

-endif.
