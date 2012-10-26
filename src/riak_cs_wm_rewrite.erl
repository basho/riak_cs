%% -------------------------------------------------------------------
%%
%% Copyright (c) 2007-2012 Basho Technologies, Inc.  All Rights Reserved.
%%
%% -------------------------------------------------------------------

-module(riak_cs_wm_rewrite).
-export([rewrite/5]).

-include("riak_cs.hrl").
-include("s3_api.hrl").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

rewrite(Headers, RawPath) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, [], ?MODULE, <<"rewrite">>, []),
    Host = mochiweb_headers:get_value("host", Headers),
    {Path, QueryString, _} = mochiweb_util:urlsplit_path(RawPath),
    do_rewrite(Path, QueryString, bucket_from_host(Host)).

do_rewrite("/", _QS, undefined) ->
    "/buckets";
do_rewrite(Path, QS, undefined) ->
    Bucket = extract_bucket_from_path(string:tokens(Path, [$/])),
    UpdPath = "/" ++ string:substr(Bucket, length(Bucket)+1),
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

bucket_from_host(undefined) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, [], ?MODULE, <<"bucket_from_host">>, []),
    undefined;
bucket_from_host(HostHeader) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, [], ?MODULE, <<"bucket_from_host">>, []),
    HostNoPort = hd(string:tokens(HostHeader, ":")),
    {ok, RootHost} = application:get_env(riak_cs, cs_root_host),
    case HostNoPort of
        RootHost ->
            undefined;
        Host ->
            case string:str(HostNoPort, RootHost) of
                0 ->
                    undefined;
                I ->
                    string:substr(Host, 1, I-2)
            end
    end.

rewrite(_Method, _Scheme, _Vsn, Headers, Path) ->
    riak_cs_dtrace:dtrace(?DT_WM_OP, 1, [], ?MODULE, <<"bucket_from_host">>, []),
    HostHeader = mochiweb_headers:get_value("host", Headers),
    case bucket_from_host(HostHeader) of
        undefined ->
            Path;
        Bucket ->
            "/" ++ Bucket ++ "/" ++ string:strip(Path, left, $/)
    end.

extract_bucket_from_path([Bucket | _]) ->
    Bucket.

format_bucket_qs([], []) ->
    "/objects";
format_bucket_qs("delete", []) ->
    "/objects";
format_bucket_qs(QS, []) ->
    ["/objects?", QS];
format_bucket_qs(_QS, SubResources) ->
    format_subresources(SubResources).

format_object_qs(SubResources) ->
    UploadId = proplists:get_value("uploadId", SubResources, []),
    PartNum = proplists:get_value("partNumber", SubResources, []),
    format_object_qs(SubResources, UploadId, PartNum).

format_object_qs(SubResources, [], []) ->
    format_subresources(SubResources);
format_object_qs(_SubResources, UploadId, []) ->
    ["/upload/", UploadId];
format_object_qs(_SubResources, UploadId, PartNum) ->
    ["/upload/", UploadId, "?partNumber=", PartNum].

%% @doc Format a string that expresses the subresource request
%% that can be appended to the URL.
-spec format_subresources([{string(), string()}]) -> string().
format_subresources([]) ->
    [];
format_subresources([{"uploads", []} | _]) ->
    "/upload";
format_subresources([{Key, []} | _]) ->
    ["/", Key].


%% @doc Parse the valid subresources from the raw path.
-spec get_subresources(string()) -> [{string(), string()}].
get_subresources(QueryString) ->
    lists:filter(fun valid_subresource/1, mochiweb_util:parse_qs(QueryString)).

%% @doc Determine if a query parameter key is a valid S3 subresource
-spec valid_subresource({string(), string()}) -> boolean().
valid_subresource({Key, _}) ->
    lists:member(Key, ?SUBRESOURCES).


%% ===================================================================
%% Eunit tests
%% ===================================================================

-ifdef(TEST).

rewrite_test() ->
    application:set_env(riak_cs, cs_root_host, ?ROOT_HOST),
    ?assertEqual("/buckets", rewrite(headers([]), "/")),
    ?assertEqual("/buckets/testbucket/objects", rewrite(headers([]), "/testbucket")),
    ?assertEqual("/buckets/testbucket/objects", rewrite(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/")),
    ?assertEqual("/buckets/testbucket/acl", rewrite(headers([]), "/testbucket?acl")),
    ?assertEqual("/buckets/testbucket/acl", rewrite(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/?acl")),
    ?assertEqual("/buckets/testbucket/location", rewrite(headers([]), "/testbucket?location")),
    ?assertEqual("/buckets/testbucket/location", rewrite(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/?location")),
    ?assertEqual("/buckets/testbucket/versioning", rewrite(headers([]), "/testbucket?versioning")),
    ?assertEqual("/buckets/testbucket/versioning", rewrite(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/?versioning")),
    ?assertEqual("/buckets/testbucket/policy", rewrite(headers([]), "/testbucket?policy")),
    ?assertEqual("/buckets/testbucket/policy", rewrite(headers([{"host", "testbucket." ++ ?ROOT_HOST}]), "/?policy")).

headers(HeadersList) ->
    mochiweb_headers:make(HeadersList).

-endif.

