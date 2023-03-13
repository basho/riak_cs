%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved,
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

%% @doc Rewrite rule of Riak CS S3 API. Following diagram is overall url quote/unquote diagram
%% in case of hosted path style object names. Bucket names are much different in case of path
%% style; they won't be doubly escaped.
%%
%% quote state     RAW    escaped(1)  escaped(2)    example
%%
%%                  *                               'baz/foo bar'        'baz/foo+bar'
%% client app       +--------+                      'baz/foo%20bar'      'baz/foo%2Bbar'
%%                           |
%% HTTP                      |
%%                           |
%% webmachine                |
%% before rewrite            |
%% rewrite                   +------------+         'baz%2Ffoo%2520bar'  'baz%2Ffoo%252Bbar'
%% after rewrite                          |
%%                                        |
%% extract_key      +---------------------+
%%                  |
%%                  v
%%                  *                               'baz/foo bar'        'baz/foo+bar'



-module(riak_cs_aws_s3_rewrite).
-behaviour(riak_cs_rewrite).

-export([rewrite/5,
         original_resource/1,
         raw_url/1
        ]).

-include("riak_cs.hrl").
-include("riak_cs_web.hrl").
-include("aws_api.hrl").
-include_lib("webmachine/include/webmachine.hrl").
-include_lib("kernel/include/logger.hrl").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-include_lib("eunit/include/eunit.hrl").
-endif.

-type subresource() :: {string(), string()}.
-type query_params() :: [{string(), string()}].
-type subresources() :: [subresource()].


%% @doc Function to rewrite headers prior to processing by webmachine.
-spec rewrite(atom(), atom(), {integer(), integer()}, mochiweb_headers(), string()) ->
          {mochiweb_headers(), string()}.
rewrite(Method, _Scheme, _Vsn, Headers, Url) ->
    riak_cs_dtrace:dt_wm_entry(?MODULE, <<"rewrite">>),
    {Path, QueryString, _} = mochiweb_util:urlsplit_path(Url),
    rewrite_path_and_headers(Method, Headers, Url, Path, QueryString).

-spec original_resource(#wm_reqdata{}) -> undefined | {string(), [{term(),term()}]}.
original_resource(RD) ->
    riak_cs_rewrite:original_resource(RD).

-spec raw_url(#wm_reqdata{}) -> undefined | {string(), [{term(), term()}]}.
raw_url(RD) ->
    riak_cs_rewrite:raw_url(RD).


rewrite_path_and_headers(Method, Headers, Url, Path, QueryString) ->
    Host = mochiweb_headers:get_value("host", Headers),
    HostBucket = bucket_from_host(Host),
    RewrittenPath = rewrite_path(Method,
                                 Path,
                                 QueryString,
                                 HostBucket),
    RewrittenHeaders = mochiweb_headers:default(
                         ?RCS_RAW_URL_HEADER, Url,
                         mochiweb_headers:default(?RCS_REWRITE_HEADER,
                                                  rcs_rewrite_header(Url, HostBucket),
                                                  Headers)),
    {RewrittenHeaders, RewrittenPath}.


%% @doc Internal function to handle rewriting the URL
rewrite_path(_Method, "/", _QS, undefined) ->
    "/buckets";
rewrite_path(Method, Path, QS, undefined) ->
    {Bucket, UpdPath} = separate_bucket_from_path(Path),
    rewrite_path(Method, UpdPath, QS, mochiweb_util:unquote(Bucket));
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
    undefined;
bucket_from_host(HostHeader) ->
    {ok, RootHost} = application:get_env(riak_cs, s3_root_host),
    bucket_from_host(HostHeader, RootHost).

bucket_from_host(HostHeader, RootHost) ->
    HostNoPort = case string:tokens(HostHeader, ":") of
                     []    -> HostHeader;
                     [H|_] -> H
                 end,
    extract_bucket_from_host(HostNoPort, RootHost).

extract_bucket_from_host(Host, RootHost) ->
    %% Take the substring of the everything up to
    %% the '.' preceding the root host
    Bucket =
        case re:run(Host, "(.+\.|)(s3\.(?:(?:[a-z0-9-])+\.)?amazonaws\.com)",
                    [{capture, all_but_first, list}]) of
            {match, [M1, M2]} ->
                if RootHost == M2 ->
                        M1;
                   el/=se ->
                        logger:warning("accepting request sent to a (legitimate) AWS host"
                                       " \"~s\" not matching cs_root_host (\"~s\")", [Host, RootHost]),
                        M1
                end;
            _ ->
                undefined
        end,
    case Bucket of
        [] ->
            undefined;
        undefined ->
            undefined;
        _ ->
            lists:droplast(Bucket)
    end.

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
    "/delete";
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
    HaveAcl = (proplists:get_value("acl", SubResources) /= undefined),
    HaveUploads = (proplists:get_value("uploads", SubResources) /= undefined),
    UploadId = proplists:get_value("uploadId", SubResources, []),
    PartNum = proplists:get_value("partNumber", SubResources, []),
    VersionId = proplists:get_value("versionId", SubResources, binary_to_list(?LFS_DEFAULT_OBJECT_VERSION)),
    format_object_qs(SubResources, QueryParams, #{have_acl => HaveAcl,
                                                  have_uploads => HaveUploads,
                                                  version_id => VersionId,
                                                  upload_id => UploadId,
                                                  part_num => PartNum}).

%% @doc Format an object operation query string to conform the the
%% rewrite rules.
format_object_qs(_SubResources, QueryParams, #{version_id := VersionId,
                                               have_acl := true})  ->
    ["/versions/", VersionId, "/acl", format_query_params(QueryParams)];
format_object_qs(_SubResources, QueryParams, #{have_uploads := true,
                                               version_id := VersionId}) ->
    ["/versions/", VersionId, "/uploads", format_query_params(QueryParams)];

format_object_qs(SubResources, QueryParams, #{version_id := VersionId,
                                              upload_id := [],
                                              part_num := []}) ->
    ["/versions/", VersionId, format_subresources(SubResources), format_query_params(QueryParams)];
format_object_qs(_SubResources, QueryParams, #{version_id := VersionId,
                                              upload_id := UploadId,
                                              part_num := []}) ->
    ["/versions/", VersionId, "/uploads/", UploadId, format_query_params(QueryParams)];
format_object_qs(_SubResources, QueryParams, #{version_id := VersionId,
                                              upload_id := UploadId,
                                              part_num := PartNum}) ->
    ["/versions/", VersionId, "/uploads/", UploadId, format_query_params([{"partNumber", PartNum} | QueryParams])].

%% @doc Format a string that expresses the subresource request
%% that can be appended to the URL.
-spec format_subresources(subresources()) -> string().
format_subresources([]) ->
    [];
format_subresources([{"versionId", _}]) ->
    [];
format_subresources([{Key, []}]) ->
    ["/", Key].

%% @doc Format a proplist of query parameters into a string
-spec format_query_params(query_params()) -> iolist().
format_query_params([]) ->
    [];
format_query_params(QueryParams) ->
    format_query_params(QueryParams, []).

%% @doc Format a proplist of query parameters into a string
-spec format_query_params(query_params(), iolist()) -> iolist().
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


-ifdef(TEST).

extract_bucket_from_host_test() ->
    Cases = [ {"asdf.s3.amazonaws.com", "asdf"}
            , {"a.s.df.s3.amazonaws.com", "a.s.df"}
            , {"a-s.df.s3.amazonaws.com", "a-s.df"}
            , {"asdfff.s3.eu-west-1.amazonaws.com", "asdfff"}
            , {"a.s.df.s2.amazonaws.com", undefined}
            , {"a.s.df.s3.amazonaws.org", undefined}
            ],
    lists:foreach(fun({A, E}) -> ?assertEqual(extract_bucket_from_host(A, "s3.amazonaws.com"), E) end, Cases),
    lists:foreach(fun({A, E}) -> ?assertEqual(extract_bucket_from_host(A, "s3.us-east-2.amazonaws.com"), E) end, Cases),
    ok.

-endif.
