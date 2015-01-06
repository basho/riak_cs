%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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

-module(riak_cs_s3_rewrite_legacy).

-export([rewrite/5, original_resource/1]).

-include("riak_cs.hrl").
-include("s3_api.hrl").

-define(RCS_REWRITE_HEADER, "x-rcs-rewrite-path").

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(export_all).
-endif.

%% @doc Function to rewrite headers prior to processing by webmachine.
-spec rewrite(atom(), atom(), {integer(), integer()}, gb_tree(), string()) ->
                     {gb_tree(), string()}.
rewrite(Method, Scheme, Vsn, Headers, Url) ->
    %% Unquote the path to accomodate some naughty client libs (looking
    %% at you Fog)
    {Path, QueryString, Fragment} = mochiweb_util:urlsplit_path(Url),
    UpdatedUrl = mochiweb_util:urlunsplit_path({mochiweb_util:unquote(Path), QueryString, Fragment}),
    riak_cs_s3_rewrite:rewrite(Method, Scheme, Vsn, Headers, UpdatedUrl).


-spec original_resource(term()) -> undefined | {string(), [{term(),term()}]}.
original_resource(RD) ->
    riak_cs_s3_rewrite:original_resource(RD).

%% ===================================================================
%% Eunit tests
%% ===================================================================

-ifdef(TEST).

rstr_test() ->
    ?assertEqual("foo." ++ ?ROOT_HOST,
                 riak_cs_s3_rewrite:bucket_from_host("foo." ++ ?ROOT_HOST ++ "." ++ ?ROOT_HOST,
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
    equal_paths("/buckets/testbucket/delete",
                rewrite_with('POST', headers([]),
                             "/testbucket/?delete")),
    equal_paths("/buckets/testbucket/delete",
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

%% This should be buggy behaviour but also we have to preserve old behaviour
legacy_rewrite_path_test() ->
    application:set_env(riak_cs, cs_root_host, ?ROOT_HOST),
    equal_paths("/buckets/testbucket/objects/testdir%2Ftestobject%2Bplus",
                rewrite_with(headers([]),
                             "/testbucket/testdir/testobject%2Bplus")),
    equal_paths("/buckets/testbucket/objects/testdir%2Ftestobject+plus",
                rewrite_with(headers([]),
                             "/testbucket/testdir/testobject+plus")),
    equal_paths("/buckets/testbucket/objects/testdir%2Ftestobject%2Bplus",
                rewrite_with(headers([]),
                             "/testbucket/testdir/testobject%2Bplus")),
    equal_paths("/buckets/testbucket/objects/testdir%2Ftestobject%25%00",
                rewrite_with(headers([]),
                             "/testbucket/testdir/testobject%%00")).

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
