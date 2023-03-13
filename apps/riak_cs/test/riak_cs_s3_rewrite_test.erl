%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved,
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

-module(riak_cs_s3_rewrite_test).

-include("riak_cs.hrl").
-include("aws_api.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(RCS_REWRITE_HEADER, "x-rcs-rewrite-path").
-compile(export_all).
-compile(nowarn_export_all).

rewrite_path_test() ->
    application:set_env(riak_cs, cs_root_host, ?ROOT_HOST),
    rstrt(),
    rewrite_header(riak_cs_s3_rewrite),
    rewrite_path(riak_cs_s3_rewrite),
    strict_rewrite_path(riak_cs_s3_rewrite).

legacy_rewrite_path_test() ->
    application:set_env(riak_cs, cs_root_host, ?ROOT_HOST),
    rewrite_header(riak_cs_s3_rewrite_legacy),
    rewrite_path(riak_cs_s3_rewrite_legacy),
    legacy_rewrite_path(riak_cs_s3_rewrite_legacy).

rstrt() ->
    ?assertEqual("foo." ++ ?ROOT_HOST,
                 riak_cs_s3_rewrite:bucket_from_host("foo." ++ ?ROOT_HOST ++ "." ++ ?ROOT_HOST,
                                                     ?ROOT_HOST)).

rewrite_path(Mod) ->
    application:set_env(riak_cs, cs_root_host, ?ROOT_HOST),
    %% List Buckets URL
    equal_paths("/buckets",
                rewrite_with(Mod, headers([]), "/")),
    %% Bucket Operations
    equal_paths("/buckets/testbucket/objects",
                rewrite_with(Mod, 'GET', headers([]),
                             "/testbucket")),
    equal_paths("/buckets/testbucket/objects",
                rewrite_with(Mod, 'GET', headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/")),
    equal_paths("/buckets/testbucket/objects?max-keys=20&delimiter=%2F&prefix=123",
                rewrite_with(Mod, 'GET', headers([]),
                             "/testbucket?prefix=123&delimiter=/&max-keys=20")),
    equal_paths("/buckets/testbucket/objects?max-keys=20&delimiter=%2F&prefix=123",
                rewrite_with(Mod, 'GET', headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/?prefix=123&delimiter=/&max-keys=20")),
    equal_paths("/buckets/testbucket",
                rewrite_with(Mod, 'HEAD', headers([]), "/testbucket")),
    equal_paths("/buckets/testbucket",
                rewrite_with(Mod, 'HEAD', headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/")),
    equal_paths("/buckets/testbucket",
                rewrite_with(Mod, 'PUT', headers([]),
                             "/testbucket")),
    equal_paths("/buckets/testbucket",
                rewrite_with(Mod, 'PUT', headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/")),
    equal_paths("/buckets/testbucket",
                rewrite_with(Mod, 'DELETE', headers([]),
                             "/testbucket")),
    equal_paths("/buckets/testbucket",
                rewrite_with(Mod, 'DELETE', headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/")),
    equal_paths("/buckets/testbucket/acl",
                rewrite_with(Mod, headers([]), "/testbucket?acl")),
    equal_paths("/buckets/testbucket/acl",
                rewrite_with(Mod, headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/?acl")),
    equal_paths("/buckets/testbucket/location",
                rewrite_with(Mod, headers([]), "/testbucket?location")),
    equal_paths("/buckets/testbucket/location",
                rewrite_with(Mod, headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/?location")),
    equal_paths("/buckets/testbucket/versioning",
                rewrite_with(Mod, headers([]), "/testbucket?versioning")),
    equal_paths("/buckets/testbucket/versioning",
                rewrite_with(Mod, headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/?versioning")),
    equal_paths("/buckets/testbucket/policy",
                rewrite_with(Mod, headers([]),
                             "/testbucket?policy")),
    equal_paths("/buckets/testbucket/policy",
                rewrite_with(Mod, headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/?policy")),
    equal_paths("/buckets/testbucket/uploads",
                rewrite_with(Mod, headers([]),
                             "/testbucket?uploads")),
    equal_paths("/buckets/testbucket/uploads",
                rewrite_with(Mod, headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/?uploads")),
    equal_paths("/buckets/testbucket/uploads?delimiter=D&prefix=ABC&max-uploads=10"
                "&key-marker=bob&upload-id-marker=blah",
                rewrite_with(Mod, headers([]),
                             "/testbucket?uploads&upload-id-marker=blah&key-marker=bob"
                             "&max-uploads=10&prefix=ABC&delimiter=D")),
    equal_paths("/buckets/testbucket/uploads?delimiter=D&prefix=ABC&max-uploads=10"
                "&key-marker=bob&upload-id-marker=blah",
                rewrite_with(Mod, headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/?uploads&upload-id-marker=blah&key-marker=bob"
                             "&max-uploads=10&prefix=ABC&delimiter=D")),
    equal_paths("/buckets/testbucket/delete",
                rewrite_with(Mod, 'POST', headers([]),
                             "/testbucket/?delete")),
    equal_paths("/buckets/testbucket/delete",
                rewrite_with(Mod, 'POST', headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/?delete")),
    %% Object Operations
    equal_paths("/buckets/testbucket/objects/testobject/versions/null",
                rewrite_with(Mod,
                             headers([]),
                             "/testbucket/testobject")),
    equal_paths("/buckets/testbucket/objects/testdir%2F/versions/null",
                rewrite_with(Mod,
                             headers([]),
                             "/testbucket/testdir/")),
    equal_paths("/buckets/testbucket/objects/testdir%2Ftestobject/versions/null",
                rewrite_with(Mod,
                             headers([]),
                             "/testbucket/testdir/testobject")),
    equal_paths("/buckets/testbucket/objects/testobject/versions/null",
                rewrite_with(Mod,
                             headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/testobject")),
    equal_paths("/buckets/testbucket/objects/testdir%2F/versions/null",
                rewrite_with(Mod,
                             headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/testdir/")),
    equal_paths("/buckets/testbucket/objects/testdir%2Ftestobject/versions/null",
                rewrite_with(Mod,
                             headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/testdir/testobject")),
    equal_paths("/buckets/testbucket/objects/testobject/versions/null/acl",
                rewrite_with(Mod,
                             headers([]),
                             "/testbucket/testobject?acl")),
    equal_paths("/buckets/testbucket/objects/testdir%2F/versions/null/acl",
                rewrite_with(Mod,
                             headers([]),
                             "/testbucket/testdir/?acl")),
    equal_paths("/buckets/testbucket/objects/testdir%2Ftestobject/versions/null/acl",
                rewrite_with(Mod,
                             headers([]),
                             "/testbucket/testdir/testobject?acl")),
    equal_paths("/buckets/testbucket/objects/testobject/versions/null/acl",
                rewrite_with(Mod,
                             headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/testobject?acl")),
    equal_paths("/buckets/testbucket/objects/testdir%2F/versions/null/acl",
                rewrite_with(Mod,
                             headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/testdir/?acl")),
    equal_paths("/buckets/testbucket/objects/testdir%2Ftestobject/versions/null/acl",
                rewrite_with(Mod,
                             headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/testdir/testobject?acl")),
    equal_paths("/buckets/testbucket/objects/testobject/versions/null/uploads",
                rewrite_with(Mod,
                             headers([]),
                             "/testbucket/testobject?uploads")),
    equal_paths("/buckets/testbucket/objects/testobject/versions/null/uploads",
                rewrite_with(Mod,
                             headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/testobject?uploads")),
    equal_paths("/buckets/testbucket/objects/testobject/versions/null/uploads/2",
                rewrite_with(Mod,
                             headers([]),
                             "/testbucket/testobject?uploadId=2")),
    equal_paths("/buckets/testbucket/objects/testobject/versions/null/uploads/2",
                rewrite_with(Mod,
                             headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/testobject?uploadId=2")),
    equal_paths("/buckets/testbucket/objects/testobject/versions/null/uploads/2?partNumber=1",
                rewrite_with(Mod,
                             headers([]),
                             "/testbucket/testobject?partNumber=1&uploadId=2")),
    equal_paths("/buckets/testbucket/objects/testobject/versions/null/uploads/2?partNumber=1",
                rewrite_with(Mod,
                             headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/testobject?partNumber=1&uploadId=2")),
    equal_paths("/buckets/testbucket/objects/testobject/versions/null/uploads/2?AWSAccessKeyId=BF_BI8XYKFJSIW-NNAIR"
                "&Expires=1364406757&Signature=x%2B0vteNN1YillZNw4yDGVQWrT2s%3D",
                rewrite_with(Mod,
                             headers([]),
                             "/testbucket/testobject?Signature=x%2B0vteNN1YillZNw4yDGVQWrT2s%3D"
                             "&Expires=1364406757&AWSAccessKeyId=BF_BI8XYKFJSIW-NNAIR&uploadId=2")),
    equal_paths("/buckets/testbucket/objects/testobject/versions/null/uploads/2?AWSAccessKeyId=BF_BI8XYKFJSIW-NNAIR"
                "&Expires=1364406757&Signature=x%2B0vteNN1YillZNw4yDGVQWrT2s%3D",
                rewrite_with(Mod,
                             headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/testobject?Signature=x%2B0vteNN1YillZNw4yDGVQWrT2s%3D"
                             "&Expires=1364406757&AWSAccessKeyId=BF_BI8XYKFJSIW-NNAIR&uploadId=2")),
    equal_paths("/buckets/testbucket/objects/testobject/versions/null/uploads/2?AWSAccessKeyId=BF_BI8XYKFJSIW-NNAIR"
                "&Expires=1364406757&Signature=x%2B0vteNN1YillZNw4yDGVQWrT2s%3D&partNumber=1",
                rewrite_with(Mod,
                             headers([]),
                             "/testbucket/testobject?Signature=x%2B0vteNN1YillZNw4yDGVQWrT2s%3D"
                             "&Expires=1364406757&AWSAccessKeyId=BF_BI8XYKFJSIW-NNAIR&partNumber=1&uploadId=2")),
    equal_paths("/buckets/testbucket/objects/testobject/versions/null/uploads/2?AWSAccessKeyId=BF_BI8XYKFJSIW-NNAIR"
                "&Expires=1364406757&Signature=x%2B0vteNN1YillZNw4yDGVQWrT2s%3D&partNumber=1",
                rewrite_with(Mod,
                             headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/testobject?Signature=x%2B0vteNN1YillZNw4yDGVQWrT2s%3D"
                             "&Expires=1364406757&AWSAccessKeyId=BF_BI8XYKFJSIW-NNAIR&partNumber=1&uploadId=2")),
    equal_paths("/buckets/testbucket/objects/testobject/versions/null?AWSAccessKeyId=BF_BI8XYKFJSIW-NNAIR"
                "&Expires=1364406757&Signature=x%2B0vteNN1YillZNw4yDGVQWrT2s%3D",
                rewrite_with(Mod,
                             headers([]),
                             "/testbucket/testobject?Signature=x%2B0vteNN1YillZNw4yDGVQWrT2s%3D"
                             "&Expires=1364406757&AWSAccessKeyId=BF_BI8XYKFJSIW-NNAIR")),
    equal_paths("/buckets/testbucket/objects/testobject/versions/null?AWSAccessKeyId=BF_BI8XYKFJSIW-NNAIR"
                "&Expires=1364406757&Signature=x%2B0vteNN1YillZNw4yDGVQWrT2s%3D",
                rewrite_with(Mod,
                             headers([{"host", "testbucket." ++ ?ROOT_HOST}]),
                             "/testobject?Signature=x%2B0vteNN1YillZNw4yDGVQWrT2s%3D"
                             "&Expires=1364406757&AWSAccessKeyId=BF_BI8XYKFJSIW-NNAIR")),
    %% Urlencoded path-style bucketname.
    equal_paths("/buckets/testbucket/objects/testobject/versions/null",
                rewrite_with(Mod,
                             headers([]),
                             "/%74estbucket/testobject")),
    equal_paths("/buckets/testbucket/objects/path%2Ftestobject/versions/null",
                rewrite_with(Mod,
                             headers([]),
                             "/%74estbucket/path/testobject")).

strict_rewrite_path(Mod) ->
    equal_paths("/buckets/testbucket/objects/testdir%2Ftestobject%252Bplus/versions/null",
                rewrite_with(Mod,
                             headers([]),
                             "/testbucket/testdir/testobject%2Bplus")),
    equal_paths("/buckets/testbucket/objects/testdir%2Ftestobject%2Bplus/versions/null",
                rewrite_with(Mod,
                             headers([]),
                             "/testbucket/testdir/testobject+plus")),
    equal_paths("/buckets/testbucket/objects/testdir%2Ftestobject%25%2500/versions/null",
                rewrite_with(Mod,
                             headers([]),
                             "/testbucket/testdir/testobject%%00")).

%% This should be buggy behaviour but also we have to preserve old behaviour
legacy_rewrite_path(Mod) ->
    equal_paths("/buckets/testbucket/objects/testdir%2Ftestobject%2Bplus/versions/null",
                rewrite_with(Mod,
                             headers([]),
                             "/testbucket/testdir/testobject%2Bplus")),
    equal_paths("/buckets/testbucket/objects/testdir%2Ftestobject+plus/versions/null",
                rewrite_with(Mod,
                             headers([]),
                             "/testbucket/testdir/testobject+plus")),
    equal_paths("/buckets/testbucket/objects/testdir%2Ftestobject%2Bplus/versions/null",
                rewrite_with(Mod,
                             headers([]),
                             "/testbucket/testdir/testobject%2Bplus")),
    equal_paths("/buckets/testbucket/objects/testdir%2Ftestobject%25%00/versions/null",
                rewrite_with(Mod,
                             headers([]),
                             "/testbucket/testdir/testobject%%00")).

rewrite_header(Mod) ->
    Path = "/testbucket?y=z&a=b&m=n",
    {Headers, _} = rewrite_with(Mod, headers([]), Path),
    ?assertEqual(Path, mochiweb_headers:get_value(?RCS_REWRITE_HEADER, Headers)).


%% Helper function for eunit tests
headers(HeadersList) ->
    mochiweb_headers:make(HeadersList).

equal_paths(EPath, {_RHeaders, RPath}) ->
    ?assertEqual(EPath, RPath).

rewrite_with(Mod, Headers, Path) ->
    rewrite_with(Mod, 'GET', Headers, Path).

rewrite_with(Mod, Method, Headers, Path) ->
    Scheme = https,
    Version = {1, 1},
    Mod:rewrite(Method, Scheme, Version, Headers, Path).
