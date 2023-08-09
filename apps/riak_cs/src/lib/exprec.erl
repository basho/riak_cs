%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved,
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

-module(exprec).

-include("riak_cs.hrl").
-include("stanchion.hrl").
-include("aws_api.hrl").

-compile([export_all, nowarn_export_all]).

-define(ALL_RECORDS,
        [ rcs_user_v3
        , moss_bucket_v2
        , acl_v3
        , acl_grant_v2
        , bucket_versioning
          %% AWS records
        , iam_policy
        , statement
        , tag
        , role_last_used
        , permissions_boundary
        , role_v1
        , saml_provider_v1
        , credentials
        ]
       ).

-export_records(?ALL_RECORDS).

-exprecs_prefix(["", operation, ""]).
-exprecs_fname([prefix, "_", record]).
-exprecs_vfname([fname, "__", version]).
-compile({parse_transform, exprecs}).
