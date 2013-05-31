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

-define(DEFAULT_OS_AUTH_URL, "http://localhost:35357/v2.0/").
-define(DEFAULT_TOKENS_RESOURCE, "tokens/").
-define(DEFAULT_S3_TOKENS_RESOURCE, "s3tokens/").
-define(DEFAULT_OS_USERS_RESOURCE, "users/").
-define(DEFAULT_OS_ADMIN_TOKEN, "ADMIN").
-define(DEFAULT_OS_OPERATOR_ROLES, [<<"admin">>, <<"swiftoperator">>]).

-record(keystone_s3_auth_req_v1, {
          access :: binary(),
          signature :: binary(),
          token :: binary()}).
-type keystone_s3_auth_req() :: #keystone_s3_auth_req_v1{}.
-define(KEYSTONE_S3_AUTH_REQ, #keystone_s3_auth_req_v1).
