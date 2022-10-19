%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
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

-ifndef(RCS_COMMON_ACL_HRL).
-define(RCS_COMMON_ACL_HRL, included).

-type acl_perm() :: 'READ' | 'WRITE' | 'READ_ACP' | 'WRITE_ACP' | 'FULL_CONTROL'.
-type acl_perms() :: [acl_perm()].
-type group_grant() :: 'AllUsers' | 'AuthUsers'.
-type acl_grantee() :: {DisplayName :: string(),
                        CanonicalID :: string()} |
                       group_grant().
-type acl_grant() :: {acl_grantee(), acl_perms()}.
%% acl_v1 owner fields: {DisplayName, CanonicalId}
-type acl_owner2() :: {string(), string()}.
%% acl_owner3: {display name, canonical id, key id}
-type acl_owner3() :: {string(), string(), string()}.
-type acl_owner() :: acl_owner2() | acl_owner3().
-record(acl_v1, {owner={"", ""} :: acl_owner(),
                 grants=[] :: [acl_grant()],
                 creation_time = erlang:timestamp() :: erlang:timestamp()}).
%% acl_v2 owner fields: {DisplayName, CanonicalId, KeyId}
-record(acl_v2, {owner={"", "", ""} :: acl_owner(),
                 grants=[] :: [acl_grant()],
                 creation_time = erlang:timestamp() :: erlang:timestamp()}).
-type acl() :: #acl_v1{} | #acl_v2{}.

-endif.
