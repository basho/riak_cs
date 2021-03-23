%% ---------------------------------------------------------------------
%%
%% Copyright (c) 2007-2014 Basho Technologies, Inc.  All Rights Reserved.
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

-module(cs782_regression_test).

-compile(export_all).
-compile(nowarn_export_all).

-include("riak_cs.hrl").
-include_lib("eunit/include/eunit.hrl").

%% TODO: import test like this to riak_cs_acl_utils.erl
well_indented_xml() ->
    Xml="<AccessControlPolicy>"
        "  <Owner>"
        "    <ID>eb874c6afce06925157eda682f1b3c6eb0f3b983bbee3673ae62f41cce21f6b1</ID>"
        "    <DisplayName>admin</DisplayName>"
        "  </Owner>"
        "  <AccessControlList>"
        "    <Grant>"
        "      <Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">"
        "        <ID>eb874c6afce06925157eda682f1b3c6eb0f3b983bbee3673ae62f41cce21f6b1</ID>"
        "        <DisplayName>admin</DisplayName>"
        "      </Grantee>"
        "      <Permission>FULL_CONTROL</Permission>"
        "    </Grant>"
        "  </AccessControlList>"
        "</AccessControlPolicy>",
    Xml.

well_indented_xml_test() ->
    Xml = well_indented_xml(),
    %% if cs782 alive, error:{badrecord,xmlElement} thrown here.
    {ok, ?ACL{} = _Acl} = riak_cs_acl_utils:acl_from_xml(Xml, boom, foo),
    ok.
