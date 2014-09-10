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

-module(riak_cs_auth).

-include("riak_cs.hrl").

-type wm_reqdata() :: tuple().
-type wm_context() :: tuple().

%% TODO: arguments of `identify/2', and 3rd and 4th arguments of
%%       authenticate/4 are actually `#wm_reqdata{}' and `#context{}'
%%       from webmachine, but can't compile after `webmachine.hrl' import.
-callback identify(RD :: wm_reqdata(), Ctx :: wm_context()) ->
    failed | {string() | undefined, string() | tuple()} |
    {string(), undefined}.

-callback authenticate(rcs_user(), string() | {string(), term()} | undefined,
                       wm_reqdata(), wm_context()) ->
    ok | {error, atom()}.     
