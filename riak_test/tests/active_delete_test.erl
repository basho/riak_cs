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

-module(active_delete_test).

%% @doc `riak_test' module for testing active delete situation behavior.

-export([confirm/0]).
config() ->
    [{riak_cs, [{active_delete_threshold, 10000000}]}].

confirm() ->
    %% 10MB threshold, for 3MB objects are used in cs_suites:run/2
    rt_cs_dev:set_advanced_conf(cs, config()),
    Setup = rtcs:setup(1),

    %% Just do verify on typical normal case
    History = [{cs_suites, run, ["run-1"]}],
    {ok, InitialState} = cs_suites:new(Setup),
    {ok, EvolvedState} = cs_suites:fold_with_state(InitialState, History),
    {ok, _FinalState}  = cs_suites:cleanup(EvolvedState),

    rtcs:pass().
