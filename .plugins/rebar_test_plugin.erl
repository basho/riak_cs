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

-module(rebar_test_plugin).

-export([
    client_test_clean/2,
    client_test_compile/2,
    client_test_run/2,
    riak_test_clean/2,
    riak_test_compile/2
]).

%% ===================================================================
%% Public API
%% ===================================================================
client_test_clean(Config, AppFile) ->
    case should_i_run(Config) of
        false -> ok;
        _ -> test_clean(client_test, Config, AppFile)
    end.

client_test_compile(Config, AppFile) ->
    case should_i_run(Config) of
        false -> ok;
        _ -> test_compile(client_test, Config, AppFile)
    end.

client_test_run(Config, AppFile) ->
    case should_i_run(Config) of
        false -> ok;
        _ -> test_run(client_test, Config, AppFile)
    end.

riak_test_clean(Config, AppFile) ->
    case should_i_run(Config) of
        false -> ok;
        _ -> test_clean(riak_test, Config, AppFile)
    end.

riak_test_compile(Config, AppFile) ->
    case should_i_run(Config) of
        false -> ok;
        _ -> test_compile(riak_test, Config, AppFile)
    end.

%% ===================================================================
%% Private Functions - pronounced Funk-tee-owns, not funk-ee-towns
%% ===================================================================
should_i_run(Config) ->
    rebar_utils:processing_base_dir(Config).

option(TestType, Key, Config) ->
    case proplists:get_value(TestType, element(3, Config), not_configured) of
        not_configured -> {error, not_configured};
        TestConfig ->
            proplists:get_value(Key, TestConfig, {error, not_set})
    end.

test_clean(TestType, Config, _AppFile) ->
    case option(TestType, test_output, Config) of
        {error, not_set} ->
            io:format("No test_output directory set, check your rebar.config");
        TestOutputDir ->
            io:format("Removing test_output dir ~s~n", [TestOutputDir]),
            rebar_file_utils:rm_rf(TestOutputDir)
    end,
    ok.

test_compile(TestType, Config, AppFile) ->
    CompilationConfig = compilation_config(TestType, Config),
    OutputDir = option(TestType, test_output, Config),
    rebar_erlc_compiler:compile(CompilationConfig, AppFile),
    ok.

test_run(TestType, Config, _AppFile) ->
    OutputDir = option(TestType, test_output, Config),
    Cwd = rebar_utils:get_cwd(),
    ok = file:set_cwd([Cwd, $/, OutputDir]),
    EunitResult = (catch eunit:test("./")),
    %% Return to original working dir
    ok = file:set_cwd(Cwd),
    EunitResult.


compilation_config(TestType, Conf) ->
    C1 = rebar_config:set(Conf, TestType, undefined),
    C2 = rebar_config:set(C1, plugins, undefined),
    ErlOpts = rebar_utils:erl_opts(Conf),
    ErlOpts1 = proplists:delete(src_dirs, ErlOpts),
    ErlOpts2 =
        case eqc_present() of
            true ->
                [{d, 'TEST'}, {d, 'EQC'}, {outdir, option(TestType, test_output, Conf)}, {src_dirs, option(TestType, test_paths, Conf)} | ErlOpts1];
            false ->
                [{d, 'TEST'}, {outdir, option(TestType, test_output, Conf)}, {src_dirs, option(TestType, test_paths, Conf)} | ErlOpts1]
        end,
    rebar_config:set(C2, erl_opts, ErlOpts2).

eqc_present() ->
    case catch eqc:version() of
        {'EXIT', {undef, _}} ->
            false;
        _ ->
            true
    end.
