%% -------------------------------------------------------------------
%%
%% Copyright (c) 2013 Basho Technologies, Inc.
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
%% -------------------------------------------------------------------

%% @private
-module(rt_cs_dev).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(DEVS(N), lists:concat(["dev", N, "@127.0.0.1"])).
-define(DEV(N), list_to_atom(?DEVS(N))).
-define(BUILD_PATHS, (rt_config:get(build_paths))).
-define(SRC_PATHS, (rt_config:get(src_paths))).

get_deps() ->
    lists:flatten(io_lib:format("~s/dev/dev1/lib", [relpath(current)])).

setup_harness(_Test, _Args) ->
    confirm_build_type(rt_config:get(build_type, oss)),
    Path = relpath(root),
    %% Stop all discoverable nodes, not just nodes we'll be using for this test.
    rt:pmap(fun(X) -> stop_all(X ++ "/dev") end, devpaths()),

    %% Reset nodes to base state
    lager:info("Resetting nodes to fresh state"),
    rtdev:run_git(Path, "reset HEAD --hard"),
    rtdev:run_git(Path, "clean -fd"),

    lager:info("Cleaning up lingering pipe directories"),
    rt:pmap(fun(Dir) ->
                    %% when joining two absolute paths, filename:join intentionally
                    %% throws away the first one. ++ gets us around that, while
                    %% keeping some of the security of filename:join.
                    %% the extra slashes will be pruned by filename:join, but this
                    %% ensures that there will be at least one between "/tmp" and Dir
                    PipeDir = filename:join(["/tmp//" ++ Dir, "dev"]),
                    %% when using filelib:wildcard/2, there must be a wildchar char
                    %% before the first '/'.
                    Files = filelib:wildcard("dev?/*.{r,w}", PipeDir),
                    [ file:delete(filename:join(PipeDir, File)) || File <- Files],
                    file:del_dir(PipeDir)
            end, devpaths()),
    ok.

confirm_build_type(BuildType) ->
    [ok = confirm_build_type(BuildType, Vsn) || Vsn <- [cs_current, cs_previous]].

confirm_build_type(BuildType, Vsn) ->
    ReplPB = filename:join([relpath(Vsn), "dev/dev1/lib/riak_repl_pb_api*"]),
    case {BuildType, filelib:wildcard(ReplPB)} of
        {oss, []} -> ok;
        {ee,  [_|_]} -> ok;
        _ ->
            lager:error("Build type of ~p is not ~p",
                        [Vsn, BuildType]),
            {error, {build_type_mismatch, Vsn}}
    end.

relpath(Vsn) ->
    Path = ?BUILD_PATHS,
    path(Vsn, Path).

srcpath(Vsn) ->
    Path = ?SRC_PATHS,
    path(Vsn, Path).

path(Vsn, Paths=[{_,_}|_]) ->
    orddict:fetch(Vsn, orddict:from_list(Paths));
path(current, Path) ->
    Path;
path(root, Path) ->
    Path;
path(_, _) ->
    throw("Version requested but only one path provided").

upgrade(Node, NewVersion) ->
    N = node_id(Node),
    Version = node_version(N),
    lager:info("Upgrading ~p : ~p -> ~p", [Node, Version, NewVersion]),
    catch stop(Node),
    rt:wait_until_unpingable(Node),
    OldPath = relpath(Version),
    NewPath = relpath(NewVersion),

    Commands = [
        io_lib:format("cp -p -P -R \"~s/dev/dev~b/data\" \"~s/dev/dev~b\"",
                       [OldPath, N, NewPath, N]),
        io_lib:format("rm -rf ~s/dev/dev~b/data/*",
                       [OldPath, N]),
        io_lib:format("cp -p -P -R \"~s/dev/dev~b/etc\" \"~s/dev/dev~b\"",
                       [OldPath, N, NewPath, N])
    ],
    [ begin
        lager:info("Running: ~s", [Cmd]),
        os:cmd(Cmd)
    end || Cmd <- Commands],
    VersionMap = orddict:store(N, NewVersion, rt_config:get(rt_versions)),
    rt_config:set(rt_versions, VersionMap),
    start(Node),
    rt:wait_until_pingable(Node),
    ok.

all_the_app_configs(DevPath) ->
    lager:error("The dev path is ~p", [DevPath]),
    case filelib:is_dir(DevPath) of
        true ->
            Devs = filelib:wildcard(DevPath ++ "/dev/dev*"),
            [ Dev ++ "/etc/app.config" || Dev <- Devs];
        _ ->
            lager:debug("~s is not a directory.", [DevPath]),
            []
    end.

update_app_config(all, Config) ->
    lager:info("rtdev:update_app_config(all, ~p)", [Config]),
    [ update_app_config(DevPath, Config) || DevPath <- devpaths()];
update_app_config(Node, Config) when is_atom(Node) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    FileFormatString = "~s/dev/dev~b/etc/~s.config",

    AppConfigFile = io_lib:format(FileFormatString, [Path, N, "app"]),
    AdvConfigFile = io_lib:format(FileFormatString, [Path, N, "advanced"]),
    %% If there's an app.config, do it old style
    %% if not, use cuttlefish's adavnced.config
    case filelib:is_file(AppConfigFile) of
        true ->
            update_app_config_file(AppConfigFile, Config);
        _ ->
            update_app_config_file(AdvConfigFile, Config)
    end;
update_app_config(DevPath, Config) ->
    [update_app_config_file(AppConfig, Config) || AppConfig <- all_the_app_configs(DevPath)].

update_app_config_file(ConfigFile, Config) ->
    lager:info("rtdev:update_app_config_file(~s, ~p)", [ConfigFile, Config]),

    BaseConfig = case file:consult(ConfigFile) of
        {ok, [ValidConfig]} ->
            ValidConfig;
        {error, enoent} ->
            []
    end,
    MergeA = orddict:from_list(Config),
    MergeB = orddict:from_list(BaseConfig),
    NewConfig =
        orddict:merge(fun(_, VarsA, VarsB) ->
                              MergeC = orddict:from_list(VarsA),
                              MergeD = orddict:from_list(VarsB),
                              orddict:merge(fun(_, ValA, _ValB) ->
                                                    ValA
                                            end, MergeC, MergeD)
                      end, MergeA, MergeB),
    NewConfigOut = io_lib:format("~p.", [NewConfig]),
    ?assertEqual(ok, file:write_file(ConfigFile, NewConfigOut)),
    ok.

%% Appropriate backend will be set by rtcs later.
get_backends() ->
    cs_multi_backend.

node_path(Node) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    lists:flatten(io_lib:format("~s/dev/dev~b", [Path, N])).

create_dirs(Nodes) ->
    Snmp = [node_path(Node) ++ "/data/snmp/agent/db" || Node <- Nodes],
    [?assertCmd("mkdir -p " ++ Dir) || Dir <- Snmp].

clean_data_dir(Nodes, SubDir) when is_list(Nodes) ->
    DataDirs = [node_path(Node) ++ "/data/" ++ SubDir || Node <- Nodes],
    lists:foreach(fun rm_dir/1, DataDirs).

rm_dir(Dir) ->
    lager:info("Removing directory ~s", [Dir]),
    ?assertCmd("rm -rf " ++ Dir),
    ?assertEqual(false, filelib:is_dir(Dir)).

add_default_node_config(Nodes) ->
    case rt_config:get(rt_default_config, undefined) of
        undefined -> ok;
        Defaults when is_list(Defaults) ->
            rt:pmap(fun(Node) ->
                            update_app_config(Node, Defaults)
                    end, Nodes),
            ok;
        BadValue ->
            lager:error("Invalid value for rt_default_config : ~p", [BadValue]),
            throw({invalid_config, {rt_default_config, BadValue}})
    end.

deploy_nodes(NodeConfig) ->
    Path = relpath(root),
    lager:info("Riak path: ~p", [Path]),
    NumNodes = length(NodeConfig),
    NodesN = lists:seq(1, NumNodes),
    Nodes = [?DEV(N) || N <- NodesN],
    NodeMap = orddict:from_list(lists:zip(Nodes, NodesN)),
    {Versions, Configs} = lists:unzip(NodeConfig),
    VersionMap = lists:zip(NodesN, Versions),

    %% Check that you have the right versions available
    [ check_node(Version) || Version <- VersionMap ],
    rt_config:set(rt_nodes, NodeMap),
    rt_config:set(rt_versions, VersionMap),

    create_dirs(Nodes),

    %% Set initial config
    add_default_node_config(Nodes),
    rt:pmap(fun({_, default}) ->
                    ok;
               ({Node, Config}) ->
                    update_app_config(Node, Config)
            end,
            lists:zip(Nodes, Configs)),

    %% create snmp dirs, for EE
    create_dirs(Nodes),

    %% Start nodes
    %%[run_riak(N, relpath(node_version(N)), "start") || N <- Nodes],
    rt:pmap(fun(N) -> rtdev:run_riak(N, relpath(node_version(N)), "start") end, NodesN),

    %% Ensure nodes started
    [ok = rt:wait_until_pingable(N) || N <- Nodes],

    %% %% Enable debug logging
    %% [rpc:call(N, lager, set_loglevel, [lager_console_backend, debug]) || N <- Nodes],

    %% We have to make sure that riak_core_ring_manager is running before we can go on.
    [ok = rt:wait_until_registered(N, riak_core_ring_manager) || N <- Nodes],

    %% Ensure nodes are singleton clusters
    [ok = rt:check_singleton_node(?DEV(N)) || {N, Version} <- VersionMap,
                                              Version /= "0.14.2"],

    lager:info("Deployed nodes: ~p", [Nodes]),
    Nodes.

stop_all(DevPath) ->
    case filelib:is_dir(DevPath) of
        true ->
            Devs = filelib:wildcard(DevPath ++ "/{dev,stanchion}*"),

            %% Works, but I'd like it to brag a little more about it.
            Stop = fun(C) ->
                Cmd = stop_command(C),
                [Output | _Tail] = string:tokens(os:cmd(Cmd), "\n"),
                Status = case Output of
                    "ok" -> "ok";
                    _ -> "wasn't running"
                end,
                lager:info("Stopping Node... ~s ~~ ~s.", [Cmd, Status])
            end,
            [Stop(D) || D <- Devs];
        _ -> lager:info("~s is not a directory.", [DevPath])
    end,
    ok.

stop_command(C) ->
    IsRiakCS = string:str(C, "riak_cs"),
    IsStanchion = string:str(C, "stanchion"),
    if
        IsRiakCS > 0 ->
            C ++ "/bin/riak-cs stop";
        IsStanchion > 0 ->
            C ++ "/bin/stanchion stop";
        true ->
            C ++ "/bin/riak stop"
    end.

stop(Node) ->
    RiakPid = rpc:call(Node, os, getpid, []),
    N = node_id(Node),
    rtdev:run_riak(N, relpath(node_version(N)), "stop"),
    F = fun(_N) ->
            os:cmd("kill -0 " ++ RiakPid) =/= []
    end,
    ?assertEqual(ok, rt:wait_until(Node, F)),
    ok.

start(Node) ->
    N = node_id(Node),
    rtdev:run_riak(N, relpath(node_version(N)), "start"),
    ok.

attach(Node, Expected) ->
    interactive(Node, "attach", Expected).

attach_direct(Node, Expected) ->
    interactive(Node, "attach-direct", Expected).

console(Node, Expected) ->
    interactive(Node, "console", Expected).

interactive(Node, Command, Exp) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    Cmd = rtdev:riakcmd(Path, N, Command),
    lager:info("Opening a port for riak ~s.", [Command]),
    lager:debug("Calling open_port with cmd ~s", [binary_to_list(iolist_to_binary(Cmd))]),
    P = open_port({spawn, binary_to_list(iolist_to_binary(Cmd))},
                  [stream, use_stdio, exit_status, binary, stderr_to_stdout]),
    interactive_loop(P, Exp).

interactive_loop(Port, Expected) ->
    receive
        {Port, {data, Data}} ->
            %% We've gotten some data, so the port isn't done executing
            %% Let's break it up by newline and display it.
            Tokens = string:tokens(binary_to_list(Data), "\n"),
            [lager:debug("~s", [Text]) || Text <- Tokens],

            %% Now we're going to take hd(Expected) which is either {expect, X}
            %% or {send, X}. If it's {expect, X}, we foldl through the Tokenized
            %% data looking for a partial match via rt:str/2. If we find one,
            %% we pop hd off the stack and continue iterating through the list
            %% with the next hd until we run out of input. Once hd is a tuple
            %% {send, X}, we send that test to the port. The assumption is that
            %% once we send data, anything else we still have in the buffer is
            %% meaningless, so we skip it. That's what that {sent, sent} thing
            %% is about. If there were a way to abort mid-foldl, I'd have done
            %% that. {sent, _} -> is just a pass through to get out of the fold.

            NewExpected = lists:foldl(fun(X, Expect) ->
                    [{Type, Text}|RemainingExpect] = case Expect of
                        [] -> [{done, "done"}|[]];
                        E -> E
                    end,
                    case {Type, rt:str(X, Text)} of
                        {expect, true} ->
                            RemainingExpect;
                        {expect, false} ->
                            [{Type, Text}|RemainingExpect];
                        {send, _} ->
                            port_command(Port, list_to_binary(Text ++ "\n")),
                            [{sent, "sent"}|RemainingExpect];
                        {sent, _} ->
                            Expect;
                        {done, _} ->
                            []
                    end
                end, Expected, Tokens),
            %% Now that the fold is over, we should remove {sent, sent} if it's there.
            %% The fold might have ended not matching anything or not sending anything
            %% so it's possible we don't have to remove {sent, sent}. This will be passed
            %% to interactive_loop's next iteration.
            NewerExpected = case NewExpected of
                [{sent, "sent"}|E] -> E;
                E -> E
            end,
            %% If NewerExpected is empty, we've met all expected criteria and in order to boot
            %% Otherwise, loop.
            case NewerExpected of
                [] -> ?assert(true);
                _ -> interactive_loop(Port, NewerExpected)
            end;
        {Port, {exit_status,_}} ->
            %% This port has exited. Maybe the last thing we did was {send, [4]} which
            %% as Ctrl-D would have exited the console. If Expected is empty, then
            %% We've met every expectation. Yay! If not, it means we've exited before
            %% something expected happened.
            ?assertEqual([], Expected)
        after rt_config:get(rt_max_wait_time) ->
            %% interactive_loop is going to wait until it matches expected behavior
            %% If it doesn't, the test should fail; however, without a timeout it
            %% will just hang forever in search of expected behavior. See also: Parenting
            ?assertEqual([], Expected)
    end.

admin(Node, Args, Options) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    Cmd = rtdev:riak_admin_cmd(Path, N, Args),
    lager:info("Running: ~s", [Cmd]),
    Result = execute_admin_cmd(Cmd, Options),
    lager:info("~s", [Result]),
    {ok, Result}.

execute_admin_cmd(Cmd, Options) ->
    {_ExitCode, Result} = FullResult = wait_for_cmd(spawn_cmd(Cmd)),
    case lists:member(return_exit_code, Options) of
        true ->
            FullResult;
        false ->
            Result
    end.

riak(Node, Args) ->
    N = node_id(Node),
    Path = relpath(node_version(N)),
    Result = rtdev:run_riak(N, Path, Args),
    lager:info("~s", [Result]),
    {ok, Result}.

node_id(Node) ->
    NodeMap = rt_config:get(rt_nodes),
    orddict:fetch(Node, NodeMap).

node_version(N) ->
    VersionMap = rt_config:get(rt_versions),
    orddict:fetch(N, VersionMap).

spawn_cmd(Cmd) ->
    spawn_cmd(Cmd, []).
spawn_cmd(Cmd, Opts) ->
    Port = open_port({spawn, Cmd}, [stream, in, exit_status, stderr_to_stdout] ++ Opts),
    Port.

wait_for_cmd(Port) ->
    rt:wait_until(node(),
                  fun(_) ->
                          receive
                              {Port, Msg={data, _}} ->
                                  self() ! {Port, Msg},
                                  false;
                              {Port, Msg={exit_status, _}} ->
                                  catch port_close(Port),
                                  self() ! {Port, Msg},
                                  true
                          after 0 ->
                                  false
                          end
                  end),
    get_cmd_result(Port, []).

cmd(Cmd) ->
    cmd(Cmd, []).

cmd(Cmd, Opts) ->
    wait_for_cmd(spawn_cmd(Cmd, Opts)).

get_cmd_result(Port, Acc) ->
    receive
        {Port, {data, Bytes}} ->
            get_cmd_result(Port, [Bytes|Acc]);
        {Port, {exit_status, Status}} ->
            Output = lists:flatten(lists:reverse(Acc)),
            {Status, Output}
    after 0 ->
            timeout
    end.

check_node({_N, Version}) ->
    case proplists:is_defined(Version, rt_config:get(build_paths)) of
        true -> ok;
        _ ->
            lager:error("You don't have Riak ~s installed or configured", [Version]),
            erlang:error("You don't have Riak " ++ atom_to_list(Version) ++ " installed or configured")
    end.

set_backend(Backend) ->
    lager:info("rtdev:set_backend(~p)", [Backend]),
    update_app_config(all, [{riak_kv, [{storage_backend, Backend}]}]),
    get_backends().

get_version() ->
    case file:read_file(relpath(cs_current) ++ "/VERSION") of
        {error, enoent} -> unknown;
        {ok, Version} -> Version
    end.

teardown() ->
    %% Stop all discoverable nodes, not just nodes we'll be using for this test.
    rt:pmap(fun(X) -> stop_all(X ++ "/dev") end,
            devpaths()).

whats_up() ->
    io:format("Here's what's running...~n"),

    Up = [rpc:call(Node, os, cmd, ["pwd"]) || Node <- nodes()],
    [io:format("  ~s~n",[string:substr(Dir, 1, length(Dir)-1)]) || Dir <- Up].

devpaths() ->
    lists:usort([ DevPath || {Name, DevPath} <- rt_config:get(build_paths),
                             not lists:member(Name, [root, ee_root, cs_root, stanchion_root])
                ]).

versions() ->
    proplists:get_keys(rt_config:get(build_paths)) -- [root].

get_node_logs() ->
    lists:flatmap(fun get_node_logs/1, [root, ee_root, cs_root, stanchion_root]).

get_node_logs(Base) ->
    Root = filename:absname(proplists:get_value(Base, ?BUILD_PATHS)),
    %% Unlike Riak, Riak CS has multiple things in the root and so we need
    %% to distinguish between them.
    RootLen = length(filename:dirname(Root)) + 1, %% Remove the leading slash
    [ begin
          {ok, Port} = file:open(Filename, [read, binary]),
          {lists:nthtail(RootLen, Filename), Port}
      end || Filename <- filelib:wildcard(Root ++ "/*/dev/dev*/log/*") ].
