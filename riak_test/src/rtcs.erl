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
-module(rtcs).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-import(rt, [join/2,
             wait_until_nodes_ready/1,
             wait_until_no_pending_changes/1]).

-define(DEVS(N), lists:concat(["dev", N, "@127.0.0.1"])).
-define(DEV(N), list_to_atom(?DEVS(N))).
-define(CSDEVS(N), lists:concat(["rcs-dev", N, "@127.0.0.1"])).
-define(CSDEV(N), list_to_atom(?CSDEVS(N))).

-define(RIAK_CURRENT, rtdev_path.root).
-define(EE_ROOT, rtdev_path.ee_root).
-define(EE_CURRENT, rtdev_path.ee_current).
-define(CS_ROOT, rtdev_path.cs_root).
-define(CS_CURRENT, rtdev_path.cs_current).
-define(STANCHION_ROOT, rtdev_path.stanchion_root).
-define(STANCHION_CURRENT, rtdev_path.stanchion_current).

-define(PROXY_HOST, "localhost").
-define(S3_HOST, "s3.amazonaws.com").
-define(S3_PORT, 80).
-define(DEFAULT_PROTO, "http").

setup(NumNodes) ->
    setup(NumNodes, default_configs()).

setup(NumNodes, Configs) ->
    %% Start the erlcloud app
    erlcloud:start(),

    %% STFU sasl
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, false),

    Cfgs = configs(Configs),
    {{AdminKeyId, AdminSecretKey},
     {RiakNodes, _CSNodes, _Stanchion}=Nodes} = build_cluster(NumNodes, Cfgs),

    AdminConfig = rtcs:config(AdminKeyId,
                              AdminSecretKey,
                              rtcs:cs_port(hd(RiakNodes))),
    {AdminConfig, Nodes}.

build_cluster(NumNodes, Configs) ->
    {_, {RiakNodes, _, _}} = Nodes =
        deploy_nodes(NumNodes, Configs),

    rt:wait_until_nodes_ready(RiakNodes),
    lager:info("Build cluster"),
    rtcs:make_cluster(RiakNodes),
    rt:wait_until_ring_converged(RiakNodes),
    Nodes.

configs(CustomConfigs) ->
    [{riak, proplists:get_value(riak, CustomConfigs, ee_config())},
     {cs, proplists:get_value(cs, CustomConfigs, cs_config())},
     {stanchion, proplists:get_value(stanchion,
                                     CustomConfigs,
                                     stanchion_config())}].

default_configs() ->
    [{riak, ee_config()},
     {stanchion, stanchion_config()},
     {cs, cs_config()}].

config(Key, Secret, Port) ->
    erlcloud_s3:new(Key,
                    Secret,
                    ?S3_HOST,
                    Port, % inets issue precludes using ?S3_PORT
                    ?DEFAULT_PROTO,
                    ?PROXY_HOST,
                    Port,
                    [{keep_alive_timeout, 100}]).

create_user(Node, UserIndex) ->
    {A, B, C} = erlang:now(),
    User = "Test User" ++ integer_to_list(UserIndex),
    Email = lists:flatten(io_lib:format("~p~p~p@basho.com", [A, B, C])),
    {KeyId, Secret, _Id} = create_user(cs_port(Node), Email, User),
    lager:info("Created user ~p with keys ~p ~p", [Email, KeyId, Secret]),
    {KeyId, Secret}.

create_admin_user(Node) ->
    User = "admin",
    Email = "admin@me.com",
    {KeyId, Secret, Id} = create_user(cs_port(Node), Email, User),
    lager:info("Riak CS Admin account created with ~p",[Email]),
    lager:info("KeyId = ~p",[KeyId]),
    lager:info("KeySecret = ~p",[Secret]),
    lager:info("Id = ~p",[Id]),
    {KeyId, Secret}.

cs_port(Node) ->
    8070 + rtdev:node_id(Node).

ee_config() ->
    CSCurrent = rt:config(rtdev_path.cs_current),
    [
     lager_config(),
     {riak_core,
      [{default_bucket_props, [{allow_mult, true}]}]},
     {riak_kv,
      [
       {add_paths, [CSCurrent ++ "/dev/dev1/lib/riak_cs/ebin"]},
       {storage_backend, riak_cs_kv_multi_backend},
       {multi_backend_prefix_list, [{<<"0b:">>, be_blocks}]},
       {multi_backend_default, be_default},
       {multi_backend,
        [{be_default, riak_kv_eleveldb_backend,
          [
           {max_open_files, 20},
           {data_root, "./leveldb"}
          ]},
         {be_blocks, riak_kv_bitcask_backend,
          [
           {data_root, "./bitcask"}
          ]}
        ]}
      ]},
     {riak_repl,
      [
       {fullsync_on_connect, false},
       {fullsync_interval, disabled},
       {proxy_get, enabled}
      ]}
    ].

cs_config() ->
    cs_config([]).

cs_config(UserExtra) ->
    [
     lager_config(),
     {riak_cs,
      UserExtra ++
      [
       {proxy_get, enabled},
       {anonymous_user_creation, true},
       {riak_pb_port, 10017},
       {stanchion_port, 9095},
       {cs_version, 010300}
      ]
     }].

stanchion_config() ->
    [
     lager_config(),
     {stanchion,
      [
       {stanchion_port, 9095},
       {riak_pb_port, 10017}
      ]
     }].

lager_config() ->
    {lager,
     [
      {handlers,
       [
        {lager_console_backend, debug},
        {lager_file_backend,
         [
          {"./log/error.log", error, 10485760, "$D0",5},
          {"./log/console.log", debug, 10485760, "$D0", 5}
         ]}
       ]}
     ]}.

riakcs_binpath(Prefix, N) ->
    io_lib:format("~s/dev/dev~b/bin/riak-cs", [Prefix, N]).

riakcs_etcpath(Prefix, N) ->
    io_lib:format("~s/dev/dev~b/etc", [Prefix, N]).

riakcscmd(Path, N, Cmd) ->
    lists:flatten(io_lib:format("~s ~s", [riakcs_binpath(Path, N), Cmd])).

stanchion_binpath(Prefix) ->
    io_lib:format("~s/dev/stanchion/bin/stanchion", [Prefix]).

stanchion_etcpath(Prefix) ->
    io_lib:format("~s/dev/stanchion/etc", [Prefix]).

stanchioncmd(Path, Cmd) ->
    lists:flatten(io_lib:format("~s ~s", [stanchion_binpath(Path), Cmd])).

deploy_nodes(NumNodes, InitialConfig) ->
    lager:info("Initial Config: ~p", [InitialConfig]),
    NodeConfig = [{current, InitialConfig} || _ <- lists:seq(1,NumNodes)],
    RiakNodes = [?DEV(N) || N <- lists:seq(1, NumNodes)],
    CSNodes = [?CSDEV(N) || N <- lists:seq(1, NumNodes)],
    StanchionNode = 'stanchion@127.0.0.1',

    lager:info("RiakNodes: ~p", [RiakNodes]),

    NodeMap = orddict:from_list(lists:zip(RiakNodes, lists:seq(1, NumNodes))),
    rt:set_config(rt_nodes, NodeMap),

    VersionMap = lists:zip(lists:seq(1, NumNodes), lists:duplicate(NumNodes, ee_current)),
    rt:set_config(rt_versions, VersionMap),

    lager:info("VersionMap: ~p", [VersionMap]),

    NL0 = lists:zip(CSNodes, RiakNodes),
    {CS1, R1} = hd(NL0),
    NodeList = [{CS1, R1, StanchionNode} | tl(NL0)],
    lager:info("NodeList: ~p", [NodeList]),

    stop_all_nodes(NodeList),

    [reset_nodes(Project, Path) ||
        {Project, Path} <- [{riak_ee, rt:config(?EE_ROOT)},
                            {riak_cs, rt:config(?CS_ROOT)},
                            {stanchion, rt:config(?STANCHION_ROOT)}]],

    rtdev:create_dirs(RiakNodes),

    {_Versions, Configs} = lists:unzip(NodeConfig),

    %% Set initial config
    set_configs(NodeList, Configs),
    start_all_nodes(NodeList),

    Nodes = {RiakNodes, CSNodes, StanchionNode},
    [ok = rt:wait_until_pingable(N) || N <- RiakNodes ++ CSNodes ++ [StanchionNode]],
    [ok = rt:check_singleton_node(N) || N <- RiakNodes],

    rt:wait_until_nodes_ready(RiakNodes),

    %% Create admin user and set in cs and stanchion configs
    AdminCreds = create_admin_user(hd(RiakNodes)),

    %% Restart cs and stanchion nodes so admin user takes effect
    stop_cs_and_stanchion_nodes(NodeList),

    set_admin_creds_in_configs(NodeList, Configs, AdminCreds),

    start_cs_and_stanchion_nodes(NodeList),
    [ok = rt:wait_until_pingable(N) || N <- CSNodes ++ [StanchionNode]],

    lager:info("Deployed nodes: ~p", [Nodes]),
    {AdminCreds, Nodes}.

start_cs_and_stanchion_nodes(NodeList) ->
    rt:pmap(fun({_CSNode, RiakNode, _Stanchion}) ->
                    N = rtdev:node_id(RiakNode),
                    start_stanchion(),
                    start_cs(N);
               ({_CSNode, RiakNode}) ->
                    N = rtdev:node_id(RiakNode),
                    start_cs(N)
            end, NodeList).

stop_cs_and_stanchion_nodes(NodeList) ->
    rt:pmap(fun({CSNode, RiakNode, Stanchion}) ->
                    N = rtdev:node_id(RiakNode),
                    stop_cs(N),
                    stop_stanchion(),
                    rt:wait_until_unpingable(CSNode),
                    rt:wait_until_unpingable(Stanchion);
               ({CSNode, RiakNode}) ->
                    N = rtdev:node_id(RiakNode),
                    stop_cs(N),
                    rt:wait_until_unpingable(CSNode)
            end, NodeList).

start_all_nodes(NodeList) ->
    rt:pmap(fun({_CSNode, RiakNode, _Stanchion}) ->
                    N = rtdev:node_id(RiakNode),
                    rtdev:run_riak(N, rtdev:relpath(rtdev:node_version(N)), "start"),
                    rt:wait_for_service(RiakNode, riak_kv),
                    start_stanchion(),
                    start_cs(N);
               ({_CSNode, RiakNode}) ->
                    N = rtdev:node_id(RiakNode),
                    rtdev:run_riak(N, rtdev:relpath(rtdev:node_version(N)), "start"),
                    rt:wait_for_service(RiakNode, riak_kv),
                    start_cs(N)
            end, NodeList).

stop_all_nodes(NodeList) ->
    rt:pmap(fun({CSNode, RiakNode, Stanchion}) ->
                    N = rtdev:node_id(RiakNode),
                    stop_cs(N),
                    stop_stanchion(),
                    rtdev:run_riak(N, rtdev:relpath(rtdev:node_version(N)), "stop"),
                    rt:wait_until_unpingable(CSNode),
                    rt:wait_until_unpingable(Stanchion),
                    rt:wait_until_unpingable(RiakNode);
               ({CSNode, RiakNode}) ->
                    N = rtdev:node_id(RiakNode),
                    stop_cs(N),
                    rtdev:run_riak(N, rtdev:relpath(rtdev:node_version(N)), "stop"),
                    rt:wait_until_unpingable(CSNode),
                    rt:wait_until_unpingable(RiakNode)
            end, NodeList).

set_configs(NodeList, Configs) ->
    rt:pmap(fun({_, default}) ->
                    ok;
               ({{_CSNode, RiakNode, _Stanchion}, Config}) ->
                    N = rtdev:node_id(RiakNode),
                    rtdev:update_app_config(RiakNode, proplists:get_value(riak,
                                                                          Config)),
                    update_cs_config(rt:config(?CS_CURRENT), N,
                                     proplists:get_value(cs, Config)),
                    update_stanchion_config(rt:config(?STANCHION_CURRENT),
                                            proplists:get_value(stanchion, Config));
               ({{_CSNode, RiakNode}, Config}) ->
                    N = rtdev:node_id(RiakNode),
                    rtdev:update_app_config(RiakNode, proplists:get_value(riak,
                                                                          Config)),
                    update_cs_config(rt:config(?CS_CURRENT), N,
                                     proplists:get_value(cs, Config))
            end,
            lists:zip(NodeList, Configs)).

set_admin_creds_in_configs(NodeList, Configs, AdminCreds) ->
    rt:pmap(fun({_, default}) ->
                    ok;
               ({{_CSNode, RiakNode, _Stanchion}, Config}) ->
                    N = rtdev:node_id(RiakNode),
                    update_cs_config(rt:config(?CS_CURRENT),
                                     N,
                                     proplists:get_value(cs, Config),
                                     AdminCreds),
                    update_stanchion_config(rt:config(?STANCHION_CURRENT),
                                            proplists:get_value(stanchion, Config),
                                            AdminCreds);
               ({{_CSNode, RiakNode}, Config}) ->
                    N = rtdev:node_id(RiakNode),
                    update_cs_config(rt:config(?CS_CURRENT),
                                     N,
                                     proplists:get_value(cs, Config),
                                     AdminCreds)
            end,
            lists:zip(NodeList, Configs)).

reset_nodes(Project, Path) ->
    %% Reset nodes to base state
    lager:info("Resetting ~p nodes to fresh state", [Project]),
    lager:debug("Project path for reset: ~p", [Path]),
    rtdev:run_git(Path, "reset HEAD --hard"),
    rtdev:run_git(Path, "clean -fd").

make_cluster(Nodes) ->
    [First|Rest] = Nodes,
    [join(Node, First) || Node <- Rest],
    ?assertEqual(ok, wait_until_nodes_ready(Nodes)),
    ?assertEqual(ok, wait_until_no_pending_changes(Nodes)).

start_cs(N) ->
    Cmd = riakcscmd(rt:config(?CS_CURRENT), N, "start"),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

stop_cs(N) ->
    Cmd = riakcscmd(rt:config(?CS_CURRENT), N, "stop"),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

start_stanchion() ->
    Cmd = stanchioncmd(rt:config(?STANCHION_CURRENT), "start"),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

stop_stanchion() ->
    Cmd = stanchioncmd(rt:config(?STANCHION_CURRENT), "stop"),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

update_cs_config(Prefix, N, Config, {AdminKey, AdminSecret}) ->
    CSSection = proplists:get_value(riak_cs, Config),
    UpdConfig = [{riak_cs, update_admin_creds(CSSection, AdminKey, AdminSecret)} |
                 proplists:delete(riak_cs, Config)],
    update_cs_config(Prefix, N, UpdConfig).

update_cs_config(Prefix, N, Config) ->
    CSSection = proplists:get_value(riak_cs, Config),
    UpdConfig = [{riak_cs, update_cs_port(CSSection, N)} |
                 proplists:delete(riak_cs, Config)],
    update_app_config(riakcs_etcpath(Prefix, N) ++ "/app.config", UpdConfig).

update_admin_creds(Config, AdminKey, AdminSecret) ->
    [{admin_key, AdminKey}, {admin_secret, AdminSecret} |
     proplists:delete(admin_secret,
                      proplists:delete(admin_key, Config))].

update_cs_port(Config, N) ->
    PbPort = 10000 + (N * 10) + 7,
    [{riak_pb_port, PbPort} | proplists:delete(riak_pb_port, Config)].

update_stanchion_config(Prefix, Config, {AdminKey, AdminSecret}) ->
    StanchionSection = proplists:get_value(stanchion, Config),
    UpdConfig = [{stanchion, update_admin_creds(StanchionSection, AdminKey, AdminSecret)} |
                 proplists:delete(stanchion, Config)],
    update_stanchion_config(Prefix, UpdConfig).

update_stanchion_config(Prefix, Config) ->
    update_app_config(stanchion_etcpath(Prefix) ++ "/app.config", Config).

update_app_config(ConfigFile,  Config) ->
    {ok, [BaseConfig]} = file:consult(ConfigFile),
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


deploy_cs(Config, N) ->
    update_cs_config(rt:config(?CS_CURRENT), N, Config),
    start_cs(N),
    lager:info("Riak CS started").

%% this differs from rtdev:deploy_xxx in that it only starts one node
deploy_stanchion(Config) ->
    %% Set initial config
    update_stanchion_config(rt:config(?STANCHION_CURRENT), Config),

    start_stanchion(),
    lager:info("Stanchion started").

create_user(Port, EmailAddr, Name) ->
    lager:debug("Trying to create user ~p", [EmailAddr]),
    Cmd="curl -s -H 'Content-Type: application/json' http://localhost:" ++
        integer_to_list(Port) ++
        "/riak-cs/user --data '{\"email\":\"" ++ EmailAddr ++  "\", \"name\":\"" ++ Name ++"\"}'",
    lager:info("Cmd: ~p", [Cmd]),
    Delay = rt:config(rt_retry_delay),
    Retries = rt:config(rt_max_wait_time) div Delay,
    OutputFun = fun() -> os:cmd(Cmd) end,
    Condition = fun(Res) -> Res /= [] end,
    Output = wait_until(OutputFun, Condition, Retries, Delay),
    lager:debug("Create user output=~p~n",[Output]),
    {struct, JsonData} = mochijson2:decode(Output),
    KeyId = binary_to_list(proplists:get_value(<<"key_id">>, JsonData)),
    KeySecret = binary_to_list(proplists:get_value(<<"key_secret">>, JsonData)),
    Id = binary_to_list(proplists:get_value(<<"id">>, JsonData)),
    {KeyId, KeySecret, Id}.

wait_until(_, _, 0, _) ->
    fail;
wait_until(Fun, Condition, Retries, Delay) ->
    Result = Fun(),
    case Condition(Result) of
        true ->
            Result;
        false ->
            wait_until(Fun, Condition, Retries-1, Delay)
    end.
