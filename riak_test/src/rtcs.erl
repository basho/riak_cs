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
-include_lib("erlcloud/include/erlcloud_aws.hrl").

-import(rt, [join/2,
             wait_until_nodes_ready/1,
             wait_until_no_pending_changes/1]).

-define(DEVS(N), lists:concat(["dev", N, "@127.0.0.1"])).
-define(DEV(N), list_to_atom(?DEVS(N))).
-define(CSDEVS(N), lists:concat(["rcs-dev", N, "@127.0.0.1"])).
-define(CSDEV(N), list_to_atom(?CSDEVS(N))).

-define(RIAK_ROOT, <<"build_paths.root">>).
-define(RIAK_CURRENT, <<"build_paths.current">>).
-define(EE_ROOT, <<"build_paths.ee_root">>).
-define(EE_CURRENT, <<"build_paths.ee_current">>).
-define(CS_ROOT, <<"build_paths.cs_root">>).
-define(CS_CURRENT, <<"build_paths.cs_current">>).
-define(STANCHION_ROOT, <<"build_paths.stanchion_root">>).
-define(STANCHION_CURRENT, <<"build_paths.stanchion_current">>).

-define(PROXY_HOST, "localhost").
-define(S3_HOST, "s3.amazonaws.com").
-define(S3_PORT, 80).
-define(DEFAULT_PROTO, "http").

-define(REQUEST_POOL_SIZE, 8).
-define(BUCKET_LIST_POOL_SIZE, 2).

request_pool_size() ->
    ?REQUEST_POOL_SIZE.

bucket_list_pool_size() ->
    ?BUCKET_LIST_POOL_SIZE.

setup(NumNodes) ->
    setup(NumNodes, default_configs()).

setup(NumNodes, Configs) ->
    Flavor = rt_config:get(flavor, basic),
    lager:info("Flavor : ~p", [Flavor]),
    flavored_setup(NumNodes, Flavor, Configs).

setup2x2() ->
    setup2x2(default_configs()).

setup2x2(Configs) ->
    JoinFun = fun(Nodes) ->
                      [A,B,C,D] = Nodes,
                      join(B,A),
                      join(D,C)
              end,
    setup_clusters(Configs, JoinFun, 4).

setupNx1x1(N) ->
    setupNx1x1(N, default_configs()).

setupNx1x1(N, Configs) ->
    JoinFun = fun(Nodes) ->
                      [Target | Joiners] = lists:sublist(Nodes, N),
                      [join(J, Target) || J <- Joiners]
              end,
    setup_clusters(Configs, JoinFun, N + 1 + 1).

flavored_setup(NumNodes, basic, Configs) ->
    JoinFun = fun(Nodes) ->
                      [First|Rest] = Nodes,
                      [join(Node, First) || Node <- Rest]
              end,
    setup_clusters(Configs, JoinFun, NumNodes);
flavored_setup(NumNodes, {multibag, _} = Flavor, Configs) ->
    rtcs_bag:flavored_setup(NumNodes, Flavor, Configs).

setup_clusters(Configs, JoinFun, NumNodes) ->
    %% Start the erlcloud app
    erlcloud:start(),

    %% STFU sasl
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, false),

    Cfgs = configs(Configs),
    lager:info("Configs = ~p", [ Cfgs]),
    {RiakNodes, _CSNodes, _Stanchion} = Nodes = deploy_nodes(NumNodes, Cfgs),
    rt:wait_until_nodes_ready(RiakNodes),
    lager:info("Make cluster"),
    JoinFun(RiakNodes),
    ?assertEqual(ok, wait_until_nodes_ready(RiakNodes)),
    ?assertEqual(ok, wait_until_no_pending_changes(RiakNodes)),
    rt:wait_until_ring_converged(RiakNodes),
    {AdminKeyId, AdminSecretKey} = setup_admin_user(NumNodes, Cfgs),
    AdminConfig = rtcs:config(AdminKeyId,
                              AdminSecretKey,
                              rtcs:cs_port(hd(RiakNodes))),
    {AdminConfig, Nodes}.


configs(CustomConfigs) ->
    [{riak, proplists:get_value(riak, CustomConfigs, riak_config())},
     {cs, proplists:get_value(cs, CustomConfigs, cs_config())},
     {stanchion, proplists:get_value(stanchion,
                                     CustomConfigs,
                                     stanchion_config())}].

default_configs() ->
    [{riak, riak_config()},
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
                    []).

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
    8070 + rt_cs_dev:node_id(Node).


riak_config() ->
    riak_config(
      rt_config:get(build_type, oss),
      rt_config:get(backend, {multi_backend, bitcask})).

riak_config(CustomConfig) ->
    orddict:merge(fun(_, LHS, RHS) -> LHS ++ RHS end,
                  orddict:from_list(lists:sort(CustomConfig)),
                  orddict:from_list(lists:sort(riak_config()))).

riak_config(oss, Backend) ->
    riak_oss_config(Backend);
riak_config(ee, Backend) ->
    riak_ee_config(Backend).

riak_oss_config(Backend) ->
    CSCurrent = rt_config:get(<<"build_paths.cs_current">>),
    AddPaths = filelib:wildcard(CSCurrent ++ "/dev/dev1/lib/riak_cs*/ebin"),
    [
     lager_config(),
     {riak_core,
      [{default_bucket_props, [{allow_mult, true}]},
       {ring_creation_size, 8}]
     },
     {riak_api,
      [{pb_backlog, 256}]},
     {riak_kv,
      [{add_paths, AddPaths}] ++
          backend_config(Backend)
      }
    ].

backend_config(memory) ->
      [{storage_backend, riak_kv_memory_backend}];
backend_config({multi_backend, BlocksBackend}) ->
      [
       {storage_backend, riak_cs_kv_multi_backend},
       {multi_backend_prefix_list, [{<<"0b:">>, be_blocks}]},
       {multi_backend_default, be_default},
       {multi_backend,
        [{be_default, riak_kv_eleveldb_backend,
          [
           {max_open_files, 20},
           {data_root, "./leveldb"}
          ]},
         blocks_backend_config(BlocksBackend)
        ]}
      ].

blocks_backend_config(fs) ->
    {be_blocks, riak_kv_fs2_backend, [{data_root, "./fs2"},
                                      {block_size, 1050000}]};
blocks_backend_config(_) ->
    {be_blocks, riak_kv_bitcask_backend, [{data_root, "./bitcask"}]}.

riak_ee_config(Backend) ->
    [repl_config() | riak_oss_config(Backend)].

repl_config() ->
    {riak_repl,
     [
      {fullsync_on_connect, false},
      {fullsync_interval, disabled},
      {proxy_get, enabled}
     ]}.

cs_config() ->
    cs_config([], []).

cs_config(UserExtra) ->
    cs_config(UserExtra, []).

cs_config(UserExtra, OtherApps) ->
    [
     lager_config(),
     {riak_cs,
      UserExtra ++
          [
           {connection_pools,
            [
             {request_pool, {request_pool_size(), 0} },
             {bucket_list_pool, {bucket_list_pool_size(), 0} }
            ]},
           {block_get_max_retries, 1},
           {proxy_get, enabled},
           {anonymous_user_creation, true},
           {riak_pb_port, 10017},
           {stanchion_port, 9095},
           {cs_version, 010300}
          ]
     }] ++ OtherApps.

stanchion_config() ->
    [
     lager_config(),
     {stanchion,
      [
       {stanchion_port, 9095},
       {riak_pb_port, 10017}
      ]
     }].

stanchion_config(UserExtra) ->
    [
     lager_config(),
     {stanchion,
      UserExtra ++
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

riakcs_switchcmd(Path, N, Cmd) ->
    lists:flatten(io_lib:format("~s-stanchion ~s", [riakcs_binpath(Path, N), Cmd])).

riakcs_gccmd(Path, N, Cmd) ->
    lists:flatten(io_lib:format("~s-gc ~s", [riakcs_binpath(Path, N), Cmd])).

riakcs_accesscmd(Path, N, Cmd) ->
    lists:flatten(io_lib:format("~s-access ~s", [riakcs_binpath(Path, N), Cmd])).

riakcs_storagecmd(Path, N, Cmd) ->
    lists:flatten(io_lib:format("~s-storage ~s", [riakcs_binpath(Path, N), Cmd])).

stanchion_binpath(Prefix) ->
    io_lib:format("~s/dev/stanchion/bin/stanchion", [Prefix]).

stanchion_etcpath(Prefix) ->
    io_lib:format("~s/dev/stanchion/etc", [Prefix]).

stanchioncmd(Path, Cmd) ->
    lists:flatten(io_lib:format("~s ~s", [stanchion_binpath(Path), Cmd])).

riak_root_and_current(oss) ->
    {?RIAK_ROOT, current};
riak_root_and_current(ee) ->
    {?EE_ROOT, ee_current}.

cs_current() ->
    ?CS_CURRENT.

deploy_nodes(NumNodes, InitialConfig) ->
    lager:info("Initial Config: ~p", [InitialConfig]),
    NodeConfig = [{current, InitialConfig} || _ <- lists:seq(1,NumNodes)],
    RiakNodes = [?DEV(N) || N <- lists:seq(1, NumNodes)],
    CSNodes = [?CSDEV(N) || N <- lists:seq(1, NumNodes)],
    StanchionNode = 'stanchion@127.0.0.1',

    lager:info("RiakNodes: ~p", [RiakNodes]),

    NodeMap = orddict:from_list(lists:zip(RiakNodes, lists:seq(1, NumNodes))),
    rt_config:set(rt_nodes, NodeMap),

    {RiakRoot, RiakCurrent} =
        riak_root_and_current(rt_config:get(build_type, oss)),

    VersionMap = lists:zip(lists:seq(1, NumNodes), lists:duplicate(NumNodes, RiakCurrent)),
    rt_config:set(rt_versions, VersionMap),

    lager:info("VersionMap: ~p", [VersionMap]),

    NL0 = lists:zip(CSNodes, RiakNodes),
    {CS1, R1} = hd(NL0),
    NodeList = [{CS1, R1, StanchionNode} | tl(NL0)],
    lager:info("NodeList: ~p", [NodeList]),

    stop_all_nodes(NodeList),

    [reset_nodes(Project, Path) ||
        {Project, Path} <- [{riak_ee, rt_config:get(RiakRoot)},
                            {riak_cs, rt_config:get(?CS_ROOT)},
                            {stanchion, rt_config:get(?STANCHION_ROOT)}]],

    rt_cs_dev:create_dirs(RiakNodes),

    {_Versions, Configs} = lists:unzip(NodeConfig),

    %% Set initial config
    set_configs(NodeList, Configs),
    start_all_nodes(NodeList),

    Nodes = {RiakNodes, CSNodes, StanchionNode},
    [ok = rt:wait_until_pingable(N) || N <- RiakNodes ++ CSNodes ++ [StanchionNode]],
    [ok = rt:check_singleton_node(N) || N <- RiakNodes],

    rt:wait_until_nodes_ready(RiakNodes),

    Nodes.

setup_admin_user(NumNodes, InitialConfig) ->
    lager:info("Initial Config: ~p", [InitialConfig]),
    NodeConfig = [{current, InitialConfig} || _ <- lists:seq(1,NumNodes)],
    RiakNodes = [?DEV(N) || N <- lists:seq(1, NumNodes)],
    CSNodes = [?CSDEV(N) || N <- lists:seq(1, NumNodes)],
    StanchionNode = 'stanchion@127.0.0.1',
    NodeMap = orddict:from_list(lists:zip(RiakNodes, lists:seq(1, NumNodes))),
    rt_config:set(rt_nodes, NodeMap),

    VersionMap = lists:zip(lists:seq(1, NumNodes), lists:duplicate(NumNodes, ee_current)),
    rt_config:set(rt_versions, VersionMap),

    NL0 = lists:zip(CSNodes, RiakNodes),
    {CS1, R1} = hd(NL0),
    NodeList = [{CS1, R1, StanchionNode} | tl(NL0)],

    {_Versions, Configs} = lists:unzip(NodeConfig),

    Nodes = {RiakNodes, CSNodes, StanchionNode},

    %% Create admin user and set in cs and stanchion configs
    {KeyID, KeySecret} = AdminCreds = create_admin_user(hd(RiakNodes)),

    %% Restart cs and stanchion nodes so admin user takes effect
    %% stop_cs_and_stanchion_nodes(NodeList),

    set_admin_creds_in_configs(NodeList, Configs, AdminCreds),

    UpdateFun = fun({Node, App}) ->
                        ok = rpc:call(Node, application, set_env,
                                      [App, admin_key, KeyID]),
                        ok = rpc:call(Node, application, set_env,
                                      [App, admin_secret, KeySecret])
                end,
    ZippedNodes = [{StanchionNode, stanchion} |
             [ {CSNode, riak_cs} || CSNode <- CSNodes ]],
    lists:foreach(UpdateFun, ZippedNodes),

    %% start_cs_and_stanchion_nodes(NodeList),
    %% [ok = rt:wait_until_pingable(N) || N <- CSNodes ++ [StanchionNode]],

    lager:info("NodeConfig: ~p", [ NodeConfig ]),
    lager:info("RiakNodes: ~p", [RiakNodes]),
    lager:info("CSNodes: ~p", [CSNodes]),
    lager:info("NodeMap: ~p", [ NodeMap ]),
    lager:info("VersionMap: ~p", [VersionMap]),
    lager:info("NodeList: ~p", [NodeList]),
    lager:info("Nodes: ~p", [Nodes]),
    lager:info("AdminCreds: ~p", [AdminCreds]),
    lager:info("Deployed nodes: ~p", [Nodes]),

    AdminCreds.


start_cs_and_stanchion_nodes(NodeList) ->
    rt:pmap(fun({_CSNode, RiakNode, _Stanchion}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    start_stanchion(),
                    start_cs(N);
               ({_CSNode, RiakNode}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    start_cs(N)
            end, NodeList).

stop_cs_and_stanchion_nodes(NodeList) ->
    rt:pmap(fun({CSNode, RiakNode, Stanchion}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    stop_cs(N),
                    stop_stanchion(),
                    rt:wait_until_unpingable(CSNode),
                    rt:wait_until_unpingable(Stanchion);
               ({CSNode, RiakNode}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    stop_cs(N),
                    rt:wait_until_unpingable(CSNode)
            end, NodeList).

start_all_nodes(NodeList) ->
    rt:pmap(fun({_CSNode, RiakNode, _Stanchion}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    rtdev:run_riak(N, rt_cs_dev:relpath(rt_cs_dev:node_version(N)), "start"),
                    rt:wait_for_service(RiakNode, riak_kv),
                    spawn(fun() -> start_stanchion() end),
                    spawn(fun() -> start_cs(N) end);
               ({_CSNode, RiakNode}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    rtdev:run_riak(N, rt_cs_dev:relpath(rt_cs_dev:node_version(N)), "start"),
                    rt:wait_for_service(RiakNode, riak_kv),
                    spawn(fun() -> start_cs(N) end)
            end, NodeList).

stop_all_nodes(NodeList) ->
    rt:pmap(fun({CSNode, RiakNode, Stanchion}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    stop_cs(N),
                    stop_stanchion(),
                    rtdev:run_riak(N, rt_cs_dev:relpath(rt_cs_dev:node_version(N)), "stop"),
                    rt:wait_until_unpingable(CSNode),
                    rt:wait_until_unpingable(Stanchion),
                    rt:wait_until_unpingable(RiakNode);
               ({CSNode, RiakNode}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    stop_cs(N),
                    rtdev:run_riak(N, rt_cs_dev:relpath(rt_cs_dev:node_version(N)), "stop"),
                    rt:wait_until_unpingable(CSNode),
                    rt:wait_until_unpingable(RiakNode)
            end, NodeList).

set_configs(NodeList, Configs) ->
    rt:pmap(fun({_, default}) ->
                    ok;
               ({{_CSNode, RiakNode, _Stanchion}, Config}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    rt_cs_dev:update_app_config(RiakNode, proplists:get_value(riak,
                                                                              Config)),
                    update_cs_config(rt_config:get(?CS_CURRENT), N,
                                     proplists:get_value(cs, Config)),
                    update_stanchion_config(rt_config:get(?STANCHION_CURRENT),
                                            proplists:get_value(stanchion, Config));
               ({{_CSNode, RiakNode}, Config}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    rt_cs_dev:update_app_config(RiakNode, proplists:get_value(riak,
                                                                              Config)),
                    update_cs_config(rt_config:get(?CS_CURRENT), N,
                                     proplists:get_value(cs, Config))
            end,
            lists:zip(NodeList, Configs)).

set_admin_creds_in_configs(NodeList, Configs, AdminCreds) ->
    rt:pmap(fun({_, default}) ->
                    ok;
               ({{_CSNode, RiakNode, _Stanchion}, Config}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    update_cs_config(rt_config:get(?CS_CURRENT),
                                     N,
                                     proplists:get_value(cs, Config),
                                     AdminCreds),
                    update_stanchion_config(rt_config:get(?STANCHION_CURRENT),
                                            proplists:get_value(stanchion, Config),
                                            AdminCreds);
               ({{_CSNode, RiakNode}, Config}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    update_cs_config(rt_config:get(?CS_CURRENT),
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

start_cs(N) ->
    Cmd = riakcscmd(rt_config:get(?CS_CURRENT), N, "start"),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

stop_cs(N) ->
    Cmd = riakcscmd(rt_config:get(?CS_CURRENT), N, "stop"),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

switch_stanchion_cs(N, Host, Port) ->
    SubCmd = io_lib:format("switch ~s ~p", [Host, Port]),
    Cmd = riakcs_switchcmd(rt_config:get(?CS_CURRENT), N, SubCmd),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

show_stanchion_cs(N) ->
    Cmd = riakcs_switchcmd(rt_config:get(?CS_CURRENT), N, "show"),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

start_stanchion() ->
    Cmd = stanchioncmd(rt_config:get(?STANCHION_CURRENT), "start"),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

stop_stanchion() ->
    Cmd = stanchioncmd(rt_config:get(?STANCHION_CURRENT), "stop"),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

flush_access(N) ->
    Cmd = riakcs_accesscmd(rt_config:get(?CS_CURRENT), N, "flush"),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

gc(N, SubCmd) ->
    Cmd = riakcs_gccmd(rt_config:get(?CS_CURRENT), N, SubCmd),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).
    

calculate_storage(N) ->
    Cmd = riakcs_storagecmd(rt_config:get(?CS_CURRENT), N, "batch -r"),
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
    update_cs_config(rt_config:get(?CS_CURRENT), N, Config),
    start_cs(N),
    lager:info("Riak CS started").

%% this differs from rt_cs_dev:deploy_xxx in that it only starts one node
deploy_stanchion(Config) ->
    %% Set initial config
    update_stanchion_config(rt_config:get(?STANCHION_CURRENT), Config),

    start_stanchion(),
    lager:info("Stanchion started").

create_user(Port, EmailAddr, Name) ->
    lager:debug("Trying to create user ~p", [EmailAddr]),
    Cmd="curl -s -H 'Content-Type: application/json' http://localhost:" ++
        integer_to_list(Port) ++
        "/riak-cs/user --data '{\"email\":\"" ++ EmailAddr ++  "\", \"name\":\"" ++ Name ++"\"}'",
    lager:info("Cmd: ~p", [Cmd]),
    Delay = rt_config:get(rt_retry_delay),
    Retries = rt_config:get(rt_max_wait_time) div Delay,
    OutputFun = fun() -> rt:cmd(Cmd) end,
    Condition = fun({Status, Res}) -> Status =:= 0 andalso Res /= [] end,
    {_Status, Output} = wait_until(OutputFun, Condition, Retries, Delay),
    lager:debug("Create user output=~p~n",[Output]),
    {struct, JsonData} = mochijson2:decode(Output),
    KeyId = binary_to_list(proplists:get_value(<<"key_id">>, JsonData)),
    KeySecret = binary_to_list(proplists:get_value(<<"key_secret">>, JsonData)),
    Id = binary_to_list(proplists:get_value(<<"id">>, JsonData)),
    {KeyId, KeySecret, Id}.

update_user(UserConfig, Port, Resource, ContentType, UpdateDoc) ->
    Date = httpd_util:rfc1123_date(),
    Cmd="curl -s -X PUT -H 'Date: " ++ Date ++
        "' -H 'Content-Type: " ++ ContentType ++
        "' -H 'Authorization: " ++
        make_authorization("PUT", Resource, ContentType, UserConfig, Date) ++
        "' http://localhost:" ++ integer_to_list(Port) ++
        Resource ++ " --data-binary " ++ UpdateDoc,
    Delay = rt_config:get(rt_retry_delay),
    Retries = rt_config:get(rt_max_wait_time) div Delay,
    OutputFun = fun() -> os:cmd(Cmd) end,
    Condition = fun(Res) -> Res /= [] end,
    Output = wait_until(OutputFun, Condition, Retries, Delay),
    lager:debug("Update user output=~p~n",[Output]),
    Output.

list_users(UserConfig, Port, Resource, AcceptContentType) ->
    Date = httpd_util:rfc1123_date(),
    Cmd="curl -s -H 'Date: " ++ Date ++
        "' -H 'Accept: " ++ AcceptContentType ++
        "' -H 'Authorization: " ++
        make_authorization("GET", Resource, "", UserConfig, Date) ++
        "' http://localhost:" ++ integer_to_list(Port) ++
        Resource,
    Delay = rt_config:get(rt_retry_delay),
    Retries = rt_config:get(rt_max_wait_time) div Delay,
    OutputFun = fun() -> os:cmd(Cmd) end,
    Condition = fun(Res) -> Res /= [] end,
    Output = wait_until(OutputFun, Condition, Retries, Delay),
    lager:debug("List users output=~p~n",[Output]),
    Output.

wait_until(_, _, 0, _) ->
    fail;
wait_until(Fun, Condition, Retries, Delay) ->
    Result = Fun(),
    case Condition(Result) of
        true ->
            Result;
        false ->
            timer:sleep(Delay),
            wait_until(Fun, Condition, Retries-1, Delay)
    end.

%% Kind = objects | blocks | users | buckets ...
pbc(RiakNodes, ObjectKind, CsBucket, CsKey) ->
    pbc(rt_config:get(flavor, basic), ObjectKind, RiakNodes, CsBucket, CsKey).

pbc(basic, _ObjectKind, RiakNodes, _CsBucket, _CsKey) ->
    rt:pbc(hd(RiakNodes));
pbc({multibag, _} = Flavor, ObjectKind, RiakNodes, CsBucket, CsKey) ->
    rtcs_bag:pbc(Flavor, ObjectKind, RiakNodes, CsBucket, CsKey).

make_authorization(Method, Resource, ContentType, Config, Date) ->
    StringToSign = [Method, $\n, [], $\n, ContentType, $\n, Date, $\n, Resource],
    Signature =
        base64:encode_to_string(sha_mac(Config#aws_config.secret_access_key, StringToSign)),
    lists:flatten(["AWS ", Config#aws_config.access_key_id, $:, Signature]).

-ifdef(new_hash).
sha_mac(Key,STS) -> crypto:hmac(sha, Key,STS).
sha(Bin) -> crypto:hash(sha, Bin).
md5(Bin) -> crypto:hash(md5, Bin).

-else.
sha_mac(Key,STS) -> crypto:sha_mac(Key,STS).
sha(Bin) -> crypto:sha(Bin).
md5(Bin) -> crypto:md5(Bin).

-endif.

datetime() ->
    {{YYYY,MM,DD}, {H,M,S}} = calendar:universal_time(),
    lists:flatten(io_lib:format("~4..0B~2..0B~2..0BT~2..0B~2..0B~2..0BZ", [YYYY, MM, DD, H, M, S])).



json_get(Key, Json) when is_binary(Key) ->
    json_get([Key], Json);
json_get([], Json) ->
    Json;
json_get([Key | Keys], {struct, JsonProps}) ->
    case lists:keyfind(Key, 1, JsonProps) of
        false ->
            notfound;
        {Key, Value} ->
            json_get(Keys, Value)
    end.
