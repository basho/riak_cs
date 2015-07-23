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
-include_lib("xmerl/include/xmerl.hrl").

-import(rt, [join/2,
             wait_until_nodes_ready/1,
             wait_until_no_pending_changes/1]).

-define(DEVS(N), lists:concat(["dev", N, "@127.0.0.1"])).
-define(DEV(N), list_to_atom(?DEVS(N))).
-define(CSDEVS(N), lists:concat(["rcs-dev", N, "@127.0.0.1"])).
-define(CSDEV(N), list_to_atom(?CSDEVS(N))).

-define(RIAK_ROOT, <<"build_paths.root">>).
-define(RIAK_CURRENT, <<"build_paths.current">>).
-define(RIAK_PREVIOUS, <<"build_paths.previous">>).
-define(EE_ROOT, <<"build_paths.ee_root">>).
-define(EE_CURRENT, <<"build_paths.ee_current">>).
-define(EE_PREVIOUS, <<"build_paths.ee_previous">>).
-define(CS_ROOT, <<"build_paths.cs_root">>).
-define(CS_CURRENT, <<"build_paths.cs_current">>).
-define(CS_PREVIOUS, <<"build_paths.cs_previous">>).
-define(STANCHION_ROOT, <<"build_paths.stanchion_root">>).
-define(STANCHION_CURRENT, <<"build_paths.stanchion_current">>).
-define(STANCHION_PREVIOUS, <<"build_paths.stanchion_previous">>).

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
    setup(NumNodes, default_configs(), current).

setup(NumNodes, Configs) ->
    setup(NumNodes, Configs, current).

setup(NumNodes, Configs, Vsn) ->
    Flavor = rt_config:get(flavor, basic),
    lager:info("Flavor : ~p", [Flavor]),
    flavored_setup(NumNodes, Flavor, Configs, Vsn).

setup2x2() ->
    setup2x2(default_configs()).

setup2x2(Configs) ->
    JoinFun = fun(Nodes) ->
                      [A,B,C,D] = Nodes,
                      join(B,A),
                      join(D,C)
              end,
    setup_clusters(Configs, JoinFun, 4, current).

%% 1 cluster with N nodes + M cluster with 1 node
setupNxMsingles(N, M) ->
    setupNxMsingles(N, M, default_configs(), current).

setupNxMsingles(N, M, Configs, Vsn)
  when Vsn =:= current orelse Vsn =:= previous ->
    JoinFun = fun(Nodes) ->
                      [Target | Joiners] = lists:sublist(Nodes, N),
                      [join(J, Target) || J <- Joiners]
              end,
    setup_clusters(Configs, JoinFun, N + M, Vsn).

flavored_setup(NumNodes, basic, Configs, Vsn) ->
    JoinFun = fun(Nodes) ->
                      [First|Rest] = Nodes,
                      [join(Node, First) || Node <- Rest]
              end,
    setup_clusters(Configs, JoinFun, NumNodes, Vsn);
flavored_setup(NumNodes, {multibag, _} = Flavor, Configs, Vsn)
  when Vsn =:= current orelse Vsn =:= previous ->
    rtcs_bag:flavored_setup(NumNodes, Flavor, Configs, Vsn).

setup_clusters(Configs, JoinFun, NumNodes, Vsn) ->
    ConfigFun = fun(_Type, Config, _Node) -> Config end,
    setup_clusters(Configs, ConfigFun, JoinFun, NumNodes, Vsn).

setup_clusters(Configs, ConfigFun, JoinFun, NumNodes, Vsn) ->
    %% Start the erlcloud app
    erlcloud:start(),

    %% STFU sasl
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, false),

    Cfgs = configs(Configs),
    lager:info("Configs = ~p", [ Cfgs]),
    {RiakNodes, _CSNodes, _Stanchion} = Nodes =
        deploy_nodes(NumNodes, Cfgs, ConfigFun, Vsn),
    rt:wait_until_nodes_ready(RiakNodes),
    lager:info("Make cluster"),
    JoinFun(RiakNodes),
    ?assertEqual(ok, wait_until_nodes_ready(RiakNodes)),
    ?assertEqual(ok, wait_until_no_pending_changes(RiakNodes)),
    rt:wait_until_ring_converged(RiakNodes),
    {AdminKeyId, AdminSecretKey} = setup_admin_user(NumNodes, Cfgs, ConfigFun, Vsn),
    AdminConfig = rtcs:config(AdminKeyId,
                              AdminSecretKey,
                              rtcs:cs_port(hd(RiakNodes))),
    {AdminConfig, Nodes}.


pass() ->
    teardown(),
    pass.

teardown() ->
    %% catch application:stop(sasl),
    catch application:stop(erlcloud),
    catch application:stop(ibrowse).

configs(CustomConfigs) ->
    [{riak, proplists:get_value(riak, CustomConfigs, riak_config())},
     {cs, proplists:get_value(cs, CustomConfigs, cs_config())},
     {stanchion, proplists:get_value(stanchion,
                                     CustomConfigs,
                                     stanchion_config())}].

previous_configs() ->
    previous_configs([]).

previous_configs(CustomConfigs) ->
    [{riak, proplists:get_value(riak, CustomConfigs, previous_riak_config())},
     {cs, proplists:get_value(cs, CustomConfigs, previous_cs_config())},
     {stanchion, proplists:get_value(stanchion, CustomConfigs,
                                     previous_stanchion_config())}].

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

%% Return Riak node IDs, one per cluster.
%% For example, in basic single cluster case, just return [1].
-spec riak_id_per_cluster(pos_integer()) -> [pos_integer()].
riak_id_per_cluster(NumNodes) ->
    case rt_config:get(flavor, basic) of
        basic -> [1];
        {multibag, _} = Flavor -> rtcs_bag:riak_id_per_cluster(NumNodes, Flavor)
    end.

deploy_stanchion(Config) ->
    %% Set initial config
    ConfigFun = fun(_, Config0, _) -> Config0 end,
    update_stanchion_config(rt_config:get(?STANCHION_CURRENT), Config, ConfigFun),

    start_stanchion(),
    lager:info("Stanchion started").

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

pb_port(N) when is_integer(N) ->
    10000 + (N * 10) + 7;
pb_port(Node) ->
    pb_port(rt_cs_dev:node_id(Node)).

cs_port(N) when is_integer(N) ->
    15008 + 10 * N;
cs_port(Node) ->
    cs_port(rt_cs_dev:node_id(Node)).


riak_config(CustomConfig) ->
    orddict:merge(fun(_, LHS, RHS) -> LHS ++ RHS end,
                  orddict:from_list(lists:sort(CustomConfig)),
                  orddict:from_list(lists:sort(riak_config()))).

riak_config() ->
    riak_config(
      ?CS_CURRENT,
      rt_config:get(build_type, oss),
      rt_config:get(backend, {multi_backend, bitcask})).

riak_config(CsVsn, oss, Backend) ->
    riak_oss_config(CsVsn, Backend);
riak_config(CsVsn, ee, Backend) ->
    riak_ee_config(CsVsn, Backend).

riak_oss_config(CsVsn, Backend) ->
    CSPath = rt_config:get(CsVsn),
    AddPaths = filelib:wildcard(CSPath ++ "/dev/dev1/lib/riak_cs*/ebin"),
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
           {data_root, "./data/leveldb"}
          ]},
         blocks_backend_config(BlocksBackend)
        ]}
      ].

blocks_backend_config(fs) ->
    {be_blocks, riak_kv_fs2_backend, [{data_root, "./data/fs2"},
                                      {block_size, 1050000}]};
blocks_backend_config(_) ->
    {be_blocks, riak_kv_bitcask_backend, [{data_root, "./data/bitcask"}]}.

riak_ee_config(CsVsn, Backend) ->
    [repl_config() | riak_oss_config(CsVsn, Backend)].

repl_config() ->
    {riak_repl,
     [
      {fullsync_on_connect, false},
      {fullsync_interval, disabled},
      {proxy_get, enabled}
     ]}.

previous_riak_config() ->
    riak_config(
      ?CS_PREVIOUS,
      rt_config:get(build_type, oss),
      rt_config:get(backend, {multi_backend, bitcask})).

previous_riak_config(CustomConfig) ->
    orddict:merge(fun(_, LHS, RHS) -> LHS ++ RHS end,
                  orddict:from_list(lists:sort(CustomConfig)),
                  orddict:from_list(lists:sort(previous_riak_config()))).

previous_riak_config(oss, Backend) ->
    riak_oss_config(?CS_PREVIOUS, Backend);
previous_riak_config(ee, Backend) ->
    riak_ee_config(?CS_PREVIOUS, Backend).

previous_cs_config() ->
    previous_cs_config([], []).

previous_cs_config(UserExtra) ->
    previous_cs_config(UserExtra, []).

previous_cs_config(UserExtra, OtherApps) ->
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
           {stanchion_host, {"127.0.0.1", 9095}},
           {riak_host, {"127.0.0.1", 10017}},
           {cs_version, 010300}
          ]
     }] ++ OtherApps.

replace_cs_config(Key, Value, Config) ->
    CSConfig0 = proplists:get_value(riak_cs, Config),
    CSConfig = replace(Key, Value, CSConfig0),
    replace(riak_cs, CSConfig, Config).

replace(Key, Value, Config0) ->
    Config1 = proplists:delete(Key, Config0),
    [proplists:property(Key, Value)|Config1].

replace_stanchion_config(Key, Value, Config) ->
    CSConfig0 = proplists:get_value(stanchion, Config),
    CSConfig = replace(Key, Value, CSConfig0),
    replace(stanchion, CSConfig, Config).

previous_stanchion_config() ->
    [
     lager_config(),
     {stanchion,
      [
       {stanchion_port, 9095},
       {riak_pb_port, 10017}
      ]
     }].

previous_stanchion_config(UserExtra) ->
    lists:foldl(fun({Key,Value}, Config0) ->
                        replace_stanchion_config(Key,Value,Config0)
                end, previous_stanchion_config(), UserExtra).

stanchion_config() ->
    [
     lager_config(),
     {stanchion,
      [
       {host, {"127.0.0.1", 9095}},
       {riak_host, {"127.0.0.1", 10017}}
      ]
     }].
    
stanchion_config(UserExtra) ->
    lists:foldl(fun({Key,Value}, Config0) ->
                        replace_stanchion_config(Key,Value,Config0)
                end, stanchion_config(), UserExtra).

lager_config() ->
    {lager,
     [
      {handlers,
       [
        {lager_file_backend,
         [
          {"./log/error.log", error, 10485760, "$D0",5},
          {"./log/console.log", rt_config:get(console_log_level, debug),
           10485760, "$D0", 5}
         ]}
       ]}
     ]}.

riak_bitcaskroot(Prefix, N) ->
    io_lib:format("~s/dev/dev~b/data/bitcask", [Prefix, N]).

riak_binpath(Prefix, N) ->
    io_lib:format("~s/dev/dev~b/bin/riak", [Prefix, N]).

riakcmd(Path, N, Cmd) ->
    lists:flatten(io_lib:format("~s ~s", [riak_binpath(Path, N), Cmd])).

riakcs_home(Prefix, N) ->
    io_lib:format("~s/dev/dev~b/", [Prefix, N]).

riakcs_binpath(Prefix, N) ->
    io_lib:format("~s/dev/dev~b/bin/riak-cs", [Prefix, N]).

riakcs_etcpath(Prefix, N) ->
    io_lib:format("~s/dev/dev~b/etc", [Prefix, N]).

riakcs_libpath(Prefix, N) ->
    io_lib:format("~s/dev/dev~b/lib", [Prefix, N]).

riakcs_logpath(Prefix, N, File) ->
    io_lib:format("~s/dev/dev~b/log/~s", [Prefix, N, File]).

riakcscmd(Path, N, Cmd) ->
    lists:flatten(io_lib:format("~s ~s", [riakcs_binpath(Path, N), Cmd])).

riakcs_switchcmd(Path, N, Cmd) ->
    lists:flatten(io_lib:format("~s-admin stanchion ~s", [riakcs_binpath(Path, N), Cmd])).

riakcs_gccmd(Path, N, Cmd) ->
    lists:flatten(io_lib:format("~s-admin gc ~s", [riakcs_binpath(Path, N), Cmd])).

riakcs_accesscmd(Path, N, Cmd) ->
    lists:flatten(io_lib:format("~s-admin access ~s", [riakcs_binpath(Path, N), Cmd])).

riakcs_storagecmd(Path, N, Cmd) ->
    lists:flatten(io_lib:format("~s-admin storage ~s", [riakcs_binpath(Path, N), Cmd])).

stanchion_binpath(Prefix) ->
    io_lib:format("~s/dev/stanchion/bin/stanchion", [Prefix]).

stanchion_etcpath(Prefix) ->
    io_lib:format("~s/dev/stanchion/etc", [Prefix]).

stanchioncmd(Path, Cmd) ->
    lists:flatten(io_lib:format("~s ~s", [stanchion_binpath(Path), Cmd])).

riak_root_and_vsn(current, oss) -> {?RIAK_ROOT, current};
riak_root_and_vsn(current, ee) ->  {?EE_ROOT, ee_current};
riak_root_and_vsn(previous, oss) -> {?RIAK_ROOT, previous};
riak_root_and_vsn(previous, ee) -> {?EE_ROOT, ee_previous}.

cs_current() ->
    ?CS_CURRENT.

-spec deploy_nodes(list(), list(), fun(), current|previous) -> any().
deploy_nodes(NumNodes, InitialConfig, ConfigFun, Vsn)
  when Vsn =:= current orelse Vsn =:= previous ->
    lager:info("Initial Config: ~p", [InitialConfig]),
    NodeConfig = [{current, InitialConfig} || _ <- lists:seq(1,NumNodes)],
    RiakNodes = [?DEV(N) || N <- lists:seq(1, NumNodes)],
    CSNodes = [?CSDEV(N) || N <- lists:seq(1, NumNodes)],
    StanchionNode = 'stanchion@127.0.0.1',

    lager:info("RiakNodes: ~p", [RiakNodes]),

    NodeMap = orddict:from_list(lists:zip(RiakNodes, lists:seq(1, NumNodes))),
    rt_config:set(rt_nodes, NodeMap),
    CSNodeMap = orddict:from_list(lists:zip(CSNodes, lists:seq(1, NumNodes))),
    rt_config:set(rt_cs_nodes, CSNodeMap),

    {RiakRoot, RiakVsn} = riak_root_and_vsn(Vsn, rt_config:get(build_type, oss)),

    lager:debug("setting rt_versions> ~p =>", [Vsn]),
    VersionMap = lists:zip(lists:seq(1, NumNodes), lists:duplicate(NumNodes, RiakVsn)),
    rt_config:set(rt_versions, VersionMap),

    lager:info("VersionMap: ~p", [VersionMap]),

    NL0 = lists:zip(CSNodes, RiakNodes),
    {CS1, R1} = hd(NL0),
    NodeList = [{CS1, R1, StanchionNode} | tl(NL0)],
    lager:info("NodeList: ~p", [NodeList]),

    stop_all_nodes(NodeList, Vsn),

    [reset_nodes(Project, Path) ||
        {Project, Path} <- [{riak_ee, rt_config:get(RiakRoot)},
                            {riak_cs, rt_config:get(?CS_ROOT)},
                            {stanchion, rt_config:get(?STANCHION_ROOT)}]],

    rt_cs_dev:create_dirs(RiakNodes),

    {_Versions, Configs} = lists:unzip(NodeConfig),

    %% Set initial config
    set_configs(NodeList, Configs, ConfigFun, Vsn),
    start_all_nodes(NodeList, Vsn),

    Nodes = {RiakNodes, CSNodes, StanchionNode},
    [ok = rt:wait_until_pingable(N) || N <- RiakNodes ++ CSNodes ++ [StanchionNode]],
    [ok = rt:check_singleton_node(N) || N <- RiakNodes],

    rt:wait_until_nodes_ready(RiakNodes),

    Nodes.

node_id(Node) ->
    NodeMap = rt_config:get(rt_cs_nodes),
    orddict:fetch(Node, NodeMap).

setup_admin_user(NumNodes, InitialConfig, ConfigFun, Vsn)
  when Vsn =:= current orelse Vsn =:= previous ->
    lager:info("Initial Config: ~p", [InitialConfig]),
    NodeConfig = [{current, InitialConfig} || _ <- lists:seq(1,NumNodes)],
    RiakNodes = [?DEV(N) || N <- lists:seq(1, NumNodes)],
    CSNodes = [?CSDEV(N) || N <- lists:seq(1, NumNodes)],
    StanchionNode = 'stanchion@127.0.0.1',
    NodeMap = orddict:from_list(lists:zip(RiakNodes, lists:seq(1, NumNodes))),
    CSNodeMap = orddict:from_list(lists:zip(CSNodes, lists:seq(1, NumNodes))),
    rt_config:set(rt_nodes, NodeMap),
    rt_config:set(rt_cs_nodes, CSNodeMap),

    {_RiakRoot, RiakVsn} = riak_root_and_vsn(Vsn, rt_config:get(build_type, oss)),
    VersionMap = lists:zip(lists:seq(1, NumNodes), lists:duplicate(NumNodes, RiakVsn)),
    lager:debug("setting rt_versions> ~p => ~p", [Vsn, VersionMap]),
    rt_config:set(rt_versions, VersionMap),

    NL0 = lists:zip(CSNodes, RiakNodes),
    {CS1, R1} = hd(NL0),
    NodeList = [{CS1, R1, StanchionNode} | tl(NL0)],

    {_Versions, Configs} = lists:unzip(NodeConfig),

    Nodes = {RiakNodes, CSNodes, StanchionNode},

    %% Create admin user and set in cs and stanchion configs
    {KeyID, KeySecret} = AdminCreds = create_admin_user(hd(RiakNodes)),

    set_admin_creds_in_configs(NodeList, Configs, ConfigFun, AdminCreds, Vsn),

    UpdateFun = fun({Node, App}) ->
                        ok = rpc:call(Node, application, set_env,
                                      [App, admin_key, KeyID]),
                        ok = rpc:call(Node, application, set_env,
                                      [App, admin_secret, KeySecret])
                end,
    ZippedNodes = [{StanchionNode, stanchion} |
             [ {CSNode, riak_cs} || CSNode <- CSNodes ]],
    lists:foreach(UpdateFun, ZippedNodes),

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


start_cs_and_stanchion_nodes(NodeList, Vsn) ->
    rt:pmap(fun({_CSNode, RiakNode, _Stanchion}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    start_stanchion(Vsn),
                    start_cs(N, Vsn);
               ({_CSNode, RiakNode}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    start_cs(N, Vsn)
            end, NodeList).

stop_cs_and_stanchion_nodes(NodeList, Vsn) ->
    rt:pmap(fun({CSNode, RiakNode, Stanchion}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    stop_cs(N, Vsn),
                    stop_stanchion(Vsn),
                    rt:wait_until_unpingable(CSNode),
                    rt:wait_until_unpingable(Stanchion);
               ({CSNode, RiakNode}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    stop_cs(N, Vsn),
                    rt:wait_until_unpingable(CSNode)
            end, NodeList).

start_all_nodes(NodeList, Vsn) ->
    rt:pmap(fun({_CSNode, RiakNode, _Stanchion}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    NodeVersion = rt_cs_dev:node_version(N),
                    lager:debug("starting riak #~p > ~p => ~p",
                                [N,  NodeVersion,
                                 rt_cs_dev:relpath(NodeVersion)]),
                    rtdev:run_riak(N, rt_cs_dev:relpath(NodeVersion), "start"),
                    rt:wait_for_service(RiakNode, riak_kv),
                    spawn(fun() -> start_stanchion(Vsn) end),
                    spawn(fun() -> start_cs(N, Vsn) end);
               ({_CSNode, RiakNode}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    rtdev:run_riak(N, rt_cs_dev:relpath(rt_cs_dev:node_version(N)), "start"),
                    rt:wait_for_service(RiakNode, riak_kv),
                    spawn(fun() -> start_cs(N, Vsn) end)
            end, NodeList).

stop_all_nodes(NodeList, Vsn) ->
    rt:pmap(fun({CSNode, RiakNode, Stanchion}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    stop_cs(N, Vsn),
                    stop_stanchion(Vsn),
                    rtdev:run_riak(N, rt_cs_dev:relpath(rt_cs_dev:node_version(N)), "stop"),
                    rt:wait_until_unpingable(CSNode),
                    rt:wait_until_unpingable(Stanchion),
                    rt:wait_until_unpingable(RiakNode);
               ({CSNode, RiakNode}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    stop_cs(N, Vsn),
                    rtdev:run_riak(N, rt_cs_dev:relpath(rt_cs_dev:node_version(N)), "stop"),
                    rt:wait_until_unpingable(CSNode),
                    rt:wait_until_unpingable(RiakNode)
            end, NodeList).

get_rt_config(riak, current) ->
    case rt_config:get(build_type, oss) of
        oss -> rt_config:get(?RIAK_CURRENT);
        ee  -> rt_config:get(?EE_CURRENT)
    end;
get_rt_config(riak, previous) ->
    case rt_config:get(build_type, oss) of
        oss -> rt_config:get(?RIAK_PREVIOUS);
        ee  -> rt_config:get(?EE_PREVIOUS)
    end;
get_rt_config(cs, current) -> rt_config:get(?CS_CURRENT);
get_rt_config(cs, previous) -> rt_config:get(?CS_PREVIOUS);
get_rt_config(stanchion, current) -> rt_config:get(?STANCHION_CURRENT);
get_rt_config(stanchion, previous) -> rt_config:get(?STANCHION_PREVIOUS).

set_configs(NodeList, Configs, ConfigFun, Vsn) ->
    rt:pmap(fun({_, default}) ->
                    ok;
               ({{_CSNode, RiakNode, _Stanchion}, Config}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    rt_cs_dev:update_app_config(RiakNode, proplists:get_value(riak,
                                                                              Config)),
                    update_cs_config(get_rt_config(cs, Vsn), N,
                                     proplists:get_value(cs, Config), ConfigFun),
                    update_stanchion_config(get_rt_config(stanchion, Vsn),
                                            proplists:get_value(stanchion, Config),
                                            ConfigFun);
               ({{_CSNode, RiakNode}, Config}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    rt_cs_dev:update_app_config(RiakNode,
                                                proplists:get_value(riak, Config)),
                    update_cs_config(get_rt_config(cs, Vsn), N,
                                     proplists:get_value(cs, Config), ConfigFun)
            end,
            lists:zip(NodeList, Configs)),
    enable_zdbbl(Vsn).

set_admin_creds_in_configs(NodeList, Configs, ConfigFun, AdminCreds, Vsn) ->
    rt:pmap(fun({_, default}) ->
                    ok;
               ({{_CSNode, RiakNode, _Stanchion}, Config}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    update_cs_config(get_rt_config(cs, Vsn),
                                     N,
                                     proplists:get_value(cs, Config),
                                     ConfigFun,
                                     AdminCreds),
                    update_stanchion_config(get_rt_config(stanchion, Vsn),
                                            proplists:get_value(stanchion, Config),
                                            ConfigFun, AdminCreds);
               ({{_CSNode, RiakNode}, Config}) ->
                    N = rt_cs_dev:node_id(RiakNode),
                    update_cs_config(get_rt_config(cs, Vsn),
                                     N,
                                     proplists:get_value(cs, Config),
                                     ConfigFun,
                                     AdminCreds)
            end,
            lists:zip(NodeList, Configs)).

reset_nodes(Project, Path) ->
    %% Reset nodes to base state
    lager:info("Resetting ~p nodes to fresh state", [Project]),
    lager:debug("Project path for reset: ~p", [Path]),
    rtdev:run_git(Path, "reset HEAD --hard"),
    rtdev:run_git(Path, "clean -fd").

start_cs(N) -> start_cs(N, current).

start_cs(N, Vsn) ->
    NodePath = get_rt_config(cs, Vsn),
    Cmd = riakcscmd(NodePath, N, "start"),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

stop_cs(N) -> stop_cs(N, current).

stop_cs(N, Vsn) ->
    Cmd = riakcscmd(get_rt_config(cs, Vsn), N, "stop"),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

repair_gc_bucket(N, Options) -> repair_gc_bucket(N, Options, current).

repair_gc_bucket(N, Options, Vsn) ->
    Prefix = get_rt_config(cs, Vsn),
    RepairScriptWild = string:join([riakcs_libpath(Prefix, N), "riak_cs*",
                                    "priv/tools/repair_gc_bucket.erl"] , "/"),
    [RepairScript] = filelib:wildcard(RepairScriptWild),
    Cmd = riakcscmd(Prefix, N, "escript " ++ RepairScript ++
                        " " ++ Options),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

exec_priv_escript(N, Command, Options) ->
    exec_priv_escript(N, Command, Options, cs).

exec_priv_escript(N, Command, Options, ByWhom) ->
    CsPrefix = get_rt_config(cs, current),
    ExecuterPrefix = get_rt_config(ByWhom, current),
    ScriptWild = string:join([riakcs_libpath(CsPrefix, N), "riak_cs*",
                              "priv/tools/"] , "/"),
    [ToolsDir] = filelib:wildcard(ScriptWild),
    Cmd = case ByWhom of
              cs ->
                  riakcscmd(ExecuterPrefix, N, "escript " ++ ToolsDir ++
                                "/" ++ Command ++
                                " " ++ Options);
              riak ->
                  riakcmd(ExecuterPrefix, N, "escript " ++ ToolsDir ++
                              "/" ++ Command ++
                              " " ++ Options)
          end,
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

switch_stanchion_cs(N, Host, Port) -> switch_stanchion_cs(N, Host, Port, current).

switch_stanchion_cs(N, Host, Port, Vsn) ->
    SubCmd = io_lib:format("switch ~s ~p", [Host, Port]),
    Cmd = riakcs_switchcmd(get_rt_config(cs, Vsn), N, SubCmd),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

show_stanchion_cs(N) -> show_stanchion_cs(N, current).

show_stanchion_cs(N, Vsn) ->
    Cmd = riakcs_switchcmd(get_rt_config(cs, Vsn), N, "show"),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

start_stanchion() -> start_stanchion(current).

start_stanchion(Vsn) ->
    Cmd = stanchioncmd(get_rt_config(stanchion, Vsn), "start"),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

stop_stanchion() -> stop_stanchion(current).

stop_stanchion(Vsn) ->
    Cmd = stanchioncmd(get_rt_config(stanchion, Vsn), "stop"),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

flush_access(N) -> flush_access(N, current).

flush_access(N, Vsn) ->
    Cmd = riakcs_accesscmd(get_rt_config(cs, Vsn), N, "flush"),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

gc(N, SubCmd) -> gc(N, SubCmd, current).

gc(N, SubCmd, Vsn) ->
    Cmd = riakcs_gccmd(get_rt_config(cs, Vsn), N, SubCmd),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

calculate_storage(N) -> calculate_storage(N, current).

calculate_storage(N, Vsn) ->
    Cmd = riakcs_storagecmd(get_rt_config(cs, Vsn), N, "batch -r"),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

enable_proxy_get(SrcN, Vsn, SinkCluster) ->
    rtdev:run_riak_repl(SrcN, get_rt_config(riak, Vsn),
                        "proxy_get enable " ++ SinkCluster).

disable_proxy_get(SrcN, Vsn, SinkCluster) ->
    rtdev:run_riak_repl(SrcN, get_rt_config(riak, Vsn),
                        "proxy_get disable " ++ SinkCluster).

read_config(Vsn, N, Who) ->
    Prefix = get_rt_config(Who, Vsn),
    EtcPath = case Who of
                  cs -> riakcs_etcpath(Prefix, N);
                  stanchion -> stanchion_etcpath(Prefix)
              end,
    case file:consult(EtcPath ++ "/advanced.config") of
         {ok, [Config]} ->
             Config;
         {error, enoent}->
             {ok, [Config]} = file:consult(EtcPath ++ "/app.config"),
             Config
     end.

update_cs_config(Prefix, N, Config, {_,_} = AdminCred) ->
    update_cs_config(Prefix, N, Config, fun(_,Config0,_) -> Config0 end, AdminCred);
update_cs_config(Prefix, N, Config, ConfigUpdateFun) when is_function(ConfigUpdateFun) ->
    update_cs_config1(Prefix, N, Config, ConfigUpdateFun).

update_cs_config(Prefix, N, Config, ConfigUpdateFun, {AdminKey, AdminSecret}) ->
    CSSection = proplists:get_value(riak_cs, Config),
    UpdConfig = [{riak_cs, update_admin_creds(CSSection, AdminKey, AdminSecret)} |
                 proplists:delete(riak_cs, Config)],
    update_cs_config1(Prefix, N, UpdConfig, ConfigUpdateFun).

update_cs_config1(Prefix, N, Config, ConfigUpdateFun) ->
    CSSection = proplists:get_value(riak_cs, Config),
    UpdConfig0 = [{riak_cs, update_cs_port(CSSection, N)} |
                  proplists:delete(riak_cs, Config)],
    UpdConfig = ConfigUpdateFun(cs, UpdConfig0, N),
    update_app_config(riakcs_etcpath(Prefix, N), UpdConfig).

update_admin_creds(Config, AdminKey, AdminSecret) ->
    [{admin_key, AdminKey}, {admin_secret, AdminSecret} |
     proplists:delete(admin_secret,
                      proplists:delete(admin_key, Config))].

update_cs_port(Config, N) ->
    Config2 = [{riak_host, {"127.0.0.1", pb_port(N)}} | proplists:delete(riak_host, Config)],
    [{listener, {"127.0.0.1", cs_port(N)}} | proplists:delete(listener, Config2)].

update_stanchion_config(Prefix, Config, {_,_} = AdminCreds) ->
    update_stanchion_config(Prefix, Config, fun(_,Config0,_) -> Config0 end, AdminCreds);
update_stanchion_config(Prefix, Config, ConfigUpdateFun) when is_function(ConfigUpdateFun) ->
    update_stanchion_config1(Prefix, Config, ConfigUpdateFun).

update_stanchion_config(Prefix, Config, ConfigUpdateFun, {AdminKey, AdminSecret}) ->
    StanchionSection = proplists:get_value(stanchion, Config),
    UpdConfig = [{stanchion, update_admin_creds(StanchionSection, AdminKey, AdminSecret)} |
                 proplists:delete(stanchion, Config)],
    update_stanchion_config1(Prefix, UpdConfig, ConfigUpdateFun).

update_stanchion_config1(Prefix, Config0, ConfigUpdateFun) when is_function(ConfigUpdateFun) ->
    Config = ConfigUpdateFun(stanchion, Config0, undefined),
    update_app_config(stanchion_etcpath(Prefix), Config).

update_app_config(Path, Config) ->
    lager:debug("rtcs:update_app_config(~s,~p)", [Path, Config]),
    FileFormatString = "~s/~s.config",
    AppConfigFile = io_lib:format(FileFormatString, [Path, "app"]),
    AdvConfigFile = io_lib:format(FileFormatString, [Path, "advanced"]),

    {BaseConfig, ConfigFile} = case file:consult(AppConfigFile) of
        {ok, [ValidConfig]} ->
            {ValidConfig, AppConfigFile};
        {error, enoent} ->
            {ok, [ValidConfig]} = file:consult(AdvConfigFile),
            {ValidConfig, AdvConfigFile}
    end,
    lager:debug("updating ~s", [ConfigFile]),

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

enable_zdbbl(Vsn) ->
    Fs = filelib:wildcard(filename:join([get_rt_config(riak, Vsn),
                                         "dev", "dev*", "etc", "vm.args"])),
    lager:info("rtcs:enable_zdbbl for vm.args : ~p~n", [Fs]),
    [os:cmd("sed -i -e 's/##+zdbbl /+zdbbl /g' " ++ F) || F <- Fs],
    ok.

create_user(Port, EmailAddr, Name) ->
    create_user(Port, undefined, EmailAddr, Name).

create_user(Port, UserConfig, EmailAddr, Name) ->
    lager:debug("Trying to create user ~p", [EmailAddr]),
    Resource = "/riak-cs/user",
    Date = httpd_util:rfc1123_date(),
    Cmd="curl -s -H 'Content-Type: application/json' " ++
        "-H 'Date: " ++ Date ++ "' " ++
        case UserConfig of
            undefined -> "";
            _ ->
                "-H 'Authorization: " ++
                    make_authorization("POST", Resource, "application/json",
                                       UserConfig, Date) ++
                    "' "
        end ++
        "http://localhost:" ++
        integer_to_list(Port) ++
        Resource ++
        " --data '{\"email\":\"" ++ EmailAddr ++  "\", \"name\":\"" ++ Name ++"\"}'",
    lager:debug("Cmd: ~p", [Cmd]),
    Delay = rt_config:get(rt_retry_delay),
    Retries = rt_config:get(rt_max_wait_time) div Delay,
    OutputFun = fun() -> rt:cmd(Cmd) end,
    Condition = fun({Status, Res}) ->
                        lager:debug("Return (~p), Res: ~p", [Status, Res]),
                        Status =:= 0 andalso Res /= []
                end,
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

assert_error_log_empty(N) ->
    assert_error_log_empty(current, N).

assert_error_log_empty(Vsn, N) ->
    ErrorLog = riakcs_logpath(get_rt_config(cs, Vsn), N, "error.log"),
    case file:read_file(ErrorLog) of
        {error, enoent} -> ok;
        {ok, <<>>} -> ok;
        {ok, Errors} ->
            lager:warning("Not empty error.log (~s): the first few lines are...~n~s",
                          [ErrorLog,
                           lists:map(
                             fun(L) -> io_lib:format("cs dev~p error.log: ~s\n", [N, L]) end,
                             lists:sublist(binary:split(Errors, <<"\n">>, [global]), 3))]),
            error(not_empty_error_log)
    end.

truncate_error_log(N) ->
    Cmd = os:find_executable("rm"),
    ErrorLog = riakcs_logpath(rt_config:get(?CS_CURRENT), N, "error.log"),
    ok = rtcs:cmd(Cmd, [{args, ["-f", ErrorLog]}]).

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
pbc(RiakNodes, ObjectKind, Opts) ->
    pbc(rt_config:get(flavor, basic), ObjectKind, RiakNodes, Opts).

pbc(basic, _ObjectKind, RiakNodes, _Opts) ->
    rt:pbc(hd(RiakNodes));
pbc({multibag, _} = Flavor, ObjectKind, RiakNodes, Opts) ->
    rtcs_bag:pbc(Flavor, ObjectKind, RiakNodes, Opts).

make_authorization(Method, Resource, ContentType, Config, Date) ->
    StringToSign = [Method, $\n, [], $\n, ContentType, $\n, Date, $\n, Resource],
    Signature =
        base64:encode_to_string(sha_mac(Config#aws_config.secret_access_key, StringToSign)),
    lists:flatten(["AWS ", Config#aws_config.access_key_id, $:, Signature]).

sha_mac(Key,STS) -> crypto:hmac(sha, Key,STS).
sha(Bin) -> crypto:hash(sha, Bin).
md5(Bin) -> crypto:hash(md5, Bin).

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

check_no_such_bucket(Response, Resource) ->
    check_error_response(Response,
                         404,
                         "NoSuchBucket",
                         "The specified bucket does not exist.",
                         Resource).

check_error_response({_, Status, _, RespStr} = _Response,
                     Status,
                     Code, Message, Resource) ->
    {RespXml, _} = xmerl_scan:string(RespStr),
    lists:all(error_child_element_verifier(Code, Message, Resource),
              RespXml#xmlElement.content).

error_child_element_verifier(Code, Message, Resource) ->
    fun(#xmlElement{name='Code', content=[Content]}) ->
            Content#xmlText.value =:= Code;
       (#xmlElement{name='Message', content=[Content]}) ->
            Content#xmlText.value =:= Message;
       (#xmlElement{name='Resource', content=[Content]}) ->
            Content#xmlText.value =:= Resource;
       (_) ->
            true
    end.

assert_versions(App, Nodes, Regexp) ->
    [begin
         {ok, Vsn} = rpc:call(N, application, get_key, [App, vsn]),
         lager:debug("~s's vsn at ~s: ~s", [App, N, Vsn]),
         {match, _} = re:run(Vsn, Regexp)
     end ||
        N <- Nodes].

%% @doc update current app.config, assuming CS is already stopped
upgrade_cs(N, AdminCreds) ->
    migrate_cs(previous, current, N, AdminCreds).

%% @doc update config file from `From' to `To' version.
migrate_cs(From, To, N, AdminCreds) ->
    migrate(From, To, N, AdminCreds, cs).

migrate(From, To, N, AdminCreds, Who) when
      (From =:= current andalso To =:= previous)
      orelse ( From =:= previous andalso To =:= current) ->
    Config0 = read_config(From, N, Who),
    Config1 = migrate_config(From, To, Config0, Who),
    Prefix = get_rt_config(Who, To),
    lager:debug("migrating ~s => ~s", [get_rt_config(Who, From), Prefix]),
    case Who of
        cs -> update_cs_config(Prefix, N, Config1, AdminCreds);
        stanchion -> update_stanchion_config(Prefix, Config1, AdminCreds)
    end.

migrate_stanchion(From, To, AdminCreds) ->
    migrate(From, To, -1, AdminCreds, stanchion).

migrate_config(previous, current, Conf, stanchion) ->
    {AddList, RemoveList} = diff_config(stanchion_config(),
                                        previous_stanchion_config()),
    migrate_config(Conf, AddList, RemoveList);
migrate_config(current, previous, Conf, stanchion) ->
    {AddList, RemoveList} = diff_config(previous_stanchion_config(),
                                        stanchion_config()),
    migrate_config(Conf, AddList, RemoveList);
migrate_config(previous, current, Conf, cs) ->
    {AddList, RemoveList} = diff_config(cs_config([{anonymous_user_creation, false}]),
                                        previous_cs_config()),
    migrate_config(Conf, AddList, RemoveList);
migrate_config(current, previous, Conf, cs) ->
    {AddList, RemoveList} = diff_config(previous_cs_config(), cs_config()),
    migrate_config(Conf, AddList, RemoveList).

migrate_config(Conf0, AddList, RemoveList) ->
    RemoveFun = fun(Key, Config) ->
                  InnerConf0 = proplists:get_value(Key, Config),
                  InnerRemoveList = proplists:get_value(Key, RemoveList),
                  InnerConf1 = lists:foldl(fun proplists:delete/2,
                                           InnerConf0,
                                           proplists:get_keys(InnerRemoveList)),
                  replace(Key, InnerConf1, Config)
          end,
    Conf1 = lists:foldl(RemoveFun, Conf0, proplists:get_keys(RemoveList)),

    AddFun = fun(Key, Config) ->
                  InnerConf = proplists:get_value(Key, Config)
                              ++ proplists:get_value(Key, AddList),
                  replace(Key, InnerConf, Config)
             end,
    lists:foldl(AddFun, Conf1, proplists:get_keys(AddList)).

diff_config(Conf, BaseConf)->
    Keys = lists:umerge(proplists:get_keys(Conf),
                        proplists:get_keys(BaseConf)),

    Fun = fun(Key, {AddList, RemoveList}) ->
                  {Add, Remove} = diff_props(proplists:get_value(Key,Conf),
                                             proplists:get_value(Key, BaseConf)),
                  case {Add, Remove} of
                      {[], []} ->
                          {AddList, RemoveList};
                      {{}, Remove} ->
                          {AddList, RemoveList++[{Key, Remove}]};
                      {Add, []} ->
                          {AddList++[{Key, Add}], RemoveList};
                      {Add, Remove} ->
                          {AddList++[{Key, Add}], RemoveList++[{Key, Remove}]}
                  end
          end,
    lists:foldl(Fun, {[], []}, Keys).

diff_props(undefined, BaseProps) ->
    {[], BaseProps};
diff_props(Props, undefined) ->
    {Props, []};
diff_props(Props, BaseProps) ->
    Keys = lists:umerge(proplists:get_keys(Props),
                        proplists:get_keys(BaseProps)),
    Fun = fun(Key, {Add, Remove}) ->
                  Values = {proplists:get_value(Key, Props),
                            proplists:get_value(Key, BaseProps)},
                  case Values of
                      {undefined, V2} ->
                          {Add, Remove++[{Key, V2}]};
                      {V1, undefined} ->
                          {Add++[{Key, V1}], Remove};
                      {V, V} ->
                          {Add, Remove};
                      {V1, V2} ->
                          {Add++[{Key, V1}], Remove++[{Key, V2}]}
                  end
          end,
    lists:foldl(Fun, {[], []}, Keys).

%% TODO: this is added as riak-1.4 branch of riak_test/src/rt_cs_dev.erl
%% throws out the return value. Let's get rid of these functions when
%% we entered to Riak CS 2.0 dev, updating to riak_test master branch
cmd(Cmd, Opts) ->
    cmd(Cmd, Opts, rt_config:get(rt_max_wait_time)).

cmd(Cmd, Opts, WaitTime) ->
    lager:info("Command: ~s", [Cmd]),
    lager:info("Options: ~p", [Opts]),
    Port = open_port({spawn_executable, Cmd},
                     [in, exit_status, binary,
                      stream, stderr_to_stdout,{line, 200} | Opts]),
    get_cmd_result(Port, WaitTime).

get_cmd_result(Port, WaitTime) ->
    receive
        {Port, {data, {Flag, Line}}} when Flag =:= eol orelse Flag =:= noeol ->
            lager:info(Line),
            get_cmd_result(Port, WaitTime);
        {Port, {exit_status, 0}} ->
            ok;
        {Port, {exit_status, Status}} ->
            {error, {exit_status, Status}};
        {Port, Other} ->
            lager:warning("Other data from port: ~p", [Other]),
            get_cmd_result(Port, WaitTime)
    after WaitTime ->
            {error, timeout}
    end.

%% Copy from rts:iso8601/1
iso8601(Timestamp) when is_integer(Timestamp) ->
    GregSec = Timestamp + 719528 * 86400,
    Datetime = calendar:gregorian_seconds_to_datetime(GregSec),
    {{Y,M,D},{H,I,S}} = Datetime,
    io_lib:format("~4..0b~2..0b~2..0bT~2..0b~2..0b~2..0bZ",
                  [Y, M, D, H, I, S]).

reset_log(Node) ->
    {ok, _Logs} = rpc:call(Node, gen_event, delete_handler,
                           [lager_event, riak_test_lager_backend, normal]),
    ok = rpc:call(Node, gen_event, add_handler,
                  [lager_event, riak_test_lager_backend,
                   [rt_config:get(lager_level, info), false]]).
