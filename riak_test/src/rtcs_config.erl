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
-module(rtcs_config).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(RIAK_CURRENT, <<"build_paths.current">>).
-define(RIAK_PREVIOUS, <<"build_paths.previous">>).
-define(EE_CURRENT, <<"build_paths.ee_current">>).
-define(EE_PREVIOUS, <<"build_paths.ee_previous">>).
-define(CS_CURRENT, <<"build_paths.cs_current">>).
-define(CS_PREVIOUS, <<"build_paths.cs_previous">>).
-define(STANCHION_CURRENT, <<"build_paths.stanchion_current">>).
-define(STANCHION_PREVIOUS, <<"build_paths.stanchion_previous">>).

-define(S3_PORT, 80).

-define(REQUEST_POOL_SIZE, 8).
-define(BUCKET_LIST_POOL_SIZE, 2).

request_pool_size() ->
    ?REQUEST_POOL_SIZE.

bucket_list_pool_size() ->
    ?BUCKET_LIST_POOL_SIZE.

configs(CustomConfigs) ->
    configs(CustomConfigs, current).

configs(CustomConfigs, current) ->
    merge(default_configs(), CustomConfigs);
configs(CustomConfigs, previous) ->
    merge(previous_default_configs(), CustomConfigs).

previous_configs() ->
    previous_configs([]).

previous_configs(CustomConfigs) ->
    merge(previous_default_configs(), CustomConfigs).

default_configs() ->
    [{riak, riak_config()},
     {stanchion, stanchion_config()},
     {cs, cs_config()}].

previous_default_configs() ->
    [{riak, previous_riak_config()},
     {stanchion, previous_stanchion_config()},
     {cs, previous_cs_config()}].

pb_port(N) when is_integer(N) ->
    10000 + (N * 10) + 7;
pb_port(Node) ->
    pb_port(rtcs_dev:node_id(Node)).

cs_port(N) when is_integer(N) ->
    15008 + 10 * N;
cs_port(Node) ->
    cs_port(rtcs_dev:node_id(Node)).

stanchion_port() -> 9095.

riak_conf() ->
    [{"ring_size", "8"},
     {"buckets.default.allow_mult", "true"},
     {"buckets.default.merge_strategy", "2"}].

riak_config(CustomConfig) ->
    orddict:merge(fun(_, LHS, RHS) -> LHS ++ RHS end,
                  orddict:from_list(lists:sort(CustomConfig)),
                  orddict:from_list(lists:sort(riak_config()))).

riak_config() ->
    riak_config(
      current,
      ?CS_CURRENT,
      rt_config:get(build_type, oss),
      rt_config:get(backend, {multi_backend, bitcask})).

riak_config(Vsn, CsVsn, oss, Backend) ->
    riak_oss_config(Vsn, CsVsn, Backend);
riak_config(Vsn, CsVsn, ee, Backend) ->
    riak_ee_config(Vsn, CsVsn, Backend).

riak_oss_config(Vsn, CsVsn, Backend) ->
    CSPath = rt_config:get(CsVsn),
    AddPaths = filelib:wildcard(CSPath ++ "/dev/dev1/lib/riak_cs*/ebin"),
    [
     lager_config(),
     riak_core_config(Vsn),
     {riak_api,
      [{pb_backlog, 256}]},
     {riak_kv,
      [{add_paths, AddPaths}] ++
          backend_config(CsVsn, Backend)
      }
    ].

riak_core_config(current) ->
    {riak_core, []};
riak_core_config(previous) ->
    {riak_core,
     [{default_bucket_props, [{allow_mult, true}]},
      {ring_creation_size, 8}]
    }.

backend_config(_CsVsn, memory) ->
    [{storage_backend, riak_kv_memory_backend}];
backend_config(_CsVsn, {multi_backend, BlocksBackend}) ->
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
    ];
backend_config(?CS_CURRENT, prefix_multi) ->
    [
     {storage_backend, riak_kv_multi_prefix_backend},
     {riak_cs_version, 20000}
    ];
backend_config(OlderCsVsn, prefix_multi) ->
    backend_config(OlderCsVsn, {multi_backend, bitcask}).

blocks_backend_config(fs) ->
    {be_blocks, riak_kv_fs2_backend, [{data_root, "./data/fs2"},
                                      {block_size, 1050000}]};
blocks_backend_config(_) ->
    {be_blocks, riak_kv_bitcask_backend, [{data_root, "./data/bitcask"}]}.

riak_ee_config(Vsn, CsVsn, Backend) ->
    [repl_config() | riak_oss_config(Vsn, CsVsn, Backend)].

repl_config() ->
    {riak_repl,
     [
      {fullsync_on_connect, false},
      {fullsync_interval, disabled},
      {proxy_get, enabled}
     ]}.

previous_riak_config() ->
    riak_config(
      previous,
      ?CS_PREVIOUS,
      rt_config:get(build_type, oss),
      rt_config:get(backend, {multi_backend, bitcask})).

previous_riak_config(CustomConfig) ->
    orddict:merge(fun(_, LHS, RHS) -> LHS ++ RHS end,
                  orddict:from_list(lists:sort(CustomConfig)),
                  orddict:from_list(lists:sort(previous_riak_config()))).

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
           {stanchion_port, stanchion_port()},
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
           {stanchion_host, {"127.0.0.1", stanchion_port()}},
           {riak_host, {"127.0.0.1", 10017}},
           {cs_version, 010300}
          ]
     }] ++ OtherApps.

replace(Key, Value, Config0) ->
    Config1 = proplists:delete(Key, Config0),
    [proplists:property(Key, Value)|Config1].

previous_stanchion_config() ->
    [
     lager_config(),
     {stanchion,
      [
       {stanchion_port, stanchion_port()},
       {riak_pb_port, 10017}
      ]
     }].

stanchion_config() ->
    [
     lager_config(),
     {stanchion,
      [
       {host, {"127.0.0.1", stanchion_port()}},
       {riak_host, {"127.0.0.1", 10017}}
      ]
     }].

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

riakcs_statuscmd(Path, N) ->
    lists:flatten(io_lib:format("~s-admin status", [riakcs_binpath(Path, N)])).

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

stanchion_statuscmd(Path) ->
    lists:flatten(io_lib:format("~s-admin status", [stanchion_binpath(Path)])).

cs_current() ->
    ?CS_CURRENT.

stanchion_current() ->
    ?STANCHION_CURRENT.

devpath(riak, current) ->
    case rt_config:get(build_type, oss) of
        oss -> rt_config:get(?RIAK_CURRENT);
        ee  -> rt_config:get(?EE_CURRENT)
    end;
devpath(riak, previous) ->
    case rt_config:get(build_type, oss) of
        oss -> rt_config:get(?RIAK_PREVIOUS);
        ee  -> rt_config:get(?EE_PREVIOUS)
    end;
devpath(cs, current) -> rt_config:get(?CS_CURRENT);
devpath(cs, previous) -> rt_config:get(?CS_PREVIOUS);
devpath(stanchion, current) -> rt_config:get(?STANCHION_CURRENT);
devpath(stanchion, previous) -> rt_config:get(?STANCHION_PREVIOUS).

set_configs(NumNodes, Config, Vsn) ->
    rtcs:set_conf({riak, Vsn}, riak_conf()),
    rt:pmap(fun(N) ->
                    rtcs_dev:update_app_config(rtcs:riak_node(N),
                                                proplists:get_value(riak, Config)),
                    update_cs_config(devpath(cs, Vsn), N,
                                     proplists:get_value(cs, Config))
            end,
            lists:seq(1, NumNodes)),
    update_stanchion_config(devpath(stanchion, Vsn),
                            proplists:get_value(stanchion, Config)),
    enable_zdbbl(Vsn).

read_config(Vsn, N, Who) ->
    Prefix = devpath(Who, Vsn),
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

update_cs_config(Prefix, N, Config, {AdminKey, _AdminSecret}) ->
    CSSection = proplists:get_value(riak_cs, Config),
    UpdConfig = [{riak_cs, update_admin_creds(CSSection, AdminKey)} |
                 proplists:delete(riak_cs, Config)],
    update_cs_config(Prefix, N, UpdConfig).

update_cs_config(Prefix, N, Config) ->
    CSSection = proplists:get_value(riak_cs, Config),
    UpdConfig = [{riak_cs, update_cs_port(CSSection, N)} |
                 proplists:delete(riak_cs, Config)],
    update_app_config(riakcs_etcpath(Prefix, N), UpdConfig).

update_admin_creds(Config, AdminKey) ->
    [{admin_key, AdminKey}|
     proplists:delete(admin_key, Config)].

update_cs_port(Config, N) ->
    Config2 = [{riak_host, {"127.0.0.1", pb_port(N)}} | proplists:delete(riak_host, Config)],
    [{listener, {"127.0.0.1", cs_port(N)}} | proplists:delete(listener, Config2)].

update_stanchion_config(Prefix, Config, {AdminKey, _AdminSecret}) ->
    StanchionSection = proplists:get_value(stanchion, Config),
    UpdConfig = [{stanchion, update_admin_creds(StanchionSection, AdminKey)} |
                 proplists:delete(stanchion, Config)],
    update_stanchion_config(Prefix, UpdConfig).

update_stanchion_config(Prefix, Config) ->
    update_app_config(stanchion_etcpath(Prefix), Config).

update_app_config(Path, Config) ->
    lager:debug("rtcs:update_app_config(~s,~p)", [Path, Config]),
    FileFormatString = "~s/~s.config",
    AppConfigFile = io_lib:format(FileFormatString, [Path, "app"]),
    AdvConfigFile = io_lib:format(FileFormatString, [Path, "advanced"]),
    %% If there's an app.config, do it old style
    %% if not, use cuttlefish's adavnced.config
    case filelib:is_file(AppConfigFile) of
        true ->
            rtcs_dev:update_app_config_file(AppConfigFile, Config);
        _ ->
            rtcs_dev:update_app_config_file(AdvConfigFile, Config)
    end.

enable_zdbbl(Vsn) ->
    Fs = filelib:wildcard(filename:join([devpath(riak, Vsn),
                                         "dev", "dev*", "etc", "vm.args"])),
    lager:info("rtcs:enable_zdbbl for vm.args : ~p~n", [Fs]),
    [os:cmd("sed -i -e 's/##+zdbbl /+zdbbl /g' " ++ F) || F <- Fs],
    ok.

merge(BaseConfig, undefined) ->
    BaseConfig;
merge(BaseConfig, Config) ->
    lager:debug("Merging Config: BaseConfig=~p", [BaseConfig]),
    lager:debug("Merging Config: Config=~p", [Config]),
    MergeA = orddict:from_list(Config),
    MergeB = orddict:from_list(BaseConfig),
    MergedConfig = orddict:merge(fun internal_merge/3, MergeA, MergeB),
    lager:debug("Merged config: ~p", [MergedConfig]),
    MergedConfig.

internal_merge(_Key, [{_, _}|_] = VarsA, [{_, _}|_] = VarsB) ->
    MergeC = orddict:from_list(VarsA),
    MergeD = orddict:from_list(VarsB),
    orddict:merge(fun internal_merge/3, MergeC, MergeD);
internal_merge(_Key, VarsA, _VarsB) ->
    VarsA.

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
    Prefix = devpath(Who, To),
    lager:debug("migrating ~s => ~s", [devpath(Who, From), Prefix]),
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

