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

setup(NumNodes) ->
    setup(NumNodes, [], current).

setup(NumNodes, Configs) ->
    setup(NumNodes, Configs, current).

setup(NumNodes, Configs, Vsn) ->
    Flavor = rt_config:get(flavor, basic),
    lager:info("Flavor : ~p", [Flavor]),
    flavored_setup(NumNodes, Flavor, Configs, Vsn).

setup2x2() ->
    setup2x2([]).

setup2x2(Configs) ->
    JoinFun = fun(Nodes) ->
                      [A,B,C,D] = Nodes,
                      join(B,A),
                      join(D,C)
              end,
    setup_clusters(Configs, JoinFun, 4, current).

%% 1 cluster with N nodes + M cluster with 1 node
setupNxMsingles(N, M) ->
    setupNxMsingles(N, M, [], current).

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
    %% Start the erlcloud app
    erlcloud:start(),

    %% STFU sasl
    application:load(sasl),
    application:set_env(sasl, sasl_error_logger, false),

    {RiakNodes, _CSNodes, _Stanchion} = Nodes =
        deploy_nodes(NumNodes, rtcs_config:configs(Configs, Vsn), Vsn),
    rt:wait_until_nodes_ready(RiakNodes),
    lager:info("Make cluster"),
    JoinFun(RiakNodes),
    ?assertEqual(ok, wait_until_nodes_ready(RiakNodes)),
    ?assertEqual(ok, wait_until_no_pending_changes(RiakNodes)),
    rt:wait_until_ring_converged(RiakNodes),
    AdminConfig = setup_admin_user(NumNodes, Vsn),
    {AdminConfig, Nodes}.


pass() ->
    teardown(),
    pass.

teardown() ->
    %% catch application:stop(sasl),
    catch application:stop(erlcloud),
    catch application:stop(ibrowse).

%% Return Riak node IDs, one per cluster.
%% For example, in basic single cluster case, just return [1].
-spec riak_id_per_cluster(pos_integer()) -> [pos_integer()].
riak_id_per_cluster(NumNodes) ->
    case rt_config:get(flavor, basic) of
        basic -> [1];
        {multibag, _} = Flavor -> rtcs_bag:riak_id_per_cluster(NumNodes, Flavor)
    end.

-spec deploy_nodes(list(), list(), current|previous) -> any().
deploy_nodes(NumNodes, InitialConfig, Vsn)
  when Vsn =:= current orelse Vsn =:= previous ->
    lager:info("Initial Config: ~p", [InitialConfig]),
    {RiakNodes, CSNodes, StanchionNode} = Nodes = {riak_nodes(NumNodes),
                                                   cs_nodes(NumNodes),
                                                   stanchion_node()},

    NodeMap = orddict:from_list(lists:zip(RiakNodes, lists:seq(1, NumNodes))),
    rt_config:set(rt_nodes, NodeMap),
    CSNodeMap = orddict:from_list(lists:zip(CSNodes, lists:seq(1, NumNodes))),
    rt_config:set(rt_cs_nodes, CSNodeMap),

    {_RiakRoot, RiakVsn} = rtcs_dev:riak_root_and_vsn(Vsn, rt_config:get(build_type, oss)),
    lager:debug("setting rt_versions> ~p =>", [Vsn]),

    VersionMap = lists:zip(lists:seq(1, NumNodes), lists:duplicate(NumNodes, RiakVsn)),
    rt_config:set(rt_versions, VersionMap),

    rtcs_exec:stop_all_nodes(node_list(NumNodes), Vsn),

    rtcs_dev:create_dirs(RiakNodes),

    %% Set initial config
    rtcs_config:set_configs(NumNodes,
                            InitialConfig,
                            Vsn),
    rtcs_exec:start_all_nodes(node_list(NumNodes), Vsn),

    [ok = rt:wait_until_pingable(N) || N <- RiakNodes ++ CSNodes ++ [StanchionNode]],
    [ok = rt:check_singleton_node(N) || N <- RiakNodes],
    rt:wait_until_nodes_ready(RiakNodes),

    lager:info("NodeMap: ~p", [ NodeMap ]),
    lager:info("VersionMap: ~p", [VersionMap]),
    lager:info("Deployed nodes: ~p", [Nodes]),

    Nodes.

node_id(Node) ->
    NodeMap = rt_config:get(rt_cs_nodes),
    orddict:fetch(Node, NodeMap).

setup_admin_user(NumNodes, Vsn)
  when Vsn =:= current orelse Vsn =:= previous ->

    %% Create admin user and set in cs and stanchion configs
    AdminCreds = rtcs_admin:create_admin_user(1),
    #aws_config{access_key_id=KeyID,
                secret_access_key=KeySecret} = AdminCreds,

    AdminConf = [{admin_key, KeyID}]
        ++ case Vsn of
               current -> [];
               previous -> [{admin_secret, KeySecret}]
           end,
    rt:pmap(fun(N) ->
                    rtcs:set_advanced_conf({cs, Vsn, N}, [{riak_cs, AdminConf}])
            end, lists:seq(1, NumNodes)),
    rtcs:set_advanced_conf({stanchion, Vsn}, [{stanchion, AdminConf}]),

    UpdateFun = fun({Node, App}) ->
                        ok = rpc:call(Node, application, set_env,
                                      [App, admin_key, KeyID]),
                        ok = rpc:call(Node, application, set_env,
                                      [App, admin_secret, KeySecret])
                end,
    ZippedNodes = [{stanchion_node(), stanchion} |
                  [{CSNode, riak_cs} || CSNode <- cs_nodes(NumNodes) ]],
    lists:foreach(UpdateFun, ZippedNodes),

    lager:info("AdminCreds: ~p", [AdminCreds]),
    AdminCreds.

-spec set_conf(atom() | {atom(), atom()} | string(), [{string(), string()}]) -> ok.
set_conf(all, NameValuePairs) ->
    lager:info("rtcs:set_conf(all, ~p)", [NameValuePairs]),
    [ set_conf(DevPath, NameValuePairs) || DevPath <- rtcs_dev:devpaths()],
    ok;
set_conf(Name, NameValuePairs) when Name =:= riak orelse
                                    Name =:= cs orelse
                                    Name =:= stanchion ->
    set_conf({Name, current}, NameValuePairs),
    ok;
set_conf({Name, Vsn}, NameValuePairs) ->
    lager:info("rtcs:set_conf({~p, ~p}, ~p)", [Name, Vsn, NameValuePairs]),
    set_conf(rtcs_dev:devpath(Name, Vsn), NameValuePairs),
    ok;
set_conf({Name, Vsn, N}, NameValuePairs) ->
    lager:info("rtcs:set_conf({~p, ~p, ~p}, ~p)", [Name, Vsn, N, NameValuePairs]),
    rtdev:append_to_conf_file(rtcs_dev:get_conf(rtcs_dev:devpath(Name, Vsn), N), NameValuePairs),
    ok;
set_conf(Node, NameValuePairs) when is_atom(Node) ->
    rtdev:append_to_conf_file(rtcs_dev:get_conf(Node), NameValuePairs),
    ok;
set_conf(DevPath, NameValuePairs) ->
    lager:info("rtcs:set_conf(~p, ~p)", [DevPath, NameValuePairs]),
    [rtdev:append_to_conf_file(RiakConf, NameValuePairs) || RiakConf <- rtcs_dev:all_the_files(DevPath, "etc/*.conf")],
    ok.

-spec set_advanced_conf(atom() | {atom(), atom()} | string(), [{string(), string()}]) -> ok.
set_advanced_conf(all, NameValuePairs) ->
    lager:info("rtcs:set_advanced_conf(all, ~p)", [NameValuePairs]),
    [ set_advanced_conf(DevPath, NameValuePairs) || DevPath <- rtcs_dev:devpaths()],
    ok;
set_advanced_conf(Name, NameValuePairs) when Name =:= riak orelse
                                             Name =:= cs orelse
                                             Name =:= stanchion ->
    set_advanced_conf({Name, current}, NameValuePairs),
    ok;
set_advanced_conf({Name, Vsn}, NameValuePairs) ->
    lager:info("rtcs:set_advanced_conf({~p, ~p}, ~p)", [Name, Vsn, NameValuePairs]),
    set_advanced_conf(rtcs_dev:devpath(Name, Vsn), NameValuePairs),
    ok;
set_advanced_conf({Name, Vsn, N}, NameValuePairs) ->
    lager:info("rtcs:set_advanced_conf({~p, ~p, ~p}, ~p)", [Name, Vsn, N, NameValuePairs]),
    rtcs_dev:update_app_config_file(rtcs_dev:get_app_config(rtcs_dev:devpath(Name, Vsn), N), NameValuePairs),
    ok;
set_advanced_conf(Node, NameValuePairs) when is_atom(Node) ->
    rtcs_dev:update_app_config_file(rtcs_dev:get_app_config(Node), NameValuePairs),
    ok;
set_advanced_conf(DevPath, NameValuePairs) ->
    AdvancedConfs = case rtcs_dev:all_the_files(DevPath, "etc/a*.config") of
                        [] ->
                            %% no advanced conf? But we _need_ them, so make 'em
                            rtdev:make_advanced_confs(DevPath);
                        Confs ->
                            Confs
                    end,
    lager:info("AdvancedConfs = ~p~n", [AdvancedConfs]),
    [rtcs_dev:update_app_config_file(RiakConf, NameValuePairs) || RiakConf <- AdvancedConfs],
    ok.

assert_error_log_empty(N) ->
    assert_error_log_empty(current, N).

assert_error_log_empty(Vsn, N) ->
    ErrorLog = rtcs_config:riakcs_logpath(rtcs_config:devpath(cs, Vsn), N, "error.log"),
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
    ErrorLog = rtcs_config:riakcs_logpath(rt_config:get(rtcs_config:cs_current()), N, "error.log"),
    ok = rtcs_exec:cmd(Cmd, [{args, ["-f", ErrorLog]}]).

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

riak_node(N) ->
    ?DEV(N).

cs_node(N) ->
    ?CSDEV(N).

stanchion_node() ->
    'stanchion@127.0.0.1'.

maybe_load_intercepts(Node) ->
    case rt_intercept:are_intercepts_loaded(Node, [intercept_path()]) of
        false ->
            ok = rt_intercept:load_intercepts([Node], [intercept_path()]);
        true ->
            ok
    end.

%% private

riak_nodes(NumNodes) ->
    [?DEV(N) || N <- lists:seq(1, NumNodes)].

cs_nodes(NumNodes) ->
    [?CSDEV(N) || N <- lists:seq(1, NumNodes)].

node_list(NumNodes) ->
    NL0 = lists:zip(cs_nodes(NumNodes),
                    riak_nodes(NumNodes)),
    {CS1, R1} = hd(NL0),
    [{CS1, R1, stanchion_node()} | tl(NL0)].

intercept_path() ->
      filename:join([rtcs_dev:srcpath(cs_src_root),
                     "riak_test", "intercepts", "*.erl"]).
