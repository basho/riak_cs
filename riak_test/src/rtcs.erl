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
    setup(NumNodes, rtcs_config:default_configs(), current).

setup(NumNodes, Configs) ->
    setup(NumNodes, Configs, current).

setup(NumNodes, Configs, Vsn) ->
    Flavor = rt_config:get(flavor, basic),
    lager:info("Flavor : ~p", [Flavor]),
    flavored_setup(NumNodes, Flavor, Configs, Vsn).

setup2x2() ->
    setup2x2(rtcs_config:default_configs()).

setup2x2(Configs) ->
    JoinFun = fun(Nodes) ->
                      [A,B,C,D] = Nodes,
                      join(B,A),
                      join(D,C)
              end,
    setup_clusters(Configs, JoinFun, 4, current).

%% 1 cluster with N nodes + M cluster with 1 node
setupNxMsingles(N, M) ->
    setupNxMsingles(N, M, rtcs_config:default_configs(), current).

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

    Cfgs = rtcs_config:configs(Configs),
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
    AdminConfig = rtcs_config:config(AdminKeyId,
                                     AdminSecretKey,
                                     rtcs_config:cs_port(hd(RiakNodes))),
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

deploy_stanchion(Config) ->
    %% Set initial config
    ConfigFun = fun(_, Config0, _) -> Config0 end,
    rtcs_config:update_stanchion_config(rt_config:get(rtcs_config:stanchion_current()), Config, ConfigFun),

    rtcs_exec:start_stanchion(),
    lager:info("Stanchion started").

create_user(Node, UserIndex) ->
    {A, B, C} = erlang:now(),
    User = "Test User" ++ integer_to_list(UserIndex),
    Email = lists:flatten(io_lib:format("~p~p~p@basho.com", [A, B, C])),
    {KeyId, Secret, _Id} = create_user(rtcs_config:cs_port(Node), Email, User),
    lager:info("Created user ~p with keys ~p ~p", [Email, KeyId, Secret]),
    {KeyId, Secret}.

create_admin_user(Node) ->
    User = "admin",
    Email = "admin@me.com",
    {KeyId, Secret, Id} = create_user(rtcs_config:cs_port(Node), Email, User),
    lager:info("Riak CS Admin account created with ~p",[Email]),
    lager:info("KeyId = ~p",[KeyId]),
    lager:info("KeySecret = ~p",[Secret]),
    lager:info("Id = ~p",[Id]),
    {KeyId, Secret}.

-spec deploy_nodes(list(), list(), fun(), current|previous) -> any().
deploy_nodes(NumNodes, InitialConfig, ConfigFun, Vsn)
  when Vsn =:= current orelse Vsn =:= previous ->
    lager:info("Initial Config: ~p", [InitialConfig]),
    {RiakNodes, CSNodes, StanchionNode} = Nodes = {riak_nodes(NumNodes),
                                                   cs_nodes(NumNodes),
                                                   stanchion_node()},

    NodeMap = orddict:from_list(lists:zip(RiakNodes, lists:seq(1, NumNodes))),
    rt_config:set(rt_nodes, NodeMap),
    CSNodeMap = orddict:from_list(lists:zip(CSNodes, lists:seq(1, NumNodes))),
    rt_config:set(rt_cs_nodes, CSNodeMap),

    {_RiakRoot, RiakVsn} = rt_cs_dev:riak_root_and_vsn(Vsn, rt_config:get(build_type, oss)),
    lager:debug("setting rt_versions> ~p =>", [Vsn]),

    VersionMap = lists:zip(lists:seq(1, NumNodes), lists:duplicate(NumNodes, RiakVsn)),
    rt_config:set(rt_versions, VersionMap),

    rtcs_exec:stop_all_nodes(node_list(NumNodes), Vsn),

    rt_cs_dev:create_dirs(RiakNodes),

    %% Set initial config
    rtcs_config:set_configs(NumNodes,
                            InitialConfig,
                            ConfigFun,
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

setup_admin_user(NumNodes, InitialConfig, ConfigFun, Vsn)
  when Vsn =:= current orelse Vsn =:= previous ->

    {KeyID, KeySecret} = AdminCreds = create_admin_user(1),

    %% Create admin user and set in cs and stanchion configs
    rtcs_config:set_admin_creds_in_configs(node_list(NumNodes),
                                           lists:duplicate(NumNodes, InitialConfig),
                                           ConfigFun, AdminCreds, Vsn),

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
    ErrorLog = rtcs_config:riakcs_logpath(rtcs_config:get_rt_config(cs, Vsn), N, "error.log"),
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

make_authorization(Method, Resource, ContentType, Config, Date) ->
    make_authorization(Method, Resource, ContentType, Config, Date, []).

make_authorization(Method, Resource, ContentType, Config, Date, AmzHeaders) ->
    make_authorization(s3, Method, Resource, ContentType, Config, Date, AmzHeaders).

make_authorization(Type, Method, Resource, ContentType, Config, Date, AmzHeaders) ->
    Prefix = case Type of
                 s3 -> "AWS";
                 velvet -> "MOSS"
             end,
    StsAmzHeaderPart = [[K, $:, V, $\n] || {K, V} <- AmzHeaders],
    StringToSign = [Method, $\n, [], $\n, ContentType, $\n, Date, $\n,
                    StsAmzHeaderPart, Resource],
    lager:debug("StringToSign~n~s~n", [StringToSign]),
    Signature =
        base64:encode_to_string(sha_mac(Config#aws_config.secret_access_key, StringToSign)),
    lists:flatten([Prefix, " ", Config#aws_config.access_key_id, $:, Signature]).

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
