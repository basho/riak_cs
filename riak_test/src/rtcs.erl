-module(rtcs).
-compile(export_all).
-include_lib("eunit/include/eunit.hrl").

-define(DEVS(N), lists:concat(["dev", N, "@127.0.0.1"])).
-define(DEV(N), list_to_atom(?DEVS(N))).
-define(CSDEVS(N), lists:concat(["rcs-dev", N, "@127.0.0.1"])).
-define(CSDEV(N), list_to_atom(?CSDEVS(N))).

riakcs_binpath(Prefix, N) ->
    io_lib:format("~s/cs/rtdev~b/sbin/riak-cs", [Prefix, N]).

riakcs_etcpath(Prefix, N) ->
    io_lib:format("~s/cs/rtdev~b/etc", [Prefix, N]).

riakcscmd(Path, N, Cmd) ->
    lists:flatten(io_lib:format("~s ~s", [riakcs_binpath(Path, N), Cmd])).

stanchion_binpath(Path, _N) ->
    io_lib:format("~s/stanchion/sbin/stanchion", [Path]).

stanchion_etcpath(Path) ->
    io_lib:format("~s/stanchion/etc", [Path]).

stanchioncmd(Path, N, Cmd) ->
    lists:flatten(io_lib:format("~s ~s", [stanchion_binpath(Path, N), Cmd])).

deploy_nodes(NumNodes, InitialConfig) ->
    NodeConfig = [{current, InitialConfig} || _ <- lists:seq(1,NumNodes)],
    RiakNodes = [?DEV(N) || N <- lists:seq(1, NumNodes)],
    CSNodes = [?CSDEV(N) || N <- lists:seq(1, NumNodes)],
    StanchionNode = 'stanchion@127.0.0.1',

    NodeMap = orddict:from_list(lists:zip(RiakNodes, lists:seq(1, NumNodes))),
    rt:set_config(rt_nodes, NodeMap),

    VersionMap = lists:zip(lists:seq(1, NumNodes), lists:duplicate(NumNodes, current)),
    rt:set_config(rt_versions, VersionMap),

    NL0 = lists:zip(CSNodes, RiakNodes),
    {CS1, R1} = hd(NL0),
    NodeList = [{CS1, R1, StanchionNode} | tl(NL0)],

    rt:pmap(fun({CSNode, RiakNode, Stanchion}) ->
                N = rtdev:node_id(RiakNode),
                stop_cs(N),
                stop_stanchion(Stanchion),
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
        end, NodeList),

    %% XXX this is a hack
    Path = os:getenv("RT_TARGET_CURRENT"),

    %% Reset nodes to base state
    lager:info("Resetting nodes to fresh state"),
    %% run_git(Path, "status"),
    rtdev:run_git(Path, "reset HEAD --hard"),
    rtdev:run_git(Path, "clean -fd"),

    rtdev:create_dirs(RiakNodes),

    {_Versions, Configs} = lists:unzip(NodeConfig),

    %% Set initial config
    rt:pmap(fun({_, default}) ->
                    ok;
               ({{_CSNode, RiakNode, _Stanchion}, Config}) ->
                    N = rtdev:node_id(RiakNode),
                    rtdev:update_app_config(RiakNode, proplists:get_value(riak,
                            Config)),
                    update_cs_config(rt:config(csroot), N,
                        proplists:get_value(cs, Config)),
                    update_stanchion_config(rt:config(csroot),
                        proplists:get_value(stanchion, Config));
                ({{_CSNode, RiakNode}, Config}) ->
                    N = rtdev:node_id(RiakNode),
                    rtdev:update_app_config(RiakNode, proplists:get_value(riak,
                            Config)),
                    update_cs_config(rt:config(csroot), N,
                        proplists:get_value(cs, Config))
            end,
            lists:zip(NodeList, Configs)),

    rt:pmap(fun({_CSNode, RiakNode, _Stanchion}) ->
                N = rtdev:node_id(RiakNode),
                rtdev:run_riak(N, rtdev:relpath(rtdev:node_version(N)), "start"),
                start_stanchion(N),
                start_cs(N);
            ({_CSNode, RiakNode}) ->
                N = rtdev:node_id(RiakNode),
                rtdev:run_riak(N, rtdev:relpath(rtdev:node_version(N)), "start"),
                start_cs(N)
        end, NodeList),

    Nodes = {RiakNodes, CSNodes, StanchionNode},

    [ok = rt:wait_until_pingable(N) || N <- RiakNodes ++ CSNodes ++
        [StanchionNode]],

    [ok = rt:check_singleton_node(N) || N <- RiakNodes],

    lager:info("Deployed nodes: ~p", [Nodes]),
    Nodes.

start_cs(N) ->
    Cmd = riakcscmd(rt:config(csroot), N, "start"),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

stop_cs(N) ->
    Cmd = riakcscmd(rt:config(csroot), N, "stop"),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

start_stanchion(N) ->
    Cmd = stanchioncmd(rt:config(csroot), N, "start"),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

stop_stanchion(N) ->
    Cmd = stanchioncmd(rt:config(csroot), N, "stop"),
    lager:info("Running ~p", [Cmd]),
    os:cmd(Cmd).

update_cs_config(Prefix, N, Config) ->
    update_app_config(riakcs_etcpath(Prefix, N) ++ "/app.config", Config).

update_stanchion_config(Prefix, Config) ->
    update_app_config(stanchion_etcpath(Prefix) ++ "/app.config", Config).

update_app_config(ConfigFile,  Config) ->
    lager:debug("~nReading config file at ~s~n", [ConfigFile]),
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
    lager:debug("CONFIG FILE=~s~n",[ConfigFile]),
    ?assertEqual(ok, file:write_file(ConfigFile, NewConfigOut)),
    ok.


deploy_cs(Config, N) ->
    %%NodeId = io_lib:format("rcs-dev~b@127.0.0.1",[N]),
    stop_cs(N),
    timer:sleep(10000),
    %% TODO: need a better method than sleep here

    update_cs_config(rt:config(csroot), N, Config),

    start_cs(N),
    %timer:sleep(10000),
    %% TODO:
    %%rt:wait_until_unpingable(NodeId),
    lager:info("Riak CS started").

%% this differes from rtdev:deploy_xxx in that it only starts one node
deploy_stanchion(Config, N) ->
    %%NodeId = "stanchion@127.0.0.1",
    stop_stanchion(N),
    timer:sleep(10000),
    %% TODO: need a better method than sleep here

    %% Set initial config
    update_stanchion_config(rt:config(csroot), Config),

    start_stanchion(N),
    %% TODO
    %%rt:wait_until_unpingable(NodeId),
    %timer:sleep(60000),
    lager:info("Stanchion started").


create_admin_user(Port, EmailAddr, Name) ->
    lager:debug("Trying to create user ~p", [EmailAddr]),
    Cmd="curl -s -H 'Content-Type: application/json' -X POST http://localhost:" ++
        integer_to_list(Port) ++
        "/user --data '{\"email\":\"" ++ EmailAddr ++  "\", \"name\":\"" ++ Name ++"\"}'",
    Output = os:cmd(Cmd),
    lager:debug("Create user output=~p~n",[Output]),
    {struct, JsonData} = mochijson2:decode(Output),
    KeyId = binary_to_list(proplists:get_value(<<"key_id">>, JsonData)),
    KeySecret = binary_to_list(proplists:get_value(<<"key_secret">>, JsonData)),
    Id = binary_to_list(proplists:get_value(<<"id">>, JsonData)),
    Email = binary_to_list(proplists:get_value(<<"email">>, JsonData)),
    lager:info("Riak CS Admin account created with ~p",[Email]),
    lager:info("KeyId = ~p",[KeyId]),
    lager:info("KeySecret = ~p",[KeySecret]),
    lager:info("Id = ~p",[Id]),
    {KeyId, KeySecret, Id}.



%% gen_s3cmd_config() ->
%%     "[default]~n" ++
%%         "access_key = $IDNOQUOTES~n" ++
%%         "bucket_location = US~n" ++
%%         "cloudfront_host = cloudfront.amazonaws.com~n" ++
%%         "cloudfront_resource = /2010-07-15/distribution~n" ++
%%         "default_mime_type = binary/octet-stream~n" ++
%%         "delete_removed = False~n" ++
%%         "dry_run = False~n" ++
%%         "enable_multipart = False~n" ++
%%         "encoding = UTF-8~n" ++
%%         "encrypt = False~n" ++
%%         "follow_symlinks = False~n" ++
%%         "force = False~n" ++
%%         "get_continue = False~n" ++
%%         "gpg_command = /usr/local/bin/gpg~n" ++
%%         "gpg_decrypt = %(gpg_command)s -d --verbose --no-use-agent --batch --yes --passphrase-fd %(passphrase_fd)s -o %(output_file)s\" %(input_file)s\"~n" ++
%%         "gpg_encrypt = %(gpg_command)s -c --verbose --no-use-agent --batch --yes --passphrase-fd %(passphrase_fd)s -o %(output_file)s\" %(input_file)s\"~n" ++
%%         "gpg_passphrase = password~n" ++
%%         "guess_mime_type = True~n" ++
%%         "host_base = s3.amazonaws.com~n" ++
%%         "host_bucket = %(bucket)s.s3.amazonaws.com~n" ++
%%         "human_readable_sizes = False~n" ++
%%         "list_md5 = False~n" ++
%%         "log_target_prefix =~n" ++
%%         "preserve_attrs = True~n" ++
%%         "progress_meter = True~n" ++
%%         "proxy_host = localhost~n" ++
%%         "proxy_port = 8080~n" ++
%%         "recursive = False~n" ++
%%         "recv_chunk = 4096~n" ++
%%         "reduced_redundancy = False~n" ++
%%         "secret_key = $KEYNOQUOTES~n" ++
%%         "send_chunk = 4096~n" ++
%%         "simpledb_host = sdb.amazonaws.com~n" ++
%%         "skip_existing = False~n" ++
%%         "socket_timeout = 300~n" ++
%%         "urlencoding_mode = normal~n" ++
%%         "use_https = False~n" ++
%%         "verbosity = WARNING~n".
