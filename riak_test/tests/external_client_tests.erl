-module(external_client_tests).

-export([confirm/0]).
-include_lib("erlcloud/include/erlcloud_aws.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, "external-client-test").

confirm() ->
    %% NOTE: This 'cs_src_root' path must appear in
    %% ~/.riak_test.config in the 'rt_cs_dev' section, 'src_paths'
    %% subsection.
    CsSrcDir = rt_cs_dev:srcpath(cs_src_root),
    lager:debug("cs_src_root = ~p", [CsSrcDir]),

    {UserConfig, {RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup(2, [{cs, cs_config()}]),
    ok = erlcloud_s3:create_bucket("external-client-test", UserConfig),
    CsPortStr = integer_to_list(rtcs:cs_port(hd(RiakNodes))),

    Cmd = os:find_executable("make"),
    Args = ["test-client"],
    Env = [{"CS_HTTP_PORT",          CsPortStr},
           {"AWS_ACCESS_KEY_ID",     UserConfig#aws_config.access_key_id},
           {"AWS_SECRET_ACCESS_KEY", UserConfig#aws_config.secret_access_key},
           {"CS_BUCKET",             ?TEST_BUCKET}],
    case execute_cmd(Cmd, [{cd, CsSrcDir}, {env, Env}, {args, Args}]) of
        ok ->
            pass;
        {error, Reason} ->
            lager:error("Error : ~p", [Reason]),
            error({external_client_tests, Reason})
    end.

execute_cmd(Cmd, Opts) ->
    lager:info("Command: ~s", [Cmd]),
    lager:info("Options: ~p", [Opts]),
    Port = open_port({spawn_executable, Cmd},
                     [in, exit_status, binary,
                      stream, stderr_to_stdout,{line, 200} | Opts]),
    get_cmd_result(Port).

get_cmd_result(Port) ->
    WaitTime = rt_config:get(rt_max_wait_time),
    receive
        {Port, {data, {Flag, Line}}} when Flag =:= eol orelse Flag =:= noeol ->
            lager:info(Line),
            get_cmd_result(Port);
        {Port, {exit_status, 0}} ->
            ok;
        {Port, {exit_status, Status}} ->
            {error, {exit_status, Status}};
        {Port, Other} ->
            lager:warning("Other data from port: ~p", [Other]),
            get_cmd_result(Port)
    after WaitTime * 2 ->
            {error, timeout}
    end.

cs_config() ->
    [
     rtcs:lager_config(),
     {riak_cs,
      [
       {proxy_get, enabled},
       {anonymous_user_creation, true},
       {riak_pb_port, 10017},
       {stanchion_port, 9095},
       {cs_version, 010300},
       {enforce_multipart_part_size, false},
       {max_buckets_per_user, 150}
      ]
     }].
