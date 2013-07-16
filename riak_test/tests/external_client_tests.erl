-module(external_client_tests).

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, "riak_test_bucket").

confirm() ->
    {_UserConfig, {RiakNodes, _CSNodes, _Stanchion}} = rtcs:setup(4, [{cs, cs_config()}]),

    CS_http_port = rtcs:cs_port(hd(RiakNodes)),

    CS_src_dir = rt_cs_dev:srcpath(cs_src_root),
    lager:debug("cs_src_root = ~p", [CS_src_dir]),
    %% NOTE: This 'cs_src_root' path must appear in
    %% ~/.riak_test.config in the 'rt_cs_dev' section, 'src_paths'
    %% subsection.

    StdoutStderr = "/tmp/my_output." ++ os:getpid(),
    os:cmd("rm -f " ++ StdoutStderr),

    Cmd = lists:flatten(
            io_lib:format("cd ~s; env CS_HTTP_PORT=~p make test-client > ~s 2>&1 ; echo Res $?",
                          [CS_src_dir, CS_http_port, StdoutStderr])),
    lager:debug("Cmd = ~s", [Cmd]),
    Res = os:cmd(Cmd),
    lager:debug("Res = ~s", [Res]),
    {ok, DebugOut} = file:read_file(StdoutStderr),
    %% This io:format will have every line tagged as "debug" output.
    io:format("Verbose Res = ~s\n", [DebugOut]),

    try
        case Res of
            "Res 0\n" ->
                pass;
            _ ->
                lager:error("Expected 'Res 0', got: ~s", [Res]),
                error(fail)
        end
    after
        os:cmd("rm -f " ++ StdoutStderr)
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
       {enforce_multipart_part_size, false}
      ]
     }].
