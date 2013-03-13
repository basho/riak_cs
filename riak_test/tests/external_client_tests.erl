-module(external_client_tests).

-export([confirm/0]).
-include_lib("eunit/include/eunit.hrl").

-define(TEST_BUCKET, "riak_test_bucket").

confirm() ->
    CS_extra = [{enforce_multipart_part_size, false}],
    {RiakNodes, _CSNodes, _Stanchion} =
        rtcs:setup(1,
                   rtcs:ee_config(),
                   rtcs:stanchion_config(),
                   rtcs:cs_config(CS_extra)),

    FirstNode = hd(RiakNodes),

    {AccessKeyId, SecretAccessKey} = rtcs:create_user(FirstNode, 1),

    %% User config
    CS_http_port = rtcs:cs_port(FirstNode),
    _UserConfig = rtcs:config(AccessKeyId, SecretAccessKey, CS_http_port),

    lager:debug("cs_src_root = ~p", [rtdev:relpath(cs_src_root)]),
    %% NOTE: This 'cs_src_root' path must appear in
    %% ~/.riak_test.config in the 'rtdev' section, 'rtdev_path'
    %% subsection.
    CS_src_dir = rtdev:relpath(cs_src_root),
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
