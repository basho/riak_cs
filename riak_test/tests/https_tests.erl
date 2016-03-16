-module(https_tests).

-export([confirm/0]).

confirm() ->
    {ok, _} = application:ensure_all_started(ssl),
    CSConfig = [{ssl, [{certfile, "./etc/cert.pem"}, {keyfile, "./etc/key.pem"}]}],
    {UserConfig, _} = rtcs:setup(1, [{cs, [{riak_cs, CSConfig}]}]),
    ok = verify_cs1025(UserConfig),
    rtcs:pass().

verify_cs1025(UserConfig) ->
    B = <<"booper">>,
    K = <<"drooper">>,
    K2 = <<"super">>,
    lager:info("Creating a bucket ~s", [B]),
    rtcs_object:upload(UserConfig, {https, 0}, B, <<>>),
    lager:info("Creating a source object to copy, ~s", [K]),
    rtcs_object:upload(UserConfig, {https, 42}, B, K),
    lager:info("Trying copy from ~s to ~s", [K, K2]),
    rtcs_object:upload(UserConfig, https_copy, B, K2, K).
