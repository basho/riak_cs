-module(wm_resource_test_utils).

-export([setup/0]).

setup() ->
    application:set_env(riak_moss, moss_ip, "127.0.0.1"),
    application:set_env(riak_moss, moss_port, 8080),
    %% Start erlang node
    application:start(sasl),
    TestNode = list_to_atom("testnode" ++ integer_to_list(element(3, now()))),
    net_kernel:start([TestNode, shortnames]),
    application:start(lager),
    application:start(riakc),
    application:start(inets),
    application:start(mochiweb),
    application:start(crypto),
    application:start(webmachine),
    application:start(riak_moss).
