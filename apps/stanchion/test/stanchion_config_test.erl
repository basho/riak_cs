-module(stanchion_config_test).
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

default_config_test() ->
    Config = cuttlefish_unit:generate_templated_config(schema_files(), [], context()),
    cuttlefish_unit:assert_config(Config, "stanchion.host", {"127.0.0.1", 8085}),
    cuttlefish_unit:assert_config(Config, "stanchion.riak_host", {"127.0.0.1", 8087}),
    cuttlefish_unit:assert_not_configured(Config, "stanchion.ssl"),
    cuttlefish_unit:assert_config(Config, "stanchion.admin_key", "admin-key"),
    cuttlefish_unit:assert_not_configured(Config, "stanchion.admin_secret"),
    cuttlefish_unit:assert_config(Config, "stanchion.auth_bypass", false),

    ok.

ssl_config_test() ->
    Conf = [{["ssl", "certfile"], "path/certfile"},
            {["ssl", "keyfile"],  "path/keyfile"}],
    Config = cuttlefish_unit:generate_templated_config(schema_files(), Conf, context()),
    cuttlefish_unit:assert_config(Config, "stanchion.ssl", [{keyfile,  "path/keyfile"},
                                                          {certfile, "path/certfile"}]),
    ok.

schema_files() ->
    ["priv/stanchion.schema"].

context() ->
    {ok, Context} = file:consult("rel/vars.config"),
    Context.
