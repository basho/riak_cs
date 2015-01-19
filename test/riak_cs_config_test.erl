-module(riak_cs_config_test).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

riak_cs_config_test() ->
    SchemaFiles = ["../rel/files/riak_cs.schema"],
    {ok, Context} = file:consult("../rel/vars.config"),
    Config = cuttlefish_unit:generate_templated_config(SchemaFiles, [], Context),
    cuttlefish_unit:assert_config(Config, "riak_cs.listener", {"127.0.0.1", 8080}),
    cuttlefish_unit:assert_config(Config, "riak_cs.riak_host", {"127.0.0.1", 8087}),
    cuttlefish_unit:assert_config(Config, "riak_cs.stanchion_host", {"127.0.0.1", 8085}),
    cuttlefish_unit:assert_config(Config, "riak_cs.stanchion_ssl", false),
    cuttlefish_unit:assert_config(Config, "riak_cs.anonymous_user_creation", false),
    cuttlefish_unit:assert_config(Config, "riak_cs.admin_key", "admin-key"),
    cuttlefish_unit:assert_config(Config, "riak_cs.admin_secret", "admin-secret"),
    cuttlefish_unit:assert_config(Config, "riak_cs.cs_root_host", "s3.amazonaws.com"),
    cuttlefish_unit:assert_config(Config, "riak_cs.cs_version", 10300),
    cuttlefish_unit:assert_config(Config, "riak_cs.rewrite_module", "riak_cs_s3_rewrite"),
    cuttlefish_unit:assert_config(Config, "riak_cs.auth_module", "riak_cs_s3_auth"),
    cuttlefish_unit:assert_config(Config, "riak_cs.fold_objects_for_list_keys", true),
    cuttlefish_unit:assert_config(Config, "riak_cs.trust_x_forwarded_for", false),
    cuttlefish_unit:assert_config(Config, "riak_cs.dtrace_support", false),
    cuttlefish_unit:assert_config(Config, "webmachine.server_name", "Riak CS"),
    ok.

