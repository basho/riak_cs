-module(riak_cs_config_test).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

default_config_test() ->
    SchemaFiles = ["../rel/files/riak_cs.schema"],
    {ok, Context} = file:consult("../rel/vars.config"),
    Config = cuttlefish_unit:generate_templated_config(SchemaFiles, [], Context),
    cuttlefish_unit:assert_config(Config, "riak_cs.listener", {"127.0.0.1", 8080}),
    cuttlefish_unit:assert_config(Config, "riak_cs.riak_host", {"127.0.0.1", 8087}),
    cuttlefish_unit:assert_config(Config, "riak_cs.stanchion_host", {"127.0.0.1", 8085}),
    cuttlefish_unit:assert_config(Config, "riak_cs.stanchion_ssl", false),
    cuttlefish_unit:assert_not_configured(Config, "riak_cs.ssl"),
    cuttlefish_unit:assert_config(Config, "riak_cs.anonymous_user_creation", false),
    cuttlefish_unit:assert_config(Config, "riak_cs.admin_key", "admin-key"),
    cuttlefish_unit:assert_config(Config, "riak_cs.admin_secret", "admin-secret"),
    cuttlefish_unit:assert_not_configured(Config, "riak_cs.admin_ip"),
    cuttlefish_unit:assert_not_configured(Config, "riak_cs.admin_port"),
    cuttlefish_unit:assert_config(Config, "riak_cs.cs_root_host", "s3.amazonaws.com"),
    cuttlefish_unit:assert_config(Config, "riak_cs.cs_version", 10300),
    cuttlefish_unit:assert_not_configured(Config, "riak_cs.rewrite_module"),
    cuttlefish_unit:assert_not_configured(Config, "riak_cs.auth_module"),
    cuttlefish_unit:assert_config(Config, "riak_cs.fold_objects_for_list_keys", true),
    cuttlefish_unit:assert_config(Config, "riak_cs.trust_x_forwarded_for", false),
    cuttlefish_unit:assert_config(Config, "riak_cs.leeway_seconds", 86400),
    cuttlefish_unit:assert_config(Config, "riak_cs.gc_interval", 900),
    cuttlefish_unit:assert_config(Config, "riak_cs.gc_retry_interval", 21600),
    cuttlefish_unit:assert_config(Config, "riak_cs.gc_paginated_indexes", true),
    cuttlefish_unit:assert_config(Config, "riak_cs.gc_max_workers", 2),
    cuttlefish_unit:assert_config(Config, "riak_cs.access_log_flush_factor", 1),
    cuttlefish_unit:assert_config(Config, "riak_cs.access_log_flush_size", 1000000),
    cuttlefish_unit:assert_config(Config, "riak_cs.access_archive_period", 3600),
    cuttlefish_unit:assert_config(Config, "riak_cs.access_archiver_max_backlog", 2),
    cuttlefish_unit:assert_config(Config, "riak_cs.access_archiver_max_workers", 2),
    cuttlefish_unit:assert_not_configured(Config, "riak_cs.storage_schedule"),
    cuttlefish_unit:assert_config(Config, "riak_cs.storage_archive_period", 86400),
    cuttlefish_unit:assert_config(Config, "riak_cs.usage_request_limit", 744),
    cuttlefish_unit:assert_config(Config, "riak_cs.dtrace_support", false),
    cuttlefish_unit:assert_config(Config, "riak_cs.connection_pools",
                                              [{request_pool, {128, 0}},
                                               {bucket_list_pool, {5, 0}}]),
    cuttlefish_unit:assert_config(Config, "webmachine.log_handlers",
                                              [{webmachine_access_log_handler, ["./log"]},
                                               {riak_cs_access_log_handler, []}]),
    cuttlefish_unit:assert_config(Config, "webmachine.server_name", "Riak CS"),
%%    cuttlefish_unit:assert_config(Config, "vm_args.+scl", false),
    ok.

modules_config_test() ->
    SchemaFiles = ["../rel/files/riak_cs.schema"],
    {ok, Context} = file:consult("../rel/vars.config"),
    Rewrite = riak_cs_oos_rewrite,
    Auth = riak_cs_keystone_auth,
    Conf = [{["rewrite_module"], Rewrite},
            {["auth_module"],  Auth}],
    Config = cuttlefish_unit:generate_templated_config(SchemaFiles, Conf, Context),
    cuttlefish_unit:assert_config(Config, "riak_cs.rewrite_module", Rewrite),
    cuttlefish_unit:assert_config(Config, "riak_cs.auth_module", Auth),
    ok.

ssl_config_test() ->
    SchemaFiles = ["../rel/files/riak_cs.schema"],
    {ok, Context} = file:consult("../rel/vars.config"),
    Conf = [{["ssl", "certfile"], "path/certfile"},
            {["ssl", "keyfile"],  "path/keyfile"}],
    Config = cuttlefish_unit:generate_templated_config(SchemaFiles, Conf, Context),
    cuttlefish_unit:assert_config(Config, "riak_cs.ssl", [{keyfile,  "path/keyfile"},
                                                          {certfile, "path/certfile"}]),
    ok.

admin_ip_config_test() ->
    SchemaFiles = ["../rel/files/riak_cs.schema"],
    {ok, Context} = file:consult("../rel/vars.config"),
    Conf = [{["admin", "listener"],   "0.0.0.0:9999"}],
    Config = cuttlefish_unit:generate_templated_config(SchemaFiles, Conf, Context),
    cuttlefish_unit:assert_config(Config, "riak_cs.admin_listener", {"0.0.0.0", 9999}),
    ok.

storage_schedule_config_test() ->
    SchemaFiles = ["../rel/files/riak_cs.schema"],
    {ok, Context} = file:consult("../rel/vars.config"),
    Conf = [{["stats", "storage", "schedule", "1"], "00:00"},
            {["stats", "storage", "schedule", "2"], "19:45"}],
    Config = cuttlefish_unit:generate_templated_config(SchemaFiles, Conf, Context),
    cuttlefish_unit:assert_config(Config, "riak_cs.storage_schedule", ["00:00", "19:45"]),
    ok.
