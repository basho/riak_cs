-module(riak_cs_config_test).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

default_config_test() ->
    Config = cuttlefish_unit:generate_templated_config(schema_files(), [], context()),
    cuttlefish_unit:assert_config(Config, "riak_cs.listener", {"127.0.0.1", 8080}),
    cuttlefish_unit:assert_config(Config, "riak_cs.riak_host", {"127.0.0.1", 8087}),
    cuttlefish_unit:assert_config(Config, "riak_cs.stanchion_host", {"127.0.0.1", 8085}),
    cuttlefish_unit:assert_config(Config, "riak_cs.stanchion_ssl", false),
    cuttlefish_unit:assert_not_configured(Config, "riak_cs.ssl"),
    cuttlefish_unit:assert_config(Config, "riak_cs.anonymous_user_creation", false),
    cuttlefish_unit:assert_config(Config, "riak_cs.admin_key", "admin-key"),
    cuttlefish_unit:assert_not_configured(Config, "riak_cs.admin_secret"),
    cuttlefish_unit:assert_not_configured(Config, "riak_cs.admin_ip"),
    cuttlefish_unit:assert_not_configured(Config, "riak_cs.admin_port"),
    cuttlefish_unit:assert_config(Config, "riak_cs.cs_root_host", "s3.amazonaws.com"),
    cuttlefish_unit:assert_config(Config, "riak_cs.cs_version", 10300),
    cuttlefish_unit:assert_config(Config, "riak_cs.proxy_get", false),
    cuttlefish_unit:assert_not_configured(Config, "riak_cs.rewrite_module"),
    cuttlefish_unit:assert_not_configured(Config, "riak_cs.auth_module"),
    cuttlefish_unit:assert_config(Config, "riak_cs.fold_objects_for_list_keys", true),
    cuttlefish_unit:assert_config(Config, "riak_cs.max_buckets_per_user", 100),
    cuttlefish_unit:assert_config(Config, "riak_cs.max_key_length", 1024),
    cuttlefish_unit:assert_config(Config, "riak_cs.trust_x_forwarded_for", false),
    cuttlefish_unit:assert_config(Config, "riak_cs.leeway_seconds", 86400),
    cuttlefish_unit:assert_config(Config, "riak_cs.max_scheduled_delete_manifests", 50),
    cuttlefish_unit:assert_config(Config, "riak_cs.gc_interval", 900),
    cuttlefish_unit:assert_config(Config, "riak_cs.gc_retry_interval", 21600),
    cuttlefish_unit:assert_config(Config, "riak_cs.gc_paginated_indexes", true),
    cuttlefish_unit:assert_config(Config, "riak_cs.gc_max_workers", 2),
    cuttlefish_unit:assert_config(Config, "riak_cs.gc_batch_size", 1000),
    cuttlefish_unit:assert_config(Config, "riak_cs.active_delete_threshold", 0),
    cuttlefish_unit:assert_config(Config, "riak_cs.fast_user_get", false),
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

    {ok, [ConsoleLog, ErrorLog]} = cuttlefish_unit:path(cuttlefish_variable:tokenize("lager.handlers"), Config),
    cuttlefish_unit:assert_config([ConsoleLog], "lager_file_backend", [{file, "./log/console.log"},
                                                                     {level, info},
                                                                     {size, 10485760},
                                                                     {date, "$D0"},
                                                                     {count, 5}]),
    cuttlefish_unit:assert_config([ErrorLog], "lager_file_backend", [{file, "./log/error.log"},
                                                                   {level, error},
                                                                   {size, 10485760},
                                                                   {date, "$D0"},
                                                                   {count, 5}]),
   cuttlefish_unit:assert_not_configured(Config, "riak_cs.supercluster_members"),
   cuttlefish_unit:assert_config(Config, "riak_cs.supercluster_weight_refresh_interval", 900),
%%    cuttlefish_unit:assert_config(Config, "vm_args.+scl", false),
    ok.

modules_config_test() ->
    Rewrite = riak_cs_oos_rewrite,
    Auth = riak_cs_keystone_auth,
    Conf = [{["rewrite_module"], Rewrite},
            {["auth_module"],  Auth}],
    Config = cuttlefish_unit:generate_templated_config(schema_files(), Conf, context()),
    cuttlefish_unit:assert_config(Config, "riak_cs.rewrite_module", Rewrite),
    cuttlefish_unit:assert_config(Config, "riak_cs.auth_module", Auth),
    ok.

ssl_config_test() ->
    Conf = [{["ssl", "certfile"], "path/certfile"},
            {["ssl", "keyfile"],  "path/keyfile"}],
    Config = cuttlefish_unit:generate_templated_config(schema_files(), Conf, context()),
    cuttlefish_unit:assert_config(Config, "riak_cs.ssl", [{keyfile,  "path/keyfile"},
                                                          {certfile, "path/certfile"}]),
    ok.

admin_ip_config_test() ->
    Conf = [{["admin", "listener"],   "0.0.0.0:9999"}],
    Config = cuttlefish_unit:generate_templated_config(schema_files(), Conf, context()),
    cuttlefish_unit:assert_config(Config, "riak_cs.admin_listener", {"0.0.0.0", 9999}),
    ok.

storage_schedule_config_test() ->
    Conf = [{["stats", "storage", "schedule", "1"], "0000"},
            {["stats", "storage", "schedule", "2"], "1945"}],
    Config = cuttlefish_unit:generate_templated_config(schema_files(), Conf, context()),
    cuttlefish_unit:assert_config(Config, "riak_cs.storage_schedule", ["0000", "1945"]),
    ok.

gc_interval_infinity_test() ->
    Conf = [{["gc", "interval"], infinity}],
    Config = cuttlefish_unit:generate_templated_config(schema_files(), Conf, context()),
    cuttlefish_unit:assert_config(Config, "riak_cs.gc_interval", infinity),
    ok.

max_scheduled_delete_manifests_unlimited_test() -> 
    Conf = [{["max_scheduled_delete_manifests"], unlimited}],
    Config = cuttlefish_unit:generate_templated_config(schema_files(), Conf, context()),
    cuttlefish_unit:assert_config(Config, "riak_cs.max_scheduled_delete_manifests", unlimited),
    ok.

active_delete_threshold_test() ->
    Conf = [{["active_delete_threshold"], "10mb"}],
    Config = cuttlefish_unit:generate_templated_config(schema_files(), Conf, context()),
    cuttlefish_unit:assert_config(Config, "riak_cs.active_delete_threshold", 10*1024*1024),
    ok.

lager_syslog_test() ->
    Conf = [{["log", "syslog"], on},
            {["log", "syslog", "ident"], "ident-test"},
            {["log", "syslog", "facility"], local7},
            {["log", "syslog", "level"], debug}
           ],
    Config = cuttlefish_unit:generate_templated_config(schema_files(), Conf, context()),
    cuttlefish_unit:assert_config(Config, "lager.handlers.lager_syslog_backend", ["ident-test", local7, debug]),
    ok.

lager_hander_test() ->
    Conf = [
            {["log", "console", "file"], "./log/consolefile.log"},
            {["log", "console", "level"], "debug"},
            {["log", "console", "size"], "1MB"},
            {["log", "console", "rotation"], "$D5"},
            {["log", "console", "rotation", "keep"], "10"},
            {["log", "error", "file"], "./log/errorfile.log"},
            {["log", "error", "size"], "1KB"},
            {["log", "error", "rotation"], "$D10"},
            {["log", "error", "rotation", "keep"], "20"}
           ],
    Config = cuttlefish_unit:generate_templated_config(schema_files(), Conf, context()),
    {ok, [ConsoleLog, ErrorLog]} = cuttlefish_unit:path(cuttlefish_variable:tokenize("lager.handlers"), Config),
    cuttlefish_unit:assert_config([ConsoleLog], "lager_file_backend", [{file, "./log/consolefile.log"},
                                                                       {level, debug},
                                                                       {size, 1048576},
                                                                       {date, "$D5"},
                                                                       {count, 10}]),
    cuttlefish_unit:assert_config([ErrorLog], "lager_file_backend", [{file, "./log/errorfile.log"},
                                                                     {level, error},
                                                                     {size, 1024},
                                                                     {date, "$D10"},
                                                                     {count, 20}]),

    CurrentConf1 = [{["log", "console", "rotation", "keep"], "current"}],
    Config1 = cuttlefish_unit:generate_templated_config(schema_files(), CurrentConf1, context()),
    {ok, [ConsoleLog1, _ErrorLog1]} = cuttlefish_unit:path(cuttlefish_variable:tokenize("lager.handlers"), Config1),
    cuttlefish_unit:assert_config([ConsoleLog1], "lager_file_backend.count", 0),

    CurrentConf2 = [{["log", "error", "rotation", "keep"], "current"}],
    Config2 = cuttlefish_unit:generate_templated_config(schema_files(), CurrentConf2, context()),
    {ok, [_ConsoleLog2, ErrorLog2]} = cuttlefish_unit:path(cuttlefish_variable:tokenize("lager.handlers"), Config2),
    cuttlefish_unit:assert_config([ErrorLog2], "lager_file_backend.count", 0),
    ok.

max_buckets_per_user_test() ->
    DefConf = [{["max_buckets_per_user"], "100"}],
    DefConfig = cuttlefish_unit:generate_templated_config(schema_files(), DefConf, context()),
    cuttlefish_unit:assert_config(DefConfig, "riak_cs.max_buckets_per_user", 100),

    UnlimitedConf = [{["max_buckets_per_user"], "unlimited"}],
    UnlimitedConfig = cuttlefish_unit:generate_templated_config(schema_files(), UnlimitedConf, context()),
    cuttlefish_unit:assert_config(UnlimitedConfig, "riak_cs.max_buckets_per_user", unlimited),
    ?assert(1000 < unlimited),

    NoConf = [],
    NoConfig = cuttlefish_unit:generate_templated_config(schema_files(), NoConf, context()),
    cuttlefish_unit:assert_config(NoConfig, "riak_cs.max_buckets_per_user", 100),
    ok.

proxy_get_test() ->
    DefConf = [{["proxy_get"], "on"}],
    DefConfig = cuttlefish_unit:generate_templated_config(schema_files(), DefConf, context()),
    cuttlefish_unit:assert_config(DefConfig, "riak_cs.proxy_get", true),
    ok.

wm_log_config_test_() ->
    {setup,
     fun() ->
             AssertAlog =
                 fun(Conf, Expected) ->
                         Config = cuttlefish_unit:generate_templated_config(
                                    schema_files(), Conf, context()),
                         case Expected of
                             no_alog ->
                                 cuttlefish_unit:assert_config(
                                   Config, "webmachine.log_handlers",
                                   [{riak_cs_access_log_handler,[]}]);
                             _ ->
                                 cuttlefish_unit:assert_config(
                                   Config, "webmachine.log_handlers",
                                   [{webmachine_access_log_handler, Expected},
                                    {riak_cs_access_log_handler,[]}])
                         end
                   end,
             AssertAlog
     end,
     fun(AssertAlog) ->
             [{"Default access log directory",
               ?_test(AssertAlog([{["log", "access", "dir"], "$(platform_log_dir)"}],
                                 ["./log"]))},
              {"Customized access log directory",
               ?_test(AssertAlog([{["log", "access", "dir"], "/path/to/custom/dir/"}],
                                 ["/path/to/custom/dir/"]))},
              {"No config, fall down to default",
               ?_test(AssertAlog([],
                                 ["./log"]))},
              {"Disable access log",
               ?_test(AssertAlog([{["log", "access", "dir"], "$(platform_log_dir)"},
                                  {["log", "access"], "off"}],
                                 no_alog))}
             ]
     end}.

supercluster_members_test_() ->
    [{"One bag",
      fun() ->
              Conf = [{["supercluster", "member", "bag-A"], "192.168.0.101:8087"}],
              Config = cuttlefish_unit:generate_templated_config(
                         schema_files(), Conf, context()),
              cuttlefish_unit:assert_config(Config, "riak_cs.supercluster_members",
                                            [{"bag-A", "192.168.0.101", 8087}])
      end},
     {"Two bags",
      fun() ->
              Conf = [{["supercluster", "member", "bag-A"], "192.168.0.101:18087"},
                      {["supercluster", "member", "bag-B"], "192.168.0.102:28087"}],
              Config = cuttlefish_unit:generate_templated_config(
                         schema_files(), Conf, context()),
              cuttlefish_unit:assert_config(Config, "riak_cs.supercluster_members",
                                            [{"bag-A", "192.168.0.101", 18087},
                                             {"bag-B", "192.168.0.102", 28087}])
      end},
     {"FQDN for host part",
      fun() ->
              Conf = [{["supercluster", "member", "bag-A"], "riak-A1.example.com:8087"}],
              Config = cuttlefish_unit:generate_templated_config(
                         schema_files(), Conf, context()),
              cuttlefish_unit:assert_config(Config, "riak_cs.supercluster_members",
                                            [{"bag-A", "riak-A1.example.com", 8087}])
      end}
    ].

supercluster_weight_refresh_interval_test_() ->
    [fun() ->
             Conf = [{["supercluster", "weight_refresh_interval"], "5s"}],
             Config = cuttlefish_unit:generate_templated_config(
                        schema_files(), Conf, context()),
             cuttlefish_unit:assert_config(
               Config, "riak_cs.supercluster_weight_refresh_interval", 5) end,
     fun() ->
             Conf = [{["supercluster", "weight_refresh_interval"], "1h"}],
             Config = cuttlefish_unit:generate_templated_config(
                        schema_files(), Conf, context()),
             cuttlefish_unit:assert_config(
               Config, "riak_cs.supercluster_weight_refresh_interval", 3600) end
    ].

schema_files() ->
    ["../rel/files/riak_cs.schema"].

context() ->
    {ok, Context} = file:consult("../rel/vars.config"),
    Context.
