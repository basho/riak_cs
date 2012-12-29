1. Ensure that s3cmd is installed and that `~/.s3cfg` contains access
   credentials for the Basho account.
1. Ensure that riak_test builds are in place for:
    * Riak
    * Riak EE
    * Riak CS
    * Stanchion

    **NOTE:** Currently you must use `stagedevrel` builds for
      `riak_cs` so that the `riak` or `riak_ee` nodes have the correct
      path to find the `riak_cs_kv_multibackend` module.
1. Setup a `~/.riak_test.config` file like this:

    <pre>
    {rtdev, [
        {rt_deps, [
                   "/Users/dparfitt/test-releases/riak_ee/deps",
                   "/Users/dparfitt/test-releases/riak_cs/deps",
                   "/Users/dparfitt/test-releases/stanchion/deps",
                   "deps"
                  ]},
        {rt_retry_delay, 500},
        {rt_harness, rtdev},
        {rtdev_path, [{root, "/tmp/rt"},
                      {current, "/tmp/rt/current"},
                      {"1.2.0", "/tmp/rt/riak-1.2.0"},
                      {"1.1.4", "/tmp/rt/riak-1.1.4"},
                      {"1.0.3", "/tmp/rt/riak-1.0.3"},
                      {ee_root, "/tmp/rtee"},
                      {ee_current, "/tmp/rtee/current"},
                      {"1.2.0-ee", "/tmp/rtee/riak_ee-1.2.0"},
                      {"1.1.4-ee", "/tmp/rtee/riak_ee-1.1.4"},
                      {"1.0.3-ee", "/tmp/rtee/riak_ee-1.0.3"},
                      {cs_root, "/tmp/rtcs"},
                      {cs_current, "/tmp/rtcs/current"},
                      {"1.2.2-cs", "/tmp/rtcs/riak-cs-1.2.2"},
                      {"1.1.0-cs", "/tmp/rtcs/riak-cs-1.1.0"},
                      {"1.0.1-cs", "/tmp/rtcs/riak-cs-1.0.1"},
                      {stanchion_root, "/tmp/rtstanchion"},
                      {stanchion_current, "/tmp/rtstanchion/current"},
                      {"1.2.2-stanchion", "/tmp/rtstanchion/stanchion-1.2.2"},
                      {"1.1.0-stanchion", "/tmp/rtstanchion/stanchion-1.1.0"},
                      {"1.0.1-stanchion", "/tmp/rtstanchion/stanchion-1.0.1"}
                     ]}
    ]}.
    </pre>

     Notice the extra `riak_ee/deps`, `riak_cs/deps` and
     `stanchion/deps` in the `rt_deps` section and the extra path
     specifications in `rtdev_path`. The path keys shown here (*e.g.*
     `ee_root`) **MUST** be used verbatim since the test setup
     specifically looks for them. You have been warned.
1. To build the `riak_test` files use the `compile-riak-test` Makefile target or run `./rebar riak_test_compile`.
1. To execute a test, run the following from the `riak_test` repo:

    ```
    ./riak_test -c rtdev -d <PATH-TO-RIAK-CS-REPO>/riak_test/ebin/ -v
    ```
