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
    {default, [
           {rt_max_wait_time, 180000},
           {rt_retry_delay, 1000}
          ]}.

    {rtdev, [
         {rt_deps, [
                    "/Users/kelly/basho/riak_test_builds/riak/deps",
                    "deps"
                   ]},
         {rt_retry_delay, 500},
         {rt_harness, rtdev},
         {rtdev_path, [{root, "/Users/kelly/rt/riak"},
                       {current, "/Users/kelly/rt/riak/current"},
                       {ee_root, "/Users/kelly/rt/riak_ee"},
                       {ee_current, "/Users/kelly/rt/riak_ee/current"}
                      ]}
        ]}.

    {rt_cs_dev, [
         {rt_deps, [
                    "/Users/kelly/basho/riak_test_builds/riak/deps",
                    "/Users/kelly/basho/riak_test_builds/riak_cs/deps",
                    "/Users/kelly/basho/riak_test_builds/stanchion/deps",
                    "deps"
                   ]},
         {rt_retry_delay, 500},
         {rt_harness, rt_cs_dev},
         {build_paths, [{root, "/Users/kelly/rt/riak"},
                        {current, "/Users/kelly/rt/riak/current"},
                        {ee_root, "/Users/kelly/rt/riak_ee"},
                        {ee_current, "/Users/kelly/rt/riak_ee/current"},
                        {cs_root, "/Users/kelly/rt/riak_cs"},
                        {cs_current, "/Users/kelly/rt/riak_cs/current"},
                        {stanchion_root, "/Users/kelly/rt/stanchion"},
                        {stanchion_current, "/Users/kelly/rt/stanchion/current"}
                       ]},
         {src_paths, [{cs_src_root, "/Users/kelly/basho/riak_test_builds/riak_cs"}]}
        ]}.
    </pre>

Running the RiakCS tests for `riak_test` use a different test harness
(`rt_cs_dev`) than running the Riak tests and so requires a separate
configuration section.  Notice the extra `riak_ee/deps`,
`riak_cs/deps` and `stanchion/deps` in the `rt_deps`
section. `RT_DEST_DIR` should be replaced by the path used when
setting up `riak_test` builds for Riak (by default
`$HOME/rt/riak`). The same should be done for `RTEE_DEST_DIR` (default
`$HOME/rt/riak_ee`), `RTCS_DEST_DIR` (default `$HOME/rt/riak_cs`) and
`RTSTANCHION_DEST_DIR` (default `$HOME/rt/stanchion`).

1. To build the `riak_test` files use the `compile-riak-test` Makefile
target or run `./rebar riak_test_compile`.

1. The Riak client tests are now automated by the
`tests/external_client_tests.erl` test.  There are several
prerequisites:

* Your $PATH must have `erl` available.
* Your $PATH must have a version of Python available that also has
  access to the Boto S3 libraries.
* Your $PATH must have Clojure's "lein" available.  "lein" is the main
  executable for the Leinigen tool.

1. Before running the Riak client tests, your
`~/.riak_test.config` file must contain an entry for `cs_src_root` in
the `src_paths` list, as shown above.  The source in this directory
must be successfully compiled using the top level `make all` target.

1. Before running the Riak client tests, you must first use the
commands `make clean-client-test` and then `make compile-client-test`.

1. To execute a test, run the following from the `riak_test` repo:

    ```
    ./riak_test -c rt_cs_dev -d <PATH-TO-RIAK-CS-REPO>/riak_test/ebin/ -v
    ```
