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
        {rtdev_path, [{root, "RT_DEST_DIR"},
                      {current, "RT_DEST_DIR/current"},
                      {previous, "RT_DEST_DIR/riak-1.2.0"},
                      {legacy, "RT_DEST_DIR/riak-1.1.4"},
                      {ee_root, "RTEE_DEST_DIR"},
                      {ee_current, "RTEE_DEST_DIR/current"},
                      {"1.2.0-ee", "RTEE_DEST_DIR/riak_ee-1.2.0"},
                      {"1.1.4-ee", "RTEE_DEST_DIR/riak_ee-1.1.4"},
                      {"1.0.3-ee", "RTEE_DEST_DIR/riak_ee-1.0.3"},

                      {cs_root, "RTCS_DEST_DIR"},
                      {cs_current, "RTCS_DEST_DIR/current"},
                      %% Add cs_src_root to get access to client tests
                      {cs_src_root, "YOUR/PATH/TO/RIAK-CS/SOURCE/TREE/riak_cs/"},

                      {"1.2.2-cs", "RTCS_DEST_DIR/riak-cs-1.2.2"},
                      {"1.1.0-cs", "RTCS_DEST_DIR/riak-cs-1.1.0"},
                      {"1.0.1-cs", "RTCS_DEST_DIR/riak-cs-1.0.1"},
                      {stanchion_root, "RTSTANCHION_DEST_DIR"},
                      {stanchion_current, "RTSTANCHION_DEST_DIR/current"},
                      {"1.2.2-stanchion", "RTSTANCHION_DEST_DIR/stanchion-1.2.2"},
                      {"1.1.0-stanchion", "RTSTANCHION_DEST_DIR/stanchion-1.1.0"},
                      {"1.0.1-stanchion", "RTSTANCHION_DEST_DIR/stanchion-1.0.1"}
                     ]}
    ]}.
    </pre>

Notice the extra `riak_ee/deps`, `riak_cs/deps` and `stanchion/deps` in
the `rt_deps` section and the extra path specifications in
`rtdev_path`. `RT_DEST_DIR` should be replaced by the path used when
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
the `rtdev_path` list, as shown above.  The source in this directory
must be successfully compiled using the top level `make all` target.

1. Before running the Riak client tests, you must first use the
commands `make clean-client-test` and then `make compile-client-test`.

1. To execute a test, run the following from the `riak_test` repo:

    ```
    ./riak_test -c rtdev -d <PATH-TO-RIAK-CS-REPO>/riak_test/ebin/ -v
    ```
