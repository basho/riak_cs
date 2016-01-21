# General instruction

1. Make sure that your `riak_test` is the latest one for 2.0.

1. Ensure that riak_test builds are in place for:
    * Riak
    * Riak EE
    * Riak CS
    * Stanchion

Example to setup old and new CS:

```bash
$ mkdir ~/rt
$ cd ~/rt
$ cd path/to/repo/riak_cs
## Only for Enterprise build
$ export RIAK_CS_EE_DEPS=true
$ riak_test/bin/rtdev-build-releases.sh
$ riak_test/bin/rtdev-setup-releases.sh
## make sure runtime is Basho's patched R16B02-basho<patch-for-release-build>
$ make devrel && riak_test/bin/rtdev-current.sh
```

*NOTE* For building Enterprise version releases, use
`rtdev-build-releases.sh` script in `riak_cs_multibag` repository
(private).

Example to setup old and new Stanchion:

```bash
$ cd path/to/repo/stanchion
$ riak_test/bin/rtdev-build-releases.sh
$ riak_test/bin/rtdev-setup-releases.sh
## make sure runtime is Basho's patched R16B02-basho<patch-for-release-build>
$ make devrel && riak_test/bin/rtdev-current.sh
```

Example to setup 1.4.x and 2.0 as old and new Riak (maybe same as Riak
OSS... while shell scripts are included in riak_test repo):

```bash
$ mkdir ~/rt/riak_ee
## make sure runtime is Basho's patched R16B02-basho<patch-for-release-build>
$ tar xzf riak-ee-2.0.1.tar.gz
$ cd riak-ee-2.0.1 && make devrel
$ riak_test/bin/rteedev-setup-releases.sh
$ riak_test/bin/rteedev-current.sh

## change runtime to Basho's patched R15B01
$ tar xzf riak-ee-1.4.10.tar.gz
$ cd riak-ee-1.4.10 && make devrel
$ mkdir ~/rt/riak_ee/riak-ee-1.4.10
$ cp -r dev ~/rt/riak_ee/riak-ee-1.4.10
$ cd ~/rt/riak_ee
$ git add riak-ee-1.4.10
$ git commit -m "Add 1.4 series Riak EE"
```


1. Setup a `~/.riak_test.config` file like this:

```erlang
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

{rtcs_dev, [
  {rt_project, "riak_cs"},
     {rt_deps, [
                "/home/kuenishi/cs-2.0/riak_cs/deps"
               ]},
     {rt_retry_delay, 500},
     {rt_harness, rtcs_dev},
     {build_paths, [{root,              "/home/kuenishi/rt/riak_ee"},
                    {current,           "/home/kuenishi/rt/riak_ee/current"},
                    {ee_root,           "/home/kuenishi/rt/riak_ee"},
                    {ee_current,        "/home/kuenishi/rt/riak_ee/current"},
                    {ee_previous,       "/home/kuenishi/rt/riak_ee/riak-ee-1.4.10"},
                    {cs_root,           "/home/kuenishi/rt/riak_cs"},
                    {cs_current,        "/home/kuenishi/rt/riak_cs/current"},
                    {cs_previous,
                           "/home/kuenishi/rt/riak_cs/riak-cs-1.5.1"},
                    {stanchion_root,    "/home/kuenishi/rt/stanchion"},
                    {stanchion_current, "/home/kuenishi/rt/stanchion/current"},
                    {stanchion_previous,
                       "/home/kuenishi/rt/stanchion/stanchion-1.5.0"}
                   ]},
     {test_paths, ["/home/kuenishi/cs-2.0/riak_cs/riak_test/ebin"]},
     {src_paths, [{cs_src_root, "/home/kuenishi/cs-2.0/riak_cs"}]},
     {lager_level, debug},
     %%{build_type, oss},
     {build_type, ee},
     {flavor, basic},
     %% {sibling_benchmark,
     %%  [{write_concurrency, 8},
     %%   {write_interval, 0},      % msec
     %%   {version, current},
     %%   %% {version, previous},
     %%   %% {leave_and_join, 100}, % times
     %%   %% {duration_sec, 1}
     %%   {duration_sec, 30}
     %%  ]
     %% },
     {backend, {multi_backend, bitcask}}
]}.
```

Running the Riak CS tests for `riak_test` use a different test harness
(`rtcs_dev`) than running the Riak tests and so requires a separate
configuration section. Notice the extra `riak_cs/deps` in the
`rt_deps` section. `RT_DEST_DIR` should be replaced by the path used
when setting up `riak_test` builds for Riak (by default
`$HOME/rt/riak`). The same should be done for `RTEE_DEST_DIR` (default
`$HOME/rt/riak_ee`), `RTCS_DEST_DIR` (default `$HOME/rt/riak_cs`) and
`RTSTANCHION_DEST_DIR` (default `$HOME/rt/stanchion`).

The `build_type` option is used to differentiate between an
open-source (`oss`) build of RiakCS and the enterprise version (`ee`).
The default is `oss` and this option can be omitted when these tests
are used by open-source users.

The `backend` option is used to indicate which Riak backend option
should be used. The valid options are `{multi_backend, bitcask}` and
`memory`. `{multi_backend, bitcask}` is the default option and
represents the default recommended backed for production use of
Riak CS.

The `test_paths` option is a list of fully-qualified paths which
`riak_test` will use to find additional tests. Since the Riak CS tests
do not live inside the `riak_test` repository and escript, this should
point to the compiled tests in `riak_cs/riak_test/ebin`.

The `flavor` option is used to vary environment setup.  Some
`riak_test` modules use only S3 API and does not depend on details,
such as number of riak nodes, riak's backend, MDC or not, multibag
(aka supercluster) or not.  By adding flavor setting to riak_test
config, such generic test cases can be utilized to verify Riak CS's
behavior in various setups.  The scope of setup functions affected by
flavors are `rtcs:setup/1` and `rtcs:setup/2`.  Other setup functions,
for example `rtcs:setup2x2` used by `repl_v3_test`, does not change
their behavior.  The valid option values are `basic` (default) and
`{multibag, disjoint}`.  `{multibag, disjoint}` setup multibag
environment with 3 bags, the master bag with two riak nodes and two
additional bags with one riak node each.


1. To build the `riak_test` files use the `compile-riak-test` Makefile
   target or run `./rebar riak_test_compile`.

1. The Riak client tests are now automated by the
   `tests/external_client_tests.erl` test.  There are several
    prerequisites:

* Your $PATH must have `erl` available.
* Your $PATH must have a version of Python available that also has
  virtualenv tool and access to the Boto S3 libraries.
* Your $PATH must have Clojure's "lein" available.  "lein" is the main
  executable for the Leinigen tool.
* Your system must have libevent installed. If you see an error for a 
  missing 'event.h' file during test runs, this is because libevent is
  not installed.
* Your system must have Ruby > 2.0 and PHP > 5.5 and PHP composer insalled.
* Your system must have Golang (go, $GOHOME, $GOROOT) correctly installed.

1. Before running the Riak client tests, your
`~/.riak_test.config` file must contain an entry for `cs_src_root` in
the `src_paths` list, as shown above.  The source in this directory
must be successfully compiled using the top level `make all` target.

1. Before running the Riak client tests, you must first use the
commands `make clean-client-test` and then `make compile-client-test`.

1. To execute a test, run the following from the `riak_test` repo:

    ```shell
    ./riak_test -c rtcs_dev -t TEST_NAME
    ```
