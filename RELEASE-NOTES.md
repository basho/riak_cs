#Riak CS 2.1.2 Release Notes

Released March 31, 2016.

This is a backwards-compatible* release that updates node_package to address a recent [Product Advisory](http://docs.basho.com/riak/latest/community/product-advisories/codeinjectioninitfiles/), as well as fixes several bugs.

Riak CS 2.1 is designed to work with Riak KV 2.1.1+.

>*This release is backwards compatible only with the Riak CS 2.x series.

###Upgrading

**For anyone updating to 2.1.2 from 2.1.1 and older versions.**

During the update to 2.1.2, a '==' omitted upload ID might be passed to a Riak CS node running an older version of CS. This may lead to process-crash by failing on decoding upload ID.

##Changes

* Due to a recent [Product Advisory](http://docs.basho.com/riak/latest/community/product-advisories/codeinjectioninitfiles/), node_package was bumped to version 3.0.0 to prevent a potential code injection on the riak init file. [[Issue 1297](https://github.com/basho/riak_cs/issues/1297), [PR 1306](https://github.com/basho/riak_cs/pull/1306), & [PR 109](https://github.com/basho/stanchion/pull/109)]
* Multipart upload IDs no longer contain trailing '=' characters, which caused trouble for some clients. This change also makes upload IDs URL-safe. This commit removes several unused functions. [[PR 1293](https://github.com/basho/riak_cs/pull/1293)]
* When Riak is unavailable due to network partition or node being offline, a 500 error is returned. [[PR 1298](https://github.com/basho/riak_cs/pull/1298)]

## Bugs Fixed

* [[Issue 1100](https://github.com/basho/riak_cs/issues/1100)/[PR 1304](https://github.com/basho/riak_cs/pull/1304)] When a multipart completion request was empty, Riak CS would return a "Completed" message. Now, during a multipart upload, the "complete" message contains a list of part IDs and MD5 hashes. If it is empty, it returns a "400 Bad Request, Malformed" XML message like S3.
* [[Issue 1288](https://github.com/basho/riak_cs/issues/1288)/[PR 1302](https://github.com/basho/riak_cs/pull/1302)] The `root_host` configuration parameter is now used to populate the `Location` response element.
* [[Issue 972](https://github.com/basho/riak_cs/issues/972)/[PR 1296](https://github.com/basho/riak_cs/pull/1296)] 'adderss' now reads 'address' in Stanchion console output.
* [[Issue 1025](https://github.com/basho/riak_cs/issues/1025)/[PR 1300](https://github.com/basho/riak_cs/pull/1300)] Copying an object used to fail when connecting via HTTPS.


#Riak S2 (Riak CS) 2.1.1 Release Notes

## General Information
This is a bugfix release.

## Bug Fixes

* Remove blocks and manifest in cases of client-side failure, eg:
  incomplete upload, network issues, or other unexpected transfer failures.
  If the manifest was writing and no overwrite happened after upload canceled,
  then the partially uploaded blocks and manifest are left as is, occupying
  disk space. This is fixed by adding an error handling routine to move the
  manifest to the garbage collection bucket by catching the socket error.
  Partially uploaded blocks and manifests will eventually be deleted by
  garbage collection. ([#770](https://github.com/basho/riak_cs/issues/770) /
  [PR#1280](https://github.com/basho/riak_cs/pull/1280))
* Remove `admin.secret` from riak-cs.conf and stanchion.conf. Both
  Riak CS and Stanchion searched riak-cs.conf and stanchion.conf to
  find who was the administrator using `admin.key` and `admin.secret`.
  Writing down `admin.secret` in non-encrypted form compromises the
  security of the system. The administrator is able to (1) list all
  users, (2) disable/enable other users, (3) changing user accounts,
  and (4) read access and storage statistics of all users. A workaround
  for this is encrypting the partition that includes the /etc/riak-cs
  directory. ([#1274](https://github.com/basho/riak_cs/issues/1274) /
  [PR#1279](https://github.com/basho/riak_cs/issues/1279) /
  [PR#108](https://github.com/basho/stanchion/pull/108))
* Use /etc/riak-cs/app.config when no generated config file is found.
  This prevents an error when using app.config/vm.args instead of
  riak.conf, such as when upgrading from 1.5.x.
  ([#1261](https://github.com/basho/riak_cs/issues/1261)/
  [PR#1263](https://github.com/basho/riak_cs/pull/1263)
  as [PR#1266](https://github.com/basho/riak_cs/pull/1266))
* Allow any bytes (including non-UTF8 ones) in List Objects response
  XML. List Objects API had been failing in cases where an object with
  non-UTF8 characters in its name was included in the result. With this
  change, such keys are represented in the result of List Objects as
  raw binaries, although it may not be proper XML 1.0.
  ([#974](https://github.com/basho/riak_cs/issues/974) /
  [PR#1255](https://github.com/basho/riak_cs/pull/1255) /
  [PR#1275](https://github.com/basho/riak_cs/pull/1275))

## Notes on Upgrade

* We strongly recommend removing `admin.secret` from
  riak-cs.conf and stanchion.conf, and `admin_secret` from the
  `advanced.config` files of Riak CS and Stanchion.
* On startup, Riak CS now verifies that the value of `admin.key`
  is either a valid Riak CS user key or the placeholder value
  `admin-key`. For an user with a non-existent or invlaid user
  key, Riak CS process won't start. Also, any `admin.secret` will
  be ignored. The initial procedure to create the very first
  account, starting `anonymous_user_creation = true`, does not change.

#Riak S2 (Riak CS) 2.1.0 Release Notes

Released October 13, 2015.

This is a backwards-compatible* release that introduces a new metrics system, garbage collection refinements, and several other new features. Riak S2 2.1 is designed to work with both Riak KV 2.0.5+ and 2.1.1+.

**Note:** This release is backwards compatible only with the Riak S2 2.x series.

###Riak KV 2.1.1 Usage Note
Riak KV 2.1.1 includes a copy of `riak_cs_kv_multi_backend`, therefore there is no need to add lines specifying special `multi_backend` and `add_paths` configurations in advanced.config.

Instead, you can set the following in riak.conf:

```
storage_backend = prefix_multi
cs_version = 20100
```

If you need storage calculation, you will still require the `add_paths` config to load MapReduce codes into Riak KV.


##New Features
###Metrics
New metrics have been added that enable you to determine the health of your Riak S2 system, as well as get reports on your storage utilization per bucket or user. The following stats items are available:
  * All calls, latencies, and counters in the S3 API
  * All calls, latencies, and counters in Stanchion
  * All Riak Erlang client operations, latencies, and counters
  * Information about the counts (active, idle, and overflow) for the process pool and connection pool
  * System information, versions, port count, and process count
  * Memory information about the riak-cs virtual machine
  * HTTP listener information: active sockets and waiting acceptors

**Note:** stats item names from prior to 2.0.x are not preserved; they have been renamed or removed. No backward consistency is maintained. Please see [the documentation](docs.basho.com/riakcs/latest/cookbooks/Monitoring-and-Metrics/) for more information.

* [[PR 1189](https://github.com/basho/riak_cs/pull/1189)]
* [[PR 1180](https://github.com/basho/riak_cs/pull/1180)]
* [[PR 1214](https://github.com/basho/riak_cs/pull/1214)]
* [[PR 1194](https://github.com/basho/riak_cs/pull/1194)]
* [[PR 99](https://github.com/basho/stanchion/pull/99)]

Additional storage usage metrics are also available. . These metrics are gathered during storage calculation. Gathering these metrics is off by default, but you can turn it on by setting `detailed_storage_calc` to `true` in advanced.config. When you enable this option, you have access to information about how many manifests are `writing`, `pending_delete`, `scheduled_delete` and `active` which is not visible via the API.

**Note:** Metrics do not always correctly reflect actual disk usage. For instance, `writing` may indicate more space than is actually used. Or, for example, if an upload was cancelled in the middle, the calculation does not know how much actual storage space is consumed. In the same way, `scheduled_delete` also may not reflect the exact amount of disk usage because blocks might already be partially deleted by garbage collection.

* [[PR 1120](https://github.com/basho/riak_cs/pull/1120)]

###`riak-cs-admin`
The following administration CLIs have been replaced by the [`riak-cs-admin` command](http://docs.basho.com/riakcs/latest/cookbooks/command-line-tools/):

* `riak-cs-storage`
* `riak-cs-gc`
* `riak-cs-access`
* `riak-cs-stanchion`

The commands listed above are deprecated and will be removed in future releases.

* [[PR 1175](https://github.com/basho/riak_cs/pull/1175)]

###Garbage Collection Refinements
Several new options have been added to the `riak-cs-admin gc` command:

* `active_delete_threshold` is an option to avoid delegating manifests and block deletion to garbage collector. This option relieves garbage collector from having to delete small objects. This can optimise performance in cases where both garbage collector does not catch up with DELETE Object API calls and garbage collector's elapsed time is dominated by small objects.[[PR 1174](https://github.com/basho/riak_cs/pull/1174)]
* `--start` and `--end` options have been added to the `riak-cs-admin gc batch` command to specify start and end in manual batch execution. Note that the `--start` flag on the command line will overwrite the `epoch_start` option in advanced.config. [[PR 1147 ](https://github.com/basho/riak_cs/pull/1147)]
* `--leeway` has been added to create a temporary leeway period whose values are used only once and not repeated at the next run, and `--max-workers` has been added to allow you to override the concurrency value temporarily for a single run of garbage collector. [[PR 1147 ](https://github.com/basho/riak_cs/pull/1147)]
* Riak S2 2.0 (and older) has a race condition where fullsync replication and garbage collection may resurrect deleted blocks without any way to delete them again. When real-time replication and replication of a garbage collection bucket entry object being dropped from the real-time queue are combined, blocks may remain on the sink side without being collected. Riak S2 2.1 introduces deterministic garbage collection to avoid fullsync replication. Additionally, garbage collection and fullsync replication run concurrently, and work on the same blocks and manifests. You can now specify the range of time using the `--start` and `--end` flags with `riak-cs-admin gc batch` for garbage collector in order to collect deleted objects synchronously on both sink and source sides. [[PR 1147 ](https://github.com/basho/riak_cs/pull/1147)]
* `riak-cs-admin gc earliest-keys` is available so you can find the oldest entry after `epoch_start` in garbage collection. With this option, you can stay informed of garbage collection progress. [[PR 1160](https://github.com/basho/riak_cs/pull/1160)]

More information on garbage collection can be found in the [documentation](http://docs.basho.com/riakcs/latest/cookbooks/garbage-collection/).


##Additions
###Open Source
* A MapReduce optimisation in fetching Riak objects was introduced in Riak 2.1. Now, Riak CS 2.1 introduces an option to use that optimisation in storage calculation. It is off by default, but it can be used by setting `use_2i_for_storage_calc` as `true` in advanced.config. This reduced 50% of I/O in LevelDB. [[PR 1089](https://github.com/basho/riak_cs/pull/1089)]
* Erlang/OTP 17 support is now included. [[PR 1245](https://github.com/basho/riak_cs/pull/1245) and [PR 1040](https://github.com/basho/stanchion/pull/1040)]
* A module-level hook point for limiting user access and quota usage is now available with very preliminary, simple, node-wide limiting example modules. Operators can make, plug in, or combine different modules as quota-limiting, rate-limiting or bandwidth-limiting depending on their unique requirements. [[PR 1118](https://github.com/basho/riak_cs/pull/1118)]
*  An orphaned block scanner is now available. [[PR 1133](https://github.com/basho/riak_cs/pull/1133)]
* `riak-cs-admin audit-bucket-ownership` is a new tool to check integrity between users and buckets added. For example, it can be used in cases where a bucket is visible when listing buckets but not accessible, or a bucket is visible and exists but could not be deleted. [[PR 1202](https://github.com/basho/riak_cs/pull/1202)]
* The following log rotation items have been added to cuttlefish:
    * log.console.size
    * log.console.rotation
    * log.console.rotation.keep
    * log.error.rotation
    * log.error.rotation.keep
    * log.error.size

[[PR 1164](https://github.com/basho/riak_cs/pull/1164) and [PR 97](https://github.com/basho/stanchion/pull/97)]

* `riak_cs_wm_common` now has a default callback of `multiple_choices`, which prevents `code_server` from becoming a bottleneck. [[PR 1181](https://github.com/basho/riak_cs/pull/1181)]
* An option has been added to replace the `PR=all user GET` option with `PR=one` just before authentication. This option improves latency, especially in the presence of slow (or actually-failing) nodes blocking the whole request flow because of PR=all. When enabled, a user's owned-bucket list is never pruned after a bucket is deleted, instead it is just marked as deleted. [[PR 1191](https://github.com/basho/riak_cs/pull/1191)]
* An info log has been added when starting a storage calculation batch. [[PR 1238](https://github.com/basho/riak_cs/pull/1238)]
* `GET Bucket` requests now have clearer responses. A 501 stub for Bucket lifecycle and a  simple stub for Bucket requestPayment have been added. [[PR 1223](https://github.com/basho/riak_cs/pull/1223)]
* Several user-friendly features have been added to [`riak-cs-debug`](http://docs.basho.com/riakcs/latest/cookbooks/command-line-tools/): fine-grained information gathering options, user-defined filtering for configuration files, and verbose output for failed commands. [[PR 1236](https://github.com/basho/riak_cs/pull/1236)]

###Enterprise
* MDC has `proxy_get`, which make block objects propagate to site clusters when they are requested. Now, multibag configuration with MDC supports `proxy_get`. [[PR 1171](https://github.com/basho/riak_cs/pull/1171) and [PR 25](https://github.com/basho/riak_cs_multibag/pull/25)]
* Multibag is now renamed to "Supercluster". A bag has been a set of replicated underlying Riak clusters, which is now **a member of a supercluster**. `riak-cs-multibag` command has been renamed as `riak-cs-supercluster` as well. [[PR 1257](https://github.com/basho/riak_cs/pull/1257)], [[PR 1260](https://github.com/basho/riak_cs/pull/1260)], [[PR 106](https://github.com/basho/stanchion/pull/106)], [[PR 107](https://github.com/basho/stanchion/pull/107)] and [[PR 31](https://github.com/basho/riak_cs_multibag/pull/31)].
* Several internal operation tools have been added to help diagnose or address
  issues. [[PR 1145](https://github.com/basho/riak_cs/pull/1145),
  [PR 1134](https://github.com/basho/riak_cs/pull/1134), and
  [PR 1133](https://github.com/basho/riak_cs/pull/1133)]
* Added a generic function for manual operations to resolve siblings of manifests and blocks, which will assist Basho Client Service Engineers with troubleshooting and solving issues. [[PR 1188](https://github.com/basho/riak_cs/pull/1188)]


##Changes
* Dependency versions have been updated in Riak S2 and Stanchion as follows: cuttlefish 2.0.4, node_package 2.0.3, riak-erlang-client 2.1.1, lager 2.2.0, lager_syslog 2.1.1, eper 0.92 (Basho patched), cluster_info 2.0.3, riak_repl_pb_api 2.1.1, and riak_cs_multibag 2.1.0. [[PR 1190](https://github.com/basho/riak_cs/pull/1190), [PR 1197 ](https://github.com/basho/riak_cs/pull/1197), [PR 27](https://github.com/basho/riak_cs_multibag/pull/27), [PR 1245](https://github.com/basho/riak_cs/pull/1245), and [PR 104](https://github.com/basho/stanchion/pull/104)].
* Riak CS has moved from Folsom to Exometer. [[PR 1165](https://github.com/basho/riak_cs/pull/1165) and [PR 1180](https://github.com/basho/riak_cs/pull/1180)]
* Improvements have been made to error tracing for retrieving blocks from client GET requests. There is a complex logic to resolve blocks when a GET is requested from the client. First, Riak CS tries to retrieve a block with `n_val=1`. If it fails, a retry will be done using `n_val=3`. If the block cannot be resolved locally, `proxy_get` is enabled, and the system is configured with datacenter replication, then Riak CS will try to perform a proxied GET to the remote site. The fallback and retry logic is complex and hard to trace, especially in a faulty or unstable situation. This improvement adds error tracing for the whole sequence described above, which will help diagnose issues. Specifically, for each block, the block server stacks all errors returned from the Riak client and reports the reason for every error as well as the type of call in which the error occurred. [[PR 1177](https://github.com/basho/riak_cs/pull/1177)]
* Using the `GET Bucket` API with a specified prefix to list objects in a bucket needed optimization. It had been specifying end keys for folding objects in Riak too loosely. With this change, a tighter end key is specified for folding objects in Riak, which omits unnecessary fold in vnodes. [[PR 1233](https://github.com/basho/riak_cs/pull/1233)]
* A limitation to the max length of keys has been introduced. This limitation can be specified as 1024 by default, meaning no keys longer than 1024 bytes can be PUT, GET or DELETED unless `max_key_length` is explicitly specified as more than '1024' in riak-cs.conf. If you want to preserve the old key length behaviour, you may specify the `max_key_length` as 'unlimited'. [[PR 1233](https://github.com/basho/riak_cs/pull/1233)]
* If a faulty cluster had several nodes down, the block server misunderstood that a block was already deleted and issued a false-notfound. This could lead to block leak. The PR default has been set to 'quorum' in an attempt to avoid this problem. Updates have also been made to make sure at least a single replica of a block is written in one of the primary nodes by setting the PW default to '1'. Additionally, measures are in place to prevent the block server from crashing  when "not found" errors are returned due to a particular block of an object not being found in the cluster. Instead, unreachable blocks are skipped and the remaining blocks and manifests are collected. Since the PR and PW values are increased at blocks, the availability of PUTs and through-put of garbage collection may decrease. A few Riak nodes being unreachable may prevent PUT requests from returning successfully and may prevent garbage collection from collecting all blocks until the unreachable nodes come back. [[PR 1242](https://github.com/basho/riak_cs/pull/1242)]
* The infinity timeout option has been set so that several functions make synchronous `gen_fsm` calls  indefinitely, which prevents unnecessary timeouts. [[PR 1249](https://github.com/basho/riak_cs/pull/1249)]


##Bugs Fixed
* [[Issue 1097](https://github.com/basho/riak_cs/issues/1097)/[PR 1212](https://github.com/basho/riak_cs/pull/1212)] When `x-amz-metadata-directive=COPY` was specified, Riak CS did not actually COPY the metadata of original resource. Instead, it would treat it as a `REPLACE`.  When directed to `x-amz-metadata-directive=REPLACE` `Content-Type`, Riack CS would `REPLACE` it. Correct handling for the `x-amz-metadata-directive` has been added to PUT Object Copy API.
* [[Issue 1099](https://github.com/basho/riak_cs/issues/1099)/[PR 1096](https://github.com/basho/riak_cs/pull/1096)] There was an unnecessary NextMarker in Get Bucket's response if `CommonPrefixes` contained the last key. Fixed handling of uploaded parts that should be deleted after Multipart Complete Request.
* [[Issue 939](https://github.com/basho/riak_cs/issues/939)/[PR 1200](https://github.com/basho/riak_cs/pull/1200)] Copy requests without Content-Length request headers failed with 5xx errors. Such requests are now allowed without Content-Length header in Copy API calls. Additionally, Copy API calls with Content-Lengths more than zero have been given explicit errors.
* [[Issue 1143](https://github.com/basho/riak_cs/issues/1143)/[PR 1144](https://github.com/basho/riak_cs/pull/1144)] Manual batch start caused the last batch time to appear to be in the future. All temporal shifts have been fixed.
* [[Issue PR 1162](https://github.com/basho/riak_cs/pull/1162)/[PR 1163](https://github.com/basho/riak_cs/pull/1163)] Fix a configuration system bug where Riak CS could not start if `log.syslog=on` was set.
* [[Issue 1169](https://github.com/basho/riak_cs/issues/1169)/[PR 1200](https://github.com/basho/riak_cs/pull/1200)] The error response of the PUT Copy API call showed the target resource path rather than the source path when the source was not found or not accessible by the request user. It now shows the source path appropriately.
* [[PR 1178](https://github.com/basho/riak_cs/pull/1178)] Multiple IP address descriptions under a single condition statement of a bucket policy were not being properly parsed as lists.
* [[PR 1185](https://github.com/basho/riak_cs/pull/1185)] If `proxy_get_active` was defined in riak-cs.conf as anything other than enabled or disabled, there would be excessive log output. Now, `proxy_get_active` also accepts non-boolean definitions.
* [[PR 1184](https://github.com/basho/riak_cs/pull/1184)] `put_gckey_timeout` was used instead of `put_manifest_timeout` when a delete process tried to update the status of manifests.
* [[Issue 1201](https://github.com/basho/riak_cs/issues/1201)/[PR 1230](https://github.com/basho/riak_cs/pull/1230)] A single slow or silently failing node caused intermittent user fetch failure. A grace period has been added so `riakc_pb_socket` can attempt to reconnect.
* [[PR 1232](https://github.com/basho/riak_cs/pull/1232)] Warning logs were being produced for unsatisfied primary reads. Since users are objects in Riak CS and CS tries to retrieve these objects for authentication for almost every request, the retrieval option (PR=all) would fail if even one primary vnode was stopped or unresponsive and a log would be created. Given that Riak is set up to be highly available, these logs were quite noisy. Now, the "No WM route" log from prior to Riak CS 2.1 has been revived. Also, the log severity has been downgraded to debug, since it indicates a client error in all but the development phase.
* [[PR 1237](https://github.com/basho/riak_cs/pull/1237)]  The `riak-cs-admin` status command exit code was non-zero, even in successful execution. It will now return zero.
* [[Issue 1097](https://github.com/basho/riak_cs/issues/1097)/[PR 1212](https://github.com/basho/riak_cs/pull/1212) and [PR 4](https://github.com/basho/s3-tests/pull/4)] Riak S2 did not copy the metadata of an original resource when the `x-amz-metadata-directive=COPY` command was used, nor when `x-amz-metadata-directive` was specified. Handling of the `x-amz-metadata-directive` command in PUT Object Copy API has been added.
* [[Issue 1097](https://github.com/basho/riak_cs/issues/1097)/[PR 1212](https://github.com/basho/riak_cs/pull/1212) and [PR 4](https://github.com/basho/s3-tests/pull/4)] Riak CS did not store `Content-Type` in COPY requests when the `x-amz-metadata-directive=REPLACE` command was used. Handling of the `x-amz-metadata-directive` command in PUT Object Copy API has been added.
*  [[Issue 1097](https://github.com/basho/riak_cs/issues/1097)/[PR 1212](https://github.com/basho/riak_cs/pull/1212) and [PR 4](https://github.com/basho/s3-tests/pull/4)] Fixed the handling of uploaded parts that should be deleted after Multipart Complete Request.
* [[Issue 1214](https://github.com/basho/riak_cs/issues/1244)/[PR 1246](https://github.com/basho/riak_cs/pull/1246)] Prior to Riak S2 2.1.0, a PUT Copy API command with identical source and destination changed user metadata (`x-amz-meta-*` headers) but failed to update Content-Type. Content-Type is now correctly updated by the API call.
* [[Issue PR 1261](https://github.com/basho/riak_cs/pull/1261), [[PR 1263](https://github.com/basho/riak_cs/pull/1263)] Fix `riak-cs-debug` to include `app.config` when no generated files are found when `riak-cs.conf` is not used.



# Riak CS 2.0.1 Release Notes

## General Information
This is a bugfix release.

## Bug Fixes

* Fix config item `gc.interval` not working when `infinity` is set ([#1125](https://github.com/basho/riak_cs/issues/1125)/[PR#1126](https://github.com/basho/riak_cs/pull/1126)).
* Add `log.access` switch to disable access logging ([#1109](https://github.com/basho/riak_cs/issues/1109)/[PR#1115](https://github.com/basho/riak_cs/pull/1115)).
* Add missing riak-cs.conf items:` max_buckets_per_user` and `gc.batch_size` ([#1109](https://github.com/basho/riak_cs/issues/1109)/[PR#1115](https://github.com/basho/riak_cs/pull/1115)).
* Fix bugs around subsequent space characters for Delete Multiple Objects API and user administration API with XML content ([#1129](https://github.com/basho/riak_cs/issues/1129)/[PR#1135](https://github.com/basho/riak_cs/pull/1135)).
* Fix URL path resource and query parameters to work in AWS v4 header authentication. Previously, `+` was being input instead of `%20` for blank spaces. ([PR#1141](https://github.com/basho/riak_cs/pull/1141))

# Riak CS 2.0.0 Release Notes

## General Information

- This release updates Riak CS to work with Riak 2.0.5.
- We have simplified the configuration system.
- All official patches for older versions of Riak and Riak CS have been included
  in these releases. There is no need to apply any patches released for Riak CS
  1.4.x or 1.5.x to the Riak CS 2.0.x series. Patches released for Riak CS 1.4.x
  or 1.5.x cannot be directly applied to Riak CS 2.0.x because the version of
  Erlang/OTP shipped with Riak CS has been updated in version 2.0.0.
- Please review the complete Release Notes before upgrading.

## Known Issues & Limitations

- Access log can't be disabled.
- Advanced.config should be used to customize value of `max_buckets_per_user`
  and `gc_batch_size`.

## Changes and Additions

- Changed the name of `gc_max_workers` to `gc.max_workers`, and lowered the
  default value from 5 to 2 (#1110) to reduce the workload on the cs cluster.
- Partial support of GET Location API (#1057)
- Add very preliminary AWS v4 header authentication - without query
  string authentication, object chunking and payload checksum (#1064).
  There is still a lot of work to reliably use v4 authentication.
- Put Enterprise deps into dependency graph (#1065)
- Introduce Cuttlefish (#1020, #1068, #1076, #1086, #1090)
  (Stanchion #88, #90, #91)
- Yessir Riak client to measure performance (#1072, #1083)
- Inspector improvement with usage change (#1084)
- Check signed date in S3 authentication (#1067)
- Update `cluster_info` and various dependent libraries (#1087, #1088)
  (Stanchion #85, #87, #93)
- Storage calculation optimization (#1089) With Riak >= 2.1 this works
  with `use_2i_for_storage_calc` flag might relieve disk read of
  storage calculation.

## Bugfixes

- Fix wrong webmachine log handler name (#1075)
- Fix lager crash (#1038)
- Fix hardcoded crashdump path (#1052)
- Suppress unnecessary warnings (#1053)
- Multibag simpler state transition (Multibag #21)
- GC block deletion failure after transition to multibag environment
  (Multibag #19)
- Connection closing caused errors for objects stored before the
  transition, after transition from single bag to multibag
  configuration (Multibag #18).

## Deprecation Notices

- Multi-Datacenter Replication using v2 replication support has been deprecated.
- Old list objects which required `fold_objects_for_list_keys` as `false` have
  been deprecated and *will be removed* in the next major version.
- Non-paginated GC in cases where `gc_paginated_indexes` is `false` has been
  deprecated and *will be removed* in the next major version.


## General Notes on Upgrading to Riak CS 2.0.0

Upgrading a Riak CS system involves upgrading the underlying Riak, Riak CS and
Stanchion installations. The upgrade process can be non-trivial depending on
your existing system configurations and the combination of sub-system versions.
This document contains general instructions and notices on upgrading the whole
system to Riak CS 2.0.0.

#### New Configuration System

Riak 2.0.0 introduced a new configuration system (`riak.conf`), and as of Riak
CS 2.0.0, Riak CS now supports the new configuration style. Both Riak and Riak
CS still support the older style configurations through `app.config` and
`vm.args`.

**Basho recommends moving to the new unified configuration system**, using the
files `riak.conf`, `riak-cs.conf` and `stanchion.conf`.

##### Note on Legacy app.config Usage

**If you choose to use the legacy `app.config` files for Riak CS and/or
Stanchion, some parameters have changed names and must be updated**.

In particular, for the Riak CS `app.config`:
- `cs_ip` and `cs_port` have been combined into `listener`.
- `riak_ip` and `riak_pb_port` have been combined into `riak_host`.
- `stanchion_ip` and `stanchion_port` have been combined into `stanchion_host`.
- `admin_ip` and `admin_port` have been combined into `admin_listener`.
- `webmachine_log_handler` has become `webmachine_access_log_handler`.

For the Stanchion `app.config`:
- `stanchion_ip` and `stanchion_port` have been combined into `listener`.
- `riak_ip` and `riak_port` have been combined into `riak_host`.

Each of the above pairs follows a similar form. Where the old form used a
separate IP and Port parameter, the new form combines those as `{new_option, {
"IP", Port}}`. For example, if your legacy `app.config` configuration was
previously:

```
{riak_cs, [
    {cs_ip, "127.0.0.1"},
    {cs_port, 8080 },
    . . .
]},
```

It should now read:

```
{riak_cs, [
    {listener, {"127.0.0.1", 8080}},
    . . .
]},
```

and so on.

#### Note: Upgrading from Riak CS 1.5.3 or Older

[Some key objects changed names][riak_cs_1.5_release_notes_upgrading] after the
upgrade. Applications may need to change their behaviour due to this bugfix.

#### Note: Upgrading from Riak CS 1.5.0 or Older

[Bucket number limitation per user][riak_cs_1.5_release_notes_upgrading_1] have
been introduced in 1.5.1. Users who have more than 100 buckets cannot create any
bucket after the upgrade unless the limit is extended in the system
configuration.

#### Note: Upgrading From Riak CS 1.4.x

An operational procedure [to clean up incomplete multipart under deleted
buckets][riak_cs_1.5_release_notes_incomplete_mutipart] is needed. Otherwise new
buckets with names that used to exist can't be created. The operation will fail
with 409 Conflict.

Leeway seconds and disk space should also be carefully watched during the
upgrade, because timestamp management of garbage collection was changed in the
1.5.0 release. Consult the "[Leeway seconds and disk
space][riak_cs_1.5_release_notes_leeway_and_disk] section of 1.5 release notes
for a more detailed description.

#### Note: Upgrading From Riak CS 1.3.x or Older

Basho supports upgrading from the two previous major versions to the latest
release. Thus, this document will only cover upgrading from Riak CS versions
1.4.x and 1.5.x.

To upgrade to Riak CS 2.0.0 from versions prior to Riak CS 1.4.0, operators will
need to first upgrade their system to Riak CS version 1.4.5 or 1.5.4. Upgrading
to Riak CS 1.5.4 is recommended. The underlying Riak installation must also be
upgraded to the Riak 1.4.x series, preferably version 1.4.12.


## General Upgrade Instructions

#### All Scenarios

We recommend updating Stanchion before all other subsystems. Be careful not to
have multiple live Stanchion nodes accessible from Riak CS nodes at the same
time.

Repeat these steps on each node running Stanchion:

1. Stop Stanchion
2. Back up all Stanchion configuration files
3. Uninstall the current Stanchion package
4. Install the new Stanchion 2.0.0 package
5. Migrate the Stanchion configuration (See below)
6. Start Stanchion

#### Scenario: If Riak CS and Riak are both running on the same host.

Repeat these steps on every host:

1. Stop Riak CS
2. Stop Riak
3. Back up all Riak and Riak CS configuration files and remove all patches
4. Uninstall the current Riak CS package
5. Uninstall the current Riak Riak packages
6. Install the new Riak package
7. Install the new Riak CS 2.0.0 package
8. Migrate the Riak configuration (See below)
9. Migrate the Riak CS configuration (See below)
10. Start Riak
11. Start Riak CS

#### Scenario: If Riak CS and Riak are running on separate hosts.

When Riak CS is not installed on the same host as Riak, Riak CS can be upgraded
at any time while the corresponding remote Riak node is alive.

Repeat these steps on every host:

1. Stop Riak CS
2. Back up all configuration files and remove all patches
3. Uninstall the current Riak CS package
4. Install the new Riak CS 2.0.0 package
5. Migrate the Riak CS configuration (See below)
6. Start Riak CS


## Upgrade Stanchion to 2.0.0

When upgrading to Stanchion 2.0.0 the files `app.config` and `vm.args` are
migrated to the single file `stanchion.conf`. Configuration files are still
stored in the same location as before.

#### Migrate the Stanchion Configuration

Using the Configuration Mapping Tables below, edit your `stanchion.conf` to
preserve your configuration preferences between Riak CS 1.5.x and 2.0.0.

The tables show the old and new configuration format and default values.

##### `stanchion` section of the Stanchion app.config

|      1.5.4 (`app.config`)          |        2.0.0 (`stanchion.conf`)       |
|:-----------------------------------|:--------------------------------------|
|`{stanchion_ip, "127.0.0.1"}`       |`listener = 127.0.0.1:8080`            |
|`{stanchion_port, 8085}`            |                                       |
|`{riak_ip, "127.0.0.1"}`            |`riak_host = 127.0.0.1:8087`           |
|`{riak_pb_port, 8087}`              |                                       |
|`{admin_key, "admin-key"}`          |`admin.key = admin-key`                |
|`{admin_secret, "admin-secret"}`    |`admin.secret = admin-secret`          |

##### `lager` section of the Stanchion app.config

Riak's Lager configuration can be copied directly to the `advanced.config` file.


##### Commented out by default and consequently `undefined` so as to disable SSL.

|      1.5.4 (`app.config`)          |        2.0.0 (`stanchion.conf`)       |
|:-----------------------------------|:--------------------------------------|
|`{ssl, [`                           |                                       |
|`  {certfile, "./etc/cert.pem"}`    |`ssl.certfile`                         |
|`  {keyfile, "./etc/key.pem"}`      |`ssl.keyfile`                          |


## Upgrade Riak to 2.0.5 and Configure for Riak CS 2.0.0

As Riak CS 2.0.0 only works on top of Riak 2.0.5 -- and does *not* work on top
of the Riak 1.x.x series -- the underlying Riak installation *must* be upgraded
to Riak 2.0.5. This document only covers upgrading from Riak 1.4.x. For more
general information on upgrading Riak, please see the Riak
[upgrading to 2.0 guide][upgrading_to_2.0].

Below are specific configuration changes required for a Riak 2.0.5 cluster
supporting Riak CS.

#### Upgrading Riak - Step 1: Set Default Bucket Properties

In older versions of Riak, default bucket properties has been configured in the
`app.config` as follows:

```erlang
{riak_core, [
   ...
   {default_bucket_props, [{allow_mult, true}]},
   ...
]}.
```

With Riak 2.0.5 in `riak.conf` this becomes:

```
buckets.default.allow_mult = true
```

#### Upgrading Riak - Step 2: `riak_kv` configuration

There are two ways to configure Riak 2.0.5 behind Riak CS 2.0.0:

**Option 1: Reuse the existing `app.config` file from Riak 1.4.x**

In this case, `add_paths` should be changed to target the new Riak CS binaries
installed by the Riak CS 2.0.0 package. These will be changed from
`"/usr/lib/riak-cs/lib/riak_cs-1.5.4/ebin"` to
`"/usr/lib/riak-cs/lib/riak_cs-2.0.0/ebin"`.

**Option 2: Use Riak 2.0.0's new `advanced.config`**

You will need to copy all riak_kv configuration items from `app.config` into
`advanced.config`, and update `add_paths` to target the new Riak CS binaries
installed by the Riak CS 2.0.0 package. In `advanced.config`, this will become:

```erlang
{riak_kv, [
  {add_paths, ["/usr/lib/riak-cs/lib/riak_cs-2.0.0/ebin"]}
]}.
```

The `app.config` file must be removed when `advanced.config` is used.

See [Setting up the Proper Riak Backend][proper_backend] for additional details.

#### Upgrading Riak - Step 3: Review Memory Size

Since the default configuration of the LevelDB memory size has changed, you will
need to review your memory size settings. The memory use of Riak CS is primarily
influenced by the Bitcask keydir and LevelDB block cache. Additionally, to
improve IO performance, some extra memory for the kernel disk cache should be
planned for. The equations below might help when specifying the memory size:

- Memory for backends = (Memory for Bitcask) + (Memory for LevelDB)
- Memory for storage = (Memory for backends) + (Memory for kernel cache)

##### LevelDB Block Cache Size

The configuration setting relating to the memory size of LevelDB has changed
from `max_open_files` to `total_leveldb_mem_percent` in 2.0. This specifies the
total amount of memory consumed by LevelDB. Note that the default memory limit
has changed from being proportional to the number of `max_open_files` to being a
percentage of the system's physical memory size.

Configuring `total_leveldb_mem_percent` is *strongly recommended* as its
[default value of 70%][configuring_elvevedb] might be too aggressive for a
multi-backend configuration that also uses Bitcask. Bitcask keeps its keydir in
memory, which could be fairly large depending on the use case.

##### Bitcask keydir Sizing

Bitcask stores all of its keys in memory as well as on disk. Correctly
estimating the total number of keys and their average size in Bitcask is very
important for estimating Bitcask memory usage. Total number of keys `N(b)` in
Bitcask across the whole cluster will be:

```
N(b) = N(o, size <= 1MB) + N(o, size > 1MB) * avg(o, size > 1MB) / 1MB
```

where `N(o, size <= 1MB)` is the number of objects with a size less than 1MB,
while `N(o, size > 1MB` is the number of objects with a size greater than 1MB.
`avg(o, size > 1MB)` is the average size of objects greater than 1MB in size.
The number of keys in Riak CS is related to the amount of data stored in MBs.
If the average lifetime of objects is significantly smaller than the leeway
period, treat objects waiting for garbage collections as live objects on disk.
Actual numbers of key count per vnode are included in the output of
`riak-admin vnode-status`. There is an item named `Status` in each vnode
section, which includes the `key_count` in the `be_blocks` section.

Once the numbers of keys is known, estimate the amount of memory used by Bitcask
keydir as per the [Bitcask Capacity Planning][bitcask_capactiy_planning]
documentation.

The bucket name size is always 19 bytes (see `riak_cs_utils:to_bucket_name/2`)
and the key size is always 20 bytes (see `riak_cs_lfs_utils:block_name/3`). The
average value size is close to 1MB if large objects are dominant, otherwise it
should be estimated according to the specific use case. The number of writes is
3.

#### Upgrading Riak - Step 4: Changes in `vm.args`

The upgrade of Riak from the 1.4.x series to the 2.0.x series is described in
[Upgrading Your Configuration System][upgrading_your_configuration]. The
following are several major configuration items which are essential for Riak CS.
`erlang.distribution_buffer_size` is commented out by default.

| Riak 1.4                        | Riak 2.0                              |
|:--------------------------------|:--------------------------------------|
|`+zdbbl`                         |`erlang.distribution_buffer_size = 1MB`|
|`-name riak@127.0.0.1`           |`nodename = riak@127.0.0.1`            |
|`-setcookie riak`                |`distributed_cookie = riak`            |

#### Upgrading Riak - Step 5: Storage Calculation

If storage statistics are desired on your system, several more configuration
options are required. Please see the [storage statistics
documentation][storage_statistics] for additional details.

#### Upgrading Riak - Additional Notes

The underlying Bitcask storage format has been changed in Riak 2.0.x to [fix
several important issues][riak_2.0_release_notes_bitcask]. The first start of
Riak after an upgrade involves an implicit data format upgrade conversion, which
means that all data files are read, and written out to new files. This might
lead to higher than normal disk load. The duration of the upgrade will depend on
the amount of data stored in Bitcask and the IO performance of the underlying
disk.

The data conversion will start with logs like this:

```
2015-03-17 02:43:20.813 [info] <0.609.0>@riak_kv_bitcask_backend:maybe_start_upgrade_if_bitcask_files:720 Starting upgrade to version 1.7.0 in /mnt/data/bitcask/1096126227998177188652763624537212264741949407232
2015-03-17 02:43:21.344 [info] <0.610.0>@riak_kv_bitcask_backend:maybe_start_upgrade_if_bitcask_files:720 Starting upgrade to version 1.7.0 in /mnt/data/bitcask/1278813932664540053428224228626747642198940975104
```

The end of the data conversion can be observed as info log entries in Riak logs
like this:

```
2015-03-17 07:18:49.754 [info] <0.609.0>@riak_kv_bitcask_backend:callback:446 Finished upgrading to Bitcask 1.7.0 in /mnt/data/bitcask/1096126227998177188652763624537212264741949407232
2015-03-17 07:23:07.181 [info] <0.610.0>@riak_kv_bitcask_backend:callback:446 Finished upgrading to Bitcask 1.7.0 in /mnt/data/bitcask/1278813932664540053428224228626747642198940975104
```


## Upgrading Riak CS to 2.0.0

#### Upgrading Riak CS - Step 1: Multibag Configuration

Multibag configurations must be moved to `advanced.config` for both Riak CS and
Stanchion.

#### Upgrading Riak CS - Step 2: Default Configurations

When upgrading to Riak CS 2.0.0 the files `app.config` and `vm.args` are
migrated to the single file `riak-cs.conf`. Configuration files are still stored
in the same location as before.

Note: **`app.config` should be removed** once you’ve completed the upgrade and
`riak-cs.conf` is being used.

Some important configuration changes occurred between 1.5.x and 2.0.0, and not
all items were translated one-to-one.

Using the Configuration Mapping Tables below, edit your `riak-cs.conf` to
preserve your configuration preferences between Riak CS 1.5.x and 2.0.0.

The tables show the old and new configuration format and default values.

Note: `storage.stats.schedule.$time` does not have any default value but an
example is added.

##### `riak_cs` section of the Riak CS app.config

|      1.5.4 (`app.config`)          |        2.x (`riak-cs.conf`)            |   Note     |
|:-----------------------------------|:---------------------------------------|:-----------|
|`{cs_ip, "127.0.0.1"}`              |`listener = 127.0.0.1:8080`             |            |
|`{cs_port, 8080}`                   |                                        |            |
|`{riak_ip, "127.0.0.1"}`            |`riak_host = 127.0.0.1:8087`            |            |
|`{riak_pb_port, 8087}`              |                                        |            |
|`{stanchion_ip, "127.0.0.1"}`       |`stanchion_host = 127.0.0.1:8085`       |            |
|`{stanchion_port, 8085 }`           |                                        |            |
|`{stanchion_ssl, false }`           |`stanchion_ssl = off`                   |            |
|`{anonymous_user_creation, false}`  |`anonymous_user_creation = off`         |            |
|`{admin_key, "admin-key"}`          |`admin.key = admin-key`                 |            |
|`{admin_secret, "admin-secret"}`    |`admin.secret = admin-secret`           |            |
|`{cs_root_host, "s3.amazonaws.com"}`|`root_host = s3.amazonaws.com`          |            |
|`{connection_pools,[`               |                                        |            |
|` {request_pool, {128, 0} },`       |`pool.request.size = 128`               |            |
|                                    |`pool.request.overflow = 0`             |            |
|` {bucket_list_pool, {5, 0} }`      |`pool.list.size = 5`                    |            |
|                                    |`pool.list.overflow = 0`                |            |
|`{max_buckets_per_user, 100}`       |`max_buckets_per_user = 100`            | from 2.0.1 |
|`{trust_x_forwarded_for, false}`    |`trust_x_forwarded_for = off`           |            |
|`{leeway_seconds, 86400}`           |`gc.leeway_period = 24h`                |            |
|`{gc_interval, 900}`                |`gc.interval = 15m`                     |            |
|`{gc_retry_interval, 21600}`        |`gc.retry_interval = 6h`                |            |
|`{gc_batch_size, 1000}`             |`gc.batch_size = 1000`                  | from 2.0.1 |
|`{access_log_flush_factor, 1}`      |`stats.access.flush_factor = 1`         |            |
|`{access_log_flush_size, 1000000}`  |`stats.access.flush_size = 1000000`     |            |
|`{access_archive_period, 3600}`     |`stats.access.archive_period = 1h`      |            |
|`{access_archiver_max_backlog, 2}`  |`stats.access.archiver.max_backlog = 2` |            |
|(no explicit default)               |`stats.access.archiver.max_workers = 2` |            |
|`{storage_schedule, []}`            |`stats.storage.schedule.$time = 0600`   |            |
|`{storage_archive_period, 86400}`   |`stats.storage.archive_period = 1d`     |            |
|`{usage_request_limit, 744}`        |`riak_cs.usage_request_limit = 31d`     |            |
|`{cs_version, 10300 }`              |`cs_version = 10300`                    |            |
|`{dtrace_support, false}`           |`dtrace = off`                          |            |

##### `webmachine` section of the Riak CS app.config

|      1.5.4 (`app.config`)          |        2.0.0 (`riak-cs.conf`)         |   note     |
|:-----------------------------------|:--------------------------------------|:-----------|
|`{server_name, "Riak CS"}`          |`server_name = Riak CS`                |            |
|`{log_handlers, ....}`              |`log.access = true`                    | from 2.0.1 |
|                                    |`log.access.dir = /var/log/riak-cs`    |            |

Due to a WebMachine change, if `log_handlers` are defined in `app.config` or
`advanced.config`, the log handler's name should be changed as follows:

```erlang
    {log_handlers, [
        {webmachine_access_log_handler, ["/var/log/riak-cs"]},
        {riak_cs_access_log_handler, []}
        ]},
```

This does not have to be changed if `log_handlers` is not defined in
`app.config` or `advanced.config`.

##### `lager` section of the Riak CS app.config

Riak's Lager configuration can be copied directly to the `advanced.config` file.

#### Upgrading Riak CS - Step 3: Commented Out Configurations

All commented out items are undefined and disabled, except modules.

`rewrite_module` and `auth_module` are commented out, but the default value does
not change from Riak CS 1.5.4. This section is for showing operators how to
change these settings to the OOS API.

|      1.5.4 (`app.config`)             |      2.0.0 (`riak-cs.conf`)     |
|:--------------------------------------|:--------------------------------|
|`{rewrite_module, riak_cs_s3_rewrite }`|`rewrite_module`                 |
|`{auth_module, riak_cs_s3_auth },`     |`auth_module`                    |
|`{admin_ip, "127.0.0.1"}`              |`admin.listener = 127.0.0.1:8000`|
|`{admin_port, 8000 }`                  |                                 |
|`{ssl, [`                              |                                 |
|`  {certfile, "./etc/cert.pem"}`       |`ssl.certfile`                   |
|`  {keyfile, "./etc/key.pem"}`         |`ssl.keyfile`                    |

#### Upgrading Riak CS - Step 4: Other Configurations

The following configurations do not have corresponding items in `riak-cs.conf`:

* `fold_objects_for_list_keys`
* `n_val_1_get_requests`
* `gc_paginated_indexes`

If these values are still set as `false`, they should be omitted from your Riak
CS 2.0.0 configuration.

If the old behavior is preferred, they must be included in the `riak_cs` section
of `advanced.config`.


## Downgrading Riak CS

### To Riak CS 1.5.x

To downgrade from Riak CS 2.0.0 to Riak CS 1.5.x and Stanchion 2.0.0 to
Stanchion 1.5.0, repeat the following instructions for each node:

1. Stop Riak CS
1. Stop Riak
1. Uninstall the Riak CS 2.0.0 package
1. Uninstall the Riak 2.0.5 package
1. Run the Bitcask downgrade script for all Bitcask directories\*
1. Install the desired Riak package
1. Install the desired Riak CS package
1. Restore configuration files
1. Start Riak
1. Start Riak CS

Finally, on any nodes running Stanchion:

1. Stop Stanchion
2. Uninstall the Stanchion 2.0.0 package
3. Install the desired Stanchion package
4. Restore Stanchion configuration files
5. Start Stanchion

\*The Bitcask file format has changed between Riak 1.4.x and 2.0.0. While the
implicit upgrade of Bitcask data files is supported, automatic downgrades of
Bitcask data files is not. For this reason downgrading requires a script to
translate data files. See also the [2.0 downgrade notes][downgrade_notes].



[riak_1.4_release_notes]: https://github.com/basho/riak/blob/1.4/RELEASE-NOTES.md
[riak_2.0_release_notes]: https://github.com/basho/riak/blob/2.0/RELEASE-NOTES.md
[riak_2.0_release_notes_bitcask]: https://github.com/basho/riak/blob/2.0/RELEASE-NOTES.md#bitcask
[riak_cs_1.4_release_notes]: https://github.com/basho/riak_cs/blob/release/1.5/RELEASE-NOTES.md#riak-cs-145-release-notes
[riak_cs_1.5_release_notes]: https://github.com/basho/riak_cs/blob/release/1.5/RELEASE-NOTES.md#riak-cs-154-release-notes
[riak_cs_1.5_release_notes_upgrading]: https://github.com/basho/riak_cs/blob/release/1.5/RELEASE-NOTES.md#notes-on-upgrading
[riak_cs_1.5_release_notes_upgrading_1]: https://github.com/basho/riak_cs/blob/release/1.5/RELEASE-NOTES.md#notes-on-upgrading-1
[riak_cs_1.5_release_notes_incomplete_mutipart]: https://github.com/basho/riak_cs/blob/release/1.5/RELEASE-NOTES.md#incomplete-multipart-uploads
[riak_cs_1.5_release_notes_leeway_and_disk]: https://github.com/basho/riak_cs/blob/release/1.5/RELEASE-NOTES.md#leeway-seconds-and-disk-space
[upgrading_to_2.0]: http://docs.basho.com/riak/2.0.5/upgrade-v20/
[proper_backend]: http://docs.basho.com/riakcs/1.5.4/cookbooks/configuration/Configuring-Riak/#Setting-up-the-Proper-Riak-Backend
[configuring_elvevedb]: http://docs.basho.com/riak/latest/ops/advanced/backends/leveldb/#Configuring-eLevelDB
[bitcask_capactiy_planning]: http://docs.basho.com/riak/2.0.5/ops/building/planning/bitcask/
[upgrading_your_configuration]: http://docs.basho.com/riak/2.0.5/upgrade-v20/#Upgrading-Your-Configuration-System
[storage_statistics]: http://docs.basho.com/riakcs/latest/cookbooks/Usage-and-Billing-Data/#Storage-Statistics
[downgrade_notes]:  https://github.com/basho/riak/wiki/2.0-downgrade-notes


