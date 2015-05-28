# Riak CS 2.0.1 Release Notes

## General Information
This is a bugfix release.

## Bug Fixes

* Fix config item `gc.interval` not working when `infinity` is set ([#1125](https://github.com/basho/riak_cs/issues/1125)/[PR#1126](https://github.com/basho/riak_cs/pull/1126)).
* Add `log.access` switch to disable access logging ([#1109](https://github.com/basho/riak_cs/issues/1109)/[PR#1115](https://github.com/basho/riak_cs/pull/1115)).
* Add missing riak-cs.conf items:` max_buckets_per_user` and `gc.batch_size` ([#1109](https://github.com/basho/riak_cs/issues/1109)/[PR#1115](https://github.com/basho/riak_cs/pull/1115)).
* Fix bugs around subsequent space characters for Delete Multiple Objects API and user administration API with XML content ([#1129](https://github.com/basho/riak_cs/issues/1129)/[PR#1135](https://github.com/basho/riak_cs/pull/1135)).

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


