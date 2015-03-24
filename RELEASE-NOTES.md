# Riak CS 2.0.0-pre1 Release Notes

- Based upon Riak 2.0
- Simplified configuration system

## Changes

- Changed default value of `gc_max_workers` from 5 to 2 with its name
  changed to `gc.max_workers` with migration to config format change.

## Deprecation Notice

- Multi-Datacenter Replication on top of v2 replication support has
  been deprecated.
- Old list objects which required `fold_objects_for_list_keys` as
  `false` is deprecated and *will be removed* in the next major version.
- Non-paginated GC in case where `gc_paginated_indexes` is `false` is
  deprecated and *will be removed* in the next major version.

# General Notes on Upgrading to Riak CS 2.0

Upgrading Riak CS system involves upgrading the underlying Riak and
Stanchion installations. The upgrade process can be non-trivial depending on
the system configuration and combination of sub-system versions. This document contains
general instructions on upgrading the whole system to Riak CS 2.0, and
notices on upgrading.

For more detailed information on previous versions, consult the Riak and Riak
CS's release notes.

* [Riak 1.4 series release notes](https://github.com/basho/riak/blob/1.4/RELEASE-NOTES.md)
* [Riak 2.0 series release notes](https://github.com/basho/riak/blob/2.0/RELEASE-NOTES.md)
* [Riak CS 1.4 series release notes](https://github.com/basho/riak_cs/blob/release/1.5/RELEASE-NOTES.md#riak-cs-145-release-notes)
* [Riak CS 1.5 series release notes](https://github.com/basho/riak_cs/blob/release/1.5/RELEASE-NOTES.md#riak-cs-154-release-notes)

## Upgrading From Riak CS < 1.4

As Basho only supports 2 older versions prior to the latest release, for Riak
CS 2.0 only upgrades from 1.4 series and 1.5 series are
supported.

To upgrade to 2.0 from these versions prior to 1.4, operators need
another extra step to upgrade the whole system to Riak CS 1.4.5 or
Riak CS 1.5.4. Upgrading to 1.5.4 rather than to 1.4.5 is recommended.
The underlying Riak installation should be upgraded to the 1.4
series - preferably the latest version 1.4.12.

# General Instructions

## Riak CS and Riak on the same host

Repeat these steps on every host:

1. Stop the Riak CS node
2. Stop the Riak node
3. Back all configuration files up and remove all patches
4. Uninstall the old Riak CS and Riak packages
5. Install the new Riak CS and Riak package
6. Migrate the Riak configuration
7. Migrate the Riak CS configuration
8. Start Riak
9. Start Riak CS

Stanchion can *theoretically* be updated at any time during the system
upgrade. In practice, we recommend updating Stanchion before all
other subsystems. Be careful not to run multiple live
Stanchion nodes accessible from CS nodes at the same time.

Patches for the CS 1.4 or 1.5 releases cannot be applied to Riak CS
2.0 because they need to be recompiles with the Erlang/OTP runtime shipped
with Riak CS 2.0. This also applies to Riak 2.0, that
cannot have patches for Riak 1.4. All previous official patches for old Riak
and Riak CS have been included in this release.

## Configuration upgrade

Riak has introduced a new configuration system (`riak.conf`) with version 2.0.
It also still supports old style configurations through `app.config` and `vm.args`.
So does Riak CS 2.0 now follows suit and supports the new configuration style.
Basho recommends moving to the new unified configuration system and use the 
files `riak.conf`, `riak-cs.conf` and `stanchion.conf`.

### Riak 1.4 to 2.0

As Riak CS 2.0 only works on top of Riak 2.0 and does not work on top of the
Riak 1.x series, the underlying Riak installation *must* be upgraded
to 2.0. General guides for upgrading Riak to 2.0 are in the
[2.0 upgrade guide](http://docs.basho.com/riak/2.0.4/upgrade-v20/).
Below are configuration changes for a Riak cluster supporting Riak CS.


#### Default bucket properties

Default bucket properties have been configured in older Riak CS as
follows:

```erlang
{riak_core, [
   ...
   {default_bucket_props, [{allow_mult, true}]},
   ...
]}.
```

With Riak 2.0 this becomes:

```
buckets.default.allow_mult = true
```

Note: this is defined in riak_kv.schema

#### `riak_kv` configuration

There are two ways to configure Riak 2.0 behind Riak CS:

1. Reuse the `app.config` file of Riak 1.4
2. Transfer selected items from the old `app.config` to the `advanced.config` file

In case of 1., `add_paths` should be changed to new Riak CS binaries
installed by Riak CS 2.0 package, from
`"/usr/lib/riak-cs/lib/riak_cs-1.5.4/ebin"` to
`"/usr/lib/riak-cs/lib/riak_cs-2.0.0/ebin"`.  ```

If the Riak 2.0 way of configuration is preferred, 2. is the
choice.  Copy all `riak_kv` configuration items of `app.config` to
`advanced.config`, and update `add_paths`.

See
[Setting up the Proper Riak Backend]
(http://docs.basho.com/riakcs/1.5.4/cookbooks/configuration/Configuring-Riak/#Setting-up-the-Proper-Riak-Backend)
for details. `app.config` must be removed when `advanced.config`
is used.

#### Review memory size

Since the default configuration of the LevelDB memory size has changed,
review your memory size settings. Riak CS' memory use is dominated by
the Bitcask keydir and LevelDB block cache. Additionally, to improve IO
performance, some extra memory for the kernel disc cache should be configured.
The equations below might help when specifying the memory size:

```
Memory for storage = (Memory for backends) + (Memory for kernel cache)
Memory for backends = (Memory for Bitcask) + (Memory for LevelDB)
```

##### LevelDB block cache size

The configuration setting relating to the memory size of LevelDB
has changed from `max_open_files` to `total_leveldb_mem_percent` in 2.0.
This specifies the total amount of memory consumed by LevelDB.
Note that the default memory limit has changed from being
proportional to the number of `max_open_files` to being a percentage of
the system's physical memory size.

Configuring `total_leveldb_mem_percent` is *strongly recommended* as
its default value of [70%]
(http://docs.basho.com/riak/latest/ops/advanced/backends/leveldb/#Configuring-eLevelDB)
might be too aggressive for a multi-backend configuration that
also uses bitcask. Bitcask keeps its keydir in memory, which could be
fairly large depending on the use case.

Note: `leveldb.maximum_memory_percent` in `riak.conf` also can be
used. It is also possible to use to use configuration settings
starting with `multi_backend.be_default...` in
`riak.conf`, but that is confusing and less simple than the recommended
way described above.

##### Bitcask keydir sizing

Bitcask stores all of its keys in memory as well as on disk. Estimating
the total number of keys and their average size stored in Bitcask is most
important to estimate the memory usage. Total number of keys `N(b)` in
Bitcask accross the whole cluster will be:

```
N(b) = N(o, size <= 1MB) + N(o, size > 1MB) * avg(o, size > 1MB) / 1MB
```

where `N(o, size <= 1MB)` is the number of objects with a size less than
1MB, while `N(o, size > 1MB` is the number of objects with a size greater
than 1MB. `avg(o, size > 1MB)` is the average size of objects greater than
1MB in size. The number of keys in Riak CS is equivalent to the amount of
data stored in MB. If the average lifetime of objects is a lot smaller
than the leeway period, treat objects waiting for garbage collections as live
objects on disk. Actual numbers of key count per vnode are included in the
output of `riak-admin vnode-status`. There is an item named `Status` in each
vnode section, which includes the `key_count` in the `be_blocks` section.

Once the numbers of keys is known, estimate the amount of
memory used by Bitcask keydir as per the
[Bitcask Capacity Planning]
(http://docs.basho.com/riak/2.0.5/ops/building/planning/bitcask/)
documentation.

The bucket name size is always 19 bytes (see
`riak_cs_utils:to_bucket_name/2`) and the key size is always 20 bytes (see
`riak_cs_lfs_utils:block_name/3`). The average value size is close to 1MB if
large objects are dominant, otherwise it should be estimated according to the
specific use case. The number of writes is 3.


#### Upcoming Riak 2.1

Riak CS on Riak 2.1 has not yet been tested. However, in the new
Riak version the configuration should be

```
storage_backend = prefix_multi
cs_version = 20000
```

`cs_version` must not be removed when Riak is running under Riak CS.
In the new configuration style, the data path for LevelDB and Bitcask can be set
with `leveldb.data_root` and `bitcask.data_root`, respectively.

#### Notable changes in `vm.args`

The upgrade of Riak from the 1.4 series to the 2.0 series is described in
[Upgrading Your Configuration System]
(http://docs.basho.com/riak/2.0.5/upgrade-v20/#Upgrading-Your-Configuration-System).
The following are several major configuration items which are essential
for Riak CS. `erlang.distribution_buffer_size` is commented out by
default.

| Riak 1.4                        | Riak 2.0                              |
|:--------------------------------|:--------------------------------------|
|`+zdbbl`                         |`erlang.distribution_buffer_size = 1MB`|
|`-name riak@127.0.0.1`           |`nodename = riak@127.0.0.1`            |
|`-setcookie riak`                |`distributed_cookie = riak`            |

#### Storage calculation

If [storage statistics]
(http://docs.basho.com/riakcs/latest/cookbooks/Usage-and-Billing-Data/#Storage-Statistics)
is desired on your system, several more configurations are required.

To include the CS path add the following setting to `advanced.config`:

```erlang
{riak_kv, [
  {add_paths, ["/usr/lib/riak-cs/lib/riak_cs-2.0.0/ebin"]}
]}.
```

### Notes on upgrading Riak to 2.0

The underlying Bitcask storage format has been changed in Riak 2.0 to
[fix several important issues]
(https://github.com/basho/riak/blob/2.0/RELEASE-NOTES.md#bitcask).
The first start of Riak after an upgrade involves an implicit data
format upgrade conversion, which means that all data files are read
and written out to new files. This might lead to higher disk load.
The duration of the upgrade will depend on the amount of data stored in
bitcask and the IO performance of the underlying disk.

The data conversion will start with logs like this:

```
2015-03-17 02:43:20.813 [info] <0.609.0>@riak_kv_bitcask_backend:maybe_start_upgrade_if_bitcask_files:720 Starting upgrade to version 1.7.0 in /mnt/data/bitcask/1096126227998177188652763624537212264741949407232
2015-03-17 02:43:21.344 [info] <0.610.0>@riak_kv_bitcask_backend:maybe_start_upgrade_if_bitcask_files:720 Starting upgrade to version 1.7.0 in /mnt/data/bitcask/1278813932664540053428224228626747642198940975104
```

The end of the data conversion can be observed as info log entries in Riak
logs like this:

```
2015-03-17 07:18:49.754 [info] <0.609.0>@riak_kv_bitcask_backend:callback:446 Finished upgrading to Bitcask 1.7.0 in /mnt/data/bitcask/1096126227998177188652763624537212264741949407232
2015-03-17 07:23:07.181 [info] <0.610.0>@riak_kv_bitcask_backend:callback:446 Finished upgrading to Bitcask 1.7.0 in /mnt/data/bitcask/1278813932664540053428224228626747642198940975104
```

### Riak CS 1.5 to 2.0, including Stanchion

Consult the Configuration Mapping Table below
to preserve same configuration between CS 1.5 and 2.0.

If obsolete configurations like `fold_objects_for_list_keys`,
`n_val_1_get_requests` or `gc_paginated_indexes` are still set as
`false`, **removing any of them from the configuration file** is strongly
recommended in Riak CS 2.0, because those configuration items are just
to preserve old and slow behaviours of Riak CS and have no inpact on
functionality.

#### Multibag configurations

Multibag configurations must be moved to `advanced.config` for both
Riak CS and Stanchion.

## Notes on Upgrading from Riak CS < 1.5.4

[Some objects changes their names]
(https://github.com/basho/riak_cs/blob/release/1.5/RELEASE-NOTES.md#notes-on-upgrading)
after the upgrade. Applications may change their behaviour due to this bugfix.

## Notes on Upgrading from Riak CS < 1.5.1

[Bucket number limitation per user]
(https://github.com/basho/riak_cs/blob/release/1.5/RELEASE-NOTES.md#notes-on-upgrading-1)
have been introduced in 1.5.1. Users who have more than 100 buckets
cannot create any bucket after the upgrade unless the limit is
extended in the system configuration.

## Special Notes on Upgrading From Riak CS 1.4

An operational procedure [to clean up incomplete multipart under deleted buckets]
(https://github.com/basho/riak_cs/blob/release/1.5/RELEASE-NOTES.md#incomplete-multipart-uploads)
is needed. Otherwise new buckets with names that used to exist in the past
can't be created. The operation will fail with 409 Conflict.

Leeway seconds and disk space should also be carefully watched during
the upgrade, because timestamp management of garbage collection has changed
since the 1.5 release. Consult the "[Leeway seconds and disk space]
(https://github.com/basho/riak_cs/blob/release/1.5/RELEASE-NOTES.md#leeway-seconds-and-disk-space)"
section of 1.5.0 release notes for more detailed description.

## Riak CS nodes on different hosts than Riak nodes

Riak CS that are not coinstalled with a Riak node can be
upgraded at any time while the corresponding remote Riak is alive.
Instructions follow:

1. Stop the Riak CS process
2. Backup configuration files
3. Uninstall the Riak CS package
4. Install the Riak CS 2.0 package
5. Update configuration
6. Start Riak CS

## Downgrading

### to CS 1.4

We have not yet tested downgrading from Riak CS 2.0 to Riak CS 1.4.

### to CS 1.5

To downgrade Riak CS 2.0 system to Riak CS 1.5 system, repeat the
following instructions for each node:

1. Stop Riak CS process
2. Stop Riak process
3. Uninstall Riak CS
4. Uninstall Riak
5. Run downgrade bitcask script for all bitcask directories
6. Install Riak
7. Install Riak CS
8. Restore configuration files
9. Start Riak Process
10. Start CS Process

The Bitcask file format has changed between Riak 1.4 and 2.0. It supports
implicit upgrade of bitcask data files, while automatic downgrading
is not supported. For this reason downgrading requires a script to
translate data files. See also the
[2.0 downgrade notes]
(https://github.com/basho/riak/wiki/2.0-downgrade-notes).


## Configuration Mapping Table between 1.5 and 2.0

Some important configuration changes happened between 1.5 and 2.0. Not all items
are translated one to one.

In the tables below, the default values are shown.

The file name has changed from `app.config` and `vm.args` to only
`riak-cs.conf`. Configuration files are still in the same location as before.

Note: **`app.config` should be removed** when
`riak-cs.conf` is used.

### Riak CS

#### Items effective in the config file in 2.0 by default

Note: `storage.stats.schedule.$time` does not have any default
value but an example is added.

`riak_cs` section in app.config

|      1.5                           |        2.0                             |
|:-----------------------------------|:---------------------------------------|
|`{cs_ip, "127.0.0.1"}`              |`listener = 127.0.0.1:8080`             |
|`{cs_port, 8080}`                   |                                        |
|`{riak_ip, "127.0.0.1"}`            |`riak_host = 127.0.0.1:8087`            |
|`{riak_pb_port, 8087}`              |                                        |
|`{stanchion_ip, "127.0.0.1"}`       |`stanchion_host = 127.0.0.1:8085`       |
|`{stanchion_port, 8085 }`           |                                        |
|`{stanchion_ssl, false }`           |`stanchion_ssl = off`                   |
|`{anonymous_user_creation, false}`  |`anonymous_user_creation = off`         |
|`{admin_key, "admin-key"}`          |`admin.key = admin-key`                 |
|`{admin_secret, "admin-secret"}`    |`admin.secret = admin-secret`           |
|`{cs_root_host, "s3.amazonaws.com"}`|`root_host = s3.amazonaws.com`          |
|`{connection_pools,[`               |                                        |
|` {request_pool, {128, 0} },`       |`pool.request.size = 128`               |
|                                    |`pool.request.overflow = 0`             |
|` {bucket_list_pool, {5, 0} }`      |`pool.list.size = 5`                    |
|                                    |`pool.list.overflow = 0`                |
|`{trust_x_forwarded_for, false}`    |`trust_x_forwarded_for = off`           |
|`{leeway_seconds, 86400}`           |`gc.leeway_period = 24h`                |
|`{gc_interval, 900}`                |`gc.interval = 15m`                     |
|`{gc_retry_interval, 21600}`        |`gc.retry_interval = 6h`                |
|`{access_log_flush_factor, 1}`      |`stats.access.flush_factor = 1`         |
|`{access_log_flush_size, 1000000}`  |`stats.access.flush_size = 1000000`     |
|`{access_archive_period, 3600}`     |`stats.access.archive_period = 1h`      |
|`{access_archiver_max_backlog, 2}`  |`stats.access.archiver.max_backlog = 2` |
|(no explicit default)               |`stats.access.archiver.max_workers = 2` |
|`{storage_schedule, []}`            |`stats.storage.schedule.$time = "06:00"`|
|`{storage_archive_period, 86400}`   |`stats.storage.archive_period = 1d`     |
|`{usage_request_limit, 744}`        |`riak_cs.usage_request_limit = 31d`     |
|`{cs_version, 10300 }`              |`cs_version = 10300`                    |
|`{dtrace_support, false}`           |`dtrace = off`                          |

`webmachine` section in app.config

|      1.5                           |        2.0                            |
|:-----------------------------------|:--------------------------------------|
|`{server_name, "Riak CS"}`          |`server_name = Riak CS`                |
|`{log_handlers, ....}`              |`log.access.dir = /var/log/riak-cs`    |

To disable access logging, comment out or remove the line beginning with
`log.access.dir` from `riak-cs.conf`.

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

#### Items commented out in 2.0 by default

All commented out items are undefined and disabled, except modules.
`rewrite_module` and `auth_module` are commented out but the default value
does not change from 1.5. This is for showing operators how to change these
to OOS API.

| 1.5                                   | 2.0                             |
|:--------------------------------------|:--------------------------------|
|`{rewrite_module, riak_cs_s3_rewrite }`|`rewrite_module`                 |
|`{auth_module, riak_cs_s3_auth },`     |`auth_module`                    |
|`{admin_ip, "127.0.0.1"}`              |`admin.listener = 127.0.0.1:8000`|
|`{admin_port, 8000 }`                  |                                 |
|`{ssl, [`                              |                                 |
|`  {certfile, "./etc/cert.pem"}`       |`ssl.certfile`                   |
|`  {keyfile, "./etc/key.pem"}`         |`ssl.keyfile`                    |

### Items not supported in `riak-cs.conf` and should be written in `advanced.config`

`fold_objects_for_list_keys`, `n_val_1_get_requests` and
`gc_paginated_indexes` do not have corresponding items in
`riak-cs.conf`. If the old behavior is preferred, they must be in
the `riak_cs` section of `advanced.config`.

## Stanchion

`stanchion` section in app.config.

|      1.5                           |        2.0                            |
|:-----------------------------------|:--------------------------------------|
|`{stanchion_ip, "127.0.0.1"}`       |`listener = 127.0.0.1:8080`            |
|`{stanchion_port, 8085}`            |                                       |
|`{riak_ip, "127.0.0.1"}`            |`riak_host = 127.0.0.1:8087`           |
|`{riak_pb_port, 8087}`              |                                       |
|`{admin_key, "admin-key"}`          |`admin.key = admin-key`                |
|`{admin_secret, "admin-secret"}`    |`admin.secret = admin-secret`          |

Commented out by default and consequently `undefined` so as to disable SSL.

|      1.5                           |        2.0                            |
|:-----------------------------------|:--------------------------------------|
|`{ssl, [`                           |                                       |
|`  {certfile, "./etc/cert.pem"}`    |`ssl.certfile`                         |
|`  {keyfile, "./etc/key.pem"}`      |`ssl.keyfile`                          |


## lager

Riak's lager configuration can be copied directly.
