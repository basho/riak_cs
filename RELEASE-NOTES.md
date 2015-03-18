# Riak CS 2.0.0-pre1 Release Notes

- based upon Riak 2.0
- configuration format moved line-by-line style

## Changes

- Changed default value of `gc_max_workers` from 5 to 2 with its name
  changed to `gc.max_workers` with migration to config format change.

## Deprecation Notice

- Multi-Datacenter Replication on top of v2 replication support has
  been deprecated.
- Old list objects which required `fold_objects_for_list_keys` as
  `false` is deprecated and *will be removed* at next major version.
- Non-paginated GC in case where `gc_paginated_indexes` is `false` is
  deprecated and *will be removed* at next major version.

# General Notes on Upgrading to Riak CS 2.0

Upgrading Riak CS system involves upgrading underlying Riak and
Stanchion process. Upgrade process might be complicated depending on
system configuration and version combination. This document describes
general instructions of upgrading the whole system to Riak CS 2.0, and
notices on upgrading.

For minor use cases and more detail information, consult Riak and Riak
CS's release notes.

* [Riak 1.4 series release notes](https://github.com/basho/riak/blob/1.4/RELEASE-NOTES.md)
* [Riak 2.0 series release notes](https://github.com/basho/riak/blob/2.0/RELEASE-NOTES.md)
* [Riak CS 1.4 series release notes](https://github.com/basho/riak_cs/blob/release/1.5/RELEASE-NOTES.md#riak-cs-145-release-notes)
* [Riak CS 1.5 series release notes](https://github.com/basho/riak_cs/blob/release/1.5/RELEASE-NOTES.md#riak-cs-154-release-notes)

## Upgrading From Riak CS < 1.4

Basho only support 2 older versions from the latest release. For Riak
CS 2.0 case, only upgrades from 1.4 series and 1.5 series are
supported. Basically this pattern is out of the scope of this
document.

To upgrade to 2.0 from these versions prior to 1.4, operators need
another extra step to upgrade the whole system to Riak CS 1.4.5 or
Riak CS 1.5.4. Upgrading to 1.5.4 would rather be recommented than
to 1.4.5. Thus underlying Riak would also better upgraded to 1.4
series - hopefully the latest 1.4.12.

# General Instructions

## Riak CS and Riak are in the same box

General Instructions are to repeat this overview on every node:

1. Stop Riak CS process
2. Stop Riak process
3. Backup all configuration files
4. Uninstall old Riak CS and Riak package
5. Install new Riak CS and Riak package
6. Migrate Riak configuration
7. Migrate Riak CS configuration
8. Start Riak
9. Start Riak CS

Stanchion can be updated at any time during the system upgrade
*theoretically*. Although, we recommend updating Stanchion before all
other instructions. Be careful enough not to run multiple live
Stanchion nodes at once where both are referred from CS nodes.

## Configuration upgrade

At 2.0, Riak has introduced a new configuration style. But it also
supports old style called `app.config` and `vm.args`. So does Riak
CS 2.0. But Basho would recommend moving on to unified configuration
file called `riak.conf`, `riak-cs.conf` and `stanchion.conf`.

### Riak 1.4 to 2.0

As Riak CS 2.0 only works on top of Riak 2.0 and does not work on
Riak 1.x series, underlying Riak should necessarily upgraded
to 2.0. Although general guides for upgrading Riak to 2.0 are in
[2.0 upgrade guide](http://docs.basho.com/riak/2.0.4/upgrade-v20/),
below are configuration changes for Riak cluster serving for Riak CS.


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

Now it is just as follows at Riak 2.0.

```
buckets.default.allow_mult = true
```

Note: this is defined in riak_kv.schema

#### Backend configuration

For Riak 2.0.5 or older, Old style backend configuration same as 1.5
should be written in `riak_kv` section of `advanced.config`. See
[Setting up the Proper Riak Backend](http://docs.basho.com/riakcs/1.5.4/cookbooks/configuration/Configuring-Riak/#Setting-up-the-Proper-Riak-Backend)
for details.

For later release than Riak 2.0.5, new configuration in Riak will be
just

```
storage_backend = prefix_multi
cs_version = 20000
```

`cs_version` could not be removed when Riak is running under Riak. Old
style backend configuration using `riak_cs_kv_multi_backend` also can
be written in `advanced.config`.

#### Notable changes in `vm.args`

Although
[Upgrading Your Configuration System](http://docs.basho.com/riak/2.0.5/upgrade-v20/#Upgrading-Your-Configuration-System)
describes general topics on upgrading from 1.4 Riak series to 2.0
Riak, here are several major configuration items which are essential
for Riak CS. `erlang.distribution_buffer_size` is commented out by
default.

| Riak 1.4                        | Riak 2.0                              |
|:--------------------------------|:--------------------------------------|
|`+zdbbl`                         |`erlang.distribution_buffer_size = 1MB`|
|`-name riak@127.0.0.1`           |`nodename = riak@127.0.0.1`            |
|`-setcookie riak`                |`distributed_cookie = riak`            |

#### Storage calculation

If
[storage calculation](http://docs.basho.com/riakcs/latest/cookbooks/Usage-and-Billing-Data/#Storage-Statistics)
is required to your system, several more configurations are required.

Including CS path: add following sentence to `advanced.config`:

```erlang
{riak_kv, [
  {add_paths, ["/usr/lib/riak-cs/lib/riak_cs-2.0.0/ebin"]}
]}.
```

### Notes on upgrading Riak to 2.0

Riak has updated its underlying Bitcask storage data format at 2.0. At
the first start of Riak after upgrade, it involves implicit data
format upgrade conversion, which means reading all data and writing
down to another file. This might lead to disk load - the duration of
upgrade will depend on the amount of data stored in bitcask and IO
performance of underlying disk.

The end of data conversion can be observed as info log at Riak logs
like this:

```
2015-03-17 07:18:49.754 [info] <0.609.0>@riak_kv_bitcask_backend:callback:446 Finished upgrading to Bitcask 1.7.0 in /mnt/data/bitcask/1096126227998177188652763624537212264741949407232
2015-03-17 07:23:07.181 [info] <0.610.0>@riak_kv_bitcask_backend:callback:446 Finished upgrading to Bitcask 1.7.0 in /mnt/data/bitcask/1278813932664540053428224228626747642198940975104
```

while the start time is like this:

```
2015-03-17 02:43:20.813 [info] <0.609.0>@riak_kv_bitcask_backend:maybe_start_upgrade_if_bitcask_files:720 Starting upgrade to version 1.7.0 in /mnt/data/bitcask/1096126227998177188652763624537212264741949407232
2015-03-17 02:43:21.344 [info] <0.610.0>@riak_kv_bitcask_backend:maybe_start_upgrade_if_bitcask_files:720 Starting upgrade to version 1.7.0 in /mnt/data/bitcask/1278813932664540053428224228626747642198940975104
```

### Riak CS 1.5 to 2.0, including Stanchion

Consult
[Configuration Mapping Table](https://github.com/basho/riak_cs/wiki/Configuration-Mapping-Table-between-1.5-and-2.0-%5BRFC%5D)
to preserve same configuration between CS 1.5 and 2.0.

If obsolete configurations like `fold_objects_for_list_keys`,
`n_val_1_get_requests` or `gc_paginated_indexes` are still set as
`false`, **removing any of them from configuraiton file** is strongly
recommended at Riak CS 2.0, because those configuration items are just
to preserve old and slow behaviours of Riak CS and have no inpact on
functionality.

#### Multibag configurations

Multibag configurations are to be moved to `advanced.config` for both
Riak CS and Stanchion.

## Notes on Upgrading from Riak CS < 1.5.4

[Some objects changes their names](https://github.com/basho/riak_cs/blob/release/1.5/RELEASE-NOTES.md#notes-on-upgrading)
after upgrade. Applications may change it's behaviour due to this bugfix.

## Notes on Upgrading from Riak CS < 1.5.1

[Bucket number limitation per user](https://github.com/basho/riak_cs/blob/release/1.5/RELEASE-NOTES.md#notes-on-upgrading-1)
had been introduced at 1.5.1. Users who have more than 100 buckets
cannot create any bucket after upgrade unless the limit number
extended at the system configuration.

## Special Notes on Upgrading From Riak CS 1.4

An operational procedure
[to clean up incomplete multipart under deleted buckets](https://github.com/basho/riak_cs/blob/release/1.5/RELEASE-NOTES.md#incomplete-multipart-uploads)
is needed. Otherwise new buckets which used to exist in the past
couldn't be created. The operation will fail with 409 Conflict.

Leeway seconds and disk space should also be carefully watched during
the upgrade, because timestamp management of garbage collection had
been changed since 1.5 release. Consult
"[Leeway seconds and disk space](https://github.com/basho/riak_cs/blob/release/1.5/RELEASE-NOTES.md#leeway-seconds-and-disk-space)"
section of 1.5.0 release notes for more detailed description.

## Riak CS nodes not corriding in a same box with live Riak node

Riak CS nodes not corriding in a same box with live Riak node can be
upgraded at any time while corresponding remote Riak is alive.
Instructions follow:

1. Stop Riak CS process
2. Backup configuration files
3. Uninstall Riak CS package
4. Install Riak CS 2.0 package
5. Update configuration
6. Start Riak CS

## Downgrading

### to CS 1.4

We have not yet tested downgrading from Riak CS 2.0 to Riak CS 1.4.

### to CS 1.5

To downgrade Riak CS 2.0 system to Riak CS 1.5 system, repeat
following instructions for each nodes:

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

Bitcask file format has changed between Riak 1.4 and 2.0. It supports
implicit upgrade of bitcask data files, while downgrading is not
supported. This is why downgrading requires a script to translate data
files. See also
[2.0 downgrade notes](https://github.com/basho/riak/wiki/2.0-downgrade-notes).


## Configuration Mapping Table between 1.5 and 2.0

Some important configuration changes happened between 1.5 and 2.0, Not
only translating items 1:1.

Value following with `=` is its default value.

The file name has changed from `app.config` and `vm.args` to only
`riak-cs.conf`, whose path haven't changed.

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

To disable access logging, just remove the line beginning with
`log.access.dir` from `riak-cs.conf`.

If `log_handlers` are defined in `app.config` or `advanced.config`,
Log handler's name should be changed due to WebMachine change as
follows:

```erlang
    {log_handlers, [
        {webmachine_access_log_handler, ["/var/log/riak-cs"]},
        {riak_cs_access_log_handler, []}
        ]},
```

This does not have to be changed if `log_handlers` is not defined in
`app.config` or `advanced.config`.

#### Items commented out in 2.0 by default

Thus all of them are undefined and disabled, except modules. `rewrite_module` and
`auth_module` will be commented out but the default value won't change from 1.5.
This is for indicating operators how to change these to OOS API.

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
`gc_paginated_indexes` - These items do not have corresponding item in
`riak-cs.conf`. If old behaviors are preferred, they must be in
`riak_cs` section of `advanced.config`.

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

Mostly same mapping with Riak or even should be same as copy-and-pasted from Riak.
