# Riak CS 2.0.0 Release Notes

- based upon Riak 2.0
- configuration format moved line-by-line style

## Changes

- Changed default value of `gc_max_workers` from 5 to 2 with its name
  changed to `gc.max_workers` with migration to config format change.

## Notes on upgrading

- Changed webmachine's access log handler module name.

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
* [Riak CS 1.4 series release notes](https://github.com/basho/riak_cs/blob/develop/RELEASE-NOTES.md#riak-cs-145-release-notes)
* [Riak CS 1.5 series release notes](https://github.com/basho/riak_cs/blob/develop/RELEASE-NOTES.md#riak-cs-154-release-notes)
* Riak CS 2.0 release notes here

## Upgrading From Riak CS < 1.4

Basho only support 2 older versions from the latest release. For Riak
CS 2.0 case, only upgrades from 1.4 series and 1.5 series are
supported. Basically this pattern is out of the scope of this
document.

To upgrade to 2.0 from these versions, operators need another extra
step to upgrade the whole system to Riak CS 1.4.5 or Riak
CS 1.5.4. Upgrading to 1.5.4 would rather be recommented than
to 1.4.5. Thus underlying Riak would also better upgraded to 1.4
series - hopefully the latest 1.4.12.

Notes TBD: any caveats on upgrading to CS 1.4/1.5? or references.

# General Instructions

## Riak CS and Riak are in the same box

General Instructions are to repeat this overview on every node:

1. Stop Riak CS process
2. Stop Riak process
3. Backup all configuration files
4. Upgrade Riak CS software
5. Upgrade Riak software
6. Update Riak configuration
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
file called `riak.conf`, `riak_cs.config` and `stanchion.conf`.

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

If [riak_kv/1082](https://github.com/basho/riak_kv/pull/1082) is
merged as it is, new configuration will be

```
storage_backend = prefix_multi
cs_version = 20000
```

Note: otherwise (or early Riak 2.0) just requires same storage backend
configuration in `advanced.config` as required in CS 1.4 or 1.5.

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

If multibag is to be used, add multibag ebin path:

```erlang
{riak_kv, [
  {add_paths, ["/usr/lib/riak-cs/lib/riak_cs-2.0.0/ebin"]}
]}.
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

1. Stop Riak CS node
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
files.



## Configuration Mapping Table between 1.5 and 2.0

Some important configuration changes happened between 1.5 and 2.0, Not
only translating items 1:1.

Value following with `=` is its default value.

The file name has changed from `app.config` and `vm.args` to only
`riak_cs.conf`, whose path haven't changed.

### Riak CS

#### Items effective in the config file in 2.0 by default

Note: `storage.stats.schedule.$time` does not have any default
value but an example is added.

`riak_cs` section in app.config

|      1.5                           |        2.0                            |
|:-----------------------------------|:--------------------------------------|
|`{cs_ip, "127.0.0.1"}`              |`listener = 127.0.0.1:8080`            |
|`{cs_port, 8080}`                   |                                       |
|`{riak_ip, "127.0.0.1"}`            |`riak_host = 127.0.0.1:8087`           |
|`{riak_pb_port, 8087}`              |                                       |
|`{stanchion_ip, "127.0.0.1"}`       |`stanchion_host = 127.0.0.=:8085`      |
|`{stanchion_port, 8085 }`           |                                       |
|`{stanchion_ssl, false }`           |`stanchion_ssl = off`                  |
|`{anonymous_user_creation, false}`  |`anonymous_user_creation = off`        |
|`{admin_key, "admin-key"}`          |`admin.key = admin-key`                |
|`{admin_secret, "admin-secret"}`    |`admin.secret = admin-secret`          |
|`{cs_root_host, "s3.amazonaws.com"}`|`root_host = s3.amazonaws.com`         |
|`{connection_pools,[`               |                                       |
|` {request_pool, {128, 0} },`       |`pool.request.size = 128`              |
|                                    |`pool.request.overflow = 0`            |
|` {bucket_list_pool, {5, 0} }`      |`pool.list.size = 5`                   |
|                                    |`pool.list.overflow = 0`               |
|`{trust_x_forwarded_for, false}`    |`trust_x_forwarded_for = off` *          |
|`{leeway_seconds, 86400}`           |`gc.leeway_seconds = 24h`              |
|`{gc_interval, 900}`                |`gc.interval = 15m`                    |
|`{gc_retry_interval, 21600}`        |`gc.retry_interval = 6h`               |
|`{access_log_flush_factor, 1}`      |`access.stats.log.flush_factor = 1`    |
|`{access_log_flush_size, 1000000}`  |`access.stats.log.flush_size = 1000000`|
|`{access_archive_period, 3600}`     |`access.stats.archive_period = 1h`     |
|`{access_archiver_max_backlog, 2}`  |`access.stats.archiver.max_backlog = 2`|
|no explicit default                 |`access.stats.archiver.max_workers = 2`|
|`{storage_schedule, []}`            |`storage.stats.schedule.$time = "06:00`|
|`{storage_archive_period, 86400}`   |`storage.stats.archive_period = 1d`    |
|`{usage_request_limit, 744}`        |`storage.stats.request_limit = 31d`    |
|`{cs_version, 10300 }`              |`cs_version = 10300`                   |
|`{dtrace_support, false}`           |`dtrace_support = off`                 |

`webmachine` section in app.config

|      1.5                           |        2.0                            |
|:-----------------------------------|:--------------------------------------|
|`{server_name, "Riak CS"}`          |`server_name = Riak CS`                |

#### Items hidden out in 2.0 by default

The default value is not changed.

| 1.5                                   | 2.0                        |
|:--------------------------------------|:---------------------------|
|`{fold_objects_for_list_keys, true}`   |`fold_objects_for_list_keys`|
|`{n_val_1_get_requests, true}`         |`n_val_1_get_requests`      |
|`{gc_paginated_indexes, true},`        |`gc.pagenated_indexes`      |

#### Items commented out in 2.0 by default

Thus all of them are undefined and disabled, except modules. `rewrite_module` and
`auth_module` will be commented out but the default value won't change from 1.5.
This is for indicating operators how to change these to OOS API.

| 1.5                                   | 2.0                        |
|:--------------------------------------|:---------------------------|
|`{rewrite_module, riak_cs_s3_rewrite }`|`rewrite_module`            |
|`{auth_module, riak_cs_s3_auth },`     |`auth_module`               |
|`{admin_ip, "127.0.0.1"}`              |`admin.ip`                  |
|`{admin_port, 8000 }`                  |`admin.port`                |
|`{ssl, [`                              |                            |
|`  {certfile, "./etc/cert.pem"}`       |`ssl.certfile`              |
|`  {keyfile, "./etc/key.pem"}`         |`ssl.keyfile`               |

#### `webmachine` section

               {log_handlers, [
                               {webmachine_log_handler, ["./log"]},
                               {riak_cs_access_log_handler, []}
                              ]}
              ]},

They will be defined in to `log.access.dir` and the output directory will
just replaced. **TODO** we have to make kill switch for webmachine access logs.


### Items not supported in `riak_cs.conf` and should be written in `advanced.config`

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

## Miscellaneous

Common both in Stanchion and CS

## lager

Mostly same mapping with Riak or even should be same as copy-and-pasted from Riak.

### vm.args

Same as Riak. See default [erlang_vm.schema](https://github.com/basho/cuttlefish/blob/develop/priv/erlang_vm.schema)
for conversion.
