# Internal Tools

All these scripts are intended for special uses and should be used
with Basho support. This document does not describe detailed usage of
these scripts, but tries to describe high level idea on how they can
compensate and help Riak CS operations.

## SYNOPSIS

### `block_audit.erl` and `ensure_orphan_blocks.erl`

```sh
riak-cs escript /usr/lib/riak-cs/lib/riak_cs-x.y.z/priv/tools/internal/block_audit.erl --help
```

Orphan blocks scanner. In case inconsistencies between manifests and
blocks happen, especially manifests deleted and blocks remaining,
there are no other way to collect those blocks because garbage
collectors or CS manifest collection system cannot find them.

The scanner `block_audit.erl` compares full list of existing blocks
and full list of manifests under single bucket and figures out missing
manifests.

```sh
riak-cs escript /usr/lib/riak-cs/lib/riak_cs-x.y.z/priv/tools/internal/block_audit.erl --help
```

As `block_audit.erl` does scan by coverage operation (listkeys) and it
is R=1 in quorum sense, there could be replica
deficit. `ensure_orphan_blocks.erl` ensures that all UUIDs found by
`block_audit.erl` are really orphans by running get against all blocks
found with usual quorum way (R=quorum), which is expected to return
notfound.

Those blocks found are to be collected by `offline_delete.erl`.

##### Detailed Steps for scanning orphan blocks

1. List keys in `moss.buckets`
2. Iterate over all CS buckets (optionally ones listed in command line)
   1. List keys (streaming listkeys) on a `<<0b:...>>` bucket
      - *We MUST list keys of blocks at first* to avoid race from API calls
   2. Collect all blocks, as `{UUID, [list of existing block seqs]}`
   3. List manifest UUIDs on `<<0o:...>>` bucket (by streaming `csbucketfold`)
   4. Check existence of corresponding manifest by subtracting the two
      results of list keys
   5. Complete maybe orphaned UUID list
3. Filter out false-orphaned block UUIDs.
   This is because list keys (or csbucketfold) of manifest buckets will
   be done by R=1, then replica deficit (e.g. by force-replace) leads to
   false-positiveness for orphan block extraction at previous step.
   R=quorum (or R=all) manifest GETs is needed to avoid false-positive.
   1. Get block object and get CS key of manifests from user metadata
   2. Get manifest for the key and verify existence of corresponding manifest
   3. Remove block UUIDs if manifest exists from orphaned list

Usage (for debugging)

```
% riak-cs escript /path/to/priv/tools/internal/block_audit.erl  \
      -h 127.0.0.1 -p 10017 \
      -dd \
      -o $PWD/blocks.local
% riak-cs escript /path/to/priv/tools/internal/ensure_orphan_blocks.erl \
      -h 127.0.0.1 -p 10017 \
      -dd \
      -i $PWD/blocks.local -o $PWD/blocks2.local
```

### `select_gc_bucket.erl`

This script scans all keys in `riak-cs-gc` to identify collection of
deleted manifests that consumes a fair amount of storage space, by
specifying a certain threshold in bytes. Its default threshold is 5MB.

```
% riak-cs escript /var/lib/riak-cs/lib/riak_cs-x.y.z/priv/tools/internal/select_gc_bucket.erl --help
```

This script outputs a list of blocks written in a flat file, which are
part of large manfiests found whild scanning the `riak-cs-gc` bucket.

### `offline_delete.erl`

The collector deletes all such blocks in local disk to reclaim disk
and memory space. **Riak should be stopped before running this
script** as it skips vnodes and directly opens bitcask databases of
each vnodes. This is because skipping vnodes and involving no network
transfers may earn performance. This is useful especially when the
disk space is running out by garbage collector being so much behind to
the latest deleted objects, to clear such data.

Operators can run this script in a rolling stop way, but during the
deletion, AAE running in other nodes may repair deleted data - it is
recommended to stop AAE repair during this delete.

Note that this command should be invoked from `riak` command.

```
% riak escript /var/lib/riak-cs/lib/riak_cs-x.y.z/priv/tools/internal/offline_delete.erl --help
```

### `riak_cs_inspector.erl`

This is exactly an inspector that directly pokes Riak's PB API and
inspects all data made by Riak CS. This script does not change any
data stored in Riak.

```
% riak-cs escript /var/lib/riak-cs/lib/riak_cs-x.y.z/priv/tools/internal/riak_cs_inspector.erl --help
```

## Use Cases

### Block audit

* Blocks leaked ...

  * By a race condition between fullsync and garbage collections
  * By an RTQ drop of keys in `riak-cs-gc`

### Offline deletion

* Garbage Collection is far behind and disk space is running out
* Rolling downtime is acceptable

### Suspecious behaviour related to inconsistency

* Users and Buckets look inconsistent
