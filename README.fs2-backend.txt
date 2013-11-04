Configuring Riak KV's multi backend storage manager + FS2 backend for use with Riak CS
======================================================================================


Step 1: If you do not wish to use the experimental FS2 backend with
Riak & Riak CS, please return to the "README.multi-backend.txt"
instructions.

Step 2: Edit the Riak "app.config" file to configure the multi backend
        storage manager.  If you wish to use the experimental FS2
        backend with Riak & Riak CS, then please jump to the
        "README.fs2-backend.txt" instructions now.

    Step a: Identify the directory that will be storing Riak's data.

            Riak Linux data directory path:
                /var/lib/riak

            Riak Solaris data directory path:
                /opt/riak/data

            Riak developer source data directory path:
                ./data

    Step b: Use a text editor to edit the 'riak_kv' section of the Riak
            "app.config" file.

            Riak Linux config file path:
                /etc/riak/app.config

            Riak Solaris config file path:
                /opt/riak/etc/app.config

            Riak developer source config file path:
                ./rel/riak/etc/app.config

    Remove the default backend configuration statement:

        {storage_backend, riak_kv_bitcask_backend},

    ... with the following lines.  In all places where the string
    DATADIR appears, substitute the data directory path from step a.

        {storage_backend, riak_cs_kv_multi_backend},
        {multi_backend_prefix_list, [
                                     {<<"0b:">>, be_legacy_blocks},
                                     {<<"1b:">>, be_small},
                                     {<<"2b:">>, be_blocks}
                                    ]},
        {multi_backend_default, be_default},
        {multi_backend, [
          {be_default, riak_kv_eleveldb_backend, [
            {max_open_files, 50},
              {data_root, "DATADIR/leveldb"}
          ]},
          {be_legacy_blocks, riak_kv_bitcask_backend, [
            %% This 'data_root' path should match exactly the
            %% path used by the multi backend + Bitcask prior to
            %% using the FS2 backend.
            {data_root, "DATADIR/bitcask"},
            {block_size, 1200000} %% 1MB = 1048576 plus fudge
          ]},
          {be_small, riak_kv_eleveldb_backend, [
            {max_open_files, 50},
            {data_root, "DATADIR/leveldb.small"}
          ]},
          {be_blocks, riak_kv_fs2_backend, [
            {data_root, "DATADIR/fs2.large"},
            {block_size, 1200000} %% 1MB = 1048576 plus fudge
          ]}
        ]},

Step 3: Continue editing "app.config" and add the following to
        the 'riak_core' section of the config.  If
        'default_bucket_props' key already exists, then edit its list
        to include both the 'chash_keyfun' and the 'allow_mult'
        properties.

        {default_bucket_props, [
            {chash_keyfun,{riak_cs_lfs_utils,chash_cs_keyfun}},
            {allow_mult,true}
        ]},

Step 4: In the "app.config" file for Riak CS (e.g. the
        "/etc/riak-cs/app.config" file), add the following config item
        to the beginning of the 'riak_cs' application section:

        {small_object_divider, 102400},

        The value of this setting can be adjusted to fit your local
        environment.  The units of this configuration option is
        "bytes".  The default value is 128,000 bytes.

        As new files are uploaded to Riak CS, the
        'small_object_divider' setting is consulted to decide whether
        the blocks for that object should be stored in 'small' block
        storage (using a bucket prefix of <<"1b:">>) or in 'large'
        storage (using a bucket prefix of <<"2b:">>).  The choice of
        bucket prefix is detected by the multi backend in order to
        segregate block storage to the ELevelDB backend or FS2
        backend, respectively.

