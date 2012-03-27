Configuring Riak KV's multi backend storage manager for use with Riak CS
========================================================================

TODO: Proofread all of these paths and steps.

Step 1: Copy the storage manager object code (a single BEAM file) from
        the Riak CS package to the Riak package's patch dir.

    Step a: Locate the multi backend storage manager object code.

            RCS Linux installation path:
                /usr/lib64/riak_cs/lib/riak_moss-*/ebin/riak_cs_kv_multi_backend.beam
            RCS Solaris installation path:
                /opt/riak_cs/lib/riak_moss-*/ebin/riak_cs_kv_multi_backend.beam

            Developer source (after a "make stage" or "make dev" compilation):
                ./rel/riak_cs/lib/riak_moss-*/ebin/riak_cs_kv_multi_backend.beam

    Step b: Locate the patch directory for Riak

            RCS Linux installation path:
                /usr/lib64/riak/lib/basho-patches

            RCS Solaris installation path:
                /opt/riak/lib/basho-patches

            Developer source (after a "make stage" or "make dev" compilation):
                ./rel/riak/lib/basho-patches

    Step c: Copy the BEAM file from step a to the directory in step b.
            For example:

                cp /usr/lib64/riak_cs/lib/riak_moss-*/ebin/riak_cs_kv_multi_backend.beam /usr/lib64/riak/lib/basho-patches

Step 2: Edit the Riak "app.config" file to configure the multi backend
        storage manager

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
        {multi_backend_prefix_list, [{<<"0b:">>, be_blocks}]},
        {multi_backend_default, be_default},
        {multi_backend, [
           % format: {name, module, [Configs]}
           {be_default, riak_kv_eleveldb_backend, [
             {max_open_files, 50},
             {data_root, "DATADIR/leveldb"}
           ]},
           {be_blocks, riak_kv_bitcask_backend, [
             {data_root, "DATADIR/bitcask"}
          ]}
         ]},
