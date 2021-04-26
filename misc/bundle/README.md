# Riak_cs bundle in a docker container

This is an example of dockerized riak_cs, riak and stanchion, running
in a docker-composed bundle.

## Building and running

With `docker-compose up` (or `make up`), you will get riak\_cs,
stanchion and riak images created and their containers
started.  Applications versions are defined by environment variables
`RIAK_VSN`, `RIAK_CS_VSN` and `STANCHION_VSN` (with 3.0.4, 3.0.0,
3.0.0, respectively, as defaults).

Riak CS is reachable at 172.18.0.4. An admin user can be then
created with

```
$ curl -sX POST 172.18.0.4:8080/riak-cs/user \
       -H 'Content-Type: application/json' \
       --data '{"name": "fafa", "email": "keke@fa.fa"}'
```

Images for riak_cs and stanchion are built from source, pulled from
repos at github.com/TI-Tokyo, while riak is installed from a deb package.

Containers can be stopped with `docker-compose down` (or `make down`).

If you want to inspect logs, run `docker-compose up` manually, without
a `--detach` option.

## Persisting state

Within containers, application data are bind-mounted to local
filesystem at locations `$RIAK_DATA`, `$RIAK_CS_DATA` and
`$STANCHION_DATA`. Unless explicitly set, these directories will be
created in "./data".

