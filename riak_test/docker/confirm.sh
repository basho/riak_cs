#!/bin/bash

trap 'echo "[ERROR] $0:$LINENO \"$BASH_COMMAND\"" ; exit 1' ERR

echo "starting all nodes"
riak start
riak-admin wait-for-service riak_kv riak@127.0.0.1
stanchion start
riak-cs start
echo "done."

riak ping
stanchion ping
riak-cs ping

riak version
stanchion version
riak-cs version

# which riak-cs-multibag
# echo `riak-cs-multibag list-bags`
# echo `riak-cs-multibag weight master 10`
# echo `riak-cs-multibag weight bag-A 10`
# echo `riak-cs-multibag weight`

echo "creating init user"

/usr/lib64/riak-cs/erts-5.10.3/bin/erl \
    -name n@127.0.0.1 -setcookie riak \
    -eval "io:format(\"~p\", [rpc:call('riak-cs@127.0.0.1', riak_cs_user, create_user, [\"name\", \"me@admin.com\", \"admin-key\",  \"admin-secret\"])])." \
    -s init stop

echo "\n... probably user created"

echo "sleeping ..."

riak-cs-admin gc set-leeway 2
riak-cs-admin gc batch
riak-cs-admin gc status
riak-cs-admin gc set-interval 5

S3CMD=s3cmd

${S3CMD} mb s3://test-bucket
${S3CMD} ls
${S3CMD} put *.rpm s3://test-bucket
${S3CMD} put -r /bin s3://test-bucket
${S3CMD} ls -r s3://test-bucket

riak-cs-admin access flush
riak-cs-admin storage batch
riak-cs-admin storage status

riak-cs-admin stanchion show
riak-cs-admin cleanup-orphan-multipart

CS_LOG_DIR=/var/log/riak-cs
touch -d "2016-03-01" ${CS_LOG_DIR}/console.log.old1
touch -d "2016-03-01" ${CS_LOG_DIR}/access.log.old1

riak-cs-debug -h
riak-cs-debug                  /tmp/rcs-debug-default.tar.gz
riak-cs-debug --log-mtime 2    /tmp/rcs-debug-log-mtime.tar.gz
riak-cs-debug --skip-accesslog /tmp/rcs-debug-log-skip-alog.tar.gz

echo "teardown"
${S3CMD} del -r -f s3://test-bucket
${S3CMD} -d rb s3://test-bucket
${S3CMD} -d ls

# /usr/lib/riak-cs/erts-5.9.1/bin/erl  -name n@127.0.0.1 -setcookie riak -eval "io:format(\"~p\", [rpc:call('riak-cs@127.0.0.1', code, ensure_loaded, [riak_repl_pb_api])])." -s init stop

riak-cs stop
riak-cs-debug                  /tmp/rcs-debug-stopped.tar.gz

for f in /tmp/rcs-debug-*.tar.gz; do
    echo "Extract "$f" to logs/riak-cs/..."
    b=$(basename $f .tar.gz)
    mkdir -p ${CS_LOG_DIR}/$b
    tar zxf $f -C ${CS_LOG_DIR}/$b
done

echo "looks like package test has passed."
