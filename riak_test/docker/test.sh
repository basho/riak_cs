#!/bin/bash

trap 'echo "[ERROR] $0:$LINENO \"$BASH_COMMAND\"" ; exit 1' ERR

VSN=$@
RIAK_LIB_VSN=$1
ST_LIB_VSN=$2
CS_LIB_VSN=$3

RIAK_VSN=${RIAK_LIB_VSN//-/.}
ST_VSN=${ST_LIB_VSN//-/.}
CS_VSN=${CS_LIB_VSN//-/.}

echo "Riak     : " ${RIAK_VSN}
echo "Stanchion: " ${ST_VSN}
echo "Riak CS  : " ${CS_VSN}

usage() {
    echo "usage: ./test.sh RIAK_LIB_VSN ST_LIB_VSN CS_LIB_VSN"
    exit 1
}

echo "Build package tester ${VSN}..."

RIAK_PKG_LOCAL=riak-ee-${RIAK_VSN}-1.el6.x86_64.rpm
ST_PKG_LOCAL=stanchion-${ST_VSN}-1.el6.x86_64.rpm
CS_PKG_LOCAL=riak-cs-ee-${CS_VSN}-1.el6.x86_64.rpm

echo "Riak     : " ${RIAK_LIB_VSN}
echo "Stanchion: " ${ST_LIB_VSN}
echo "Riak CS  : " ${CS_LIB_VSN}

echo "Riak     : " ${RIAK_PKG_LOCAL}
echo "Stanchion: " ${ST_PKG_LOCAL}
echo "Riak CS  : " ${CS_PKG_LOCAL}


# rpm riak-cs-1.5.4.114.g9b7ca08-1
# /usr/lib64/riak-cs/lib/riak_cs-1.5.4-114-g9b7ca08

cp riak.advanced.config.tmpl riak.advanced.config
sed -i s:@@CS_LIB_VSN@@:${CS_LIB_VSN}:g riak.advanced.config

cp Dockerfile.tmpl Dockerfile
sed -i s:@@RIAK_PKG@@:$RIAK_PKG_LOCAL:g Dockerfile
sed -i s:@@ST_PKG@@:$ST_PKG_LOCAL:g     Dockerfile
sed -i s:@@CS_PKG@@:$CS_PKG_LOCAL:g     Dockerfile

TAG=package-tester-${CS_VSN}
echo "building package ${TAG}"

sudo docker build -t ${TAG} .

# sudo docker run \
#   --name riak-cs \
#   -p 8080:8080 -p 8098:8098 -p 8087:8087\
#   -v `pwd`/logs:/var/log/supervisor \
#   -v `pwd`/logs/riak:/var/log/riak \
#   -v `pwd`/logs/riak-cs:/var/log/riak-cs \
#   -v `pwd`/logs/stanchion:/var/log/stanchion \
#   -d ksauzz/riak-cs

rm -rf ${PWD}/logs/
mkdir -p ${PWD}/logs/
mkdir -p ${PWD}/logs/riak/
mkdir -p ${PWD}/logs/stanchion/
mkdir -p ${PWD}/logs/riak-cs/
chmod -R 777 ${PWD}/logs/

sudo docker \
    run -it \
    -v ${PWD}/logs:/var/log/supervisor \
    -v ${PWD}/logs/riak:/var/log/riak \
    -v ${PWD}/logs/riak-cs:/var/log/riak-cs \
    -v ${PWD}/logs/stanchion:/var/log/stanchion \
    ${TAG} \
    /root/confirm.sh

echo "Package built as ${TAG}"
