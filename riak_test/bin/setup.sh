#!/bin/sh
mkdir ~/test-releases
cd ~/test-releases
git clone git@github.com:basho/riak_cs.git
cp ./riak_cs/riak_test/bin/buildcs.sh ./
chmod 700 buildcs.sh
./buildcs.sh
export RT_TARGET_CURRENT=/tmp/rt/current
cd riak_cs
make && ./rebar rt_run config=rtdev test=repl_test
