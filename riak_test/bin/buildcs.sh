#!/bin/sh

# This script will build a Riak CS instance from source
basho_checkout_source() {
    git clone git@github.com:basho/riak_ee.git
    git clone git@github.com:basho/riak_cs.git
    git clone git@github.com:basho/stanchion.git
}

basho_make_all() {

	mkdir -p $RT_TARGET_CURRENT

	# build Riak EE
	cd riak_ee
	make devclean
	make
	make stagedevrel
	cd ..

	cp -a riak_ee/dev /tmp/rt/current

	# build stanchion
	cd stanchion
	make devclean
	make
	make devrel
	cp -R ./dev/stanchion /tmp/rt/current
	cd ..

	# build CS
	cd riak_cs
	make devclean
	make
	make rtdevrel
	#cp -R ./dev/riak_cs  /tmp/rt/current
	cd ..
	mkdir -p $RT_TARGET_CURRENT/stanchion/log
	for i in `seq 1 6`
	do
		mkdir -p $RT_TARGET_CURRENT/cs/rtdev$i/log
	done
	touch $RT_TARGET_CURRENT/stanchion/log/.keep
	for i in `seq 1 6`
	do
		touch $RT_TARGET_CURRENT/cs/rtdev$i/log/.keep
	done

	cwd=$(pwd) 
	cd $RT_TARGET_CURRENT
	git init
	git add .
	git commit -a -m "riak_test cs init"
	cd $cwd
}

basho_buildriakcs_proxyget() {
  basho_checkout_source
  basho_make_all
}

export RT_TARGET_CURRENT=/tmp/rt/current
export RT_TARGET_STANCHION=$RT_TARGET_CURRENT/stanchion
export RT_TARGET_CS=$RT_TARGET_CURRENT/cs

basho_buildriakcs_proxyget
