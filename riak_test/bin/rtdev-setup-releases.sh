#!/bin/bash

# just bail out if things go south
set -e

# Creates a mixed-version directory structure for running riak_test
# using rtdev-mixed.config settings. Should be run inside a directory
# that contains devrels for prior Riak CS releases. Easy way to create this
# is to use the rtdev-build-releases.sh script

: ${RTCS_DEST_DIR:="$HOME/rt/riak_cs"}

rm -rf $RTCS_DEST_DIR
mkdir $RTCS_DEST_DIR
for rel in */dev; do
    vsn=$(dirname "$rel")
    mkdir "$RTCS_DEST_DIR/$vsn"
    cp -p -P -R "$rel" "$RTCS_DEST_DIR/$vsn"
done
cd $RTCS_DEST_DIR
git init
git add .
git commit -a -m "riak_test init"

