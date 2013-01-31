#!/bin/bash

: ${RTCS_DEST_DIR:="$HOME/rt/riak_cs"}

cwd=$(pwd)
cd $RTCS_DEST_DIR
git reset HEAD --hard
git clean -fd
rm -rf $RTCS_DEST_DIR/current
mkdir $RTCS_DEST_DIR/current
cd $cwd
cp -p -P -R dev $RTCS_DEST_DIR/current
cd $RTCS_DEST_DIR
git add .
git commit -a -m "riak_test init" --amend

