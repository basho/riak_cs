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

count=$(ls */dev 2> /dev/null | wc -l)
if [ "$count" -ne "0" ]
then
    for rel in */dev; do
        vsn=$(dirname "$rel")
        echo " - Initializing $RTCS_DEST_DIR/$vsn"
        mkdir -p "$RTCS_DEST_DIR/$vsn"
        cp -p -P -R "$rel" "$RTCS_DEST_DIR/$vsn"
    done
else
    # This is useful when only testing with 'current'
    # The repo still needs to be initialized for current
    # and we don't want to bomb out if */dev doesn't exist
    touch $RTCS_DEST_DIR/.current_init
    echo "No devdirs found. Not copying any releases."
fi

cd $RTCS_DEST_DIR
git init

## Some versions of git and/or OS require these fields
git config user.name "Riak Test"
git config user.email "dev@basho.com"
git config --local core.excludesfile global_dot_gitignore_should_be_ignored

git add .
git commit -a -m "riak_test init" > /dev/null
echo " - Successfully completed initial git commit of $RTCS_DEST_DIR"
