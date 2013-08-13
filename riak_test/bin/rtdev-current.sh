#!/bin/bash

# just bail out if things go south
set -e

: ${RTCS_DEST_DIR:="$HOME/rt/riak_cs"}

echo "Making $(pwd) the current release:"
cwd=$(pwd)
echo -n " - Determining version: "
if [ -f $cwd/dependency_manifest.git ]; then
    VERSION=`cat $cwd/dependency_manifest.git | awk '/^-/ { print $NF }'`
else
    VERSION="$(git describe --tags)-$(git branch | awk '/\*/ {print $2}')"
fi
echo $VERSION
cd $RTCS_DEST_DIR
echo " - Resetting existing $RTCS_DEST_DIR"
git reset HEAD --hard > /dev/null 2>&1
git clean -fd > /dev/null 2>&1
rm -rf $RTCS_DEST_DIR/current
mkdir $RTCS_DEST_DIR/current
cd $cwd
echo " - Copying devrel to $RTCS_DEST_DIR/current"
cp -p -P -R dev $RTCS_DEST_DIR/current
echo " - Writing $RTCS_DEST_DIR/current/VERSION"
echo -n $VERSION > $RTCS_DEST_DIR/current/VERSION
cd $RTCS_DEST_DIR
echo " - Reinitializing git state"
git add .
git commit -a -m "riak_test init" --amend > /dev/null 2>&1
