#!/bin/bash

# just bail out if things go south
set -e

# You need to use this script once to build a set of devrels for prior
# releases of Riak (for mixed version / upgrade testing). You should
# create a directory and then run this script from within that directory.
# I have ~/test-releases that I created once, and then re-use for testing.
#
# See rtdev-setup-releases.sh as an example of setting up mixed version layout
# for testing.

# Different versions of Riak were released using different Erlang versions,
# make sure to build with the appropriate version.

# This is based on my usage of having multiple Erlang versions in different
# directories. If using kerl or whatever, modify to use kerl's activate logic.
# Or, alternatively, just substitute the paths to the kerl install paths as
# that should work too.

R15B01=${R15B01:-$HOME/erlang/R15B01-64}
R16B02=${R16B02:-$HOME/erlang/R16B02-64}
: ${RTCS_DEST_DIR:="$HOME/rt/riak_cs"}

checkbuild()
{
    ERLROOT=$1

    if [ ! -d $ERLROOT ]; then
        echo -n "$ERLROOT cannot be found, install kerl? [y|N]: "
        read ans
        if [[ $ans == n || $ans == N ]]; then
            exit 1
        fi
    fi
}

kerl()
{
    RELEASE=$1
    BUILDNAME=$2

    if [ ! -x kerl ]; then
        curl -O https://raw.github.com/spawngrid/kerl/master/kerl; chmod a+x kerl
    fi

    ./kerl build $RELEASE $BUILDNAME
    ./kerl install $BUILDNAME $HOME/$BUILDNAME
}

build()
{
    SRCDIR=$1
    ERLROOT=$2

    if [ ! -d $ERLROOT ]; then
        BUILDNAME=`basename $ERLROOT`
        RELEASE=`echo $BUILDNAME | awk -F- '{ print $2 }'`
        kerl $RELEASE $BUILDNAME
    fi

    echo
    echo "Building $SRCDIR"
    cd $SRCDIR

    RUN="env PATH=$ERLROOT/bin:$ERLROOT/lib/erlang/bin:$PATH \
             C_INCLUDE_PATH=$ERLROOT/usr/include \
             LD_LIBRARY_PATH=$ERLROOT/usr/lib \
             DEVNODES=8"
    echo $RUN
    $RUN make -j 8 && $RUN make -j devrel
    cd ..
}

setup()
{
    SRCDIR=$1
    cd $SRCDIR
    VERSION=$SRCDIR
    echo " - Copying devrel to $RTCS_DEST_DIR/$VERSION "
    mkdir -p $RTCS_DEST_DIR/$VERSION/
    cp -p -P -R dev $RTCS_DEST_DIR/$VERSION/
    ## echo " - Writing $RTCS_DEST_DIR/$VERSION/VERSION"
    ## echo -n $VERSION > $RTCS_DEST_DIR/$VERSION/VERSION
    cd $RTCS_DEST_DIR
    echo " - Adding $VERSION to git state of $RTCS_DEST_DIR"
    git add $VERSION
    git commit -a -m "riak_test adding version $VERSION" ## > /dev/null 2>&1
}

download()
{
  URI=$1
  FILENAME=`echo $URI | awk -F/ '{ print $8 }'`
  if [ ! -f $FILENAME ]; then
    wget $URI
  fi
}

checkbuild $R15B01
checkbuild $R16B02


if env | grep -q 'RIAK_CS_EE_DEPS='
then
    echo "RIAK_CS_EE_DEPS is set to \"$RIAK_CS_EE_DEPS\"."
    echo "This script if for OSS version."
    echo "unset RIAK_CS_EE_DEPS or use script for ee build."
    exit 1
fi

echo "Download and build OSS package..."
download http://s3.amazonaws.com/downloads.basho.com/riak-cs/1.5/1.5.4/riak-cs-1.5.4.tar.gz

tar -xf riak-cs-1.5.4.tar.gz
build "riak-cs-1.5.4" $R15B01
