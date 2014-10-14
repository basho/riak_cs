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
             LD_LIBRARY_PATH=$ERLROOT/usr/lib"
    echo $RUN
    $RUN make && $RUN make devrel
    cd ..
}

download()
{
  URI=$1
  FILENAME=`echo $URI | awk -F/ '{ print $7 }'`
  if [ ! -f $FILENAME ]; then
    wget $URI
  fi
}

checkbuild $R15B01
checkbuild $R16B02

download http://s3.amazonaws.com/builds.basho.com/riak-cs/1.5/1.5.1/riak-cs-1.5.1.tar.gz

tar -xf riak-cs-1.5.1.tar.gz
build "riak-cs-1.5.1" $R15B01
