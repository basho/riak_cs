# Test release packages

This directory contains some configuration file templates and a Dockerfile template which
will enable you to construct a Docker container to test riak_cs release packages on various
operating system flavors.

Currently the Dockerfile builds an image based on centos6

## Prequisites

- docker properly installed and ready
- Riak EE, Riak CS EE and Stanchion RPM packages

## Usage

Make sure the rpm files are in this directory. Docker assumes you have root, which
is why the test script uses sudo. Make sure your user id has sufficient permissions
in the sudoers file to execute the commands in the test shell script.

The versions following the test script are for the riak_ee, riak_cs and stanchion
RPMs. Please make sure they match the version strings encoded in the RPM filenames.

```
./test.sh 2.0.5 2.0.0 2.0.0
```
