Welcome to Riak CS.

# Overview

Riak CS is an object storage system built on top of Riak. It
facilitates storing large objects in Riak and presents an
S3-compatible interface. It also provides multi-tenancy features such
as user accounts, authentication, access control mechanisms, and
per account usage reporting.

Below, you will find a link to the "fast track" directions for setting up and
using Riak CS. For more information, browse the following files:

- README:  this file
- LICENSE: the license under which Riak CS is released

# Compatible clients and libraries

Any client or library that faithfully implements the S3 API should also
work with Riak CS. We have tested quite a few and try to make adjustments as
necessary to support as many as possible. Feel free to open an issue if there
is an issue using a particular library.

The following is a sample of the clients and libraries that have been
tested with Riak CS:

- [s3cmd](https://github.com/s3tools/s3cmd)
- [s3curl](http://aws.amazon.com/code/128)
- [boto](https://github.com/boto/boto)
- [erlcloud](https://github.com/basho/erlcloud)
- [AWS Java SDK](http://aws.amazon.com/sdk-for-java/)
- [AWS Ruby SDK](http://aws.amazon.com/sdk-for-ruby/)
- [Fog](http://fog.io/)

## Administrative interface

The Riak CS administrative interface is accessible via HTTP just like
the object storage interface. All functions can be accessed under the
`/riak-cs/` resource. The administrative interface features the
following capabilities:

- Create user accounts
- Modify user account status or generate new user account credentials
- List all user accounts or a specific user account
- Report the network or storage usage of a user account
- Get performance statistics of the system operations
- Ping the Riak CS node to verify it is alive and functional

In the default mode of operation the administrative interface
accepts requests on the same IP address and port number as the
object storage requests and requires each request to be properly authenticated.

It is also possible to have administrative requests handled on a
separate IP address and port number. This allows the system to be
setup so that administrative requests are handled on a private
interface. The `admin_ip` and `admin_port` configuration options are
used to control this behavior. Additionally, it is also possible to
disable administrative request authentication using the
`admin_auth_enabled` option. Setting this option to `false` disables
request authentication for administrative commands. This option
should be used with /caution/ and it is only recommended when the
administrative requests are handled by a private interface that only
system administrators may access.

## Repo organization

Riak CS uses the [git-flow](http://nvie.com/posts/a-successful-git-branching-model/)
branching model and we have the default
branch on this repo set to `develop` so that when you browse here on
GitHub you always see the latest changes and to serve as a reminder
when opening pull requests for new features.

All releases are tagged off of the `master` branch so to access a
specific release from the git repo simply checkout the appropriate
tag. *e.g.* For the *1.3.0* release use the following command:
`git checkout 1.3.0`.

# Where to find more

Below, you'll find a direct link to a guide to quickly getting started
with Riak CS. For more information about Riak CS please visit our
[docs](http://docs.basho.com/riakcs/latest/).

# Riak CS Fast Track

http://docs.basho.com/riakcs/latest/tutorials/fast-track/

# Contributing to Riak CS

Basho encourages contributions to Riak CS from the community. Here is
how to get started.

- Fork the appropriate repos that are affected by your
  change.
- Make your changes and run the test suite. (see below)
- Commit your changes and push them to your fork.
- Open pull-requests for the appropriate projects. **Note:** Riak CS
  development uses the git-flow model for branching and most
  pull-requests should be targeted against the `develop` branch.
- Basho engineers will review your pull-request, suggest changes,
  and merge it when it is ready.

## How to Report a Bug

Please open a GitHub issue that fully describes the bug and be sure
to include steps on how to reproduce.

## Testing

To make sure your patch works, be sure to run the test suite in each
modified sub-project, and dialyzer from the top-level project to
detect static code errors.

To run the QuickCheck properties included with Riak CS,
download QuickCheck Mini: http://quviq.com/downloads.htm NOTE: Some
properties that require features in the Full version will fail.

Also see the *Testing* section of the Riak CS repo
[wiki](https://github.com/basho/riak_cs/wiki) for more
testing options and guidance.

### Running unit tests

The unit tests for each subproject can be run with `make` or
`rebar` like so:

```
make test
```

```
./rebar skip_deps=true eunit
```

### Running dialyzer

Dialyzer performs static analysis of the code to discover defects,
edge-cases and discrepancies between type specifications and the
actual implementation.

Dialyzer requires a pre-built code analysis table called a PLT.
Building a PLT is expensive and can take up to 30 minutes on some
machines.  Once built, you generally want to avoid clearing or
rebuilding the PLT unless you have had significant changes in your
build (a new version of Erlang, for example).

Dialyzer can be run with `make`:

```
make dialyzer
```

### `riak_test` modules

Riak CS has a number of integration and regression tests in the
`/riak_test/` subdirectory. Please refer `README.md` under the directory.
