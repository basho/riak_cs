# Riak CS Boto Python Tests

## Dependencies

* [virtualenv](http://www.virtualenv.org/en/latest/#installation) (I'm using 1.9.1)
* Python (I'm using 2.7.2)

## Configuration

Ensure that the Riak CS `advanced.config` has the following items:

```erlang
{anonymous_user_creation, true},
{enforce_multipart_part_size, false},
{max_buckets_per_user, 300},
{auth_v4_enabled, true},
```

## Execution

There is a `Makefile` that will set everything up for you, including all of the
dependencies. The `all` target will install everything and run the integration
tests:

```bash
make
```

Take a look at the `Makefile` for more detail about how the test is set up.
