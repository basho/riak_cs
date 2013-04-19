# Riak CS Boto Python Tests

## Dependencies

Install [Pip](http://www.pip-installer.org/en/latest/):

```bash
$ curl -O https://raw.github.com/pypa/pip/master/contrib/get-pip.py
$ python get-pip.py
```

Install dependencies:

```bash
$ pip install boto
```

## Configuration

Ensure that the Riak CS `app.config` has `anonymous_user_creation` set to
`true`. If it was previously set to `false`, make sure that the `riak-cs`
service is restarted:

```bash
$ riak-cs restart
```

## Execution

```bash
$ cd client_tests/python && python boto_test.py
```
