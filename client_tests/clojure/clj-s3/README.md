# Riak CS clj-s3 Tests

## Overview

These tests are written using a modified copy of
[clj-aws-s3](https://github.com/weavejester/clj-aws-s3) at tag 0.3.10,
for S3 API call. On the other hand, Riak CS specific administrative API
call is implemented in

```bash
client_tests/clojure/clj-s3/src/java_s3_tests/user_creation.clj
```

New tests cases are added to:

```bash
client_tests/clojure/clj-s3/test/java_s3_tests/test/client.clj
```

The tests are written using the Clojure testing library
[midje](https://github.com/marick/Midje), which has
[great documentation](https://github.com/marick/Midje/wiki).

## Dependencies

Install [Leiningen](http://leiningen.org/):

```bash
$ curl -O https://raw.github.com/technomancy/leiningen/stable/bin/lein
$ mv lein ~/bin # Or some other directory in your $PATH
$ chmod 755 ~/bin/lein
```

Install dependencies:

```bash
$ lein deps
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
$ lein midje
```
