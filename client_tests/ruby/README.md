# Riak CS AWS Ruby SDK Tests

## Dependencies

Install [Bundler](http://gembundler.com/):

```bash
$ gem install bundler
```

Install dependencies:

```bash
$ bundle --gemfile client_tests/ruby/Gemfile --path vendor
```

## Configuration

Copy the configuration file in `client_tests/ruby/conf/s3.yml.sample` to
`client_tests/ruby/conf/s3.yml` and add your `access_key_id` and
`secret_access_key`. You may also need to edit the `proxy_uri` setting if
Riak CS isn't running locally.

```yaml
access_key_id:  "YOWNMEHYXLXIGJS5AZ5U"
secret_access_key: "UkDFRI3O_Vi4vYXqNTsjQCymvcHfzXUErRKgwg=="
proxy_uri: "http://localhost:8080"
```

## Execution

```bash
$ cd client_tests/ruby && bundle exec rake spec
```
