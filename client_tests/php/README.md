# Riak CS AWS PHP SDK Tests

## Dependencies

Install [Composer](https://getcomposer.org/):

```bash
$ curl -s https://getcomposer.org/installer | php && mv -v composer.phar /usr/local/bin/composer
```

Install dependencies:

```bash
$ composer install --dev
```

## Configuration

Copy the configuration file in `client_tests/php/test_services.json.sample` to
`client_tests/php/test_services.json` and add your `key` and `secret`. You may
also need to edit the `base_url` and  `curl.options` settings if Riak CS isn't
running locally.

```javascript
{
  "includes" : ["_aws"],
  "services" : {
    "default_settings" : {
      "params" : {
        "key"    : "YOWNMEHYXLXIGJS5AZ5U",
        "secret" : "UkDFRI3O_Vi4vYXqNTsjQCymvcHfzXUErRKgwg==",
        "region" : "us-east-1",
        "base_url" : "http://s3.amazonaws.com",
        "curl.options": {"CURLOPT_PROXY" : "localhost:8080"},
        "scheme" : "http"
      }
    }
  }
}
```

## Execution

```bash
$ cd client_tests/php && ./phpunit
```
