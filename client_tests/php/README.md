## installing composer

```
curl -s https://getcomposer.org/installer | php && mv -v composer.phar /usr/local/bin/composer
```

## configuring RiakCS

RiakCS to use is specified in a configuration file, `test_services.json`.
```javascript
{
    "includes" : ["_aws"],
    "services" : {
        "default_settings" : {
            "params" : {
                "key"    : "access_key",
                "secret" : "secret_key",
                "region" : "us-east-1",
                "base_url" : "http://s3.amazonaws.com",
                "curl.options": {"CURLOPT_PROXY" : "hostname:port"},
                "scheme" : "http"
            }
        }
    }
}
```

## running test
```
composer install --dev
./phpunit
```
