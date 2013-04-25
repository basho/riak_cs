# Riak CS AWS PHP SDK Tests

## Dependencies

**Note**: The test is currently only known to work on php 5.4.x.

Install [Composer](https://getcomposer.org/):

```bash
$ curl -s https://getcomposer.org/installer | php && mv -v composer.phar /usr/local/bin/composer
```

**Note**: If you attempt to install Composer on PHP 5.3.x, you may need to add
`-d detect_unicode=Off` as an argument to the `php` command above.

Install dependencies:

```bash
$ composer install --dev
```

## Execution

```bash
$ cd client_tests/php && ./phpunit
```
