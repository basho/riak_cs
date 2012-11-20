<?php
require_once 'vendor/autoload.php';
Guzzle\Tests\GuzzleTestCase::setServiceBuilder(Aws\Common\Aws::factory($_SERVER['CONFIG']));
?>
