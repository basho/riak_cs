<?php
/*
---------------------------------------------------------------------

Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.

This file is provided to you under the Apache License,
Version 2.0 (the "License"); you may not use this file
except in compliance with the License.  You may obtain
a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

---------------------------------------------------------------------
*/
require_once 'vendor/autoload.php';
use Guzzle\Http\Client;

function newServiceBuilder() {
    $user = creat_user();
    return Aws\Common\Aws::factory($_SERVER['CONFIG'], array(
        'key' => $user['key_id'],
        'secret' => $user['key_secret'],
        'curl.options' => array('CURLOPT_PROXY' => 'localhost:' . cs_port())
    ));
}

function creat_user() {
    $name = uniqid('riakcs-');
    $client = new Client('http://localhost:' . cs_port());
    $request = $client->put('/riak-cs/user',
                    array('Content-Type' => 'application/json'),
                    "{\"name\":\"{$name}\", \"email\":\"{$name}@example.com\"}");
    return $request->send()->json();
}

function cs_port() {
    return getenv('CS_HTTP_PORT') ? getenv('CS_HTTP_PORT') : 8080;
}


Guzzle\Tests\GuzzleTestCase::setServiceBuilder(newServiceBuilder());

?>
