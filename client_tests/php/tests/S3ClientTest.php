<?php
use Guzzle\Plugin\Backoff\BackoffPlugin;

// http://docs.amazonwebservices.com/aws-sdk-php-2/latest/class-Aws.S3.S3Client.html
class S3ClientTest extends \Guzzle\Tests\GuzzleTestCase
{
    const ALL_USER_URL = "http://acs.amazonaws.com/groups/global/AllUsers";

    public function setUp()
    {
        $this->client = $this->getServiceBuilder()->get('s3');
        $this->client->getEventDispatcher()->removeSubscriber(BackoffPlugin::getExponentialBackoff()); // disable retry
        $this->bucket = randBucket();
    }

    public function tearDown()
    {
        if ( ! $this->client->doesBucketExist($this->bucket)) { return; }

        $objectKeys = $this->client->listObjects(array('Bucket' => $this->bucket))['Contents'];
        foreach ($objectKeys as $key) {
            $this->client->deleteObject(array('Bucket' => $this->bucket, 'Key' => $key['Key']));
        }
        $this->client->deleteBucket(array('Bucket' => $this->bucket));
    }

    public function testBucketNotExists()
    {
        $this->assertFalse($this->client->doesBucketExist($this->bucket));
        try {
            $this->client->getBucketAcl(array('Bucket' => $this->bucket))['Grants'];
            $this->fail();
        } catch (Aws\S3\Exception\S3Exception $e) { /* noop */ }

        try {
            $this->client->getObject(array('Bucket' => $this->bucket, 'Key' => randKey()));
            $this->fail();
        } catch (Aws\S3\Exception\S3Exception $e) { /* noop */ }
    }

    public function testCreateDeleteBucket()
    {
        $this->client->createBucket(array('Bucket' => $this->bucket));
        $this->assertTrue($this->client->doesBucketExist($this->bucket));

        $this->client->deleteBucket(array('Bucket' => $this->bucket));
        $this->assertFalse($this->client->doesBucketExist($this->bucket));
    }

    public function testPutGetDeleteObject()
    {
        $this->client->createBucket(array('Bucket' => $this->bucket));
        $key = randKey();

        $this->client->putObject(
            array(
                'Bucket' => $this->bucket,
                'Key' => $key,
                'Content-Type' => 'text/plain',
                'Body' => "This is a entity body."));
        $this->assertTrue($this->client->doesObjectExist($this->bucket, $key));

        $object = $this->client->getObject(array('Bucket' => $this->bucket, 'Key' => $key));
        $this->assertEquals('This is a entity body.',(string)$object['Body']);

        $this->client->deleteObject(array('Bucket' => $this->bucket, 'Key' => $key));
        $this->assertFalse($this->client->doesObjectExist($this->bucket, $key));

    }

    public function testBucketACL()
    {
        $this->client->createBucket(array('Bucket' => $this->bucket));
        $this->client->putBucketAcl(array('Bucket' => $this->bucket, 'ACL' => 'public-read'));
        $grants = $this->client->getBucketAcl(array('Bucket' => $this->bucket))['Grants'];

        $filtered_grants = array_filter($grants, function($item){
            return array_key_exists('URI', $item['Grantee'])
                && $item['Grantee']['URI'] == self::ALL_USER_URL
                && $item['Permission'] == 'READ';
        });
        $this->assertGreaterThanOrEqual(1, sizeof($filtered_grants));
    }

    public function testObjectACL()
    {
        $this->client->createBucket(array('Bucket' => $this->bucket));
        $key = randKey();
        $this->client->putObject(
            array(
                'Bucket' => $this->bucket,
                'Key' => $key,
                'Content-Type' => 'text/plain',
                'Body' => "This is a entity body."));
        $this->client->putObjectAcl(array('Bucket' => $this->bucket, 'Key' => $key,'ACL' => 'public-read'));
        $grants = $this->client->getObjectAcl(array('Bucket' => $this->bucket, 'Key' => $key))['Grants'];
        $filtered_grants = array_filter($grants, function($item){
            return array_key_exists('URI', $item['Grantee'])
                && $item['Grantee']['URI'] == self::ALL_USER_URL
                && $item['Permission'] == 'READ';
        });
        $this->assertGreaterThanOrEqual(1, sizeof($filtered_grants));
    }
}
function randBucket()
{
    return uniqid('aws-sdk-test-');
}

function randKey()
{
    return uniqid('key-');
}
?>
