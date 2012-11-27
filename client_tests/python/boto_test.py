#!/usr/bin/env python

from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from boto.s3.key import Key
import httplib, json
import unittest, time, uuid

def create_user(host, port, name, email):
    url = '/riak-cs/user'
    body = json.dumps({"email": email, "name": name})
    conn = httplib.HTTPConnection(host, port)
    headers = {"Content-Type": "application/json"}
    conn.request("POST", url, body, headers)
    response = conn.getresponse()
    data = response.read()
    conn.close()
    return json.loads(data)

class S3ApiVerificationTest(unittest.TestCase):
    host="127.0.0.1"
    port=8080

    user1=None
    user2=None

    SimpleAcl = "<AccessControlPolicy>" + \
                      "<Owner>" + \
                        "<ID>%s</ID>" + \
                        "<DisplayName>%s</DisplayName>" + \
                      "</Owner>" + \
                    "<AccessControlList>" + \
                      "<Grant>" + \
                        "<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">" + \
                          "<ID>%s</ID>" + \
                          "<DisplayName>%s</DisplayName>" + \
                        "</Grantee>" + \
                        "<Permission>%s</Permission>" + \
                       "</Grant>" + \
                    "</AccessControlList>" + \
                  "</AccessControlPolicy>"
    PublicReadAcl = "<AccessControlPolicy>" + \
                      "<Owner>" + \
                        "<ID>%s</ID>" + \
                        "<DisplayName>%s</DisplayName>" + \
                      "</Owner>" + \
                    "<AccessControlList>" + \
                      "<Grant>" + \
                        "<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\">" + \
                          "<URI>http://acs.amazonaws.com/groups/global/AllUsers</URI>" + \
                        "</Grantee>" + \
                          "<Permission>READ</Permission>" + \
                      "</Grant>" + \
                      "<Grant>" + \
                        "<Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\">" + \
                          "<ID>%s</ID>" + \
                          "<DisplayName>%s</DisplayName>" + \
                        "</Grantee>" + \
                        "<Permission>%s</Permission>" + \
                       "</Grant>" + \
                    "</AccessControlList>" + \
                  "</AccessControlPolicy>"


    def make_connection(self, user):
        return S3Connection(user['key_id'], user['key_secret'], is_secure=False,
                            host=self.host, port=self.port, debug=False,
                            calling_format=OrdinaryCallingFormat() )

    @classmethod
    def setUpClass(self):
        # Create test user so credentials don't have to be updated
        # for each test setup.
        # TODO: Once changes are in place so users can be deleted, use
        # userX@example.me for email addresses and clean up at the end of
        # the test run.
        self.maxDiff = 10000000000
        self.user1 = create_user(self.host, self.port, "user1", str(uuid.uuid4()) + "@example.me")
        self.user2 = create_user(self.host, self.port, "user2", str(uuid.uuid4()) + "@example.me")
        self.bucket_name = str(uuid.uuid4())
        self.key_name = str(uuid.uuid4())
        self.data = file("/dev/random").read(1024)

    def defaultAcl(self, user):
        return self.SimpleAcl % (user['id'], user['display_name'], user['id'], user['display_name'], 'FULL_CONTROL')

    def prAcl(self, user):
        return self.PublicReadAcl % (user['id'], user['display_name'], user['id'], user['display_name'], 'FULL_CONTROL')

    def setUp(self):
        self.conn = self.make_connection(self.user1)

    def test_auth(self):
        bad_user = json.loads('{"email":"baduser@example.me","display_name":"baduser","name":"user1","key_id":"bad_key","key_secret":"BadSecret","id":"bad_canonical_id"}')
        conn = self.make_connection(bad_user)
        self.assertRaises(S3ResponseError, conn.get_canonical_user_id)

    def test_create_bucket(self):
        self.conn.create_bucket(self.bucket_name)
        self.assertIn(self.bucket_name,
                      [b.name for b in self.conn.get_all_buckets()])

    def test_put_object(self):
         bucket = self.conn.get_bucket(self.bucket_name)
         k = Key(bucket)
         k.key = self.key_name
         k.set_contents_from_string(self.data)
         self.assertEqual(k.get_contents_as_string(), self.data)
         self.assertIn(self.key_name,
                       [k.key for k in bucket.get_all_keys()])

    def test_delete_object(self):
        bucket = self.conn.get_bucket(self.bucket_name)
        k = Key(bucket)
        k.key = self.key_name
        k.delete()
        self.assertNotIn(self.key_name,
                         [k.key for k in bucket.get_all_keys()])

    # TODO: Uncomment this test once bucket deletion issue is resolved.
    # def test_delete_bucket(self):
    #      bucket = self.conn.get_bucket(self.bucket_name)
    #      bucket.delete()
    #      self.assertNotIn(self.bucket_name,
    #                       [b.name for b in self.conn.get_all_buckets()])

    def test_get_bucket_acl(self):
        # self.conn.create_bucket(self.bucket_name)
        # self.assertIn(self.bucket_name,
        #               [b.name for b in self.conn.get_all_buckets()])
         bucket = self.conn.get_bucket(self.bucket_name)
         self.assertEqual(bucket.get_acl().to_xml(), self.defaultAcl(self.user1))

    def test_set_bucket_acl(self):
         bucket = self.conn.get_bucket(self.bucket_name)
         bucket.set_canned_acl('public-read')
         self.assertEqual(bucket.get_acl().to_xml(), self.prAcl(self.user1))

    def test_get_object_acl(self):
         bucket = self.conn.get_bucket(self.bucket_name)
         k = Key(bucket)
         k.key = self.key_name
         k.set_contents_from_string(self.data)
         self.assertEqual(k.get_contents_as_string(), self.data)
         self.assertEqual(k.get_acl().to_xml(), self.defaultAcl(self.user1))

    def test_set_object_acl(self):
        bucket = self.conn.get_bucket(self.bucket_name)
        k = Key(bucket)
        k.key = self.key_name
        k.set_canned_acl('public-read')
        self.assertEqual(k.get_acl().to_xml(), self.prAcl(self.user1))

if __name__ == "__main__":
    unittest.main()
