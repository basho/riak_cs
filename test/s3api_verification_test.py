#!/usr/bin/env python

from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from boto.s3.key import Key
import unittest, time, uuid

DISPLAY_NAME="kelly"
CANONICAL_ID="e05c1e14d17aeb65e9445f957e0fc28b892b40caf9ffb4fa5a8b1d454270544c"
KEY_ID="0FQIMM2WSEDA0GBRBQ9A"
SECRET_KEY="wJBF8Cg0LkvpFKTg_VaM-odkMYk2PoweqBlC9Q=="
HOST="127.0.0.1"
PORT=8080

class S3ApiVerificationTest(unittest.TestCase):

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

    def make_connection(self, key_id=KEY_ID, secret_key=SECRET_KEY):
        return S3Connection(key_id, secret_key, is_secure=False,
                            host=HOST, port=PORT, debug=False,
                            calling_format=OrdinaryCallingFormat() )

    @classmethod
    def setUpClass(self):
        # TODO: Create test user so credentials don't have to be updated
        # for each test setup.
        self.bucket_name = str(uuid.uuid4())
        self.key_name = str(uuid.uuid4())
        self.data = file("/dev/random").read(1024)
        self.default_acl = self.SimpleAcl % (CANONICAL_ID, DISPLAY_NAME, CANONICAL_ID, DISPLAY_NAME, 'FULL_CONTROL')
        self.pr_acl = self.PublicReadAcl % (CANONICAL_ID, DISPLAY_NAME, CANONICAL_ID, DISPLAY_NAME, 'FULL_CONTROL')

    def setUp(self):
        self.conn = self.make_connection()

    def test_auth(self):
        conn = self.make_connection("BAD_ID", "BAD_KEY")
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
         self.assertEqual(bucket.get_acl().to_xml(), self.default_acl)

    def test_set_bucket_acl(self):
         bucket = self.conn.get_bucket(self.bucket_name)
         bucket.set_canned_acl('public-read')
         self.assertEqual(bucket.get_acl().to_xml(), self.pr_acl)

    def test_get_object_acl(self):
         bucket = self.conn.get_bucket(self.bucket_name)
         k = Key(bucket)
         k.key = self.key_name
         k.set_contents_from_string(self.data)
         self.assertEqual(k.get_contents_as_string(), self.data)
         self.assertEqual(k.get_acl().to_xml(), self.default_acl)

    def test_set_object_acl(self):
         bucket = self.conn.get_bucket(self.bucket_name)
         k = Key(bucket)
         k.key = self.key_name
         k.set_canned_acl('public-read')
         self.assertEqual(k.get_acl().to_xml(), self.pr_acl)

if __name__ == "__main__":
    unittest.main()
