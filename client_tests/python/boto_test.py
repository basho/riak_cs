#!/usr/bin/env python

import httplib, json, unittest, uuid, md5
from cStringIO import StringIO

from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from boto.s3.key import Key

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

# Take a boto 'Key' and returns a hex
# digest of the md5 (calculated by actually
# retrieving the bytes and calculating the md5)
def md5_from_key(boto_key):
    m = md5.new()
    for byte in boto_key:
        m.update(byte)
    return m.hexdigest()

# `parts_list` should be a list of file-like objects
def upload_multipart(bucket, key_name, parts_list):
    upload = bucket.initiate_multipart_upload(key_name)
    for index, val in enumerate(parts_list):
        upload.upload_part_from_file(val, index + 1)
    upload.complete_upload()
    return upload

class S3ApiVerificationTestBase(unittest.TestCase):
    host = "127.0.0.1"
    port = 8080

    user1 = None
    user2 = None

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
    def setUpClass(cls):
        # Create test user so credentials don't have to be updated
        # for each test setup.
        # TODO: Once changes are in place so users can be deleted, use
        # userX@example.me for email addresses and clean up at the end of
        # the test run.
        cls.maxDiff = 10000000000
        cls.user1 = create_user(cls.host, cls.port, "user1", str(uuid.uuid4()) + "@example.me")
        cls.user2 = create_user(cls.host, cls.port, "user2", str(uuid.uuid4()) + "@example.me")
        cls.bucket_name = str(uuid.uuid4())
        cls.key_name = str(uuid.uuid4())
        cls.data = file("/dev/urandom").read(1024)

    def defaultAcl(self, user):
        return self.SimpleAcl % (user['id'], user['display_name'], user['id'], user['display_name'], 'FULL_CONTROL')

    def prAcl(self, user):
        return self.PublicReadAcl % (user['id'], user['display_name'], user['id'], user['display_name'], 'FULL_CONTROL')

    def setUp(self):
        self.conn = self.make_connection(self.user1)

class BasicTests(S3ApiVerificationTestBase):
    def test_auth(self):
        bad_user = json.loads('{"email":"baduser@example.me","display_name":"baduser","name":"user1","key_id":"bad_key","key_secret":"BadSecret","id":"bad_canonical_id"}')
        conn = self.make_connection(bad_user)
        self.assertRaises(S3ResponseError, conn.get_canonical_user_id)

    def test_create_bucket(self):
        self.conn.create_bucket(self.bucket_name)
        self.assertIn(self.bucket_name,
                      [b.name for b in self.conn.get_all_buckets()])

    def test_put_object(self):
        bucket = self.conn.create_bucket(self.bucket_name)
        k = Key(bucket)
        k.key = self.key_name
        k.set_contents_from_string(self.data)
        self.assertEqual(k.get_contents_as_string(), self.data)
        self.assertIn(self.key_name,
                      [k.key for k in bucket.get_all_keys()])

    def test_delete_object(self):
        bucket = self.conn.create_bucket(self.bucket_name)
        k = Key(bucket)
        k.key = self.key_name
        k.delete()
        self.assertNotIn(self.key_name,
                         [k.key for k in bucket.get_all_keys()])

    def test_delete_bucket(self):
        bucket = self.conn.get_bucket(self.bucket_name)
        bucket.delete()
        self.assertNotIn(self.bucket_name,
                         [b.name for b in self.conn.get_all_buckets()])

    def test_get_bucket_acl(self):
        bucket = self.conn.create_bucket(self.bucket_name)
        self.assertEqual(bucket.get_acl().to_xml(), self.defaultAcl(self.user1))

    def test_set_bucket_acl(self):
        bucket = self.conn.get_bucket(self.bucket_name)
        bucket.set_canned_acl('public-read')
        self.assertEqual(bucket.get_acl().to_xml(), self.prAcl(self.user1))

    def test_get_object_acl(self):
        bucket = self.conn.create_bucket(self.bucket_name)
        k = Key(bucket)
        k.key = self.key_name
        k.set_contents_from_string(self.data)
        self.assertEqual(k.get_contents_as_string(), self.data)
        self.assertEqual(k.get_acl().to_xml(), self.defaultAcl(self.user1))

    def test_set_object_acl(self):
        bucket = self.conn.create_bucket(self.bucket_name)
        k = Key(bucket)
        k.key = self.key_name
        k.set_canned_acl('public-read')
        self.assertEqual(k.get_acl().to_xml(), self.prAcl(self.user1))

class MultiPartUploadTests(S3ApiVerificationTestBase):
    def multipart_md5_helper(self, parts):
        key_name = str(uuid.uuid4())
        stringio_parts = [StringIO(p) for p in parts]
        expected_md5 = md5.new(''.join(parts)).hexdigest()
        bucket = self.conn.create_bucket(self.bucket_name)
        upload = upload_multipart(bucket, key_name, stringio_parts)
        key = Key(bucket, key_name)
        actual_md5 = md5_from_key(key)
        self.assertEqual(expected_md5, actual_md5)

    def test_small_strings_upload_1(self):
        parts = ['this is part one', 'part two is just a rewording',
                 'surprise that part three is pretty much the same',
                 'and the last part is number four']
        self.multipart_md5_helper(parts)

    def test_small_strings_upload_2(self):
        parts = ['just one lonely part']
        self.multipart_md5_helper(parts)

    def test_small_strings_upload_3(self):
        parts = [str(uuid.uuid4()) for _ in xrange(100)]
        self.multipart_md5_helper(parts)

    def test_small_strings_upload_4(self):
        parts = [str(uuid.uuid4()) for _ in xrange(20)]
        self.multipart_md5_helper(parts)

if __name__ == "__main__":
    unittest.main()
