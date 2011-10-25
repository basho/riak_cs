#!/usr/bin/env python

from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from boto.s3.key import Key
import unittest, time, uuid

KEY_ID="A854BE7021A625131CD4E35D7D6B7B4A9CFE9435"
SECRET_KEY="D5DE587D0122A2074BE91B9BBF8B9A3499283910"
HOST="127.0.0.1"
PORT=8080

class ParityWithPrototypeTest(unittest.TestCase):

    def make_connection(self, key_id=KEY_ID, secret_key=SECRET_KEY):
        return S3Connection(key_id, secret_key, is_secure=False, 
                            host=HOST, port=PORT, debug=True, 
                            calling_format=OrdinaryCallingFormat() )       
    @classmethod
    def setUpClass(self):
        self.bucket_name = str(uuid.uuid4())
        self.key_name = str(uuid.uuid4())
        self.data = file("/dev/random").read(1024)

    def setUp(self):
        self.conn = self.make_connection()

    def test_0_auth(self):
        conn = self.make_connection("BAD_ID", "BAD_KEY")
        self.assertRaises(S3ResponseError, conn.get_canonical_user_id)

    def test_1_create_bucket(self):
        self.conn.create_bucket(self.bucket_name)

    def test_2_list_buckets(self):
        self.assertIn(self.bucket_name, 
                      [b.name for b in self.conn.get_all_buckets()])        

    def test_3_put_object(self):
         bucket = self.conn.get_bucket(self.bucket_name)
         k = Key(bucket)
         k.key = self.key_name
         k.set_contents_from_string(self.data)

    def test_4_get_object(self):
         bucket = self.conn.get_bucket(self.bucket_name)
         k = Key(bucket)
         k.key = self.key_name        
         self.assertEqual(k.get_contents_as_string(), self.data)

    def test_5_list_bucket(self):
         bucket = self.conn.get_bucket(self.bucket_name)
         self.assertIn(self.key_name,
                       [k.key for k in bucket.get_all_keys()])

    def test_6_delete_objet(self):
        bucket = self.conn.get_bucket(self.bucket_name)
        k = Key(bucket)
        k.key = self.key_name                
        k.delete()

    def test_7_list_bucket(self):
         time.sleep(3) # XXX HACK
         bucket = self.conn.get_bucket(self.bucket_name)
         self.assertNotIn(self.key_name,
                          [k.key for k in bucket.get_all_keys()])

    def test_8_delete_bucket(self):
         bucket = self.conn.get_bucket(self.bucket_name)
         bucket.delete()

    def test_9_list_buckets(self):
         self.assertNotIn(self.bucket_name, 
                          [b.name for b in self.conn.get_all_buckets()]) 
               

if __name__ == "__main__":
    unittest.main()
