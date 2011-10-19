#!/usr/bin/env python

from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from boto.s3.key import Key
import unittest, time

KEY_ID="0"
SECRET_KEY="9372f58b69b05001"
HOST="127.0.0.1"
PORT=80
data = file("/dev/random").read(1024)


class ParityWithPrototypeTest(unittest.TestCase):
    def setUp(self):
        self.bucket_name = "parity-test"
        self.key_name = "parity-test"
        self.conn = S3Connection(KEY_ID, SECRET_KEY, is_secure=False, 
                                 host=HOST, port=PORT, debug=True, 
                                 calling_format=OrdinaryCallingFormat())


    def test_1_create_bucket(self):
        self.conn.create_bucket(self.bucket_name)

    def test_2_list_buckets(self):
        self.assertIn(self.bucket_name, 
                      [b.name for b in self.conn.get_all_buckets()])        

    def test_3_put_object(self):
        bucket = self.conn.get_bucket(self.bucket_name)
        k = Key(bucket)
        k.key = self.key_name
        k.set_contents_from_string("parity-test")

    def test_4_get_object(self):
        bucket = self.conn.get_bucket(self.bucket_name)
        k = Key(bucket)
        k.key = self.key_name        
        self.assertEqual(k.get_contents_as_string(), "parity-test")

    def test_5_list_bucket(self):
        bucket = self.conn.get_bucket(self.bucket_name)
        self.assertIn(self.key_name,
                      [k.key for k in bucket.get_all_keys()])

    def test_6_delete_object(self):
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
