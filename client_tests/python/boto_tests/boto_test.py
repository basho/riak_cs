#!/usr/bin/env python
# -*- coding: utf-8 -*-
## ---------------------------------------------------------------------
##
## Copyright (c) 2007-2013 Basho Technologies, Inc.  All Rights Reserved.
##
## This file is provided to you under the Apache License,
## Version 2.0 (the "License"); you may not use this file
## except in compliance with the License.  You may obtain
## a copy of the License at
##
##   http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing,
## software distributed under the License is distributed on an
## "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
## KIND, either express or implied.  See the License for the
## specific language governing permissions and limitations
## under the License.
##
## ---------------------------------------------------------------------
import os, httplib, json, unittest, uuid, md5
from cStringIO import StringIO

from file_generator import FileGenerator

from boto.exception import S3ResponseError
from boto.s3.connection import S3Connection, OrdinaryCallingFormat
from boto.s3.key import Key
from boto.utils import compute_md5
import boto


def setup_auth_scheme():
    auth_mech=os.environ.get('CS_AUTH', 'auth-v2')
    if not boto.config.has_section('s3'):
        boto.config.add_section('s3')
    if auth_mech == 'auth-v4':
        setup_auth_v4()
    else:
        setup_auth_v2()

def setup_auth_v4():
    print('Use AWS Version 4 authentication')
    if not boto.config.get('s3', 'use-sigv4'):
        boto.config.set('s3', 'use-sigv4', 'True')

def setup_auth_v2():
    print('Use AWS Version 2 authentication')
    if not boto.config.get('s3', 'use-sigv4'):
        boto.config.set('s3', 'use-sigv4', '')

setup_auth_scheme()

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
def upload_multipart(bucket, key_name, parts_list, metadata={}, policy=None):
    upload = bucket.initiate_multipart_upload(key_name, metadata=metadata,
                                              policy=policy)
    for index, val in enumerate(parts_list):
        upload.upload_part_from_file(val, index + 1)
    result = upload.complete_upload()
    return upload, result


class S3ApiVerificationTestBase(unittest.TestCase):
    host="127.0.0.1"
    try:
        port=int(os.environ['CS_HTTP_PORT'])
    except KeyError:
        port=8080

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
                            host="s3.amazonaws.com", debug=False,
                            proxy="127.0.0.1", proxy_port=self.port,
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

    def test_put_object_with_trailing_slash(self):
        bucket = self.conn.create_bucket(self.bucket_name)
        key_name_with_slash = self.key_name + "/"
        k = Key(bucket)
        k.key = key_name_with_slash
        k.set_contents_from_string(self.data)
        self.assertEqual(k.get_contents_as_string(), self.data)
        self.assertIn(key_name_with_slash,
                      [k.key for k in bucket.get_all_keys()])

    def test_delete_object(self):
        bucket = self.conn.create_bucket(self.bucket_name)
        k = Key(bucket)
        k.key = self.key_name
        k.delete()
        self.assertNotIn(self.key_name,
                         [k.key for k in bucket.get_all_keys()])

    def test_delete_objects(self):
        bucket = self.conn.create_bucket(self.bucket_name)
        keys = ['0', '1', u'Unicodeあいうえお', '2', 'multiple   spaces']
        keys.sort()
        for key in keys:
            k = Key(bucket)
            k.key = key
            k.set_contents_from_string(key)

        all_keys = [k.key for k in bucket.get_all_keys()]
        all_keys.sort()
        self.assertEqual(keys, all_keys)
        result = bucket.delete_keys(keys)

        self.assertEqual(keys, [k.key for k in result.deleted])
        self.assertEqual([], result.errors)
        result = bucket.delete_keys(['nosuchkeys'])
        self.assertEqual([], result.errors)
        self.assertEqual(['nosuchkeys'], [k.key for k in result.deleted])
        all_keys = [k.key for k in bucket.get_all_keys()]
        self.assertEqual([], all_keys)

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
    def multipart_md5_helper(self, parts, key_suffix=u''):
        key_name = unicode(str(uuid.uuid4())) + key_suffix
        stringio_parts = [StringIO(p) for p in parts]
        expected_md5 = md5.new(''.join(parts)).hexdigest()
        bucket = self.conn.create_bucket(self.bucket_name)
        upload, result = upload_multipart(bucket, key_name, stringio_parts)
        key = Key(bucket, key_name)
        actual_md5 = md5_from_key(key)
        self.assertEqual(expected_md5, actual_md5)
        self.assertEqual(key_name, result.key_name)
        return upload, result

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

    def test_acl_is_set(self):
        parts = [str(uuid.uuid4()) for _ in xrange(5)]
        key_name = str(uuid.uuid4())
        stringio_parts = [StringIO(p) for p in parts]
        expected_md5 = md5.new(''.join(parts)).hexdigest()
        bucket = self.conn.create_bucket(self.bucket_name)
        upload = upload_multipart(bucket, key_name, stringio_parts,
                                  policy='public-read')
        key = Key(bucket, key_name)
        actual_md5 = md5_from_key(key)
        self.assertEqual(expected_md5, actual_md5)
        self.assertEqual(key.get_acl().to_xml(), self.prAcl(self.user1))

    def test_standard_storage_class(self):
        # Test for bug reported in
        # https://github.com/basho/riak_cs/pull/575
        bucket = self.conn.create_bucket(self.bucket_name)
        key_name = 'test_standard_storage_class'
        _never_finished_upload = bucket.initiate_multipart_upload(key_name)
        uploads = list(bucket.list_multipart_uploads())
        for u in uploads:
            self.assertEqual(u.storage_class, 'STANDARD')
        self.assertTrue(True)

    def test_upload_japanese_key(self):
        parts = ['this is part one', 'part two is just a rewording',
                 'surprise that part three is pretty much the same',
                 'and the last part is number four']
        self.multipart_md5_helper(parts, key_suffix=u'日本語サフィックス')

    def test_list_japanese_key(self):
        bucket = self.conn.create_bucket(self.bucket_name)
        key_name = u'test_日本語キーのリスト'
        _never_finished_upload = bucket.initiate_multipart_upload(key_name)
        uploads = list(bucket.list_multipart_uploads())
        for u in uploads:
            self.assertEqual(u.key_name, key_name)


def one_kb_string():
    "Return a 1KB string of all a's"
    return ''.join(['a' for _ in xrange(1024)])

def kb_gen_fn(num_kilobytes):
    s = one_kb_string()
    def fn():
        return (s for _ in xrange(num_kilobytes))
    return fn

def kb_file_gen(num_kilobytes):
    gen_fn = kb_gen_fn(num_kilobytes)
    return FileGenerator(gen_fn, num_kilobytes * 1024)

def mb_file_gen(num_megabytes):
    return kb_file_gen(num_megabytes * 1024)

def md5_from_file(file_object):
    m = md5.new()
    update_md5_from_file(m, file_object)
    return m.hexdigest()

def md5_from_files(file_objects):
    "note the plural"
    m = md5.new()
    for f in file_objects:
        update_md5_from_file(m, f)
    return m.hexdigest()

def update_md5_from_file(md5_object, file_object):
    "Helper function for calculating the hex md5 of a file-like object"
    go = True
    while go:
        byte = file_object.read(8196)
        if byte:
            md5_object.update(byte)
        else:
            go = False
    return md5_object

def remove_double_quotes(string):
    "remove double quote from a string"
    return string.replace('"', '')

class FileGenTest(unittest.TestCase):
    def test_read_twice(self):
        """ Read 2KB file and reset (seek to the head) and re-read 2KB """
        num_kb = 2
        f = kb_file_gen(num_kb)

        first1 = f.read(1024)
        self.assertEqual(1024, len(first1))
        first2 = f.read(1024)
        self.assertEqual(1024, len(first2))
        self.assertEqual(2048, f.pos)
        self.assertEqual('', f.read(1))
        self.assertEqual('', f.read(1))
        self.assertEqual(2048, f.pos)

        f.seek(0)
        self.assertEqual(0, f.pos)
        second1 = f.read(1024)
        self.assertEqual(1024, len(first1))
        second2 = f.read(1024)
        self.assertEqual(1024, len(second2))
        self.assertEqual(2048, f.pos)
        self.assertEqual('', f.read(1))
        self.assertEqual('', f.read(1))

class LargerFileUploadTest(S3ApiVerificationTestBase):
    "Larger, regular key uploads"

    def upload_helper(self, num_kilobytes):
        key_name = str(uuid.uuid4())
        bucket = self.conn.create_bucket(self.bucket_name)
        md5_expected = md5_from_file(kb_file_gen(num_kilobytes))
        file_obj = kb_file_gen(num_kilobytes)
        key = Key(bucket, key_name)
        key.set_contents_from_file(file_obj,
                                   md5=key.get_md5_from_hexdigest(md5_expected))
        self.assertEqual(md5_expected, remove_double_quotes(key.etag))

    def test_1kb(self):
        return self.upload_helper(1)

    def test_2kb(self):
        return self.upload_helper(2)

    def test_256kb(self):
        return self.upload_helper(256)

    def test_512kb(self):
        return self.upload_helper(512)

    def test_1mb(self):
        return self.upload_helper(1 * 1024)

    def test_4mb(self):
        return self.upload_helper(4 * 1024)

    def test_8mb(self):
        return self.upload_helper(8 * 1024)

    def test_16mb(self):
        return self.upload_helper(16 * 1024)

    def test_32mb(self):
        return self.upload_helper(32 * 1024)

class LargerMultipartFileUploadTest(S3ApiVerificationTestBase):
    """
    Larger, multipart file uploads - to pass this test,
    requires '{enforce_multipart_part_size, false},' entry at riak_cs's app.config
    """

    def upload_parts_helper(self, zipped_parts_and_md5s, expected_md5):
        key_name = str(uuid.uuid4())
        bucket = self.conn.create_bucket(self.bucket_name)
        upload = bucket.initiate_multipart_upload(key_name)
        key = Key(bucket, key_name)
        for idx, (part, md5_of_part) in enumerate(zipped_parts_and_md5s):
            upload.upload_part_from_file(part, idx + 1,
                                         md5=key.get_md5_from_hexdigest(md5_of_part))
        upload.complete_upload()
        actual_md5 = md5_from_key(key)
        self.assertEqual(expected_md5, actual_md5)

    def from_mb_list(self, mb_list):
        md5_list = [md5_from_file(mb_file_gen(m)) for m in mb_list]
        expected_md5 = md5_from_files([mb_file_gen(m) for m in mb_list])
        parts = [mb_file_gen(m) for m in mb_list]
        self.upload_parts_helper(zip(parts, md5_list), expected_md5)

    def test_upload_1(self):
        mb_list = [5, 6, 5, 7, 8, 9]
        self.from_mb_list(mb_list)

    def test_upload_2(self):
        mb_list = [10, 11, 5, 7, 9, 14, 12]
        self.from_mb_list(mb_list)

    def test_upload_3(self):
        mb_list = [15, 14, 13, 12, 11, 10]
        self.from_mb_list(mb_list)

class UnicodeNamedObjectTest(S3ApiVerificationTestBase):
    ''' test to check unicode object name works '''
    utf8_key_name = u"utf8ファイル名.txt"
    #                     ^^^^^^^^^ filename in Japanese

    def test_unicode_object(self):
        bucket = self.conn.create_bucket(self.bucket_name)
        k = Key(bucket)
        k.key = UnicodeNamedObjectTest.utf8_key_name
        k.set_contents_from_string(self.data)
        self.assertEqual(k.get_contents_as_string(), self.data)
        self.assertIn(UnicodeNamedObjectTest.utf8_key_name,
                      [obj.key for obj in bucket.list()])

    def test_delete_object(self):
        bucket = self.conn.create_bucket(self.bucket_name)
        k = Key(bucket)
        k.key = UnicodeNamedObjectTest.utf8_key_name
        k.delete()
        self.assertNotIn(UnicodeNamedObjectTest.utf8_key_name,
                         [obj.key for obj in bucket.list()])


class BucketPolicyTest(S3ApiVerificationTestBase):
    "test bucket policy"

    def test_no_policy(self):
        bucket = self.conn.create_bucket(self.bucket_name)
        bucket.delete_policy()
        try:                    bucket.get_policy()
        except S3ResponseError: pass
        else:                   self.fail()

    def create_bucket_and_set_policy(self, policy_template):
        bucket = self.conn.create_bucket(self.bucket_name)
        bucket.delete_policy()
        policy = policy_template % bucket.name
        bucket.set_policy(policy, headers={'content-type':'application/json'})
        return bucket

    def test_put_policy_invalid_ip(self):
        policy_template = '''
{"Version":"2008-10-17","Statement":[{"Sid":"Stmtaaa","Effect":"Allow","Principal":"*","Action":["s3:GetObjectAcl","s3:GetObject"],"Resource":"arn:aws:s3:::%s/*","Condition":{"IpAddress":{"aws:SourceIp":"0"}}}]}
'''
        try:
            self.create_bucket_and_set_policy(policy_template)
        except S3ResponseError as e:
            self.assertEqual(e.status, 400)
            self.assertEqual(e.reason, 'Bad Request')

    def test_put_policy(self):
        ### old version name
        policy_template = '''
{"Version":"2008-10-17","Statement":[{"Sid":"Stmtaaa","Effect":"Allow","Principal":"*","Action":["s3:GetObjectAcl","s3:GetObject"],"Resource":"arn:aws:s3:::%s/*","Condition":{"IpAddress":{"aws:SourceIp":"127.0.0.1/32"}}}]}
'''
        bucket = self.create_bucket_and_set_policy(policy_template)
        got_policy = bucket.get_policy()
        self.assertEqual(policy_template % bucket.name , got_policy)

    def test_put_policy_2(self):
        ### new version name, also regression of #911
        policy_template = '''
{"Version":"2012-10-17","Statement":[{"Sid":"Stmtaaa","Effect":"Allow","Principal":"*","Action":["s3:GetObjectAcl","s3:GetObject"],"Resource":"arn:aws:s3:::%s/*","Condition":{"IpAddress":{"aws:SourceIp":"127.0.0.1/32"}}}]}
'''
        bucket = self.create_bucket_and_set_policy(policy_template)
        got_policy = bucket.get_policy()
        self.assertEqual(policy_template % bucket.name, got_policy)

    def test_put_policy_3(self):
        policy_template = '''
{"Version":"somebadversion","Statement":[{"Sid":"Stmtaaa","Effect":"Allow","Principal":"*","Action":["s3:GetObjectAcl","s3:GetObject"],"Resource":"arn:aws:s3:::%s/*","Condition":{"IpAddress":{"aws:SourceIp":"127.0.0.1/32"}}}]}
'''
        try:
            self.create_bucket_and_set_policy(policy_template)
        except S3ResponseError as e:
            self.assertEqual(e.status, 400)
            self.assertEqual(e.reason, 'Bad Request')


    def test_ip_addr_policy(self):
        policy_template = '''
{"Version":"2008-10-17","Statement":[{"Sid":"Stmtaaa","Effect":"Deny","Principal":"*","Action":["s3:GetObject"],"Resource":"arn:aws:s3:::%s/*","Condition":{"IpAddress":{"aws:SourceIp":"%s"}}}]}
''' % ('%s', self.host)
        bucket = self.create_bucket_and_set_policy(policy_template)

        key_name = str(uuid.uuid4())
        key = Key(bucket, key_name)

        key.set_contents_from_string(self.data)
        bucket.list()
        try:
            key.get_contents_as_string()
            self.fail()
        except S3ResponseError as e:
            self.assertEqual(e.status, 404)
            self.assertEqual(e.reason, 'Object Not Found')

        policy = '''
{"Version":"2008-10-17","Statement":[{"Sid":"Stmtaaa","Effect":"Allow","Principal":"*","Action":["s3:GetObject"],"Resource":"arn:aws:s3:::%s/*","Condition":{"IpAddress":{"aws:SourceIp":"%s"}}}]}
''' % (bucket.name, self.host)
        self.assertTrue(bucket.set_policy(policy, headers={'content-type':'application/json'}))
        key.get_contents_as_string() ## throws nothing


    def test_invalid_transport_addr_policy(self):
        bucket = self.conn.create_bucket(self.bucket_name)
        key_name = str(uuid.uuid4())
        key = Key(bucket, key_name)
        key.set_contents_from_string(self.data)

        ## anyone may GET this object
        policy = '''
{"Version":"2008-10-17","Statement":[{"Sid":"Stmtaaa0","Effect":"Allow","Principal":"*","Action":["s3:GetObject"],"Resource":"arn:aws:s3:::%s/*","Condition":{"Bool":{"aws:SecureTransport":wat}}}]}
''' % bucket.name
        try:
            bucket.set_policy(policy, headers={'content-type':'application/json'})
        except S3ResponseError as e:
            self.assertEqual(e.status, 400)
            self.assertEqual(e.reason, 'Bad Request')

    def test_transport_addr_policy(self):
        bucket = self.conn.create_bucket(self.bucket_name)
        key_name = str(uuid.uuid4())
        key = Key(bucket, key_name)
        key.set_contents_from_string(self.data)

        ## anyone may GET this object
        policy = '''
{"Version":"2008-10-17","Statement":[{"Sid":"Stmtaaa0","Effect":"Allow","Principal":"*","Action":["s3:GetObject"],"Resource":"arn:aws:s3:::%s/*","Condition":{"Bool":{"aws:SecureTransport":false}}}]}
''' % bucket.name
        self.assertTrue(bucket.set_policy(policy, headers={'content-type':'application/json'}))
        key.get_contents_as_string()

        ## policy accepts anyone who comes with http
        conn = httplib.HTTPConnection(self.host, self.port)
        headers = { "Host" : "%s.s3.amazonaws.com" % bucket.name }
        conn.request('GET', ("/%s" % key_name) , None, headers)
        response = conn.getresponse()
        self.assertEqual(response.status, 200)
        self.assertEqual(response.read(), key.get_contents_as_string())

        ## anyone without https may not do any operation
        policy = '''
{"Version":"2008-10-17","Statement":[{"Sid":"Stmtaaa0","Effect":"Deny","Principal":"*","Action":"*","Resource":"arn:aws:s3:::%s/*","Condition":{"Bool":{"aws:SecureTransport":false}}}]}
''' % bucket.name
        self.assertTrue(bucket.set_policy(policy, headers={'content-type':'application/json'}))

        ## policy accepts anyone who comes with http
        conn = httplib.HTTPConnection(self.host, self.port)
        headers = { "Host" : "%s.s3.amazonaws.com" % bucket.name }
        conn.request('GET', ("/%s" % key_name) , None, headers)
        response = conn.getresponse()
        self.assertEqual(response.status, 403)
        self.assertEqual(response.reason, 'Forbidden')


class MultipartUploadTestsUnderPolicy(S3ApiVerificationTestBase):

    def test_small_strings_upload_1(self):
        bucket = self.conn.create_bucket(self.bucket_name)
        parts = ['this is part one', 'part two is just a rewording',
                 'surprise that part three is pretty much the same',
                 'and the last part is number four']
        stringio_parts = [StringIO(p) for p in parts]
        expected_md5 = md5.new(''.join(parts)).hexdigest()

        key_name = str(uuid.uuid4())
        key = Key(bucket, key_name)

        ## anyone may PUT this object
        policy = '''
{"Version":"2008-10-17","Statement":[{"Sid":"Stmtaaa0","Effect":"Allow","Principal":"*","Action":["s3:PutObject"],"Resource":"arn:aws:s3:::%s/*","Condition":{"Bool":{"aws:SecureTransport":false}}}]}
''' % bucket.name
        self.assertTrue(bucket.set_policy(policy, headers={'content-type':'application/json'}))

        upload = upload_multipart(bucket, key_name, stringio_parts)
        actual_md5 = md5_from_key(key)
        self.assertEqual(expected_md5, actual_md5)

        ## anyone without https may not do any operation
        policy = '''
{"Version":"2008-10-17","Statement":[{"Sid":"Stmtaaa0","Effect":"Deny","Principal":"*","Action":["s3:PutObject"],"Resource":"arn:aws:s3:::%s/*","Condition":{"Bool":{"aws:SecureTransport":false}}}]}
''' % bucket.name
        self.assertTrue(bucket.set_policy(policy, headers={'content-type':'application/json'}))

        try:
            upload = upload_multipart(bucket, key_name, stringio_parts)
            self.fail()
        except S3ResponseError as e:
            self.assertEqual(e.status, 403)
            self.assertEqual(e.reason, 'Forbidden')

class ObjectMetadataTest(S3ApiVerificationTestBase):
    "Test object metadata, e.g. Content-Encoding, x-amz-meta-*, for PUT/GET"

    metadata = {
        "Content-Disposition": 'attachment; filename="metaname.txt"',
        "Content-Encoding": 'identity',
        "Cache-Control": "max-age=3600",
        "Expires": "Tue, 19 Jan 2038 03:14:07 GMT",
        "mtime": "1364742057",
        "UID": "0",
        "with-hypen": "1"}

    updated_metadata = {
        "Content-Disposition": 'attachment; filename="newname.txt"',
        "Cache-Control": "private",
        "Expires": "Tue, 19 Jan 2038 03:14:07 GMT",
        "mtime": "2222222222",
        "uid": "0",
        "new-entry": "NEW"}

    def test_normal_object_metadata(self):
        key_name = str(uuid.uuid4())
        bucket = self.conn.create_bucket(self.bucket_name)
        key = Key(bucket, key_name)
        for k,v in self.metadata.items():
            key.set_metadata(k, v)
        key.set_contents_from_string("test_normal_object_metadata")
        self.assert_metadata(bucket, key_name)
        self.change_metadata(bucket, key_name)
        self.assert_updated_metadata(bucket, key_name)

    def test_mp_object_metadata(self):
        key_name = str(uuid.uuid4())
        bucket = self.conn.create_bucket(self.bucket_name)
        upload = upload_multipart(bucket, key_name, [StringIO("part1")],
                                  metadata=self.metadata)
        self.assert_metadata(bucket, key_name)
        self.change_metadata(bucket, key_name)
        self.assert_updated_metadata(bucket, key_name)

    def assert_metadata(self, bucket, key_name):
        key = Key(bucket, key_name)
        key.get_contents_as_string()

        self.assertEqual(key.content_disposition,
                         'attachment; filename="metaname.txt"')
        self.assertEqual(key.content_encoding, "identity")
        self.assertEqual(key.cache_control, "max-age=3600")
        # TODO: Expires header can be accessed by boto?
        # self.assertEqual(key.expires, "Tue, 19 Jan 2038 03:14:07 GMT")
        self.assertEqual(key.get_metadata("mtime"), "1364742057")
        self.assertEqual(key.get_metadata("uid"), "0")
        self.assertEqual(key.get_metadata("with-hypen"), "1")
        # x-amz-meta-* headers should be normalized to lowercase
        self.assertEqual(key.get_metadata("Mtime"), None)
        self.assertEqual(key.get_metadata("MTIME"), None)
        self.assertEqual(key.get_metadata("Uid"), None)
        self.assertEqual(key.get_metadata("UID"), None)
        self.assertEqual(key.get_metadata("With-Hypen"), None)

    def change_metadata(self, bucket, key_name):
        key = Key(bucket, key_name)
        key.copy(bucket.name, key_name, self.updated_metadata)

    def assert_updated_metadata(self, bucket, key_name):
        key = Key(bucket, key_name)
        key.get_contents_as_string()

        # unchanged
        self.assertEqual(key.get_metadata("uid"), "0")
        # updated
        self.assertEqual(key.content_disposition,
                         'attachment; filename="newname.txt"')
        self.assertEqual(key.cache_control, "private")
        self.assertEqual(key.get_metadata("mtime"), "2222222222")
        # removed
        self.assertEqual(key.content_encoding, None)
        self.assertEqual(key.get_metadata("with-hypen"), None)
        # inserted
        self.assertEqual(key.get_metadata("new-entry"), "NEW")
        # TODO: Expires header can be accessed by boto?
        # self.assertEqual(key.expires, "Tue, 19 Jan 2038 03:14:07 GMT")

class ContentMd5Test(S3ApiVerificationTestBase):
    def test_catches_bad_md5(self):
        '''Make sure Riak CS catches a bad content-md5 header'''
        key_name = str(uuid.uuid4())
        bucket = self.conn.create_bucket(self.bucket_name)
        key = Key(bucket, key_name)
        s = StringIO('not the real content')
        x = compute_md5(s)
        with self.assertRaises(S3ResponseError):
            key.set_contents_from_string('this is different from the md5 we calculated', md5=x)

    def test_bad_md5_leaves_old_object_alone(self):
        '''Github #705 Regression test:
           Make sure that overwriting an object using a bad md5
           simply leaves the old version in place.'''
        key_name = str(uuid.uuid4())
        bucket = self.conn.create_bucket(self.bucket_name)
        value = 'good value'

        good_key = Key(bucket, key_name)
        good_key.set_contents_from_string(value)

        bad_key = Key(bucket, key_name)
        s = StringIO('not the real content')
        x = compute_md5(s)
        try:
            bad_key.set_contents_from_string('this is different from the md5 we calculated', md5=x)
        except S3ResponseError:
            pass
        self.assertEqual(good_key.get_contents_as_string(), value)

class SimpleCopyTest(S3ApiVerificationTestBase):

    def create_test_object(self):
        bucket = self.conn.create_bucket(self.bucket_name)
        k = Key(bucket)
        k.key = self.key_name
        k.set_contents_from_string(self.data)
        self.assertEqual(k.get_contents_as_string(), self.data)
        self.assertIn(self.key_name,
                      [k.key for k in bucket.get_all_keys()])
        return k

    def test_put_copy_object(self):
        k = self.create_test_object()

        target_bucket_name = str(uuid.uuid4())
        target_key_name = str(uuid.uuid4())
        target_bucket = self.conn.create_bucket(target_bucket_name)

        target_bucket.copy_key(target_key_name, self.bucket_name, self.key_name)

        target_key = Key(target_bucket)
        target_key.key = target_key_name
        self.assertEqual(target_key.get_contents_as_string(), self.data)
        self.assertIn(target_key_name,
                      [k.key for k in target_bucket.get_all_keys()])

    def test_put_copy_object_from_mp(self):
        bucket = self.conn.create_bucket(self.bucket_name)
        (upload, result) = upload_multipart(bucket, self.key_name, [StringIO(self.data)])

        target_bucket_name = str(uuid.uuid4())
        target_key_name = str(uuid.uuid4())
        target_bucket = self.conn.create_bucket(target_bucket_name)

        target_bucket.copy_key(target_key_name, self.bucket_name, self.key_name)

        target_key = Key(target_bucket)
        target_key.key = target_key_name
        self.assertEqual(target_key.get_contents_as_string(), self.data)
        self.assertIn(target_key_name,
                      [k.key for k in target_bucket.get_all_keys()])

    def test_upload_part_from_non_mp(self):
        k = self.create_test_object()

        target_bucket_name = str(uuid.uuid4())
        target_key_name = str(uuid.uuid4())
        target_bucket = self.conn.create_bucket(target_bucket_name)
        start_offset=0
        end_offset=9
        target_bucket = self.conn.create_bucket(target_bucket_name)
        upload = target_bucket.initiate_multipart_upload(target_key_name)
        upload.copy_part_from_key(self.bucket_name, self.key_name, part_num=1,
                                  start=start_offset, end=end_offset)
        upload.complete_upload()
        print([k.key for k in target_bucket.get_all_keys()])

        target_key = Key(target_bucket)
        target_key.key = target_key_name
        self.assertEqual(self.data[start_offset:(end_offset+1)],
                         target_key.get_contents_as_string())

    def test_upload_part_from_mp(self):
        bucket = self.conn.create_bucket(self.bucket_name)
        key_name = str(uuid.uuid4())
        (upload, result) = upload_multipart(bucket, key_name, [StringIO(self.data)])

        target_bucket_name = str(uuid.uuid4())
        target_bucket = self.conn.create_bucket(target_bucket_name)
        start_offset=0
        end_offset=9
        upload2 = target_bucket.initiate_multipart_upload(key_name)
        upload2.copy_part_from_key(self.bucket_name, self.key_name, part_num=1,
                                   start=start_offset, end=end_offset)
        upload2.complete_upload()

        target_key = Key(target_bucket, key_name)
        self.assertEqual(self.data[start_offset:(end_offset+1)],
                         target_key.get_contents_as_string())

    def test_put_copy_from_non_existing_key_404(self):
        bucket = self.conn.create_bucket(self.bucket_name)

        target_bucket_name = str(uuid.uuid4())
        target_key_name = str(uuid.uuid4())
        target_bucket = self.conn.create_bucket(target_bucket_name)
        try:
            target_bucket.copy_key(target_key_name, self.bucket_name, 'not_existing')
            self.fail()
        except S3ResponseError as e:
            print e
            self.assertEqual(e.status, 404)
            self.assertEqual(e.reason, 'Object Not Found')
    
if __name__ == "__main__":
    unittest.main(verbosity=2)
