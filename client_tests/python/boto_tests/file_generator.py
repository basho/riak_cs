#!/usr/bin/env python
## ---------------------------------------------------------------------
##
## Copyright (c) 2007-2015 Basho Technologies, Inc.  All Rights Reserved.
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

import os

class FileGenerator(object):
    def __init__(self, gen_fn, size=None):
        """

        Create a read-only file-like object that is driven by a generator.
        The file can only be read once or sought to the head.  The file also
        supports a limited usage of `tell`, mostly just to appease boto's use of
        those methods to determine file-size. The optional `size` parameter
        _must_ be used if you intend to use this object with boto.
        To support seek to the head, generators are created from `gen_fn`
        once per seek to the head. The `gen_fn` must return the same
        generators for every call. Currently, the total length of the generated
        values from generators must agree with `size`, no automatic truncation
        will be performed for you.

        Here are some example uses:

        ```
        # create a 1kb string first, to be used by our
        # file generator
        s = ''.join('a' for _ in xrange(1024))
        # the generator to drive the file, 1MB (1KB * 1024)
        def fn():
            return (s for _ in xrange(1024))
        fg = FileGenerator(fn, 1024 ** 2)

        # now remember, each FileGenerator instance can only
        # be used once, so pretend a new one is created for each
        # of the following example:

        # copy it to 'real' file
        import shutil
        shutil.copyfileobj(fg, open('destination-file', 'w'))

        # compute the md5
        import md5

        m = md5.new()
        go = True
        while go:
            byte = fg.read(8196)
            if byte:
                m.update(byte)
            else:
                go = False
        print m.hexdigest()
        """
        self.gen_fn = gen_fn
        self.gen = gen_fn()
        self.size = size
        self.pos = 0

        self.closed = False
        self.buf = None

    def close(self):
        self.closed = True
        return None

    def tell(self):
        return self.pos

    def seek(self, offset, whence=None):
        if offset != 0:
            raise ValueError('offset must be ZERO')

        if whence == os.SEEK_END and offset == 0:
            self.pos = self.size
        else:
            self.pos = 0
            self.gen = self.gen_fn()
        return None

    def read(self, max_size=None):
        if not max_size:
            res = self.buf + ''.join(list(self.gen))
            self.pos = len(res)
            self.buf = ''
            return res
        else:
            if self.buf:
                res = self.buf[:max_size]
                self.buf = self.buf[max_size:]
                self.pos += len(res)
                return res
            else:
                try:
                    data = self.gen.next()
                    res = data[:max_size]
                    self.buf = data[max_size:]
                    self.pos += len(res)
                    return res
                except StopIteration:
                    return ''

