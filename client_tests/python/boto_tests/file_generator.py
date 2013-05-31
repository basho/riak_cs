#!/usr/bin/env python
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

import os

class FileGenerator(object):
    def __init__(self, gen, size=None):
        """
        Create a read-only file-like object that is driven by a generator.
        The file can only be read once. The file also supports a limited
        usage of `seek` and `tell`, mostly just to appease boto's use of
        those methods to determine file-size. The optional `size` parameter
        _must_ be used if you intend to use this object with boto. Currently,
        the total length of the generated values from `gen` must agree
        with `size`, no automatic truncation will be performed for you.

        Here are some example uses:

        ```
        # create a 1kb string first, to be used by our
        # file generator
        s = ''.join('a' for _ in xrange(1024))
        # the generator to drive the file, 1MB (1KB * 1024)
        gen = (s for _ in xrange(1024))
        fg = FileGenerator(gen, 1024 ** 2)

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
        self.gen = gen
        self.size = size
        self.pos = 0

        self.closed = False
        self.buf = ''

    def close(self):
        self.closed = True
        return None

    def tell(self):
        return self.pos

    def seek(self, offset, whence=None):
        if whence == os.SEEK_END and offset == 0:
            self.pos = self.size
        else:
            self.pos = 0
        return None

    def read(self, max_size=None):
        if not max_size:
            return self.buf + ''.join(list(self.gen))
        else:
            if self.buf:
                self.buf = self.buf[max_size:]
                return self.buf[:max_size]
            else:
                try:
                    data = self.gen.next()
                    self.buf = data[max_size:]
                    return data[:max_size]
                except StopIteration:
                    return ''

