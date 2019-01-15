# MIT License
#
# Copyright (c) 2018-2019 Garmin International or its subsidiaries
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from pyradur import Dict
from pyradur.db import Sqlite3DB
from pyradur.server import SockServer
import tempfile
import threading
import unittest
import shutil
import os

class Common(unittest.TestCase):
    use_cache = True

    def _server_thread(self, event):
        self.server.db.add_db('var', Sqlite3DB(':memory:'))
        event.set()
        self.server.serve_forever()

    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix='pyradur-')
        self.addCleanup(shutil.rmtree, self.tempdir, ignore_errors=True)

        self.sock_path = os.path.join(self.tempdir, 'sock')

        self.server = SockServer(self.sock_path)
        self.sever_suspended = False
        try:
            event = threading.Event()
            self.server_thread = threading.Thread(target=self._server_thread, args=[event])
            self.server_thread.start()
            event.wait()

            self.addCleanup(self.server_thread.join)
            self.addCleanup(self.server.close)
            self.addCleanup(self.server.shutdown)
        except Exception as e:
            self.server.close()
            raise e

    def get_dict(self, name, share_connection=True):
        return Dict(self.sock_path, name, use_cache=self.use_cache, share_connection=share_connection)

    def test_basic_get_set(self):
        d = self.get_dict('var')
        d['foo'] = 'bar'
        self.assertEqual(d['foo'], 'bar')

        with self.assertRaises(KeyError):
            d['baz']

    def test_get_set_shared(self):
        a = self.get_dict('var')
        b = self.get_dict('var')
        a['foo'] = 'bar'
        self.assertEqual(b['foo'], 'bar')

    def test_get_set_nonshared(self):
        a = self.get_dict('var', share_connection=False)
        b = self.get_dict('var', share_connection=False)
        a['foo'] = 'bar'
        a.sync()
        self.assertEqual(b['foo'], 'bar')

        self.assertEqual(a.get('bat', 'baz'), 'baz')
        a.sync()
        self.assertFalse('baz' in b)

        a.set('test', 'blah')
        a.sync()
        self.assertEqual(b['test'], 'blah')

    def test_del_nonshared(self):
        a = self.get_dict('var', share_connection=False)
        b = self.get_dict('var', share_connection=False)

        a['foo'] = 'bar'
        a.sync()
        self.assertEqual(b['foo'], 'bar')

        del a['foo']
        a.sync()
        with self.assertRaises(KeyError):
            b['foo']

    def test_setdefault(self):
        a = self.get_dict('var', share_connection=False)
        b = self.get_dict('var', share_connection=False)

        self.assertEqual(a.setdefault('foo', 'bar'), 'bar')
        a.sync()
        self.assertEqual(b['foo'], 'bar')

    def test_server_suspend(self):
        a = self.get_dict('var', share_connection=False)
        a['foo'] = 'bar'

        with self.server.suspended():
            a['foo'] = 'test'

        a.sync()
        self.assertEqual(a['foo'], 'test')

    def test_contains(self):
        a = self.get_dict('var', share_connection=False)
        b = self.get_dict('var', share_connection=False)

        a['foo'] = 'bar'
        a.sync()
        self.assertTrue('foo' in b)
        self.assertFalse('bar' in b)

    def test_close(self):
        import gc

        a = self.get_dict('var')
        b = self.get_dict('var', share_connection=False)
        c = self.get_dict('var')

        a['foo'] = 'bar'
        a.sync()

        self.assertEqual(b['foo'], 'bar')
        self.assertEqual(c['foo'], 'bar')

        a.close()

        c['baz'] = 'bat'
        c.sync()

        self.assertEqual(b['baz'], 'bat')

        del c
        del a

        gc.collect()

        b['test'] = 'blah'


    def test_cache_grow(self):
        import mmap

        a = self.get_dict('var', share_connection=False)
        b = self.get_dict('var', share_connection=False)

        count = mmap.PAGESIZE * 2

        for i in range(count):
            key = 'foo%d' % i
            val = 'bar%d' % i
            a[key] = val
            self.assertEqual(a[key], val)

        a.sync()

        for i in range(count):
            key = 'foo%d' % i
            val = 'bar%d' % i
            self.assertEqual(a[key], val)
            self.assertEqual(b[key], val)

    def test_missing_var(self):
        a = self.get_dict('var')

        with self.assertRaises(NameError):
            b = self.get_dict('does-not-exist', share_connection=False)

        with self.assertRaises(NameError):
            b = self.get_dict('does-not-exist')

    def test_var_factory(self):
        def factory(name):
            return Sqlite3DB(':memory:')

        a = self.get_dict('var')

        self.server.db.set_db_factory(factory)

        b = self.get_dict('test1', share_connection=False)
        c = self.get_dict('test2')

    def test_cross_var(self):
        def factory(name):
            return Sqlite3DB(':memory:')

        self.server.db.set_db_factory(factory)

        a = self.get_dict('var', share_connection=False)
        b = self.get_dict('test', share_connection=False)

        a['foo'] = 'bar'
        a.sync()

        with self.assertRaises(KeyError):
            b['foo']

        b['foo'] = 'baz'
        b.sync()

        self.assertEqual(a['foo'], 'bar')
        self.assertEqual(b['foo'], 'baz')

class NoCacheTests(Common):
    use_cache = False

    def test_cached(self):
        a = self.get_dict('var', share_connection=False)
        b = self.get_dict('var', share_connection=False)

        a['foo'] = 'bar'
        a.sync()
        self.assertEqual(b['foo'], 'bar')
        self.assertFalse(b.is_cached('foo'))
        self.assertFalse(b.is_cached('not-present'))

        a['foo'] = 'test'
        b.invalidate('foo')
        self.assertFalse(b.is_cached('foo'))

        # Note: No sync should be necessary because of the invalidation
        self.assertEqual(b['foo'], 'test')

    def test_invalidate_all(self):
        a = self.get_dict('var', share_connection=False)
        b = self.get_dict('var', share_connection=False)

        a['foo'] = 'bar'
        a.sync()
        self.assertEqual(b['foo'], 'bar')
        self.assertFalse(b.is_cached('foo'))

        with self.server.suspended():
            a['foo'] = 'test'

            b.invalidate_all()
            self.assertFalse(b.is_cached('foo'))

        # Note: No sync should be necessary because of the invalidation
        self.assertEqual(b['foo'], 'test')

class CacheTests(Common):
    def test_cached(self):
        a = self.get_dict('var', share_connection=False)
        b = self.get_dict('var', share_connection=False)

        a['foo'] = 'bar'
        a.sync()
        self.assertEqual(b['foo'], 'bar')
        self.assertTrue(b.is_cached('foo'))
        self.assertFalse(b.is_cached('not-present'))

        with self.server.suspended():
            a['foo'] = 'test'
            self.assertEqual(b['foo'], 'bar')

            b.invalidate('foo')
            self.assertFalse(b.is_cached('foo'))

        # Note: No sync should be necessary because of the invalidation
        self.assertEqual(b['foo'], 'test')

    def test_invalidate_all(self):
        a = self.get_dict('var', share_connection=False)
        b = self.get_dict('var', share_connection=False)

        a['foo'] = 'bar'
        a.sync()
        self.assertEqual(b['foo'], 'bar')
        self.assertTrue(b.is_cached('foo'))

        with self.server.suspended():
            a['foo'] = 'test'
            self.assertEqual(b['foo'], 'bar')

            b.invalidate_all()
            self.assertFalse(b.is_cached('foo'))

        # Note: No sync should be necessary because of the invalidation
        self.assertEqual(b['foo'], 'test')



