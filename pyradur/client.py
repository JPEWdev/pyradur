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

from .ipc import IPC, STATUS_OK, STATUS_NO_VAR, STATUS_NO_KEY
from .shm import SHMSlot, SLOT_UNUSED, SLOT_OK, SLOT_OUT_OF_DATE
import logging
import mmap
import os
import socket
import tempfile

logger = logging.getLogger('pyradur.client')

class Client(IPC):
    class Cache(SHMSlot):
        def __init__(self, slot, value, shm):
            super().__init__(slot, shm, logger=logger)
            self.value = value

    def __init__(self, sock_path, use_cache=True):
        super().__init__(socket.socket(socket.AF_UNIX, socket.SOCK_STREAM), logger=logger)
        self.sock_path = sock_path
        self.sock.connect(self.sock_path)
        self.use_cache = use_cache
        self.seq = 0
        self.known_vars = set()

        if self.use_cache:
            self.cache = {}

            self.shm_fd, shm_path = tempfile.mkstemp()
            os.unlink(shm_path)

            os.ftruncate(self.shm_fd, mmap.PAGESIZE)

            self.shm = mmap.mmap(self.shm_fd, 0)

            self._send_shm_message()

    def close(self):
        if self.use_cache:
            self.shm.close()
            os.close(self.shm_fd)

        super().close()

    def validate_var(self, var):
        if var in self.known_vars:
            return

        self.send_message({
            'validate-var': {
                'seq': self._next_seq(),
                'var': var,
                }
            })

        self._wait_for_response()

        self.known_vars.add(var)

    def _send_shm_message(self):
        self.send_message({
            'shm': {
                'seq': self._next_seq(),
                'size': self.shm.size()
                }
            }, [self.shm_fd])

        # Must wait for response to ensure server sees the new map
        self._wait_for_response()

    def _grow_shm(self):
        new_size = self.shm.size() + mmap.PAGESIZE

        # Some systems don't allow resizing a mmap (e.g. FreeBSD), so we do do
        # it manually for consistency. This is safe because the size is only
        # ever increased
        os.ftruncate(self.shm_fd, new_size)
        old_shm = self.shm
        self.shm = mmap.mmap(self.shm_fd, 0)
        old_shm.close()

        for c in self.cache.values():
            c.shm = self.shm

        self._send_shm_message()

    def _next_seq(self):
        self.seq += 1
        return self.seq

    def _get_free_slot(self):
        s = chr(SLOT_UNUSED).encode('utf-8')

        self.shm.seek(0, os.SEEK_SET)
        slot = self.shm.find(s)

        if slot == -1:
            self._grow_shm()

            self.shm.seek(0, os.SEEK_SET)
            slot = self.shm.find(s)

            if slot == -1:
                raise Exception("Cannot find free slot after resizing?")

        return slot

    def _wait_for_response(self):
        def handle_response(m, fds):
            nonlocal response
            if m['seq'] == self.seq:
                response = m

        response = None

        while response is None:
            self.process_receive({
                'response': handle_response
                })

        code = response['status'][0]
        args = response['status'][1:]
        if code == STATUS_NO_VAR:
            raise NameError("name '%s' is not defined" % args[0])
        elif code == STATUS_NO_KEY:
            raise KeyError(args[0])
        elif code != STATUS_OK:
            raise Exception("Unknown status '%s' from server" % code)

        return response

    def _get_cache(self, var, key):
        k = (var, key)
        if k in self.cache:
            return self.cache[k]

        slot = self._get_free_slot()

        logger.debug("Adding %s at slot %d", k, slot)
        c = self.Cache(slot, None, self.shm)
        self.cache[k] = c

        return c

    def invalidate(self, var, key):
        if self.use_cache:
            k = (var, key)
            if k in self.cache:
                self.send_message({
                    'release': {
                        'seq': self._next_seq(),
                        'var': var,
                        'key': key,
                        }
                    })

                # The server is responsible for marking the state back to unused
                # once it is sure all other clients are done with it. The client
                # can't change the state otherwise it might get overwritten.
                del self.cache[k]

    def invalidate_all(self):
        if self.use_cache:
            self.send_message({
                'release-all': {
                    'seq': self._next_seq()
                    }
                })

            self.cache = {}

    def is_cached(self, var, key):
        if not self.use_cache:
            return False

        return (var, key) in self.cache

    def get(self, var, key):
        if self.use_cache:
            cache = self._get_cache(var, key)

            if cache.status != SLOT_OK:
                self.send_message({
                    'get': {
                        'seq': self._next_seq(),
                        'var': var,
                        'key': key,
                        'slot': cache.slot
                        }
                    })

                cache.value = self._wait_for_response()['value']
                cache.status = SLOT_OK

            value = cache.value
        else:
            self.send_message({
                'get': {
                    'seq': self._next_seq(),
                    'var': var,
                    'key': key,
                    }
                })
            value = self._wait_for_response()['value']

        if value is None:
            raise KeyError(key)

        return value

    def delete(self, var, key):
        m = {
            'seq': self._next_seq(),
            'var': var,
            'key': key,
            }

        if self.use_cache:
            cache = self._get_cache(var, key)
            cache.value = None
            cache.status = SLOT_OK
            m['slot'] = cache.slot

        self.send_message({'del': m})

    def contains(self, var, key):
        try:
            value = self.get(var, key)
            return True
        except KeyError:
            return False

    def getdefault(self, var, key, default=None):
        try:
            return self.get(var, key)
        except KeyError:
            return default

    def set(self, var, key, value):
        m = {
            'seq': self._next_seq(),
            'var': var,
            'key': key,
            'value': value,
            }

        if self.use_cache:
            cache = self._get_cache(var, key)
            cache.value = value
            cache.status = SLOT_OK
            m['slot'] = cache.slot

        self.send_message({'set': m})

    def setdefault(self, var, key, default=None):
        try:
            return self.get(var, key)
        except KeyError:
            self.set(var, key, default)
            return default

    def sync(self):
        m = {
            'seq': self._next_seq()
            }

        self.send_message({'sync': m})
        self._wait_for_response()

