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

from .client import Client
import logging
import os
import weakref

logger = logging.getLogger('pyradur')

class Dict(object):
    _shared_clients = []

    @classmethod
    def _cleanup_client(cls, ref):
        cls._shared_clients.remove(ref)

    def __init__(self, sock_path, var, *, use_cache=True, share_connection=True):
        if share_connection:
            for client_ref in self._shared_clients:
                client = client_ref()
                try:
                    if client is not None and os.path.samefile(sock_path, client.sock_path):
                        logger.debug('Sharing existing client %s', id(client))
                        break
                except FileNotFoundError:
                    pass
            else:
                client = Client(sock_path, use_cache)
                self._shared_clients.append(weakref.ref(client, self._cleanup_client))
                logger.debug('New shared client %s', id(client))
        else:
            client = Client(sock_path, use_cache)
            logger.debug('New non-shared client %s', id(client))

        self.client = client
        self.var = var
        self.client.validate_var(var)

    def close(self):
        self.client = None

    def invalidate(self, key):
        return self.client.invalidate(self.var, key)

    def invalidate_all(self):
        return self.client.invalidate_all()

    def is_cached(self, key):
        return self.client.is_cached(self.var, key)

    def __getitem__(self, key):
        return self.client.get(self.var, key)

    def __setitem__(self, key, value):
        self.client.set(self.var, key, value)

    def __delitem__(self, key):
        self.client.delete(self.var, key)

    def __contains__(self, key):
        return self.client.contains(self.var, key)

    def get(self, key, default=None):
        return self.client.getdefault(self.var, key, default)

    def set(self, key, value):
        return self.client.set(self.var, key, value)

    def setdefault(self, key, default=None):
        return self.client.setdefault(self.var, key, default)

    def sync(self):
        self.client.sync()

