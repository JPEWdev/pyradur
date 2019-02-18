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
from .db import DBManager
from contextlib import contextmanager
import logging
import mmap
import os
import select
import socket
import threading

logger = logging.getLogger('pyradur.server')

class SockServer(object):
    class Client(IPC):
        def __init__(self, sock, addr, db, report_change):
            super().__init__(sock, logger=logger)
            self.addr = addr
            self.buffer = []
            self.db = db
            self.shm_fd = -1
            self.shm = None
            self.cache = {}
            self.report_change = report_change

        def _do_close(self):
            self.close_shm()
            super()._do_close()

        def close_shm(self):
            if self.shm is not None:
                self.shm.close()
            self.shm = None

            if self.shm_fd != -1:
                os.close(self.shm_fd)
            self.shm_fd = -1

        def mark_change(self, var, key):
            k = (var, key)
            if k in self.cache:
                self.cache[k].status = SLOT_OUT_OF_DATE

        def _send_response(self, response):
            self.send_message({'response': response})

        def _add_slot(self, req):
            slot = req.get('slot', None)
            if slot is not None and self.shm is not None:
                logger.debug('Added slot %d', slot)
                k = (req['var'], req['key'])
                self.cache[k] = SHMSlot(slot, self.shm, logger=logger)

        def _db_op(self, var, key, op):
            try:
                db = self.db.get_db(var)
                try:
                    op(db)
                    return [STATUS_OK]
                except KeyError:
                    logger.debug('No key %s.%s', var, key)
                    return [STATUS_NO_KEY, key]
            except KeyError:
                return [STATUS_NO_VAR, var]

        def _process_get(self, m, fds):
            def op(db):
                response['value'] = db[key]

            var = m['var']
            key = m['key']

            response = {'seq': m['seq']}
            response['status'] = self._db_op(var, key, op)

            self._send_response(response)
            self._add_slot(m)

        def _process_set(self, m, fds):
            def op(db):
                db[key] = m['value']

            var = m['var']
            key = m['key']

            self._db_op(var, key, op)
            self._add_slot(m)
            self.report_change(self, var, key)

        def _process_del(self, m, fds):
            def op(db):
                del db[key]

            var = m['var']
            key = m['key']

            self._db_op(var, key, op)
            self._add_slot(m)
            self.report_change(self, var, key)

        def _process_shm(self, m, fds):
            self.close_shm()

            self.shm_size = m['size']

            if self.shm_size > 0:
                self.shm_fd = os.dup(fds[0])

                self.shm = mmap.mmap(self.shm_fd, self.shm_size)

            self._send_response({
                'seq': m['seq'],
                'status': [STATUS_OK]
                })
            logger.debug("shm fd is now %d", self.shm_fd)

        def _process_release(self, m, fds):
            var = m['var']
            key = m['key']

            k = (var, key)

            if k in self.cache:
                logger.debug('Released slot %d', self.cache[k].slot)
                self.cache[k].status = SLOT_UNUSED
                del self.cache[k]

        def _process_release_all(self, m, fds):
            if self.shm is not None:
                self.shm[:] = bytearray(self.shm_size)
            self.cache = {}

        def _process_sync(self, m, fds):
            self._send_response({
                'seq': m['seq'],
                'status': [STATUS_OK]
                })

        def _process_validate_var(self, m, fds):
            response = {'seq': m['seq']}
            response['status'] = self._db_op(m['var'], None, lambda db: None)

            self._send_response(response)

        def handle_poll(self, event):
            if event.read:
                self.process_receive({
                    'get': self._process_get,
                    'set': self._process_set,
                    'del': self._process_del,
                    'shm': self._process_shm,
                    'release': self._process_release,
                    'release-all': self._process_release_all,
                    'sync': self._process_sync,
                    'validate-var': self._process_validate_var,
                    })

    class Event(object):
        def __init__(self, fd, read, hup):
            self.fd = fd
            self.read = bool(read)
            self.hup = bool(hup)

    class EPoll(object):
        def __init__(self):
            self.epoll = select.epoll()

        def register(self, fd):
            self.epoll.register(fd, select.EPOLLIN)

        def unregister(self, fd):
            self.epoll.unregister(fd)

        def poll(self, timeout):
            return [SockServer.Event(fd, events & select.EPOLLIN, events & select.EPOLLHUP)
                    for fd, events in self.epoll.poll(timeout)]

        def fileno(self):
            return self.epoll.fileno()

    def __init__(self, sock_path, db=None):
        self.sock_path = sock_path
        if db is None:
            self.db = DBManager()
        else:
            self.db = db
        self.clients = {}

        try:
            os.unlink(self.sock_path)
        except OSError:
            pass

        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.setblocking(False)
        self.sock.bind(self.sock_path)
        self.sock.listen(10)

        for p in (self.EPoll,):
            try:
                self.poll = p()
                break
            except AttributeError:
                pass
        else:
            raise Exception("No suitable poll interface found")

        self.poll.register(self.sock.fileno())

        self.done = threading.Event()
        self.done.set()

        self._suspended = 0
        self._suspended_cond = threading.Condition()

    def close(self):
        self.sock.close()

        for client in self.clients.values():
            client.close()

    def _report_change(self, source, var, key):
        for c in self.clients.values():
            if c is not source:
                c.mark_change(var, key)

    def _handle_poll_events(self, events):
        for e in events:
            if e.fd == self.sock.fileno():
                try:
                    conn, addr = self.sock.accept()
                    logger.debug('New client %d, %s', conn.fileno(), addr)

                    self.clients[conn.fileno()] = self.Client(conn, addr, self.db, self._report_change)

                    self.poll.register(conn.fileno())
                except socket.timeout:
                    pass

            elif e.fd in self.clients:
                client = self.clients[e.fd]
                try:
                    client.handle_poll(e)
                except (BrokenPipeError, ConnectionResetError):
                    pass

                if e.hup or client.eof:
                    logging.debug('Client %d disconnected', e.fd)
                    self.poll.unregister(e.fd)
                    client.close()
                    del self.clients[e.fd]

    def get_fd(self):
        return self.poll.fileno()

    def handle_event(self):
        events = self.poll.poll(0)
        self._handle_poll_events(events)

    def suspend(self):
        with self._suspended_cond:
            self._suspended += 1

    def resume(self):
        with self._suspended_cond:
            if self._suspended > 0:
                self._suspended -= 1
            self._suspended_cond.notify_all()

    @contextmanager
    def suspended(self):
        self.suspend()
        try:
            yield None
        finally:
            self.resume()

    def service_actions(self):
        pass

    def handle_request(self):
        events = self.poll.poll(0)
        self._handle_poll_events(events)
        return bool(events)

    def serve_forever(self, poll_interval=0.5):
        self.keep_serving = True
        self.done.clear()

        try:
            while self.keep_serving:
                events = self.poll.poll(poll_interval)

                retry = False
                with self._suspended_cond:
                    while self._suspended:
                        self._suspended_cond.wait()
                        retry = True

                if retry:
                    continue

                self._handle_poll_events(events)
                self.service_actions()
        finally:
            self.done.set()

    def shutdown(self):
        self.keep_serving = False
        self.done.wait()


