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

import array
import copy
import json
import logging
import os
import socket
import weakref

logger = logging.getLogger('pyradur.ipc')

MAX_FDS = 1
MAX_MESSAGE = 4096

STATUS_OK = 'ok'
STATUS_NO_VAR = 'no_var'
STATUS_NO_KEY = 'no_key'

class IPC(object):
    def __init__(self, sock, logger=logger):
        self.sock = sock
        self.recv_buffer = []
        self.recv_fds = []
        self._finalized = weakref.finalize(self, self.close)
        self.logger = logger

    def __del__(self):
        for fd in self.recv_fds:
            os.close(fd)

    def close(self):
        self.sock.close()

    def send_message(self, r, fds=[]):
        if fds:
            r = copy.copy(r)
            r['fds'] = len(fds)
        msg = json.dumps(r)
        self.logger.debug('sending message %s, %s', msg, fds)
        ret = self.sock.sendmsg([(msg + '\n').encode('utf-8')], [(socket.SOL_SOCKET, socket.SCM_RIGHTS, array.array("i", fds))])

    def _recv(self, buflen):
        recv_fds = array.array("i")

        buf, ancdata, flags, addr = self.sock.recvmsg(buflen, socket.CMSG_SPACE(MAX_FDS * recv_fds.itemsize))

        for cmsg_level, cmsg_type, cmsg_data in ancdata:
            if (cmsg_level == socket.SOL_SOCKET and cmsg_type == socket.SCM_RIGHTS):
                # Append data, ignoring any truncated integers at the end.
                recv_fds.frombytes(cmsg_data[:len(cmsg_data) - (len(cmsg_data) % recv_fds.itemsize)])
                self.recv_fds.extend(list(recv_fds))
                self.logger.debug("Received fds: %s, %s", list(recv_fds), self.recv_fds)

        return buf

    def process_receive(self, handlers):
        buf = self._recv(MAX_MESSAGE)

        new_messages = buf.decode('utf-8').splitlines(True)

        # Check for messages that span the buffer boundary
        if self.recv_buffer and not self.recv_buffer[-1].endswith('\n'):
            self.recv_buffer[-1] += new_messages.pop(0)

        self.recv_buffer.extend(new_messages)

        while self.recv_buffer and self.recv_buffer[0].endswith('\n'):
            s = self.recv_buffer.pop(0).rstrip()
            message = json.loads(s)

            num_fds = message.get('fds', 0)

            if len(self.recv_fds) < num_fds:
                raise Exception("Not enough file descriptors. Want %d, have %d" % (num_fds, len(num_fds)))

            # If the handler needs to keep around a file descriptor, it must
            # dup them since they will be closed later
            message_fds = self.recv_fds[:num_fds]
            self.recv_fds = self.recv_fds[num_fds:]

            self.logger.debug('Got message: %s, %s', s, message_fds)

            for k, v in handlers.items():
                if k in message:
                    v(message[k], message_fds)

            # Close all received fds
            for fd in message_fds:
                os.close(fd)

