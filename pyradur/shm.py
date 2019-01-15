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

import os
import logging

logger = logging.getLogger('pyradur.shm')

# Note: The unused slot is carefully chosen to be 0 so that it can be
# efficiently cleared
SLOT_UNUSED = 0
SLOT_OK = ord('+')
SLOT_OUT_OF_DATE = ord('*')

class SHMSlot(object):
    """
    Represents a slot in the SHM file
    """
    def __init__(self, slot, shm, logger=logger):
        self.slot = slot
        self.shm = shm
        self.logger = logger

    @property
    def status(self):
        self.shm.seek(self.slot, os.SEEK_SET)
        return self.shm.read_byte()

    @status.setter
    def status(self, status):
        self.logger.debug("Setting slot %d to %r", self.slot, status)
        self.shm.seek(self.slot, os.SEEK_SET)
        self.shm.write_byte(status)


