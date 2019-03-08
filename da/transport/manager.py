# Copyright (c) 2010-2017 Bo Lin
# Copyright (c) 2010-2017 Yanhong Annie Liu
# Copyright (c) 2010-2017 Stony Brook University
# Copyright (c) 2010-2017 The Research Foundation of SUNY
#
# Permission is hereby granted, free of charge, to any person
# obtaining a copy of this software and associated documentation files
# (the "Software"), to deal in the Software without restriction,
# including without limitation the rights to use, copy, modify, merge,
# publish, distribute, sublicense, and/or sell copies of the Software,
# and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

import logging

from .base import ChannelCaps, TransportException
from .mesgloop import SelectorLoop
from ..common import WaitableQueue

__all__ = ["TransportTypes", "TransportManager", "transport"]

logger = logging.getLogger(__name__)

TransportTypes = []

class TransportManager:
    """Manages all DistAlgo transports within a process.

    """
    log = None

    def __init__(self, cookie=None):
        self.queue = None
        self.mesgloop = None
        self.transports = []
        self.initialized = False
        self.started = False
        self.authkey = cookie
        if self.__class__.log is None:
            self.__class__.log = logger.getChild(self.__class__.__name__)

    def __getstate__(self):
        return (self.initialized, self.started, self.authkey)

    def __setstate__(self, state):
        self.initialized, self.started, self.authkey = state
        if self.__class__.log is None:
            self.__class__.log = logger.getChild(self.__class__.__name__)

    @property
    def transport_addresses(self):
        return tuple(t.address if t is not None else None
                     for t in self.transports)

    @property
    def transport_addresses_str(self):
        return ", ".join(["{}={}".format(typ.__name__,
                                         tr.address
                                         if tr is not None else "<None>")
                          for typ, tr in zip(TransportTypes, self.transports)])

    def initialize(self, pipe=None, **params):
        """Initialize all transports.

        """
        self.log.debug("Initializing with key %r...", self.authkey)
        total = len(TransportTypes)
        cnt = 0
        res = []
        for cls in TransportTypes:
            try:
                if pipe is not None:
                    assert pipe.recv() is trsp.__class__
                trsp = cls(self.authkey)
                trsp.initialize(pipe=pipe, **params)
                cnt += 1
                res.append(trsp)
            except Exception as err:
                self.log.debug("Failed to initialize transport %s: %r",
                               transport, err, exc_info=1)
                res.append(None)
        self.transports = tuple(res)
        if pipe:
            pipe.send('done')
        if cnt != total:
            self.log.warning(
                "Initialization failed for {}/{} transports.".format(
                    (total - cnt), total))
        self.initialized = True

    def start(self):
        """Start all transports.

        """
        self.log.debug("Starting...")
        self.queue = WaitableQueue()
        self.mesgloop = SelectorLoop()
        started, total = 0, 0
        res = []
        for trsp in self.transports:
            if trsp is not None:
                total += 1
                try:
                    trsp.start(self.queue, self.mesgloop)
                    started += 1
                    res.append(trsp)
                except Exception as err:
                    self.log.error("Failed to start transport %s: %r",
                                   transport, err)
                    res.append(None)
            else:
                res.append(None)
        if started != total:
            self.log.warning(
                "Start failed for {}/{} transports.".format(
                    (total - cnt), total))
        self.started = True
        self.transports = tuple(res)

    def close(self):
        """Shut down all transports.

        """
        self.log.debug("Stopping...")
        total = len(TransportTypes)
        cnt = 0
        for trsp in self.transports:
            try:
                if trsp is not None:
                    trsp.close()
                cnt += 1
            except Exception as err:
                self.log.warning("Exception when stopping transport %s: %r",
                                 transport, err)
        if self.mesgloop:
            self.mesgloop.stop()
        self.started = False
        self.initialized = False
        self.log.debug("%d/%d transports stopped.", cnt, total)

    def serialize(self, pipe, pid):
        """Sends all transports to child process.
        """
        for trsp in self.transports:
            pipe.send(trsp.__class__)
            trsp.serialize(pipe, pid)

    def get_transport(self, flags):
        """Returns the first transport instance satisfying `flags`, or None if
        no transport satisfies `flags`.
        """
        flags &= ~(ChannelCaps.BROADCAST)
        for tr in self.transports:
            if tr is not None and (flags & tr.capabilities) == 0:
                return tr
        return None

def transport(cls):
    """Decorator to register `cls` as a transport.

    """
    cls.slot_index = len(TransportTypes)
    TransportTypes.append(cls)
    return cls
