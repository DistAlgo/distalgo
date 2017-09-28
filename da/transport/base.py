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

from ..common import get_runtime_option

__all__ = ["ChannelCaps", "TransportException",
           "VersionMismatchException", "AuthenticationException",
           "BindingException", "NoAvailablePortsException",
           "NoTargetTransportException", "InvalidTransportStateException",
           "PacketSizeExceededException",
           "Transport"]

logger = logging.getLogger(__name__)

class ChannelCaps:
    """An enum of channel capabilities."""
    FIFO         = 1
    RELIABLE     = 2
    INTERHOST    = 4
    BROADCAST    = 8
    RELIABLEFIFO = FIFO | RELIABLE

class TransportException(Exception): pass
class VersionMismatchException(TransportException): pass
class AuthenticationException(TransportException): pass
class BindingException(TransportException): pass
class NoAvailablePortsException(BindingException): pass
class NoTargetTransportException(TransportException): pass
class InvalidTransportStateException(TransportException): pass
class PacketSizeExceededException(TransportException): pass

class Transport:
    """Implements an inter-process communication channel for sending of data.

    Transports are responsible for sending and receiving messages across OS
    process boundaries, and optionally, across processes running on remote
    hosts. This is the abstract base class for all types of transports in
    DistAlgo.

    """
    slot_index = 0
    capabilities = ~0
    def __init__(self, authkey):
        super().__init__()
        self.queue = None
        self.hostname = None
        self.authkey = authkey
        self.mesgloop = None
        self._name = None

    def initialize(self, hostname=None, **_):
        if hostname is None:
            hostname = get_runtime_option('hostname', default='localhost')
        self.hostname = hostname

    def start(self, queue, mesgloop=None):
        """Starts the transport.

        """
        raise NotImplementedError()

    def close(self):
        """Stops the transport and clean up resources.

        """
        pass

    def send(self, data, dest, **params):
        """Send `data` to `dest`.

        `data` should be a `bytes` or `bytearray` object. `dest` should be a
        DistAlgo process id.

        """
        raise NotImplementedError()

    def setname(self, name):
        self._name = name

    @classmethod
    def address_from_id(cls, target):
        """Returns the transport address of `target`.

        Given process id `target`, return the address of it's corresponding
        transport, or None if `target` does not have a corresponding transport.

        """
        address = target.transports[cls.slot_index]
        if address is None:
            return None
        else:
            return (target.hostname, address)

    @property
    def address(self):
        return None

    def __str__(self):
        return self.__class__.__name__

