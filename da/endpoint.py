# Copyright (c) 2010-2014 Bo Lin
# Copyright (c) 2010-2014 Yanhong Annie Liu
# Copyright (c) 2010-2014 Stony Brook University
# Copyright (c) 2010-2014 The Research Foundation of SUNY
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

import sys
import pickle
import random
import select
import socket
import logging

class EndPoint:
    """Represents a target for sending of messages.

    This is the base class for all types of communication channels in
    DistAlgo. It uniquely identifies a "node" in the distributed system. In
    most scenarios, a process will only be associated with one EndPoint
    instance. The 'self' keyword in DistAlgo is ultimately translated into an
    instance of this class.

    """

    def __init__(self, name=None, proctype=None):
        if name is None:
            self._name = socket.gethostname()
        else:
            self._name = name
        self._proc = None
        self._proctype = proctype
        self._log = logging.getLogger("runtime.EndPoint")
        self._address = None

    def send(self, data, src, timestamp = 0):
        pass

    def recv(self, block, timeout = None):
        pass

    def setname(self, name):
        self._name = name

    def getlogname(self):
        if self._address is not None:
            return "%s_%s" % (self._address[0], str(self._address[1]))
        else:
            return self._name

    def close(self):
        pass

    ###################################################
    # Make the EndPoint behave like a Process object:

    def is_alive(self):
        if self._proc is not None:
            return self._proc.is_alive()
        else:
            self._log.warn("is_alive can only be called from parent process.")
            return self

    def join(self):
        if self._proc is not None:
            return self._proc.join()
        else:
            self._log.warn("join can only be called from parent process.")
            return self

    def terminate(self):
        if self._proc is not None:
            return self._proc.terminate()
        else:
            self._log.warn("terminate can only be called from parent process.")
            return self

    ###################################################

    def __getstate__(self):
        return ("EndPoint", self._address, self._name, self._proctype)

    def __setstate__(self, value):
        proto, self._address, self._name, self._proctype = value
        self._log = logging.getLogger("runtime.EndPoint")

    def __str__(self):
        if self._address is not None:
            return str(self._address)
        else:
            return self._name

    def __repr__(self):
        if self._proctype is not None:
            return "<" + self._proctype.__name__ + str(self) + ">"
        else:
            return "<process " + str(self) + ">"

    def __hash__(self):
        return hash(self._address)

    def __eq__(self, obj):
        if not hasattr(obj, "_address"):
            return False
        return self._address == obj._address
    def __lt__(self, obj):
        return self._address < obj._address
    def __le__(self, obj):
        return self._address <= obj._address
    def __gt__(self, obj):
        return self._address > obj._address
    def __ge__(self, obj):
        return self._address >= obj._address
    def __ne__(self, obj):
        if not hasattr(obj, "_address"):
            return True
        return self._address != obj._address


# TCP Implementation:

INTEGER_BYTES = 8

MAX_TCP_CONN = 200
MIN_TCP_PORT = 10000
MAX_TCP_PORT = 40000
MAX_TCP_BUFSIZE = 200000          # Maximum pickled message size
MAX_RETRY = 5

class TcpEndPoint(EndPoint):
    """Endpoint based on TCP.

    """

    senders = None
    receivers = None

    def __init__(self, name=None, proctype=None, port=None):
        super().__init__(name, proctype)

        TcpEndPoint.receivers = dict()
        TcpEndPoint.senders = LRU(MAX_TCP_CONN)

        self._conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        if port is None:
            while True:
                self._address = (self._name,
                                 random.randint(MIN_TCP_PORT, MAX_TCP_PORT))
                try:
                    self._conn.bind(self._address)
                    break
                except socket.error:
                    pass
        else:
            self._address = (self._name, port)
            self._conn.bind(self._address)

        self._conn.listen(10)
        TcpEndPoint.receivers[self._conn] = self._address

        self._log = logging.getLogger("runtime.TcpEndPoint(%s)" %
                                      super().getlogname())
        self._log.debug("TcpEndPoint %s initialization complete",
                        str(self._address))

    def send(self, data, src, timestamp = 0):
        retry = 1
        while True:
            conn = TcpEndPoint.senders.get(self)
            if conn is None:
                conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    conn.connect(self._address)
                    TcpEndPoint.senders[self] = conn
                except socket.error:
                    self._log.debug("Can not connect to %s. Peer is down.",
                                   str(self._address))
                    return False

            bytedata = pickle.dumps((src, timestamp, data))
            l = len(bytedata)
            header = int(l).to_bytes(INTEGER_BYTES, sys.byteorder)
            mesg = header + bytedata

            if len(mesg) > MAX_TCP_BUFSIZE:
                self._log.warn("Packet size exceeded maximum buffer size! "
                               "Outgoing packet dropped.")
                self._log.debug("Dropped packet: %s",
                                str((src, timestamp, data)))
                break

            else:
                try:
                    if self._send_1(mesg, conn):
                        break
                except socket.error as e:
                    pass
                self._log.debug("Error sending packet, retrying.")
                retry += 1
                if retry > MAX_RETRY:
                    self._log.debug("Max retry count reached, reconnecting.")
                    conn.close()
                    del TcpEndPoint.senders[self]
                    retry = 1

        self._log.debug("Sent packet %r to %r." % (data, self))
        return True

    def _send_1(self, data, conn):
        msglen = len(data)
        totalsent = 0
        while totalsent < msglen:
            sent = conn.send(data[totalsent:])
            if sent == 0:
                return False
            totalsent += sent
        return True

    def recvmesgs(self):
        try:
            while True:
                r, _, _ = select.select(TcpEndPoint.receivers.keys(), [], [])

                if self._conn in r:
                    # We have pending new connections, handle the first in
                    # line. If there are any more they will have to wait until
                    # the next iteration
                    conn, addr = self._conn.accept()
                    TcpEndPoint.receivers[conn] = addr
                    r.remove(self._conn)

                for c in r:
                    try:
                        bytedata = self._receive_1(INTEGER_BYTES, c)
                        datalen = int.from_bytes(bytedata, sys.byteorder)

                        bytedata = self._receive_1(datalen, c)
                        src, tstamp, data = pickle.loads(bytedata)
                        bytedata = None

                        if not isinstance(src, TcpEndPoint):
                            raise TypeError()
                        else:
                            yield (src, tstamp, data)

                    except pickle.UnpicklingError as e:
                        self._log.warn("UnpicklingError, packet from %s dropped",
                                       TcpEndPoint.receivers[c])

                    except socket.error as e:
                        self._log.debug("Remote connection %s terminated.",
                                        str(c))
                        del TcpEndPoint.receivers[c]

        except select.error as e:
            self._log.debug("select.error occured, terminating receive loop.")

    def _receive_1(self, totallen, conn):
        msg = bytes()
        while len(msg) < totallen:
            chunk = conn.recv(totallen-len(msg))
            if len(chunk) == 0:
                raise socket.error("EOF received")
            msg += chunk
        return msg

    def close(self):
        pass

    def __getstate__(self):
        return ("TCP", self._address, self._name, self._proctype)

    def __setstate__(self, value):
        proto, self._address, self._name, self._proctype = value
        self._conn = None
        self._log = logging.getLogger("runtime.TcpEndPoint(%s)" % self._name)


class Node(object):
    __slots__ = ['prev', 'next', 'me']
    def __init__(self, prev, me):
        self.prev = prev
        self.me = me
        self.next = None
    def __str__(self):
        return str(self.me)
    def __repr__(self):
        return self.me.__repr__()

class LRU:
    """
    Implementation of a length-limited O(1) LRU queue.
    Built for and used by PyPE:
    http://pype.sourceforge.net
    Copyright 2003 Josiah Carlson.
    """
    def __init__(self, count, pairs=[]):
        self.count = max(count, 1)
        self.d = {}
        self.first = None
        self.last = None
        for key, value in pairs:
            self[key] = value
    def __contains__(self, obj):
        return obj in self.d
    def __getitem__(self, obj):
        a = self.d[obj].me
        self[a[0]] = a[1]
        return a[1]
    def __setitem__(self, obj, val):
        if obj in self.d:
            del self[obj]
        nobj = Node(self.last, (obj, val))
        if self.first is None:
            self.first = nobj
        if self.last:
            self.last.next = nobj
        self.last = nobj
        self.d[obj] = nobj
        if len(self.d) > self.count:
            if self.first == self.last:
                self.first = None
                self.last = None
                return
            a = self.first
            a.next.prev = None
            self.first = a.next
            a.next = None
            del self.d[a.me[0]]
            del a
    def __delitem__(self, obj):
        nobj = self.d[obj]
        if nobj.prev:
            nobj.prev.next = nobj.next
        else:
            self.first = nobj.next
        if nobj.next:
            nobj.next.prev = nobj.prev
        else:
            self.last = nobj.prev
        del self.d[obj]
    def __iter__(self):
        cur = self.first
        while cur != None:
            cur2 = cur.next
            yield cur.me[1]
            cur = cur2
    def __str__(self):
        return str(self.d)
    def __repr__(self):
        return self.d.__repr__()
    def iteritems(self):
        cur = self.first
        while cur != None:
            cur2 = cur.next
            yield cur.me
            cur = cur2
    def iterkeys(self):
        return iter(self.d)
    def itervalues(self):
        for i,j in self.iteritems():
            yield j
    def keys(self):
        return self.d.keys()
    def get(self, k, d=None):
        v = self.d.get(k)
        if v is None: return None
        a = v.me
        self[a[0]] = a[1]
        return a[1]


# UDP Implementation:

MIN_UDP_PORT = 10000
MAX_UDP_PORT = 40000
MAX_UDP_BUFSIZE = 20000

class UdpEndPoint(EndPoint):
    sender = None

    def __init__(self, name=None, proctype=None, port=None):
        super().__init__(name, proctype)

        UdpEndPoint.sender = None

        self._conn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        if port is None:
            while True:
                self._address = (self._name,
                                 random.randint(MIN_UDP_PORT, MAX_UDP_PORT))
                try:
                    self._conn.bind(self._address)
                    break
                except socket.error:
                    pass
        else:
            self._address = (self._name, port)
            self._conn.bind(self._address)

        self._log = logging.getLogger("runtime.UdpEndPoint(%s)" %
                                      super().getlogname())
        self._log.debug("UdpEndPoint %s initialization complete",
                        str(self._address))


    def send(self, data, src, timestamp = 0):
        if UdpEndPoint.sender is None:
            UdpEndPoint.sender = socket.socket(socket.AF_INET,
                                               socket.SOCK_DGRAM)

        bytedata = pickle.dumps((src, timestamp, data))
        if len(bytedata) > MAX_UDP_BUFSIZE:
            self._log.warn("Data size exceeded maximum buffer size!" +
                           " Outgoing packet dropped.")
            self._log.debug("Dropped packet: %s", str((src, timestamp, data)))

        elif (UdpEndPoint.sender.sendto(bytedata, self._address) !=
              len(bytedata)):
            raise socket.error()

    def recvmesgs(self):
        flags = 0

        try:
            while True:
                bytedata = self._conn.recv(MAX_UDP_BUFSIZE, flags)
                src, tstamp, data = pickle.loads(bytedata)
                if not isinstance(src, UdpEndPoint):
                    raise TypeError()
                else:
                    yield (src, tstamp, data)
        except socket.error as e:
            self._log.debug("socket.error occured, terminating receive loop.")

    def __getstate__(self):
        return ("UDP", self._address, self._name, self._proctype)

    def __setstate__(self, value):
        proto, self._address, self._name, self._proctype = value
        self._conn = None
        self._log = logging.getLogger("runtime.UdpEndPoint")

