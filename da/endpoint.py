# Copyright (c) 2010-2016 Bo Lin
# Copyright (c) 2010-2016 Yanhong Annie Liu
# Copyright (c) 2010-2016 Stony Brook University
# Copyright (c) 2010-2016 The Research Foundation of SUNY
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
import enum
import time
import random
import socket
import logging
import threading
import selectors
import multiprocessing
from collections import namedtuple

from .common import get_runtime_option, LRU, WaitableQueue

logger = logging.getLogger(__name__)

HEADER_SIZE = 8
BYTEORDER = 'big'
MAX_RETRY = 10

class ChannelCaps:
    """An enum of channel capabilities."""
    FIFO         = 1
    RELIABLE     = 2
    INTERHOST    = 4
    BROADCAST    = 8
    RELIABLEFIFO = FIFO | RELIABLE

class TransportException(Exception): pass
class BindingException(Exception): pass
class NoAvailablePortsException(BindingException): pass
class NoTargetTransportException(TransportException): pass
class InvalidTransportStateException(TransportException): pass
class PacketSizeExceededException(TransportException): pass

TransportTypes = []

class TransportManager:
    """Manages all DistAlgo transports within a process.

    """
    def __init__(self):
        self.log = logger.getChild(self.__class__.__qualname__)
        self.queue = WaitableQueue()
        self.transports = tuple(cls(queue=self.queue) for cls in TransportTypes)
        self.initialized = False
        self.started = False

    @property
    def transport_addresses(self):
        return tuple(t.address for t in self.transports)

    @property
    def transport_addresses_str(self):
        return ", ".join(["{}={}".format(tr.__class__.__name__, tr.address)
                        for tr in self.transports])

    def initialize(self, **params):
        """Initialize all transports.

        """
        self.log.debug("Initializing...")
        total = len(TransportTypes)
        cnt = 0
        for transport in self.transports:
            try:
                transport.initialize(**params)
                cnt += 1
            except Exception as err:
                self.log.debug("Failed to initialize transport %s: %r",
                               transport, err)
        if cnt != total:
            raise TransportException(
                "Initialization failed for {}/{} transports.".format(
                    (total - cnt), total))
        else:
            self.initialized = True

    def start(self):
        """Start all transports.

        """
        self.log.debug("Starting...")
        total = len(TransportTypes)
        cnt = 0
        for transport in self.transports:
            try:
                transport.start()
                cnt += 1
            except Exception as err:
                self.log.error("Failed to start transport %s: %r", transport, err)
        if cnt != total:
            raise TransportException(
                "Start failed for {}/{} transports.".format(
                    (total - cnt), total))
        else:
            self.started = True

    def close(self):
        """Shut down all transports.

        """
        self.log.debug("Stopping...")
        total = len(TransportTypes)
        cnt = 0
        for transport in self.transports:
            try:
                transport.close()
                cnt += 1
            except Exception as err:
                self.log.warning("Exception when stopping transport %s: %r",
                                 transport, err)
        self.started = False
        self.initialized = False
        self.log.debug("%d/%d transports stopped.", cnt, total)

    def get_transport(self, flags):
        """Returns the first transport instance satisfying `flags`, or None if
        no transport satisfies `flags`.
        """
        flags &= ~(ChannelCaps.BROADCAST)
        for tr in self.transports:
            if (flags & tr.capabilities) == 0:
                return tr
        return None


class Transport:
    """Represents a type of communication channel for sending of data.

    This is the base class for all types of communication channels in DistAlgo.

    """
    slot_index = 0
    def __init__(self, queue):
        super().__init__()
        self._log = logger.getChild(self.__class__.__qualname__)
        self.queue = queue
        self.hostname = None

    def initialize(self, hostname=None, **params):
        if hostname is None:
            hostname = get_runtime_option('hostname')
        self.hostname = hostname

    def start(self):
        """Starts the transport.

        """
        pass

    def close(self):
        """Stops the transport and clean up resources.

        """
        pass

    def send(self, data, dest, **params):
        """Send `data` to `dest`.

        `data` should be a `bytes` or `bytearray` object. `dest` should be a
        DistAlgo process id.

        """
        pass

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

def transport(cls):
    """Decorator to register `cls` as a transport.

    """
    cls.slot_index = len(TransportTypes)
    TransportTypes.append(cls)


class SocketTransport(Transport):
    """Base class for socket-based transports.

    """
    capabilities = ~(ChannelCaps.INTERHOST)
    def __init__(self, queue):
        super().__init__(queue)
        self.port = None
        self.conn = None
        self.worker = None
        self.buffer_size = 0

    def initialize(self, port=None, strict=False, linear=False,
                   retries=MAX_RETRY, **rest):
        super().initialize(**rest)
        self.port = port
        if self.port is None:
            if not strict:
                self.port = random.randint(MIN_TCP_PORT, MAX_TCP_PORT)
            else:
                raise NoAvailablePortsException("Port number not specified!")
        self.buffer_size = get_runtime_option('message_buffer_size')
        address = None
        retry = 1
        assert self.conn is not None
        while True:
            address = (self.hostname, self.port)
            try:
                self.conn.bind(address)
                break
            except socket.error as e:
                address = None
                if not strict and retry < retries:
                    if linear:
                        self.port += 1
                    else:
                        self.port = random.randint(MIN_TCP_PORT, MAX_TCP_PORT)
                    retry += 1
                else:
                    raise BindingException(
                        "Failed to bind to an available port.") from e
        self._log.debug("Transport initialized at address %s", address)

    def close(self):
        if self.conn is None:
            self._log.debug("Already stopped.")
        else:
            try:
                self.conn.close()
            except OSError:
                pass
            finally:
                self.conn = None
        # No need to care about the worker thread since it's a daemon:
        self.worker = None
        self._log.debug("Transport stopped.")

    @property
    def address(self):
        return self.port

    def __str__(self):
        fmt = "<{0.__class__.__qualname__}({0.hostname}:{0.port})>"
        return fmt.format(self)


# UDP Implementation:

MIN_UDP_PORT = 10000
MAX_UDP_PORT = 40000

@transport
class UdpTransport(SocketTransport):
    """A channel that supports sending and receiving messages via UDP.

    """
    capabilities = ~(ChannelCaps.INTERHOST)
    def __init__(self, queue):
        super().__init__(queue)

    def initialize(self, **params):
        try:
            self.conn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.conn.set_inheritable(True)
            super().initialize(**params)
        except Exception as e:
            if self.conn is not None:
                self.conn.close()
            self.conn = None
            raise e

    def start(self):
        if self.conn is None:
            raise InvalidTransportStateException(
                "Transport has not been initialized!")

        if self.worker is None or not self.worker.is_alive():
            self.worker = threading.Thread(target=self.recvmesgs, daemon=True)
            self.worker.start()
        self._log.debug("Transport started.")

    def close(self):
        super().close()

    def send(self, chunk, dest, wait=0.01, retries=MAX_RETRY, **rest):
        if self.conn is None:
            raise InvalidTransportStateException(
                "Invalid transport state for sending.")

        if len(chunk) > self.buffer_size:
            self._log.warning("Data size exceeded maximum buffer size!"
                              " Outgoing packet dropped.")
            self._log.debug("Dropped packet: %s", chunk)
            raise PacketSizeExceededException()
        else:
            target = self.address_from_id(dest)
            if target is None:
                raise NoTargetTransportException()
            cnt = 0
            while True:
                try:
                    if self.conn.sendto(chunk, target) == len(chunk):
                        return
                    else:
                        raise TransportException("Unable to send full chunk.")
                except PermissionError as e:
                    # The 'conntrack' module of iptables will cause UDP `sendto`
                    # to return `EPERM` if it's sending too fast:
                    self._log.debug("Packet to %s dropped by iptables, "
                                    "reduce send rate.", target)
                    cnt += 1
                    if cnt >= retries:
                        raise TransportException("Packet blocked by OS.") from e
                    else:
                        time.sleep(wait)

    def recvmesgs(self):
        if self.conn is None:
            raise InvalidTransportStateException(
                "Invalid transport state for receiving.")

        try:
            while True:
                chunk, _, flags, remote= self.conn.recvmsg(self.buffer_size)
                if not chunk:
                    # XXX: zero length packet == closed socket??
                    self._log.debug("Transport closed, terminating receive loop.")
                    break
                elif flags & socket.MSG_TRUNC:
                    self._log.debug("Dropped truncated packet. ")
                elif flags & socket.MSG_ERRQUEUE:
                    self._log.debug("No data received. ")
                else:
                    self.queue.append((self.__class__.slot_index, chunk, remote))
        except (socket.error, AttributeError) as e:
            self._log.debug("Terminating receive loop due to %r", e)

    @property
    def started(self):
        return self.conn is not None


# TCP Implementation:

MAX_TCP_BACKLOG = 10
MAX_TCP_CONN = 200
MIN_TCP_PORT = 10000
MAX_TCP_PORT = 40000

class AuxConnectionData:
    """Auxiliary data associated with each TCP connection.

    """
    def __init__(self, peername, message_size):
        self.peername = peername
        self.buf = bytearray(message_size * 2)
        self.view = memoryview(self.buf)
        self.lastptr = 0
        self.freeptr = 0

@transport
class TcpTransport(SocketTransport):
    """A channel that supports sending and receiving messages via TCP.

    """
    capabilities = ~((ChannelCaps.FIFO) |
                     (ChannelCaps.RELIABLE) |
                     (ChannelCaps.INTERHOST))
    def __init__(self, queue):
        super().__init__(queue)
        self.cache = None
        self.lock = threading.Lock()
        self.selector = None

    def initialize(self, strict=False, **params):
        try:
            self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if strict and not get_runtime_option('tcp_dont_reuse_addr'):
                self.conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            super().initialize(strict=strict, **params)
        except Exception as e:
            if self.conn is not None:
                self.conn.close()
            self.conn = None
            raise e

    def start(self):
        if self.conn is None:
            raise InvalidTransportStateException(
                "Transport has not been initialized!")

        self.conn.listen(MAX_TCP_BACKLOG)
        # self.conn.setblocking(False)
        self.conn.settimeout(5)
        self.cache = dict()
        self.selector = selectors.DefaultSelector()
        self.selector.register(self.conn, selectors.EVENT_READ,
                               (self._accept, None))
        if self.worker is None or not self.worker.is_alive():
            self.worker = threading.Thread(target=self.recvmesgs, daemon=True)
            self.worker.start()
        self._log.debug("Transport started.")

    def close(self):
        if self.selector is not None:
            self.selector.close()
        super().close()
        self.cache = None

    def _accept(self, conn, auxdata):
        conn, addr = self.conn.accept()
        self._log.debug("Accepted connection from %s.", addr)
        if addr in self.cache:
            self._log.warning("Double connection from %s!", addr)
        with self.lock:
            self.cache[addr] = conn
        self.selector.register(conn, selectors.EVENT_READ,
                               (self._receive_1,
                                AuxConnectionData(addr, self.buffer_size)))

    def _connect(self, target):
        self._log.debug("Initiating connection to %s.", target)
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.settimeout(3)
        conn.connect(target)
        self._log.debug("Connection to %s established.", target)
        return conn

    def _cleanup(self, conn, remote):
        self._log.debug("Cleanup connection to %s.", remote)
        try:
            self.selector.unregister(conn)
        except KeyError:
            pass
        except ValueError:
            pass
        if remote in self.cache:
            with self.lock:
                if self.cache[remote] is conn:
                    del self.cache[remote]
                else:
                    self._log.warning("Possible corrupted cache entry for %s",
                                      remote)
        try:
            conn.close()
        except OSError:
            pass

    def send(self, chunk, dest, retries=MAX_RETRY, wait=0.1,
             retry_refused_connections=False, **rest):
        target = self.address_from_id(dest)
        if target is None:
            raise NoTargetTransportException(
                "Process {} does not have TCP transport!".format(dest))

        retry = 1
        saved = conn = None
        with self.lock:
            saved = conn = self.cache.get(target)
        try:
            while True:
                try:
                    if conn is None:
                        conn = self._connect(target)
                    l = len(chunk)
                    header = int(l).to_bytes(HEADER_SIZE, BYTEORDER)
                    self._send_1((header, chunk), conn, target)
                    return
                except ConnectionRefusedError as e:
                    if (not retry_refused_connections) or retry > retries:
                        self._log.warning(
                            "Sending to %s: connection refused at address %s, "
                            " perhaps the target process has terminated?",
                            dest, target)
                        raise TransportException() from e
                except socket.error as e:
                    if conn is not None:
                        conn.close()
                        conn = None
                    if retry > retries:
                        self._log.warning(
                            "Sending to %s: max retries reached, abort.", dest)
                        raise TransportException() from e
                    else:
                        self._log.debug("Sending to %s: failed on %dth try.",
                                        dest, retry)
                retry += 1
                time.sleep(wait)
        finally:
            if conn is not None:
                if saved != conn:
                    with self.lock:
                        self.cache[target] = conn
                    self.selector.register(
                        conn, selectors.EVENT_READ,
                        (self._receive_1,
                         AuxConnectionData(target, self.buffer_size)))
            else:
                if target in self.cache:
                    with self.lock:
                        del self.cache[target]

    def _send_1(self, data, conn, target):
        msglen = sum(len(chunk) for chunk in data)
        sent = conn.sendmsg(data)
        if sent != msglen:
            self._log.debug("_send_1: only sent %d/%d bytes. ", sent, msglen)
            raise socket.error("Unable to send full chunk.")
        else:
            self._log.debug("Sent %d bytes to %s.", msglen, target)

    def recvmesgs(self):
        if self.conn is None:
            raise InvalidTransportStateException(
                "Invalid transport state for receiving.")
        try:
            while True:
                events = self.selector.select()
                for key, mask in events:
                    callback, aux = key.data
                    try:
                        callback(key.fileobj, aux)
                    except socket.error as e:
                        if key.fileobj is self.conn:
                            self._log.error("socket.error on listener: %r", e)
                            break
                        else:
                            self._log.debug(
                                "socket.error when receiving from %s: %r",
                                aux.peername, e)
                            self._cleanup(key.fileobj, aux.peername)
        except Exception as e:
            self._log.debug("(recvmesgs): caught exception %r", e)

    def _receive_1(self, conn, aux):
        buf = aux.buf
        view = aux.view
        fptr = 0
        remote = aux.peername
        if aux.freeptr > 0:
            fptr = aux.freeptr
            aux.freeptr = 0
            rbuf = view[fptr:]
        else:
            rbuf = buf

        rlen = conn.recv_into(rbuf)
        if rlen == 0:
            self._log.debug("Peer disconnected: %s.", remote)
            self._cleanup(conn, remote)
            return

        self._log.debug("%d/%d bytes received from %s.", rlen, len(rbuf), remote)
        datalen = fptr + rlen
        fptr = aux.lastptr
        aux.lastptr = 0
        cnt = 0
        while fptr < (datalen - HEADER_SIZE):
            pstart = fptr + HEADER_SIZE
            psize = int.from_bytes(view[fptr:pstart], BYTEORDER)
            pend = pstart + psize
            if psize > 0:
                if pend <= datalen:
                    chunk = bytes(view[pstart:pend])
                    self.queue.append((self.__class__.slot_index, chunk, remote))
                    cnt += 1
                else:
                    break
            else:
                self._log.debug("Invalid message header: %d!", psize)
            fptr = pend
        self._log.debug("%d message(s) received.", cnt)
        if fptr != datalen:
            leftover = datalen - fptr
            self._log.debug("%d bytes leftover.", leftover)
            if fptr > len(buf) / 2:
                buf[:leftover] = buf[fptr:datalen]
                aux.freeptr = leftover
                self._log.debug("Leftover bytes moved to buffer start.")
            else:
                aux.lastptr = fptr
                aux.freeptr = datalen
