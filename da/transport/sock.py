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

import sys
import time
import random
import socket
import logging
import threading

from .base import *
from .manager import transport
from .mesgloop import SelectorLoop
from ..common import VERSION_BYTES, get_runtime_option

__all__ = [
    "SocketTransport", "UdpTransport", "TcpTransport",
    "HEADER_SIZE", "BYTEORDER"
]

logger = logging.getLogger(__name__)

HEADER_SIZE = 8
ADDRESS_SIZE = 4
BYTEORDER = 'big'
MAX_RETRIES = 3

DEFAULT_MESSAGE_BUFFER_SIZE = (4 * 1024)
DEFAULT_MIN_PORT = 10000
DEFAULT_MAX_PORT = 65535

class SocketTransport(Transport):
    """Base class for socket-based transports.

    """
    capabilities = ~(ChannelCaps.INTERHOST)
    def __init__(self, authkey):
        super().__init__(authkey)
        self._log = logger.getChild(self.__class__.__name__)
        self.port = None
        self._port_bytes = None
        self.conn = None
        self.mesgloop = None
        self.shared_loop = False
        self.buffer_size = 0

    def initialize(self, port=None, strict=False, linear=False,
                   retries=MAX_RETRIES, **rest):
        super().initialize(**rest)
        self.buffer_size = get_runtime_option('message_buffer_size',
                                              DEFAULT_MESSAGE_BUFFER_SIZE)
        min_port = get_runtime_option('min_port', DEFAULT_MIN_PORT)
        max_port = get_runtime_option('max_port', DEFAULT_MAX_PORT)
        assert self.conn is not None
        try:
            _, bound_port = self.conn.getsockname()
            if bound_port != 0:
                # We've already inherited the socket from the parent
                self.port = bound_port
                return
        except OSError as e:
            # This is what we get on Windows if we call `getsockname()` on an
            # unbound socket...
            pass
        self.port = port
        if self.port is None:
            if not strict:
                self.port = random.randint(min_port, max_port)
            else:
                raise NoAvailablePortsException("Port number not specified!")
        address = None
        retry = 1
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
                        self.port = random.randint(min_port, max_port)
                    retry += 1
                else:
                    raise BindingException(
                        "Failed to bind to an available port.") from e
        self._log.debug("Transport initialized at address %s", address)

    def serialize(self, pipe, pid):
        from multiprocessing.reduction import send_handle
        send_handle(pipe, self.conn.fileno(), pid)

    def start(self, queue, mesgloop=None):
        if self.conn is None:
            raise InvalidTransportStateException(
                "Transport has not been initialized!")
        self.queue = queue
        self.mesgloop = mesgloop
        if self.mesgloop is None:
            self.mesgloop = SelectorLoop()
            self.shared_loop = False
        else:
            self.shared_loop = True
        self.mesgloop.start()

    def close(self):
        if self.conn is None:
            self._log.debug("Already stopped.")
        else:
            if self.mesgloop:
                if not self.shared_loop:
                    self.mesgloop.stop()
                else:
                    self.mesgloop.deregister(self.conn)
            try:
                self.conn.close()
            except OSError:
                pass
            finally:
                self.conn = None
        self.queue = None
        self._log.debug("Transport stopped.")

    @property
    def address(self):
        return self.port

    @property
    def address_bytes(self):
        if self._port_bytes is None:
            self._port_bytes = int(self.port).to_bytes(ADDRESS_SIZE, BYTEORDER)
        return self._port_bytes

    @property
    def started(self):
        return self.queue is not None

    def __str__(self):
        fmt = "<{0.__class__.__qualname__}({0.hostname}:{0.port})>"
        return fmt.format(self)


# UDP Implementation:
DIGEST_LENGTH = 16
DIGEST_HOLDER = b'0' * DIGEST_LENGTH

@transport
class UdpTransport(SocketTransport):
    """A channel that supports sending and receiving messages via UDP.

    """
    capabilities = ~(ChannelCaps.INTERHOST)
    data_offset = 4 + DIGEST_LENGTH

    def __init__(self, authkey):
        super().__init__(authkey)

    def initialize(self, strict=False, pipe=None, **params):
        try:
            if pipe is None:
                self.conn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                self.conn.set_inheritable(True)
                if strict:
                    self.conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            else:
                from multiprocessing.reduction import recv_handle
                self.conn = socket.fromfd(recv_handle(pipe),
                                          socket.AF_INET, socket.SOCK_DGRAM)
            super().initialize(strict=strict, **params)
        except Exception as e:
            if self.conn is not None:
                self.conn.close()
            self.conn = None
            raise e

    def start(self, queue, mesgloop=None):
        super().start(queue, mesgloop)
        assert self.mesgloop is not None
        self.mesgloop.register(self.conn, self._recvmesg1)
        self._log.debug("Transport started.")

    def _packet_from(self, chunk):
        if self.authkey is not None:
            import hmac
            digest = hmac.new(self.authkey, chunk, 'md5').digest()
            return (VERSION_BYTES, digest, chunk)
        else:
            return (VERSION_BYTES, DIGEST_HOLDER, chunk)

    def _verify_packet(self, chunk, addr):
        if chunk[:4] != VERSION_BYTES:
            raise VersionMismatchException("wrong version: {}".format(chunk[:4]))
        if self.authkey is not None:
            with memoryview(chunk)[self.data_offset:] as data:
                import hmac
                digest = hmac.new(self.authkey, data, 'md5').digest()
                if digest != chunk[4:self.data_offset]:
                    raise AuthenticationException(
                        "wrong digest from {}: {}"
                        .format(addr, chunk[4:self.data_offset]))
        else:
            if chunk[4:self.data_offset] != DIGEST_HOLDER:
                raise AuthenticationException('{} requires a cookie.'
                                              .format(addr))

    def _sendmsg_nix(self, packet, target):
        packet_size = sum(len(e) for e in packet)
        return self.conn.sendmsg(packet, [], 0, target) == packet_size

    def _sendmsg_nt(self, packet, target):
        from itertools import chain
        buf = bytes(chain(*packet))
        return self.conn.sendto(buf, target) == len(buf)

    if sys.platform == 'win32':
        _sendmsg = _sendmsg_nt
    else:
        _sendmsg = _sendmsg_nix

    def send(self, chunk, target, wait=0.01, retries=MAX_RETRIES, **_):
        if self.conn is None:
            raise InvalidTransportStateException(
                "Invalid transport state for sending.")

        if len(chunk) > self.buffer_size:
            self._log.warning("Data size exceeded maximum buffer size!"
                              " Outgoing packet dropped.")
            self._log.debug("Dropped packet: %s", chunk)
            raise PacketSizeExceededException()
        else:
            if target is None:
                raise NoTargetTransportException()
            packet = self._packet_from(chunk)
            cnt = 0
            while True:
                try:
                    if self._sendmsg(packet, target):
                        return
                    else:
                        raise TransportException("Unable to send full chunk.")
                except PermissionError as e:
                    # The 'conntrack' module of iptables will cause UDP `sendto`
                    # to return `EPERM` if it's sending too fast:
                    self._log.debug("Packet to %s dropped by kernel, "
                                    "reduce send rate.", target)
                    cnt += 1
                    if cnt >= retries:
                        raise TransportException("Packet blocked by OS.") from e
                    else:
                        time.sleep(wait)

    def _recvmsg_nt(self):
        chunk, remote = self.conn.recvfrom(self.buffer_size)
        return chunk, None, 0, remote

    def _recvmsg_nix(self):
        return self.conn.recvmsg(self.buffer_size)

    if sys.platform == 'win32':
        _recvmsg = _recvmsg_nt
        socket.MSG_ERRQUEUE = 0
    elif sys.platform == 'darwin':
        _recvmsg = _recvmsg_nix
        socket.MSG_ERRQUEUE = 0
    else:
        _recvmsg = _recvmsg_nix

    def _recvmesg1(self, _conn, _data):
        try:
            chunk, _, flags, remote= self._recvmsg()
            if not chunk:
                # XXX: zero length packet == closed socket??
                self._log.debug("Transport closed, terminating receive loop.")
            elif flags & socket.MSG_TRUNC:
                self._log.debug("Dropped truncated packet. ")
            elif flags & socket.MSG_ERRQUEUE:
                self._log.debug("No data received. ")
            else:
                try:
                    self._verify_packet(chunk, remote)
                    self.queue.append((self, chunk, remote))
                except TransportException as e:
                    self._log.warning("Packet from %s dropped due to: %r",
                                      remote, e)
        except (socket.error, AttributeError) as e:
            self._log.debug("Terminating receive loop due to %r", e)


# TCP Implementation:
MAX_TCP_BACKLOG = 10
MAX_TCP_CONN = 200
TCP_RECV_BUFFER_SIZE = 256
TCP_DEFAULT_TIMEOUT = 5
#
# Authentication stuff
#

MESSAGE_LENGTH = 20

KEY_CHALLENGE = b'#KY#'
VER_CHALLENGE = b'#VR#'
WELCOME = b'#WELCOME#'
FAILURE = b'#FAILURE#'

class AuxConnectionData:
    """Auxiliary data associated with each TCP connection.

    """
    def __init__(self, peername, message_size, digest=None, provision=False):
        self.peername = peername
        self.message_size = message_size
        self.digest = digest
        if provision:
            self.provision()

    def provision(self):
        del self.digest
        self.buf = bytearray(self.message_size * 2)
        self.view = memoryview(self.buf)
        self.lastptr = 0
        self.freeptr = 0

class ConnectionClosedException(TransportException): pass
@transport
class TcpTransport(SocketTransport):
    """A channel that supports sending and receiving messages via TCP.

    """
    capabilities = ~((ChannelCaps.FIFO) |
                     (ChannelCaps.RELIABLE) |
                     (ChannelCaps.INTERHOST))
    data_offset = 0

    def __init__(self, authkey):
        super().__init__(authkey)
        self.cache = None
        self.lock = None

    def initialize(self, strict=False, pipe=None, **params):
        try:
            if pipe is None:
                self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                if strict and not get_runtime_option('tcp_dont_reuse_addr', False):
                    self.conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            else:
                from multiprocessing.reduction import recv_handle
                self.conn = socket.fromfd(recv_handle(pipe),
                                          socket.AF_INET, socket.SOCK_STREAM)
            super().initialize(strict=strict, **params)
        except Exception as e:
            if self.conn is not None:
                self.conn.close()
            self.conn = None
            raise e

    def start(self, queue, mesgloop=None):
        self.conn.listen(MAX_TCP_BACKLOG)
        self.conn.settimeout(TCP_DEFAULT_TIMEOUT)
        super().start(queue, mesgloop)
        assert self.mesgloop is not None
        self.mesgloop.register(self.conn, self._accept)
        if self.cache is None:
            self.cache = dict()
        if self.lock is None:
            self.lock = threading.Lock()
        self._log.debug("Transport started.")

    def close(self):
        if self.lock:
            with self.lock:
                for conn in self.cache.values():
                    self.mesgloop.deregister(conn)
                    conn.close()
                self.cache.clear()
        super().close()

    def _deliver_challenge(self, conn, addr):
        import os
        digest = None
        if self.authkey is not None:
            import hmac
            message = os.urandom(MESSAGE_LENGTH)
            self._send_1((KEY_CHALLENGE, VERSION_BYTES, message),
                         conn, addr)
            digest = hmac.new(self.authkey, message, 'md5').digest()
        else:
            self._send_1((VER_CHALLENGE, VERSION_BYTES), conn, addr)
        return digest

    def _verify_challenge(self, conn, auxdata):
        """Verify a remote peer has the proper key and version."""
        addr = auxdata.peername
        # FIXME: is it possible we may not get the whole message in one go?
        message = conn.recv(TCP_RECV_BUFFER_SIZE)
        self.mesgloop.deregister(conn)
        port_bytes = message[:ADDRESS_SIZE]
        message = message[ADDRESS_SIZE:]
        if self.authkey is not None:
            if message != auxdata.digest:
                self._send_1((FAILURE,), conn, addr)
                raise AuthenticationException(
                    'Digest from {0.peername} was wrong.'.format(auxdata))
        else:
            if message == KEY_CHALLENGE:
                raise AuthenticationException(
                    '{0.peername} requires a cookie.'.format(auxdata))
            if message != VER_CHALLENGE:
                raise VersionMismatchException(
                    'Version from {0.peername} is different.'.format(auxdata))
        # Set the remote peer's port number to its listen port:
        remote_port = int.from_bytes(port_bytes, BYTEORDER)
        auxdata.peername  = addr[0], remote_port
        with self.lock:
            if auxdata.peername in self.cache:
                self._log.debug("Dropping duplicate connection from %s.",
                                auxdata.peername)
                conn.close()
                return
            else:
                self.cache[auxdata.peername] = conn
        self._send_1((WELCOME,), conn, addr)
        auxdata.provision()
        self.mesgloop.register(conn, self._recvmesg_wrapper,
                               (self._receive_1, auxdata))

    def _answer_challenge(self, conn, addr):
        # FIXME: same here...
        message = conn.recv(TCP_RECV_BUFFER_SIZE)
        self._log.debug("=========answering %r", message)
        if self.authkey is not None:
            import hmac
            if message[:len(KEY_CHALLENGE)] != KEY_CHALLENGE:
                self._send_challenge_reply(KEY_CHALLENGE, conn, addr)
                raise AuthenticationException('{} has no cookie.'.
                                              format(addr))
            if message[len(KEY_CHALLENGE):len(KEY_CHALLENGE)+4] != VERSION_BYTES:
                raise VersionMismatchException('Version at {} is different.'.
                                               format(addr))
            message = message[len(KEY_CHALLENGE)+4:]
            digest = hmac.new(self.authkey, message, 'md5').digest()
            self._send_challenge_reply(digest, conn, addr)
        else:
            if message[:len(KEY_CHALLENGE)] == KEY_CHALLENGE:
                self._send_challenge_reply(KEY_CHALLENGE, conn, addr)
                raise AuthenticationException('{} requires a cookie.'.
                                              format(addr))
            elif message != VER_CHALLENGE + VERSION_BYTES:
                self._send_challenge_reply(FAILURE, conn, addr)
                raise VersionMismatchException('Version at {} is different.'.
                                               format(addr))
            else:
                self._send_challenge_reply(VER_CHALLENGE, conn, addr)
        # FIXME: ...and here
        response = conn.recv(len(WELCOME))
        if len(response) == 0:
            # Remote side dropped the connection, either because they
            # terminated, or we already have a connection
            raise ConnectionClosedException()
        elif response != WELCOME:
            raise AuthenticationException('digest was rejected by {}.'.
                                          format(addr))

    def _send_challenge_reply(self, result, conn, addr):
        self._send_1((self.address_bytes, result), conn, addr)

    def _accept(self, conn, auxdata):
        conn, addr = self.conn.accept()
        self._log.debug("Accepted connection from %s.", addr)
        digest = self._deliver_challenge(conn, auxdata)
        self.mesgloop.register(conn, self._recvmesg_wrapper,
                               (self._verify_challenge,
                                AuxConnectionData(addr,
                                                  self.buffer_size,
                                                  digest)))

    def _connect(self, target):
        self._log.debug("Initiating connection to %s.", target)
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        conn.settimeout(TCP_DEFAULT_TIMEOUT)
        conn.connect(target)
        try:
            self._answer_challenge(conn, target)
            self._log.debug("Connection to %s established.", target)
            return conn
        except TransportException as e:
            conn.close()
            raise e

    def _cleanup(self, conn, remote):
        if conn is None:
            return
        self._log.debug("Cleanup connection to %s.", remote)
        if self.mesgloop:
            self.mesgloop.deregister(conn)
        if remote in self.cache:
            with self.lock:
                try:
                    if self.cache.get(remote) is conn:
                        del self.cache[remote]
                except AttributeError:
                    pass
        try:
            conn.close()
        except OSError:
            pass

    def send(self, chunk, target, retries=MAX_RETRIES, wait=0.05,
             retry_refused_connections=False, **_):
        """Send `chunk` to `target`."""
        if target is None:
            raise NoTargetTransportException()

        header = int(len(chunk)).to_bytes(HEADER_SIZE, BYTEORDER)
        message = (header, chunk)
        retry = 1
        saved = conn = None
        try:
            while True:
                with self.lock:
                    saved = conn = self.cache.get(target)
                try:
                    if conn is None:
                        conn = self._connect(target)
                    self._send_1(message, conn, target)
                    return
                except ConnectionRefusedError as e:
                    if (not retry_refused_connections) or retry > retries:
                        raise TransportException(
                            'connection refused by {}'.format(target)) from e
                except (socket.error, socket.timeout) as e:
                    self._log.debug("Sending to %s failed on %dth try: %r",
                                    target, retry, e)
                    if conn is not None:
                        conn.close()
                        conn = None
                except ConnectionClosedException:
                    pass

                if retry > retries:
                    raise TransportException('max retries reached.') from e
                if retry > 1:
                    time.sleep(wait)
                retry += 1
        finally:
            if conn is not None:
                if saved is not conn:
                    self._cleanup(saved, target)
                    with self.lock:
                        self.cache[target] = conn
                    self.mesgloop.register(
                        conn, self._recvmesg_wrapper,
                        (self._receive_1,
                         AuxConnectionData(target, self.buffer_size,
                                           provision=True)))
            else:
                if target in self.cache:
                    with self.lock:
                        try:
                            del self.cache[target]
                        except KeyError:
                            pass

    def _send_1(self, data, conn, target=None):
        msglen = sum(len(chunk) for chunk in data)
        sent = conn.sendmsg(data)
        if sent != msglen:
            self._log.debug("_send_1: only sent %d/%d bytes. ", sent, msglen)
            raise socket.error("Unable to send full chunk.")
        else:
            self._log.debug("Sent %d bytes to %s.", msglen, target)

    def _send_1_nt(self, data, conn, target=None):
        from itertools import chain
        buf = bytes(chain(*data))
        conn.sendall(buf)
        self._log.debug("Sent %d bytes to %s.", len(buf), target)

    if sys.platform == 'win32':
        _send_1 = _send_1_nt

    def _recvmesg_wrapper(self, conn, job):
        callback, aux = job
        try:
            callback(conn, aux)
        except TransportException as e:
            self._log.warning("Exception when handling %s: %r",
                              aux.peername, e)
            self._cleanup(conn, aux.peername)
        except socket.error as e:
            self._log.debug(
                "socket.error when receiving from %s: %r",
                aux.peername, e)
            self._cleanup(conn, aux.peername)

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
                    self.queue.append((self, chunk, remote))
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
