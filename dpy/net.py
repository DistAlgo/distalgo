import select
import threading
import socket as so

class Channel:
    def __init__(self, upper, lower):
        self.lower = lower
        self.upper = upper

    def init(self):
        pass

    def send(self, iov, timeout=None):
        self.lower.send(iov, timeout)

    def recv(self, data):
        self.upper.recv(data)

class BaseChannel(Channel):
    def __init__(self, sock, upper, lower):
        super.__init__(self, upper, None) # We are the lowest level
        self.sock = sock        # the underlying socket

    def send(self, iov, timeout=None):
        buf = iov.join()
        self.sock.send(buf, timeout)

    def recv(self, data):
        self.upper.recv(data)

class StreamChannel(BaseChannel):
    def __init__(self, sock, upper):
        super.__init__(self, upper, None)
        self.sock = sock

class ChannelManager(threading.Thread):
    def __init__(self):
        self.tcpsock = None
        self.udpsock = None
        self.daemon = True
        self.readlist = []
        self.lock = threading.Lock()
        self.tcp_add_map = dict()
        self.udp_add_map = dict()

    def _new_connection(self, proto):
        if proto.init():
            self.readlist.append(proto)

    def listener(self, serversock):
        try:
            while True:
                conn, addr = serversock.accept()
                threading.Thread(protocls(conn, addr))

    def run(self):
        try:
            while True:
                r, _, _ = select.select(TcpEndPoint.receivers.keys(), [], [])

                if self.tcpsock in r:
                    # We have pending new connections, handle the first in
                    # line. If there are any more they will have to wait until
                    # the next iteration
                    conn, addr = self.tcpsock.accept()
                    TcpEndPoint.receivers[conn] = addr
                    r.remove(self._conn)

                for c in r:
                    try:
                        msg = c.recv()
                        yield obj, c.getpeername()

                    except pickle.UnpicklingError as e:
                        self._log.warn("UnpicklingError, packet from %s dropped",
                                       TcpEndPoint.receivers[c])

                    except socket.error as e:
                        self._log.debug("Remote connection %s terminated.", str(c))
                        del TcpEndPoint.receivers[c]

        except select.error as e:
            self._log.debug("select.error occured, terminating receive loop.")
