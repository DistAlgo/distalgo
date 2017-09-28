import time
import socket
import unittest

from da.common import WaitableQueue, QueueEmpty
from da.transport import UdpTransport, TcpTransport, SelectorLoop, \
    AuthenticationException

LOCALHOST = socket.gethostbyname('localhost')

KEY = b'1'
DATA = b'1234'
DEFAULT_PORT = 16000
class TestUdpTransport(unittest.TestCase):
    def setUp(self):
        self.transport = UdpTransport(KEY)
        self.queue = WaitableQueue()

    def test_init(self):
        self.transport.initialize()
        self.assertIsNotNone(self.transport.conn)
        self.transport.close()
        self.assertIsNone(self.transport.conn)

    def test_init_strict(self):
        self.transport.initialize(port=DEFAULT_PORT, strict=True)
        self.assertEqual(self.transport.conn.getsockname()[1], DEFAULT_PORT)
        self.transport.close()

    def test_sendrecv(self):
        self.transport.initialize(port=DEFAULT_PORT, strict=True)
        self.transport.start(self.queue)
        self.assertTrue(self.transport.started)
        sender = UdpTransport(KEY)
        sender.initialize()
        sender.send(DATA, (LOCALHOST, DEFAULT_PORT))
        transport, packet, _ = self.queue.pop(block=True)
        self.assertIs(transport, self.transport)
        self.assertEqual(packet[transport.data_offset:], DATA)
        self.transport.close()
        sender.close()

    def test_auth(self):
        self.transport.initialize(port=DEFAULT_PORT, strict=True)
        self.transport.start(self.queue)
        sender = UdpTransport(authkey=b'2')
        sender.initialize()
        with self.assertLogs('da.transport.sock.UdpTransport', level='WARN'):
            sender.send(DATA, (LOCALHOST, DEFAULT_PORT))
            with self.assertRaises(QueueEmpty):
                self.queue.pop(timeout=0.01)
        sender.close()
        self.transport.close()

class TestTcpTransport(unittest.TestCase):
    def setUp(self):
        self.transport = TcpTransport(KEY)
        self.queue = WaitableQueue()
        self.mesgloop = SelectorLoop()

    def test_init(self):
        self.transport.initialize()
        self.assertIsNotNone(self.transport.conn)
        self.transport.close()
        self.assertIsNone(self.transport.conn)

    def test_init_strict(self):
        self.transport.initialize(port=DEFAULT_PORT, strict=True)
        self.assertEqual(self.transport.conn.getsockname()[1], DEFAULT_PORT)
        self.transport.close()

    def test_start(self):
        self.transport.initialize()
        self.transport.start(self.queue, self.mesgloop)
        self.assertIs(self.transport.mesgloop, self.mesgloop)
        self.assertEqual(len(self.mesgloop), 1)
        self.transport.close()
        self.assertEqual(len(self.mesgloop), 0)

    def test_sendrecv(self):
        self.transport.initialize(port=DEFAULT_PORT, strict=True)
        self.transport.start(self.queue, self.mesgloop)
        self.assertTrue(self.transport.started)
        self.assertEqual(len(self.mesgloop), 1)
        sender = TcpTransport(KEY)
        sender.initialize()
        sender.start(self.queue, self.mesgloop)
        self.assertEqual(len(self.mesgloop), 2)
        sender.send(DATA, (LOCALHOST, DEFAULT_PORT))
        transport, packet, _ = self.queue.pop(block=True)
        self.assertIs(transport, self.transport)
        self.assertEqual(packet[transport.data_offset:], DATA)
        self.assertEqual(len(self.transport.cache), 1)

        # The other way:
        self.transport.send(DATA, sender.conn.getsockname())
        transport, packet, _ = self.queue.pop(block=True)
        self.assertIs(transport, sender)
        self.assertEqual(packet[transport.data_offset:], DATA)
        self.assertEqual(len(self.transport.cache), 1)
        self.transport.close()
        sender.close()

    def test_auth(self):
        self.transport.initialize(port=DEFAULT_PORT, strict=True)
        self.transport.start(self.queue, self.mesgloop)
        sender = TcpTransport(authkey=b'wrong key')
        sender.initialize()
        sender.start(self.queue, self.mesgloop)
        with self.assertLogs('da.transport.sock.TcpTransport', level='WARN'):
            with self.assertRaises(AuthenticationException):
                sender.send(DATA, (LOCALHOST, DEFAULT_PORT))
            with self.assertRaises(QueueEmpty):
                self.queue.pop(timeout=0.01)
        sender.close()
        self.transport.close()

if __name__ == '__main__':
    unittest.main()
