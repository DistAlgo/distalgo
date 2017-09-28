import socket
import unittest

from da.common import WaitableQueue
from da.transport import TransportManager, TransportTypes,\
    UdpTransport, TcpTransport

KEY = b'1abc'
DATA = b'1234'
LOCALHOST = socket.gethostbyname('localhost')
DEFAULT_PORT = 16000
class TestTransportManager(unittest.TestCase):
    def setUp(self):
        self.manager = TransportManager(cookie=KEY)

    def test_init_close(self):
        self.manager.initialize()
        self.assertTrue(self.manager.initialized)
        self.manager.close()
        self.assertFalse(self.manager.initialized)

    def test_start(self):
        self.manager.initialize()
        self.assertTrue(self.manager.initialized)
        self.manager.start()
        self.assertTrue(self.manager.started)
        self.assertEqual(
            len([t for t in self.manager.transports if t is not None]),
            len(TransportTypes))
        self.manager.close()
        self.assertFalse(self.manager.initialized)

    def test_sendrecv(self):
        self.manager.initialize(port=DEFAULT_PORT, strict=True)
        self.manager.start()

        queue = WaitableQueue()
        udpsender = UdpTransport(KEY)
        udpsender.initialize()
        udpsender.start(queue)
        udpsender.send(DATA, (LOCALHOST, DEFAULT_PORT))
        udpsender.close()
        tcpsender = TcpTransport(KEY)
        tcpsender.initialize()
        tcpsender.start(queue)
        tcpsender.send(DATA, (LOCALHOST, DEFAULT_PORT))
        tcpsender.close()

        queue = self.manager.queue
        transport, packet, _ = queue.pop(block=True)
        self.assertEqual(packet[transport.data_offset:], DATA)
        transport, packet, _ = queue.pop(block=True)
        self.assertEqual(packet[transport.data_offset:], DATA)

        self.manager.close()

if __name__ == '__main__':
    unittest.main()
