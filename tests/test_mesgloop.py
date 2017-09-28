import time
import socket
import unittest

from da.transport.mesgloop import *

DATA = b'1234'
DATA2 = b'9876'

class SelectorLoopTest(unittest.TestCase):
    def setUp(self):
        self.sender, self.recver = socket.socketpair()
        self.loop = SelectorLoop()

    def test_A_size(self):
        self.assertEqual(len(self.loop), 0)
        self.loop.start()
        self.loop.register(self.recver, (lambda a, b: None))
        time.sleep(0.01)
        self.assertEqual(len(self.loop), 1)
        self.loop.deregister(self.recver)
        self.assertEqual(len(self.loop), 0)
        self.loop.stop()
        time.sleep(0.01)
        self.assertFalse(self.loop.is_alive())

    def test_B_sendrecv1(self):
        loop = self.loop
        self.assertFalse(loop.is_alive())
        loop.start()
        data = None
        def callback(conn, aux):
            nonlocal data
            self.assertEqual(aux, self)
            data = conn.recv(100)
        time.sleep(0.01)
        self.assertTrue(loop.is_alive())
        loop.register(self.recver, callback, data=self)
        self.sender.send(DATA)
        time.sleep(0.01)
        loop.stop()
        time.sleep(0.01)
        self.assertEqual(data, DATA)
        loop.deregister(self.recver)
        self.assertFalse(loop.is_alive())

    def test_C_sendrecv2(self):
        loop = self.loop
        sender2, recver2 = socket.socketpair()
        data1, data2 = None, None
        def callback1(conn, aux):
            nonlocal data1
            self.assertEqual(aux, self)
            data1 = conn.recv(100)
        def callback2(conn, aux):
            nonlocal data2
            self.assertEqual(aux, self)
            data2 = conn.recv(100)
        loop.register(self.recver, callback1, data=self)
        loop.start()
        time.sleep(0.01)
        loop.register(recver2, callback2, data=self)
        self.assertTrue(loop.is_alive())
        self.sender.send(DATA)
        sender2.send(DATA2)
        time.sleep(0.01)
        loop.stop()
        time.sleep(0.01)
        self.assertEqual(data1, DATA)
        self.assertEqual(data2, DATA2)
        loop.deregister(self.recver)
        self.assertFalse(loop.is_alive())
        sender2.close()
        recver2.close()

    def test_D_close_connection(self):
        loop = self.loop
        data = None
        def callback(conn, _):
            nonlocal data
            data = 1
            conn.close()
            data = conn.recv(100)
        loop.register(self.recver, callback)
        loop.start()
        time.sleep(0.01)
        with self.assertLogs('da.transport.mesgloop', level='DEBUG') as cm:
            self.sender.send(DATA)
            time.sleep(0.01)
        self.assertEqual(data, 1)
        self.assertEqual(len(cm.output), 1)
        self.assertRegex(cm.output[0], "socket.error when receiving")
        loop.stop()

    def tearDown(self):
        self.sender.close()
        self.recver.close()

if __name__ == '__main__':
    unittest.main()
