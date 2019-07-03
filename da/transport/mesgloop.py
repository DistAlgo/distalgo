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
import socket
import selectors
import threading

__all__ = ["SelectorLoop"]

logger = logging.getLogger(__name__)

RECV_BUF_SIZE = 32

class TerminateLoop(Exception): pass
class SelectorLoop(object):
    """Wrapper around a Selector object providing a background message loop.

    """
    def __init__(self, selectorcls=selectors.DefaultSelector):
        super().__init__()
        # Multiplexer for all sockets:
        self.selector = selectorcls()
        # Class logger instance:
        self._log = logger.getChild(self.__class__.__name__)
        # A dummy socket pair for waking up the message-loop:
        self.notifier, self.event = None, None
        # Background thread:
        self.worker = None

    def _handle_event(self, sock, _):
        # Just drain the event socket buffer:
        data = sock.recv(RECV_BUF_SIZE)
        if not data:
            raise TerminateLoop()

    def __len__(self):
        """Returns the number of registered callbacks."""
        reg = self.selector.get_map()
        if self.event and self.event in reg:
            return len(reg) - 1
        else:
            return len(reg)

    def register(self, conn, callback, data=None):
        """Registers a new connection object.

        """
        try:
            self.selector.register(conn, selectors.EVENT_READ, (callback, data))
            self.notify()
        except ValueError as e:
            # The conn object was already closed, so call the callback to
            # trigger any cleanup routines from the caller
            self._log.debug("Registering invalid connection %s: %r",
                            conn, e, exc_info=1)
            callback(conn, data)

    def deregister(self, conn):
        try:
            self.selector.unregister(conn)
            # No need to wake the main loop here
        except (KeyError, ValueError):
            pass

    def notify(self):
        """Wake the main message loop."""
        if self.notifier:
            try:
                self.notifier.send(b'x')
            except (AttributeError, OSError):
                # socket already closed, just ignore
                pass

    def is_alive(self):
        return self.worker is not None and self.worker.is_alive()

    def start(self):
        """Starts the message loop thread."""
        if self.worker is None:
            self.worker = threading.Thread(target=self.run, daemon=True)
            self.worker.start()

    def stop(self):
        """Stops the message loop thread."""
        if self.notifier:
            try:
                self.notifier.close()
            except (AttributeError, OSError):
                pass

    def run(self):
        try:
            self.notifier, self.event = socket.socketpair()
            self.selector.register(self.event, selectors.EVENT_READ,
                                   (self._handle_event, None))
            while True:
                events = self.selector.select()
                for key, _ in events:
                    callback, aux = key.data
                    try:
                        callback(key.fileobj, aux)
                    except socket.error as e:
                        if key.fileobj is self.event:
                            self._log.error("Notifier socket failed: %r", e)
                            break
                        else:
                            self._log.debug(
                                "socket.error when receiving from %s: %r",
                                key, e, exc_info=1)
                            self.deregister(key.fileobj)
        except TerminateLoop:
            pass
        except Exception as e:
            self._log.error("Message loop terminated abnormally: %r", e)
            self._log.debug("Uncaught exception %r", e, exc_info=1)
        finally:
            if self.notifier:
                try:
                    self.notifier.close()
                except OSError:
                    pass
            if self.event:
                self.deregister(self.event)
                try:
                    self.event.close()
                except OSError:
                    pass
            self.notifier, self.event = None, None
            self.worker = None
