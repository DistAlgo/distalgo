import os
import abc
import sys
import time
import queue
import select
import socket
import random
import pickle
import logging
import threading
import traceback
import multiprocessing

from . import pattern

class Null(object):
    def __init__(self, *args, **kwargs): pass
    def __call__(self, *args, **kwargs): return self
    def __getattribute__(self, attr): return self
    def __setattr__(self, attr, value): pass
    def __delattr__(self, attr): pass

class EndPoint:
    """Represents a target for sending of messages.

    This is the base class for all types of communication channels in
    DistAlgo. It uniquely identifies a "node" in the distributed system. In
    most scenarios, a process will only be associated with one EndPoint
    instance. The 'self' keyword in DistAlgo is ultimately translated into an
    instance of this class.

    """

    def __init__(self, name=None):
        if name is None:
            self._name = socket.gethostname()
        else:
            self._name = name
        self._proc = None
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
        return ("EndPoint", self._address, self._name)

    def __setstate__(self, value):
        proto, self._address, self._name = value
        self._log = logging.getLogger("runtime.EndPoint")

    def __str__(self):
        if self._address is not None:
            return str(self._address)
        else:
            return self._name

    def __repr__(self):
        if self._address is not None:
            return str(self._address[1])
        else:
            return self._name

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

    def __init__(self, name=None, port=None):
        super().__init__(name)

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
        return ("TCP", self._address, self._name)

    def __setstate__(self, value):
        proto, self._address, self._name = value
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

    def __init__(self, name=None, port=None):
        super().__init__(name)

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
        return ("UDP", self._address, self._name)

    def __setstate__(self, value):
        proto, self._address, self._name = value
        self._conn = None
        self._log = logging.getLogger("runtime.UdpEndPoint")

class DistProcess(multiprocessing.Process):
    """Abstract base class for DistAlgo processes.

    Each instance of this class enbodies the runtime activities of a DistAlgo
    process in a distributed system. Each process is uniquely identified by a
    two-ary tuple (address, port), where 'address' is the name or IP of the
    host machine and 'port' is an integer corresponding to the port number on
    which this process listens for incoming messages from other DistAlgo
    processes. Messages exchanged between DistAlgo processes are instances of
    `DistMessage`.

    DistAlgo processes can spawn more processes by calling `createprocs()`.
    The domain of `DistProcess` instances are flat, in the sense that all
    processes are created "equal" -- no parent-child relationship is
    maintained. Any DistProcess can send messages to any other DistProcess,
    given that it knows the unique id of the target process. However, the
    terminal is shared between all processes spawned from that terminal. This
    includes the stdout, stdin, and stderr streams. In addition, each
    DistProcess also maintains a TCP connection to the master control node
    (the first node started in a distributed system) where DistAlgo commands
    are passed (see `distalgo.runtime.proto`).

    Concrete subclasses of `DistProcess` must define the functions:

    - `setup`: A function that initializes the process-local variables.

    - `main`: The entry point of the process. This function defines the
      activities of the process.

    Users should not instantiate this class directly, process instances should
    be created by calling `createprocs()`.

    """

    class Comm(threading.Thread):
        """The background communications thread.

        Creates an event object for each incoming message, and appends the
        event object to the main process' event queue.
        """

        def __init__(self, parent):
            threading.Thread.__init__(self)
            self._parent = parent

        def run(self):
            try:
                for msg in self._parent._recvmesgs():
                    (src, clock, data) = msg
                    e = pattern.ReceivedEvent(
                            message=data,
                            timestamp=clock,
                            source=src,
                            destination=None)
                    self._parent._eventq.put(e)
            except KeyboardInterrupt:
                pass

    def __init__(self, parent, initpipe, channel, loginfo, name=None):
        multiprocessing.Process.__init__(self)

        self._running = False
        self._channel = channel

        self._logical_clock = 0

        self._events = []
        self._received_q = []
        self._jobqueue = []
        self._timer = None
        self._timer_expired = False
        self._failures = {'send': 0,
                          'receive': 0,
                          'crash': 0}
        self._evtimeout = None

        # Performance counters:
        self._usrtime_st = 0
        self._systime_st = 0
        self._waltime_st = 0
        self._usrtime = 0
        self._systime = 0
        self._waltime = 0
        self._is_timer_running = False

        self._loglevel = False
        self._dp_name = name
        self._log = None
        self._loginfo = loginfo

        self._parent = parent
        self._initpipe = initpipe
        self._child_procs = []

    def _wait_for_go(self):
        self._initpipe.send(self._id)
        while True:
            act = self._initpipe.recv()

            if act == "start":
                self._running = True
                del self._initpipe
                return
            else:
                inst, args = act
                if inst == "setup":
                    self.setup(*args)
                else:
                    m = getattr(self, "set_" + inst)
                    m(*args)

    def _start_comm_thread(self):
        self._eventq = queue.Queue()
        self._comm = DistProcess.Comm(self)
        self._comm.daemon =True
        self._comm.start()

    def _sighandler(self, signum, frame):
        import os
        import signal
        for cpid, _ in self._child_procs:
            os.kill(cpid, signal.SIGTERM)
        sys.exit(0)

    def run(self):
        try:
            import signal
            signal.signal(signal.SIGTERM, self._sighandler)

            self._id = self._channel(self._dp_name)
            self._start_comm_thread()

            self._log = logging.getLogger(str(self))
            self._log.setLevel(logging.DEBUG)
            formatter, consolelvl, filelvl, logdir = self._loginfo

            ch = logging.StreamHandler()
            ch.setLevel(consolelvl)
            ch.setFormatter(formatter)
            self._log.addHandler(ch)

            if logdir is not None:
                logfile = os.path.join(logdir, self._id.getlogname())
                fh = logging.FileHandler(logfile)
                fh.setLevel(filelvl)
                fh.setFormatter(formatter)
                self._log.addHandler(fh)

            self._wait_for_go()

            self.main()

            #            self._id.close()

        except Exception as e:
            sys.stderr.write("Unexpected error at process %s:%r"% (str(self), e))
            traceback.print_tb(e.__traceback__)

        except KeyboardInterrupt as e:
            self._log.debug("Received KeyboardInterrupt, exiting")
            pass

    def start_timers(self):
        if not self._is_timer_running:
            self._usrtime_st, self._systime_st, _, _, _ = os.times()
            self._waltime_st = time.clock()
            self._is_timer_running = True

    def stop_timers(self):
        if self._is_timer_running:
            usrtime, systime, _, _, _ = os.times()
            self._usrtime += usrtime - self._usrtime_st
            self._systime += systime - self._systime_st
            self._waltime_st += time.clock() - self._waltime_st
            self._is_timer_running = False

    def report_times(self):
        self._parent.send(('totalusrtime', self._usrtime), self._id)
        self._parent.send(('totalsystime', self._systime), self._id)
        self._parent.send(('totaltime', self._waltime), self._id)

    def report_mem(self):
        import pympler.asizeof
        memusage = pympler.asizeof.asizeof(self) / 1024
        self._parent.send(('mem', memusage), self._id)

    def exit(self, code):
        raise SystemExit(10)

    def output(self, message, level=logging.INFO):
        self._log.log(level, message)

    def purge_received(self):
        for attr in dir(self):
            if attr.startswith("_receive_messages_"):
                setattr(self, attr, [])

    def purge_sent(self):
        for attr in dir(self):
            if attr.startswith("_sent_messages_"):
                setattr(self, attr, [])

    def spawn(self, pcls, args):
        """Spawns a child process"""

        childp, ownp = multiprocessing.Pipe()
        p = pcls(self._id, childp, self._channel, self._loginfo)
        p._loglevel = self._loglevel
        p.start()

        childp.close()
        cid = ownp.recv()
        ownp.send(("setup", args))
        ownp.send("start")

        #self._child_procs.append((p.pid, cid))

        return cid

    # Wrapper functions for message passing:
    def _send(self, data, to):
        self.incr_logical_clock()
        if (self._fails('send')):
            return False

        result = True
        if (hasattr(to, '__iter__')):
            for t in to:
                r = t.send(data, self._id, self._logical_clock)
                if not r: result = False
        else:
            result = to.send(data, self._id, self._logical_clock)

        if (self._loglevel):
            self.output("Sent %s -> %r"%(str(data), to))
        self._trigger_event(pattern.Event(pattern.SentEvent, self._id,
                                          self._logical_clock,data))
        self._parent.send(('sent', 1), self._id)
        return result

    def _recvmesgs(self):
        for mesg in self._id.recvmesgs():
            if not (self._fails('receive')):
                yield mesg

    def _timer_start(self):
        self._timer = time.time()
        self._timer_expired = False

    def _timer_end(self):
        self._timer = None

    def _fails(self, failtype):
        if not failtype in self._failures.keys():
            return False
        if (random.randint(0, 100) < self._failures[failtype]):
            return True
        return False

    def _label(self, name, block=False, timeout=None):
        """This simulates the controlled "label" mechanism.

        Currently we simply handle one event on one label call.

        """
        if (self._fails('crash')):
            self.output("Stuck in label: %s" % name)
            self.exit(10)

        if timeout is not None:
            if self._timer is None:
                self._timer_start()
            timeleft = timeout - (time.time() - self._timer)
            if timeleft <= 0:
                self._timer_end()
                self._timer_expired = True
                return
        else:
            timeleft = None
        self._process_event(block, timeleft)
        self._process_jobqueue(name)

    def _process_jobqueue(self, label=None):
        newq = []
        for handler, args in self._jobqueue:
            if ((handler._labels is None or label in handler._labels) and
                (handler._notlabels is None or label not in handler._notlabels)):
                try:
                    handler(**args)
                except TypeError as e:
                    self._log.warn("Insufficient bindings to call handler:", e)
            else:
                newq.append((handler, args))
        self._jobqueue = newq

    def _process_event(self, block, timeout=None):
        """Retrieves one message, then process the backlog event queue.

        Parameter 'block' indicates whether to block waiting for next message
        to come in if the queue is currently empty. 'timeout' is the maximum
        time to wait for an event.

        """
        if timeout is not None and timeout < 0:
            timeout = 0
        try:
            event = self._eventq.get(block, timeout)
            self._logical_clock = max(self._logical_clock, event.timestamp) + 1
            self._trigger_event(event)
        except queue.Empty:
            return
        except Exception as e:
            self._log.error("Caught exception while waiting for events: %r", e)
            return

    def _trigger_event(self, event):
        """Immediately triggers 'event', skipping the event queue.

        """

        self._log.debug("triggering event %s%r%r%r%r" %
                        (type(event).__name__,
                         event.message, event.timestamp,
                         event.destination, event.source))
        for p in self._events:
            bindings = dict()
            if (p.match(event, bindings=bindings,
                        ignore_bound_vars=True, **self.__dict__)):
                if p.record_history:
                    getattr(self, p.name).append(event)
                for h in p.handlers:
                    self._jobqueue.append((h, bindings))

    def _forever_message_loop(self):
        while (True):
            self._process_event(self._events, True)

    def _has_received(self, mess):
        try:
            self._received_q.remove(mess)
            return True
        except ValueError:
            return False

    def __str__(self):
        s = self.__class__.__name__
        if self._dp_name is not None:
            s += "(" + self._dp_name + ")"
        else:
            s += "(" + str(self._id) + ")"
        return s

    def work(self):
        """Waste some random amount of time."""
        time.sleep(random.randint(0, 200) / 100)
        pass

    def logical_clock(self):
        """Returns the current value of Lamport clock."""
        return self._logical_clock

    def incr_logical_clock(self):
        """Increment Lamport clock by 1."""
        self._logical_clock += 1


    ### Various attribute setters:
    def set_loglevel(self, level):
        self._loglevel = level

    def set_send_fail_rate(self, rate):
        self._failures['send'] = rate

    def set_receive_fail_rate(self, rate):
        self._failures['receive'] = rate

    def set_crash_rate(self, rate):
        self._failures['crash'] = rate

    def set_event_timeout(self, time):
        self._evtimeout = time

    def set_name(self, name):
        self._dp_name = name
