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

import os
import abc
import sys
import copy
import enum
import time
import signal
import random
import logging
import threading
import traceback
import collections
import multiprocessing

from . import pattern, common, endpoint
builtin = common.builtin

logger = logging.getLogger(__name__)

class Command(enum.Enum):
    """An enum of process commands.
    """
    Start = 1
    Setup = 2

_config_object = dict()

class DistProcess():
    """Abstract base class for DistAlgo processes.

    Each instance of this class enbodies the runtime activities of a DistAlgo
    process in a distributed system. Each process is uniquely identified by a
    two-ary tuple (address, port), where 'address' is the name or IP of the
    host machine and 'port' is an integer corresponding to the port number on
    which this process listens for incoming messages from other DistAlgo
    processes. Messages exchanged between DistAlgo processes are instances of
    `DistMessage`.

    DistAlgo processes can spawn more processes by calling `new`.
    The domain of `DistProcess` instances are flat, in the sense that all
    processes are created "equal" -- no parent-child relationship is
    maintained. Any DistProcess can send messages to any other DistProcess,
    given that it knows the unique id of the target process. However, the
    terminal is shared between all processes spawned from that terminal. This
    includes the stdout, stdin, and stderr streams. In addition, each
    DistProcess also maintains a TCP connection to the master control node
    (the first node started in a distributed system) where DistAlgo commands
    are passed (see `distalgo.runtime.proto`).

    Concrete subclasses of `DistProcess` must define the methods:

    - `setup`: A function that initializes the process-local variables.

    - `run`: The entry point of the process. This function defines the
      activities of the process.

    Users should not instantiate this class directly, process instances should
    be created by calling `new`.

    """
    def __init__(self, procimpl, props):
        self._log = logging.getLogger(self.__class__.__module__) \
                           .getChild(self.__class__.__qualname__)
        self.__procimpl = procimpl
        self.__router = procimpl._comm
        self.__channel = procimpl.endpoint
        self.__messageq = procimpl.message_queue
        self.__properties = props
        self.__jobq = collections.deque()
        self.__lock = threading.Lock()
        self.__lock.acquire()
        if self.get_config('handling', default='one').casefold() == 'all':
            self.__do_label = self.__label_all
        else:
            self.__do_label = self.__label_one

        self.id = procimpl.endpoint
        self._state = common.Namespace()
        self._events = []
        self._timer = None
        self._timer_expired = False
        if self.get_config('clock', default='none').casefold() == 'lamport':
            self._logical_clock = 0
        else:
            self._logical_clock = None
        self._dp_name = procimpl.name

    def setup(self):
        pass

    def run(self):
        pass

    @classmethod
    def get_channel_type(cls):
        ept = endpoint.UdpEndPoint
        props = cls.get_config("channel", default=[])
        if isinstance(props, str):
            props = [props]
        for prop in props:
            prop = prop.casefold()
            if prop == "fifo":
                ept = endpoint.TcpEndPoint
            elif prop == "reliable":
                ept = endpoint.TcpEndPoint
            elif prop not in {"unfifo", "unreliable"}:
                logger.error("Unknown channel property %r", prop)
        return ept

    _config_object = dict()
    @classmethod
    def get_config(cls, key, default=None):
        """Returns the configuration value for specified 'key'.
        """
        if key in common.global_options().config:
            return common.global_options().config[key]
        elif key in common.global_config():
            return common.global_config()[key]
        elif key in cls._config_object:
            return cls._config_object[key]
        elif key in sys.modules[cls.__module__]._config_object:
            return sys.modules[cls.__module__]._config_object[key]
        else:
            return default

    @builtin
    def exit(self, code=0):
        raise SystemExit(code)

    @builtin
    def output(self, *value, sep=' ', level=logging.INFO+1):
        """Prints arguments to the process log.

        Optional argument 'level' is a positive integer that specifies the
        logging level of the message, defaults to 'logging.INFO'(20). Refer to
        [https://docs.python.org/3/library/logging.html#levels] for a list of
        predefined logging levels.

        When the level of the message is equal to or higher than the
        configured level of a log handler, the message is logged to that
        handler; otherwise, it is ignored. DistAlgo processes are
        automatically configured with two log handlers:, one logs to the
        console, the other to a log file; the handlers' logging levels are
        controlled by command line parameters.

        """
        msg = sep.join([str(v) for v in value])
        self._log.log(level, msg)

    @builtin
    def work(self):
        """Waste some random amount of time."""
        time.sleep(random.randint(0, 200) / 100)
        pass

    @builtin
    def logical_clock(self):
        """Returns the current value of Lamport clock."""
        return self._logical_clock

    @builtin
    def incr_logical_clock(self):
        """Increment Lamport clock by 1."""
        if isinstance(self._logical_clock, int):
            self._logical_clock += 1

    # Wrapper functions for message passing:
    def _send(self, message, to):
        self.incr_logical_clock()
        if (self.__fails('send')):
            self._log.warning(
                "Dropped outgoing message due to lottery: %r", message)
            return

        if (hasattr(to, '__iter__')):
            targets = to
        else:
            targets = [to]
        for t in targets:
            t.send(message, self.id, self._logical_clock)

        self.__trigger_event(pattern.SentEvent(
            (self._logical_clock, to, self.id), message))

    def _timer_start(self):
        self._timer = time.time()
        self._timer_expired = False

    def _timer_end(self):
        self._timer = None

    def __fails(self, failtype):
        if failtype not in self.__properties:
            return False
        if (random.random() < self.__properties[failtype]):
            return True
        return False

    def _label(self, name, block=False, timeout=None):
        """This simulates the controlled "label" mechanism.

        The number of pending events handled at each label is controlled by the
        'handling' configuration key -- if 'handling' is 'one' then `__do_label`
        will be set to `__label_one`, otherwise `__do_label` will be set to
        `_label_all`(see `__init__`).

        """
        if self.__fails('hang'):
            self._log.warning("Hanged(@label %s)", name)
            self._lock.acquire()
        if self.__fails('crash'):
            self._log.warning("Crashed(@label %s)", name)
            self.exit(10)

        self.__do_label(name, block, timeout)
        self.__process_jobqueue(name)

    def __label_one(self, name, block=False, timeout=None):
        """Handle at most one pending event at a time.

        """
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
        self.__process_event(block, timeleft)

    def __label_all(self, name, block=False, timeout=None):
        """Handle up to all pending events at the time this function is called.

        """
        # 'nmax' is a "snapshot" of the queue size at the time we're called. We
        # only attempt to process up to 'nmax' events, since otherwise we could
        # potentially block the process forever if the events come in faster
        # than we can process them:
        nmax = len(self.__messageq)
        i = 0
        while True:
            i += 1
            if timeout is not None:
                if self._timer is None:
                    self._timer_start()
                timeleft = timeout - (time.time() - self._timer)
                if timeleft <= 0:
                    self._timer_end()
                    self._timer_expired = True
                    break
            else:
                timeleft = None
            if not self.__process_event(block, timeleft) or i >= nmax:
                break

    def __process_jobqueue(self, label=None):
        """Runs all pending handlers jobs permissible at `label`.
        """
        leftovers = []
        handler = args = None
        while self.__jobq:
            try:
                handler, args = self.__jobq.popleft()
            except IndexError:
                self._log.debug("Job item stolen by another thread.")
                break
            except ValueError:
                self._log.error("Corrupted job item!")
                continue

            if ((handler._labels is None or label in handler._labels) and
                (handler._notlabels is None or label not in handler._notlabels)):
                try:
                    handler(**args)
                except Exception as e:
                    self._log.error(
                        "%r when calling handler '%s' with '%s': %s",
                        e, handler.__name__, args, e)
            else:
                self._log.debug("Skipping (%s, %r) due to label constraint.",
                                handler, args)
                leftovers.append((handler, args))
        self.__jobq.extend(leftovers)

    def __process_event(self, block, timeout=None):
        """Retrieves and processes one pending event.

        Parameter 'block' indicates whether to block waiting for next message
        to come in if the queue is currently empty. 'timeout' is the maximum
        time to wait for an event. Returns True if an event was successfully
        processes, False otherwise.

        """
        event = None
        if timeout is not None and timeout < 0:
            timeout = 0

        try:
            event = self.__messageq.pop(block, timeout)
        except common.QueueEmpty:
            pass
        except Exception as e:
            self._log.error("Caught exception while waiting for events: %r", e)
            return False

        if event is None:
            if block:
                self._log.debug(
                    "__process_event: event was stolen by another thread.")
            return False

        if self.__fails('receive'):
            self._log.warning("Dropped incoming message due to lottery: %s", event)
            return False

        if isinstance(self._logical_clock, int):
            if not isinstance(event.timestamp, int):
                # Most likely some peer did not turn on lamport clock, issue
                # a warning and skip this message:
                self._log.warning(
                    "Invalid logical clock value: %r; message dropped. ",
                    event.timestamp)
                return False
            self._logical_clock = max(self._logical_clock, event.timestamp) + 1
        self.__trigger_event(event)
        return True

    def __trigger_event(self, event):
        """Immediately triggers 'event', skipping the event queue.

        """
        self._log.debug("triggering event %s", event)
        for p in self._events:
            bindings = dict()
            if (p.match(event, bindings=bindings,
                        ignore_bound_vars=True, **self.__dict__)):
                if p.record_history is True:
                    getattr(self, p.name).append(event.to_tuple())
                elif p.record_history is not None:
                    # Call the update stub:
                    p.record_history(getattr(self, p.name), event.to_tuple())
                for h in p.handlers:
                    self.__jobq.append((h, copy.deepcopy(bindings)))

    def _forever_message_loop(self):
        while (True):
            self.__process_event(self._events, True)

    def __repr__(self):
        res = "{cname}<{pname}, {pid}>"
        return res.format(cname=self.__class__.__name__,
                          pname=self._dp_name,
                          pid=(self.id))

    def __str__(self):
        s = self.__class__.__name__
        if self._dp_name is not None:
            s += "<" + self._dp_name + ">"
        else:
            s += "<" + str(self.id) + ">"
        return s


class Comm(threading.Thread):
    """The background communications thread.

    Creates an event object for each incoming message, and appends the event
    object to the target process' event queue.

    """
    def __init__(self, endpoint):
        threading.Thread.__init__(self)
        self.log = logging.getLogger(__name__) \
                          .getChild(self.__class__.__qualname__)
        self.daemon = True
        self.endpoint = endpoint
        self.num_waiting = 0
        self.q = common.WaitableQueue()

    def run(self):
        try:
            self.mesgloop()
        except KeyboardInterrupt:
            self.log.debug("Received KeyboardInterrupt, stopping.")

    def mesgloop(self):
        for msg in self.endpoint.recvmesgs():
            try:
                (src, clock, data) = msg
            except ValueError as e:
                self.log.warning("Dropped invalid message: %r", msg)
                continue

            e = pattern.ReceivedEvent(envelope=(clock, None, src),
                                      message=data)
            self.q.append(e)


class OSProcessImpl(multiprocessing.Process):
    """An implementation of processes using OS process.

    """
    def __init__(self, dacls, initpipe, name="", props=None):
        assert issubclass(dacls, DistProcess)

        super().__init__()
        # Logger can not be serialized so it has to be instantiated in the child
        # proc's address space:
        self._log = None
        self._dacls = dacls
        self._daobj = None
        self._running = False
        self._setup_called = False
        self._initpipe = initpipe
        self._cmdline = common.global_options()
        if props is not None:
            self._properties = props
        else:
            self._properties = dict()
        self.daemon = props['daemon'] if 'daemon' in props else False
        self.name = name
        self.endpoint = None
        self._child_procs = []

    def _wait_for_go(self):
        self._log.debug("Sending id to parent...")
        self._initpipe.send(self.endpoint)
        while True:
            act = self._initpipe.recv()

            if act == Command.Start:
                self._running = True
                del self._initpipe
                self._log.debug("`start` command received, commencing...")
                return
            else:
                inst, args = act
                if inst == Command.Setup:
                    if self._setup_called:
                        self._log.warning(
                            "`setup` already called for this process!")
                    else:
                        self._log.debug("Running `setup`...")
                        self._daobj.setup(*args)
                        self._setup_called = True
                        self._log.debug("`setup` complete.")
                else:
                    m = getattr(self, "set_" + inst)
                    m(*args)

    def _start_comm_thread(self):
        self._comm = Comm(self.endpoint)
        self.message_queue = self._comm.q
        self._comm.start()

    def _sighandler(self, signum, frame):
        for cpid, _ in self._child_procs:
            os.kill(cpid, signal.SIGTERM)
        sys.exit(0)

    def _debug_handler(self, sig, frame):
        self._debugger.set_trace(frame)

    def run(self):
        self._log = logger.getChild(self.__class__.__qualname__)
        if len(self.name) == 0:
            self.name = str(self.pid)
        try:
            self._cmdline.this_module_name = self.__class__.__module__
            if multiprocessing.get_start_method() == 'spawn':
                common.set_global_options(self._cmdline)
                common.sysinit()
            common.set_current_process()
            signal.signal(signal.SIGTERM, self._sighandler)
            signal.signal(signal.SIGUSR1, self._debug_handler)
            self.endpoint = self._dacls.get_channel_type()(self._dacls)
            self._start_comm_thread()
            pattern.initialize(self.endpoint)
            self._daobj = self._dacls(self, self._properties)
            self._wait_for_go()

            return self._daobj.run()

        except Exception as e:
            sys.stderr.write("Unexpected error at process %s:%r"% (str(self), e))
            traceback.print_tb(e.__traceback__)

        except KeyboardInterrupt as e:
            self._log.debug("Received KeyboardInterrupt, exiting")
            pass

    def __str__(self):
        return "{0}<{1}>".format(self.__class__.__qualname__, str(self.pid))
