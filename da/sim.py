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
import time
import signal
import random
import logging
import threading
import traceback
import collections
import multiprocessing

from da import pattern, common, endpoint

builtin = common.builtin

_config_object = dict()

class Comm(threading.Thread):
    """The background communications thread.

    Creates an event object for each incoming message, and appends the
    event object to the main process' event queue.
    """

    def __init__(self, router):
        threading.Thread.__init__(self)
        self.daemon = True
        self.router = router
        self.condition = threading.Condition()
        self.num_waiting = 0
        self.q = collections.deque()
        self.log = logging.getLogger(str(self))

    def run(self):
        try:
            self.mesgloop()
        except KeyboardInterrupt:
            pass

    def mesgloop(self):
        for msg in self.router.recvmesgs():
            try:
                (src, clock, data) = msg
            except ValueError as e:
                self.log.warn("Invalid message dropped: {0}".format(str(msg)))
                continue

            e = pattern.ReceivedEvent(
                envelope=(clock, None, src),
                message=data)
            self.q.append(e)
            if self.num_waiting > 0:
                with self.condition:
                    self.condition.notify_all()


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

    - `run`: The entry point of the process. This function defines the
      activities of the process.

    Users should not instantiate this class directly, process instances should
    be created by calling `createprocs()`.

    """

    def __init__(self, parent, initpipe, props=None):
        multiprocessing.Process.__init__(self)

        self.id = None
        self._state = common.Namespace()
        self._running = False
        self._parent = parent
        self._initpipe = initpipe
        self._cmdline = common.global_options()
        if props is not None:
            self._properties = props
        else:
            self._properties = dict()

        self._logical_clock = None
        self._events = []
        self._jobqueue = list()

        self._timer = None
        self._timer_expired = False
        self._lock = None
        self._setup_called = False

        self._dp_name = self._properties.get('name', None)
        self._log = None

        self._child_procs = []

    def _wait_for_go(self):
        self._log.debug("Sending id to parent...")
        self._initpipe.send(self.id)
        while True:
            act = self._initpipe.recv()

            if act == "start":
                self._running = True
                del self._initpipe
                self._log.debug("'start' command received, commencing...")
                return
            else:
                inst, args = act
                if inst == "setup":
                    if self._setup_called:
                        self._log.warn(
                            "setup() already called for this process!")
                    else:
                        self._log.debug("Running setup..")
                        self.setup(*args)
                        self._setup_called = True
                else:
                    m = getattr(self, "set_" + inst)
                    m(*args)

    def _start_comm_thread(self):
        self._comm = Comm(self.id)
        self._comm.start()

    def _sighandler(self, signum, frame):
        for cpid, _ in self._child_procs:
            os.kill(cpid, signal.SIGTERM)
        sys.exit(0)

    def _debug_handler(self, sig, frame):
        self._debugger.set_trace(frame)

    _config_object = dict()

    def get_channel_type(self):
        ept = endpoint.UdpEndPoint
        props = self.get_config("channel", default=[])
        if isinstance(props, str):
            props = [props]
        for prop in props:
            prop = prop.casefold()
            if prop == "fifo":
                ept = endpoint.TcpEndPoint
            elif prop == "reliable":
                ept = endpoint.TcpEndPoint
            elif prop not in {"unfifo", "unreliable"}:
                log.error("Unknown channel property %s", str(prop))
        return ept

    @builtin
    def get_config(self, key, default=None):
        """Returns the configuration value for specified 'key'.
        """
        if key in common.global_options().config:
            return common.global_options().config[key]
        elif key in self._config_object:
            return self._config_object[key]
        elif key in sys.modules[self.__module__]._config_object:
            return sys.modules[self.__module__]._config_object[key]
        else:
            return default

    def run(self):
        try:
            self._cmdline.this_module_name = self.__class__.__module__
            if multiprocessing.get_start_method() == 'spawn':
                common.set_global_options(self._cmdline)
                common.sysinit()

            signal.signal(signal.SIGTERM, self._sighandler)
            signal.signal(signal.SIGUSR1, self._debug_handler)
            self.id = self.get_channel_type()(self._dp_name, self.__class__)
            common.set_current_process(self.id)
            pattern.initialize(self.id)
            if self.get_config('clock', default='none').casefold() == 'lamport':
                self._logical_clock = 0
            if self.get_config('handling', default='one').casefold() == 'all':
                self._do_label = self._label_all
            else:
                self._do_label = self._label_one
            self._log = logging.getLogger(str(self))
            self._start_comm_thread()
            self._lock = threading.Lock()
            self._lock.acquire()
            self._wait_for_go()

            if not hasattr(self, '_da_run_internal'):
                self._log.error("Process does not have entry point!")
                sys.exit(1)

            result = self._da_run_internal()

        except Exception as e:
            sys.stderr.write("Unexpected error at process %s:%r"% (str(self), e))
            traceback.print_tb(e.__traceback__)

        except KeyboardInterrupt as e:
            self._log.debug("Received KeyboardInterrupt, exiting")
            pass

    @builtin
    def exit(self, code=0):
        raise SystemExit(code)

    @builtin
    def output(self, *value, sep=' ', level=logging.INFO):
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
    def _send(self, data, to):
        self.incr_logical_clock()
        if (self._fails('send')):
            self.output("Simulated send fail: %s" % str(data), logging.WARNING)
            return

        if (hasattr(to, '__iter__')):
            targets = to
        else:
            targets = [to]
        for t in targets:
            t.send(data, self.id, self._logical_clock)

        self._trigger_event(pattern.SentEvent((self._logical_clock,
                                               to, self.id),
                                              copy.deepcopy(data)))
        self._parent.send(('sent', 1), self.id)

    def _recvmesgs(self):
        for mesg in self.id.recvmesgs():
            if self._fails('receive'):
                self.output("Simulated receive fail: %s" % str(mesg),
                            logging.WARNING)
            else:
                yield mesg

    def _timer_start(self):
        self._timer = time.time()
        self._timer_expired = False

    def _timer_end(self):
        self._timer = None

    def _fails(self, failtype):
        if failtype not in self._properties:
            return False
        if (random.random() < self._properties[failtype]):
            return True
        return False

    def _label(self, name, block=False, timeout=None):
        """This simulates the controlled "label" mechanism.

        The number of pending events handled at each label is controlled by the
        'handling' configuration key -- if 'handling' is 'one' then `_do_label'
        will be set to `_label_one', otherwise `_do_label' will be set to
        `_label_all'.

        """
        if self._fails('hang'):
            self.output("Hanged(@label %s)" % name, logging.WARNING)
            self._lock.acquire()
        if self._fails('crash'):
            self.output("Crashed(@label %s)" % name, logging.WARNING)
            self.exit(10)

        self._do_label(name, block, timeout)
        self._process_jobqueue(name)

    def _label_one(self, name, block=False, timeout=None):
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
        self._process_event(block, timeleft)

    def _label_all(self, name, block=False, timeout=None):
        """Handle up to all pending events at the time this function is called.

        """
        # 'nmax' is a "snapshot" of the queue size at the time we're called. We
        # only attempt to process up to 'nmax' events, since otherwise we could
        # potentially block the process forever if the events come in faster
        # than we can process them:
        nmax = len(self._comm.q)
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
            if not self._process_event(block, timeleft) or i >= nmax:
                break

    def _process_jobqueue(self, label=None):
        leftovers = []
        for handler, args in self._jobqueue:
            if ((handler._labels is None or label in handler._labels) and
                (handler._notlabels is None or label not in handler._notlabels)):
                try:
                    handler(**args)
                except TypeError as e:
                    self._log.error(
                        "%s when calling handler '%s' with '%s': %s",
                        type(e).__name__, handler.__name__, str(args), str(e))
            else:
                leftovers.append((handler, args))
        self._jobqueue = leftovers

    def _process_event(self, block, timeout=None):
        """Retrieves and processes one pending event.

        Parameter 'block' indicates whether to block waiting for next message
        to come in if the queue is currently empty. 'timeout' is the maximum
        time to wait for an event. Returns True if an event was successfully
        processes, False otherwise.

        """
        event = None
        if timeout is not None and timeout < 0:
            timeout = 0

        # Opportunistically try to get the next event off the queue:
        try:
            event = self._comm.q.popleft()
        except IndexError:
            pass

        if event is None:
            # The queue was empty, if we don't need to block then we're done:
            if not block or timeout == 0:
                return False
            # Otherwise, we have to acquire the condition object and block:
            else:
                try:
                    with self._comm.condition:
                        self._comm.num_waiting += 1
                        self._comm.condition.wait(timeout)
                        self._comm.num_waiting -= 1
                    # We have to try fetching an event again to preserve the
                    # semantics of 'handling=one':
                    event = self._comm.q.popleft()
                except IndexError:
                    # If the queue is still empty, it means that the new event
                    # was picked up by another thread, so it's ok for us to
                    # return:
                    return False
                except Exception as e:
                    self._log.error(
                        "Caught exception while waiting for events: %r", e)
                    return False

        if isinstance(self._logical_clock, int):
            if not isinstance(event.timestamp, int):
                # Most likely some peer did not turn on lamport clock, issue
                # a warning and skip this message:
                self._log.warn(
                    "Invalid logical clock value: {0}; message dropped. "
                    "".format(event.timestamp))
                return False
            self._logical_clock = max(self._logical_clock, event.timestamp) + 1
        self._trigger_event(event)
        return True

    def _trigger_event(self, event):
        """Immediately triggers 'event', skipping the event queue.

        """

        self._log.debug("triggering event %s" % event)
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
                    self._jobqueue.append((h, copy.deepcopy(bindings)))

    def _forever_message_loop(self):
        while (True):
            self._process_event(self._events, True)

    def __str__(self):
        s = self.__class__.__name__
        if self._dp_name is not None:
            s += "[" + self._dp_name + "]"
        else:
            s += "[" + str(self.id) + "]"
        return s

    ### Various attribute setters:
    def set_name(self, name):
        self._dp_name = name
