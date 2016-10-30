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
import pickle
import logging
import functools
import threading
import traceback
import collections
import multiprocessing

from . import common, pattern, endpoint
from .common import builtin, ProcessId, get_runtime_option
from .endpoint import ChannelCaps

logger = logging.getLogger(__name__)

class Command(enum.Enum):
    """An enum of process commands.

    """
    Start     = 1
    Setup     = 2
    Config    = 3
    End       = 5
    New       = 6
    StartAck  = 11
    SetupAck  = 12
    ConfigAck = 13
    NewAck    = 16
    Message   = 20
    RPC       = 30
    Sentinel  = 40

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

    DistAlgo processes can spawn more processes by calling `new`. Any
    DistProcess can send messages to any other DistProcess, given that it knows
    the unique id of the target process. However, the terminal is shared between
    all processes spawned from that terminal. This includes the stdout, stdin,
    and stderr streams. In addition, each DistProcess also maintains a TCP
    connection to the master control node (the first node started in a
    distributed system) where DistAlgo commands are passed (see
    `distalgo.runtime.proto`).

    Concrete subclasses of `DistProcess` must define the methods:

    - `setup`: A function that initializes the process-local variables.

    - `run`: The entry point of the process. This function defines the
      activities of the process.

    Users should not instantiate this class directly, process instances should
    be created by calling `new`.

    """
    def __init__(self, procimpl, props):
        self.__procimpl = procimpl
        self._id = procimpl.dapid
        self._log = logging.LoggerAdapter(
            logging.getLogger(self.__class__.__module__)
            .getChild(self.__class__.__qualname__), {'daPid' : self._id})
        self.__newcmd_seqno = procimpl.seqno
        self.__router = procimpl.router
        self.__messageq = self.__router.get_queue_for_process(self._id)
        self.__properties = props
        self.__jobq = collections.deque()
        self.__lock = threading.Lock()
        self.__local = threading.local()
        self.__local.timer = None
        self.__local.timer_expired = False
        self.__setup_called = False
        self.__running = False
        self.__parent = procimpl.daparent
        self.__init_dispatch_table()
        self.__init_config()

        self._state = common.Namespace()
        self._events = []

    def setup(self):
        """Initialization routine for the DistAlgo process.

        Should be overridden by child classes to initialize process states.

        """
        pass

    def run(self):
        """Entry point for the DistAlgo process.

        This is the starting point of execution for user code.

        """
        pass

    def _delayed_start(self):
        assert self.__messageq is not None
        self._log.debug("Delayed start.")
        if self.__newcmd_seqno is not None:
            self.__send(msgtype=Command.NewAck, message=self.__newcmd_seqno,
                        to=self.__parent, flags=ChannelCaps.RELIABLEFIFO)
        self.__wait_for(lambda: self.__running)
        return self.run()

    def __init_config(self):
        if self.get_config('handling', default='one').casefold() == 'all':
            self.__do_label = self.__label_all
        else:
            self.__do_label = self.__label_one
        self.__default_flags = self.__get_channel_flags(
            self.get_config("channel", default=[]))
        if self.get_config('clock', default='').casefold() == 'lamport':
            self._logical_clock = 0
        else:
            self._logical_clock = None

    def __init_dispatch_table(self):
        self.__command_dispatch_table = [None] * Command.Sentinel.value
        self.__async_events = [None] * Command.Sentinel.value

        self._cmd_NewAck = functools.partial(self.__cmd_handle_Ack,
                                             cmdtype=Command.NewAck.value)
        self._cmd_StartAck = functools.partial(self.__cmd_handle_Ack,
                                               cmdtype=Command.StartAck.value)
        self._cmd_SetupAck = functools.partial(self.__cmd_handle_Ack,
                                               cmdtype=Command.SetupAck.value)

        for cmdname in Command.__members__:
            handlername = '_cmd_' + cmdname
            cmd = Command.__members__[cmdname]
            if hasattr(self, handlername):
                self.__async_events[cmd.value] = dict()
                self.__command_dispatch_table[cmd.value] = \
                        getattr(self, handlername)

    def __get_channel_flags(self, props):
        flags = 0
        if isinstance(props, str):
            props = [props]
        for prop in props:
            pflag = getattr(ChannelCaps, prop.upper(), None)
            if pflag is not None:
                flags |= pflag
            else:
                logger.error("Unknown channel property %r", prop)
        return flags

    _config_object = dict()
    @classmethod
    def get_config(cls, key, default=None):
        """Returns the configuration value for specified 'key'.

        """
        cfgobj = get_runtime_option('config')
        if key in cfgobj:
            return cfgobj[key]
        elif key in common.global_config():
            return common.global_config()[key]
        elif key in cls._config_object:
            return cls._config_object[key]
        elif key in sys.modules[cls.__module__]._config_object:
            return sys.modules[cls.__module__]._config_object[key]
        else:
            return default

    @builtin
    def new(self, pcls, args=None, num=None, **props):
        if not issubclass(pcls, DistProcess):
            self._log.error("Can not create non-DistProcess classes.")
            return set()

        iterator = []
        if num is None:
            iterator = range(1)
        elif isinstance(num, int):
            iterator = range(num)
        elif isinstance(num, collections.abc.Iterable):
            iterator = num
        else:
            self._log.error("Invalid value for `num`: %r", num)
            return set()

        self._log.debug("Creating instances of %s..", pcls.__name__)
        seqno = self.__create_cmd_seqno()
        self.__register_async_event(Command.NewAck, seqno)
        children = self.__procimpl.spawn(pcls, iterator, self._id, props, seqno)
        self.__sync_async_event(Command.NewAck, seqno, children)
        self._log.debug("%d instances of %s created.", len(children), pcls)

        if args is not None:
            tmp = []
            for cid in children:
                if self._setup(cid, args, seqno=seqno):
                    tmp.append(cid)
                else:
                    self._log.debug("`setup` failed for %r, terminating.", cid)
                    self.end(cid)
            children = tmp
        if num is None:
            return children[0] if len(children) > 0 else None
        else:
            return set(children)

    def _setup(self, procs, args, seqno=None):
        res = True
        if seqno is None:
            seqno = self.__create_cmd_seqno()
        self.__register_async_event(msgtype=Command.SetupAck, seqno=seqno)
        if self.__send(msgtype=Command.Setup,
                       message=(seqno, args),
                       to=procs,
                       flags=ChannelCaps.RELIABLEFIFO):
            self.__sync_async_event(msgtype=Command.SetupAck,
                                    seqno=seqno,
                                    srcs=procs)
        else:
            res = False
            self.__deregister_async_event(msgtype=Command.SetupAck,
                                          seqno=seqno)
        return res

    def _start(self, procs, args=None):
        res = True
        seqno = self.__create_cmd_seqno()
        if args is not None:
            if not self._setup(procs, args, seqno=seqno):
                return False
        self.__register_async_event(msgtype=Command.StartAck, seqno=seqno)
        if self.__send(msgtype=Command.Start, message=seqno, to=procs,
                       flags=ChannelCaps.RELIABLEFIFO):
            self.__sync_async_event(msgtype=Command.StartAck,
                                    seqno=seqno,
                                    srcs=procs)
        else:
            res = False
            self.__deregister_async_event(msgtype=Command.StartAck,
                                          seqno=seqno)
        return res

    @builtin
    def _config(self, **props):
        """Set global configuration overrides.

        Configurations set by this function have higher priority than those
        declared at module and process level, but lower priority than '--config'
        command line option. This function is only callable from the `main`
        method of a node process and affects all processes created on that node.

        """
        common.set_global_config(props)
        # XXX: Hack: we have to update our configurations to reflect the new
        # settings here, but certain configuration items (such as 'clock') need
        # to take affect from beginning of process execution (in order to affect
        # all incoming and outgoing messages), so this might not always work as
        # intended:
        self.__init_config()

    @builtin
    def parent(self):
        """Returns the id of our parent process.

        The parent process is the one that called `new` to create this process.

        """
        return self.__parent

    @builtin
    def node(self):
        """Returns the id of our node process."""
        return self.__procimpl._nodeid

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
        if level > self._log.getEffectiveLevel():
            msg = sep.join([str(v) for v in value])
            self._log.log(level, msg)

    @builtin
    def debug(self, *value, sep=' '):
        """Prints debugging output to the process log."""
        self.output(*value, sep, level=logging.DEBUG+1)

    @builtin
    def error(self, *value, sep=' '):
        """Prints error message to the process log."""
        self.output(*value, sep, level=logging.INFO+2)

    @builtin
    def work(self):
        """Waste some random amount of time."""
        time.sleep(random.randint(0, 200) / 100)
        pass

    @builtin
    def end(self, target, exit_code=1):
        """Terminate child processes."""
        if isinstance(target, ProcessId):
            self.__send(Command.End, exit_code, to=target,
                        flags=ChannelCaps.RELIABLEFIFO)
        else:
            for t in target:
                self.__send(Command.End, exit_code, to=t,
                            flags=ChannelCaps.RELIABLEFIFO)

    @builtin
    def logical_clock(self):
        """Returns the current value of Lamport clock."""
        return self._logical_clock

    @builtin
    def incr_logical_clock(self):
        """Increment Lamport clock by 1."""
        if isinstance(self._logical_clock, int):
            self._logical_clock += 1

    @builtin
    def send(self, message, to, channel=None):
        """Send DistAlgo message.

        """
        self.incr_logical_clock()
        if (self.__fails('send')):
            self._log.info("Dropped outgoing message due to lottery: %r", message)
            return False

        flags = None
        if channel is not None:
            flags = self.__get_channel_flags(channel)
        res = self.__send(msgtype=Command.Message,
                          message=(self._logical_clock, message),
                          to=to,
                          flags=flags)
        self.__trigger_event(pattern.SentEvent(
            (self._logical_clock, to, self._id), message))
        return res

    def __send(self, msgtype, message, to, flags=None):
        """Internal send.

        Forwards message to router.

        """
        if flags is None:
            flags = self.__default_flags
        protocol_message = (msgtype, message)
        res = True
        if type(to) is ProcessId:
            res = self.__router.send(self._id, to, protocol_message, flags)
        else:
            # to must a collection of `ProcessId`s:
            for t in to:
                if not self.__router.send(self._id, t, protocol_message, flags):
                    res = False
        return res

    def _timer_start(self):
        self.__local.timer = time.time()
        self.__local.timer_expired = False

    def _timer_end(self):
        self.__local.timer = None

    @property
    def _timer_expired(self):
        return self.__local.timer_expired

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
            if self.__local.timer is None:
                self._timer_start()
            timeleft = timeout - (time.time() - self.__local.timer)
            if timeleft <= 0:
                self._timer_end()
                self.__local.timer_expired = True
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
                if self.__local.timer is None:
                    self._timer_start()
                timeleft = timeout - (time.time() - self.__local.timer)
                if timeleft <= 0:
                    self._timer_end()
                    self.__local.timer_expired = True
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

    def __create_cmd_seqno(self):
        return time.time()

    def __register_async_event(self, msgtype, seqno):
        self.__async_events[msgtype.value][seqno] = list()

    def __deregister_async_event(self, msgtype, seqno):
        with self.__lock:
            del self.__async_events[msgtype.value][seqno]

    def __sync_async_event(self, msgtype, seqno, srcs):
        if isinstance(srcs, ProcessId):
            remaining = {srcs}
        else:
            remaining = set(srcs)
        container = self.__async_events[msgtype.value][seqno]
        with self.__lock:
            remaining.difference_update(container)
            self.__async_events[msgtype.value][seqno] = remaining
        self.__wait_for(lambda: remaining)
        self.__deregister_async_event(msgtype, seqno)

    def __wait_for(self, predicate, timeout=None):
        while not predicate():
            self.__process_event(block=True, timeout=timeout)

    def __cmd_handle_Ack(self, src, seqno, cmdtype):
        registered_evts = self.__async_events[cmdtype]
        with self.__lock:
            if seqno in registered_evts:
                # XXX: we abuse type(container) to indicate whether we need to
                # aggregate or discard:
                container = registered_evts[seqno]
                if type(container) is list:
                    # `__sync_event` hasn't been called -- we don't yet know
                    # the exact set of peers to wait for, so we just aggregate
                    # all the acks:
                    container.append(src)
                else:
                    # Otherwise, we can just mark the peer off the list:
                    container.discard(src)

    def __process_event(self, block, timeout=None):
        """Retrieves and processes one pending event.

        Parameter 'block' indicates whether to block waiting for next message
        to come in if the queue is currently empty. 'timeout' is the maximum
        time to wait for an event. Returns True if an event was successfully
        processed, False otherwise.

        """
        event = None
        if timeout is not None and timeout < 0:
            timeout = 0

        try:
            message = self.__messageq.pop(block, timeout)
        except common.QueueEmpty:
            message = None
        except Exception as e:
            self._log.error("Caught exception while waiting for events: %r", e)
            return False

        if message is None:
            if block:
                self._log.debug(
                    "__process_event: message was stolen by another thread.")
            return False

        try:
            src, (cmd, args) = message
            self.__command_dispatch_table[cmd.value](src, args)
            return True
        except Exception as e:
            self._log.error(
                "Exception while processing message %r: %r", message, e)
        return False

    def _cmd_Start(self, src, seqno):
        if self.__running:
            self._log.warning("Process already started but got `start` again.")
        else:
            if not self.__setup_called:
                self._log.error("`start` received before `setup`!")
            else:
                self._log.debug("`start` command received, commencing...")
                self.__running = True
        self.__send(msgtype=Command.StartAck,
                    message=seqno,
                    to=src,
                    flags=ChannelCaps.RELIABLEFIFO)

    def _cmd_End(self, src, args):
        if src == self.__parent:
            self._log.debug("`End(%r)` command received, terminating..", args)
            self.exit(args)
        else:
            self._log.warning("Ignoring `End(%r)` command from non-parent(%r)!",
                              args, src)

    def _cmd_Setup(self, src, args):
        seqno, realargs = args
        if self.__setup_called:
            self._log.warning("`setup` already called for this process!")
        else:
            self._log.debug("Running `setup` with args %r.", args)
            try:
                self.setup(*realargs)
                self.__setup_called = True
                self._log.debug("`setup` complete.")
            except Exception as e:
                self._log.error("Exception during setup(%r): %r", args, e)
        self.__send(msgtype=Command.SetupAck,
                    message=seqno,
                    to=src,
                    flags=ChannelCaps.RELIABLEFIFO)

    def _cmd_Config(self, src, args):
        try:
            key, val = args
            m = getattr(self, "set_" + key, default=None)
            if callable(m):
                m(*args)
            else:
                self._log.warning("Missing setter: %s", key)
        except ValueError:
            self._log.warning("Corrupted 'Config' command: %r", args)

    def _cmd_Message(self, peer_id, message):
        if self.__fails('receive'):
            self._log.warning(
                "Dropped incoming message due to lottery: %s", message)
            return False

        try:
            peer_clk, payload = message
        except ValueError as e:
            self._log.error("Corrupted message: %r", message)
            return False

        if isinstance(self._logical_clock, int):
            if not isinstance(peer_clk, int):
                # Most likely some peer did not turn on lamport clock, issue
                # a warning and skip this message:
                self._log.warning(
                    "Invalid logical clock value: %r; message dropped. ",
                    peer_clk)
                return False
            self._logical_clock = max(self._logical_clock, peer_clk) + 1

        self.__trigger_event(
            pattern.ReceivedEvent(envelope=(peer_clk, None, peer_id),
                                  message=payload))
        return True

    def __trigger_event(self, event):
        """Immediately triggers 'event', skipping the event queue.

        """
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
        res = "<process {0._id}#{0.__procimpl}>"
        return res.format(self)

    __str__ = __repr__

class Router(threading.Thread):
    """The router thread.

    Creates an event object for each incoming message, and appends the event
    object to the target process' event queue.

    """
    def __init__(self, endpoint):
        threading.Thread.__init__(self)
        self.log = logging.getLogger(__name__) \
                          .getChild(self.__class__.__qualname__)
        self.daemon = True
        self.endpoint = endpoint
        self.incomingq = endpoint.queue
        self.hostname = get_runtime_option('hostname')
        self.num_waiting = 0
        self.local_procs = dict()
        self.local = threading.local()
        self.local.buf = None

    def register_local_process(self, pid):
        if pid in self.local_procs:
            self.log.warning("Registering duplicate process: %s.", pid)
        self.local_procs[pid] = common.WaitableQueue()
        self.log.debug("Process %s registered.", pid)

    def get_queue_for_process(self, pid):
        return self.local_procs.get(pid, None)

    def run(self):
        try:
            self.mesgloop()
        except KeyboardInterrupt:
            self.log.debug("Received KeyboardInterrupt, stopping.")

    def send(self, src, dest, mesg, flags=0):
        return self._dispatch(src, dest, mesg, flags)

    def _send_remote(self, src, dest, mesg, flags):
        """Forward `mesg` to remote process `dest`.

        """
        self.log.debug("* Received forwarding request: %r to %s with flags=%d",
                       mesg, dest, flags)
        if dest.hostname != self.hostname:
            flags |= ChannelCaps.INTERHOST
        transport = self.endpoint.get_transport(flags)
        if transport is None:
            self.log.error("** No suitable transport for sending to %s!", dest)
            return False
        if not hasattr(self.local, 'buf') or self.local.buf is None:
            self.local.buf = bytearray(endpoint.MAX_PAYLOAD_SIZE)

        wrapper = common.BufferIOWrapper(self.local.buf)
        try:
            pickle.dump((src, dest, mesg), wrapper)
        except OSError as e:
            self.log.error(
                "** Outgoing message object too big to fit in buffer, dropped.")
            return False
        self.log.debug("** Forwarding %r(%d bytes) to %s with flags=%d using %s.",
                       mesg, wrapper.fptr, dest, flags, transport)
        with memoryview(self.local.buf)[0:wrapper.fptr] as chunk:
            return transport.send(chunk, dest)

    def _dispatch(self, src, dest, payload, flags=0):
        if dest in self.local_procs:
            self.log.debug("Local forward from %s to %s: %r", src, dest, payload)
            try:
                # Only needs to copy if message is from local to local:
                if src in self.local_procs:
                    payload = copy.deepcopy(payload)
                self.local_procs[dest].append((src, payload))
                return True
            except Exception as e:
                self.log.warning("Failed to deliver to local process %s.", dest)
                return False
        else:
            return self._send_remote(src, dest, payload, flags)

    def mesgloop(self):
        while True:
            try:
                transport, chunk, remote = self.incomingq.pop(block=True)
                src, dest, mesg = pickle.loads(chunk)
                chunk = None
                self._dispatch(src, dest, mesg)
            except ValueError as e:
                self.log.warning("Dropped invalid message: %r", chunk)
                continue


class OSProcessImpl(multiprocessing.Process):
    """An implementation of processes using OS process.

    """
    def __init__(self, process_class, transport_manager, process_id, parent_id,
                 process_name="", cmd_seqno=None, props=None):
        assert issubclass(process_class, DistProcess)

        super().__init__()
        # Logger can not be serialized so it has to be instantiated in the child
        # proc's address space:
        self._log = None
        self._dacls = process_class
        self._daobj = None
        self._nodeid = common.pid_of_node()
        self._properties = props if props is not None else dict()
        self._lock = threading.Lock()
        self.daemon = props['daemon'] if 'daemon' in self._properties else False
        self.dapid = process_id
        self.daparent = parent_id
        self.router = None
        self.seqno = cmd_seqno
        if len(process_name) > 0:
            self.name = process_name
        self.endpoint = transport_manager
        if multiprocessing.get_start_method() == 'spawn':
            self._rtopts = (common.GlobalOptions, common.GlobalConfig)

    def start_router(self):
        self.endpoint.start()
        self.router = Router(self.endpoint)
        self.router.start()

    def _sighandler(self, signum, frame):
        for child in multiprocessing.active_children():
            child.terminate()
        sys.exit(0)

    def _debug_handler(self, sig, frame):
        self._debugger.set_trace(frame)

    def is_node(self):
        return self.dapid == common.pid_of_node()

    def _spawn_1(self, pcls, name, parent, props, seqno=None):
        with self._lock:
            trman = None
            p = None
            cid = None
            try:
                trman = endpoint.TransportManager()
                trman.initialize()
                cid = ProcessId._create(pcls, trman.transport_addresses, name)
                p = OSProcessImpl(process_class=pcls,
                                  transport_manager=trman,
                                  process_id=cid,
                                  parent_id=parent,
                                  process_name=name,
                                  cmd_seqno=seqno,
                                  props=props)
                p.start()
                p.join(timeout=0.01)
                if not p.is_alive():
                    self._log.error("%r terminated prematurely.", cid)
                    cid = None
            except Exception as e:
                cid = None
                self._log.error("Failed to create instance (%s) of %s: %r",
                                name, pcls, e)
                if p is not None:
                    p.terminate()
            finally:
                if trman is not None:
                    trman.close()
            return cid

    def spawn(self, pcls, names, parent, props, seqno=None):
        children = []
        for name in names:
            if not isinstance(name, str):
                name = ""
            cid = self._spawn_1(pcls, name, parent, props, seqno)
            if cid is not None:
                children.append(cid)
        self._log.debug("%d instances of %s created.",
                        len(children), pcls.__name__)
        return children

    def run(self):
        self._log = logger.getChild(self.__class__.__qualname__)
        if len(self.name) == 0:
            self.name = str(self.pid)
        try:
            if multiprocessing.get_start_method() == 'spawn':
                common._restore_runtime_options(self._rtopts)
                common._set_node(self._nodeid)
            common.set_runtime_option('this_module_name',
                                      self.__class__.__module__)
            if multiprocessing.get_start_method() == 'spawn':
                common.sysinit()
            pattern.initialize(self.dapid)            # XXX:: FIXME!
            signal.signal(signal.SIGTERM, self._sighandler)
            signal.signal(signal.SIGUSR1, self._debug_handler)

            self.start_router()
            self.router.register_local_process(self.dapid)
            self._daobj = self._dacls(self, self._properties)
            self._log.debug("Process object initialized.")

            if self.is_node():
                return self._daobj.run()
            else:
                return self._daobj._delayed_start()

        except Exception as e:
            sys.stderr.write("Unexpected error at process %s:%r"% (str(self), e))
            traceback.print_tb(e.__traceback__)

        except KeyboardInterrupt as e:
            self._log.debug("Received KeyboardInterrupt, exiting")
            pass

    def __str__(self):
        return "{0}<{1}>".format(self.__class__.__qualname__, str(self.pid))
