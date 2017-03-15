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

import sys
import copy
import enum
import time
import random
import pickle
import logging
import functools
import threading
import collections
import multiprocessing

from . import common, pattern, endpoint
from .common import (builtin, internal, name_split_host, name_split_node,
                     ProcessId, get_runtime_option)
from .endpoint import ChannelCaps

logger = logging.getLogger(__name__)

class DistProcessExit(BaseException):
    def __init__(self, code=0):
        super().__init__()
        self.exit_code = code

class Command(enum.Enum):
    """An enum of process commands.

    """
    Start      = 1
    Setup      = 2
    Config     = 3
    End        = 5
    New        = 6
    Resolve    = 7
    NodeJoin   = 8
    NodeLeave  = 9
    StartAck   = 11
    SetupAck   = 12
    ConfigAck  = 13
    EndAck     = 15
    NewAck     = 16
    ResolveAck = 17
    NodeAck    = 18
    NodePing   = 19
    Message    = 20
    RPC        = 30
    RPCReply   = 31
    Sentinel   = 40

_config_object = dict()

class DistProcess():
    """Abstract base class for DistAlgo processes.

    Each instance of this class enbodies the runtime state and activities of a
    DistAlgo process in a distributed system. Each process is uniquely
    identified by a `ProcessId` object. Messages exchanged between DistAlgo
    processes can be any picklable Python object.

    DistAlgo processes can spawn more processes by calling `new`. The process
    that called `new` is known as the parent process of the newly spawned
    processes. Any DistProcess can send messages to any other DistProcess, given
    that it knows the `ProcessId` of the target process. However, only the
    parent process can `end` a child process. The terminal is shared between all
    processes spawned from that terminal, which includes the stdout, stdin, and
    stderr streams.

    Concrete subclasses of `DistProcess` must define the methods:

    - `setup`: A function that initializes the process-local variables.

    - `run`: The entry point of the process. This function defines the
      activities of the process.

    Users should not instantiate this class directly, process instances should
    be created by calling `new`.

    """
    def __init__(self, procimpl, props):
        self.__procimpl = procimpl
        self.__id = procimpl.dapid
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
        self.__local.seqcnt = 0
        self.__setup_called = False
        self.__running = False
        self.__parent = procimpl.daparent
        self._init_dispatch_table()
        self._init_config()

        self._state = common.Namespace()
        self._events = []

    def setup(self, **rest):
        """Initialization routine for the DistAlgo process.

        Should be overridden by child classes to initialize process states.

        """
        pass

    def run(self):
        """Entry point for the DistAlgo process.

        This is the starting point of execution for user code.

        """
        pass

    @property
    @internal
    def _id(self):
        return self.__id

    @internal
    def _delayed_start(self):
        assert self.__messageq is not None
        self._log.debug("Delayed start.")
        if self.__newcmd_seqno is not None:
            self._send1(msgtype=Command.NewAck,
                        message=(self.__newcmd_seqno, None),
                        to=self.__parent, flags=ChannelCaps.RELIABLEFIFO)
        self._wait_for(lambda: self.__running)
        try:
            return self.run()
        except Exception as e:
            self._log.error("Unrecoverable error in DistAlgo process: %r",
                            e, exc_info=1)
            return -1

    @internal
    def _init_config(self):
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

    AckCommands = [Command.NewAck, Command.EndAck, Command.StartAck,
                   Command.SetupAck, Command.ResolveAck, Command.RPCReply]
    @internal
    def _init_dispatch_table(self):
        self.__command_dispatch_table = [None] * Command.Sentinel.value
        self.__async_events = [None] * Command.Sentinel.value

        for ack in self.__class__.AckCommands:
            handlername = '_cmd_' + ack.name
            setattr(self, handlername,
                    functools.partial(self.__cmd_handle_Ack,
                                      cmdtype=ack.value))

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
    def new(self, pcls, args=None, num=None, at=None, method=None, **props):
        """Creates new DistAlgo processes.

        `pcls` specifies the DistAlgo process class. Optional argument `args` is
        a list of arguments that is used to call the `setup` method of the child
        processes. Optional argument `num` specifies the number of processes to
        create on each node. Optional argument `at` specifies the node or nodes
        on which the new processes are to be created. If `num` is not specified
        then it defaults to one process. If `at` is not specified then it
        defaults to the same node as the current process. Optional argument
        `method` specifies the type of implementation used to run the new
        process(es), and can be one of 'process', in which case the new
        processes will be run inside operating system processes, or 'thread' in
        which case the processes will be run inside operating system threads. If
        method is not specified then its default value is taken from the
        '--default_proc_impl' command line option.

        If neither `num` nor `at` is specified, then `new` will return the
        process id of child process if successful, or None otherwise. If either
        `num` or `at` is specified, then `new` will return a set containing the
        process ids of the processes that was successfully created.

        """
        if not issubclass(pcls, DistProcess):
            raise TypeError("new: can not create DistAlgo process using "
                            "non-DistProcess class: {}.".format(pcls))
        if args is not None and not isinstance(args, collections.abc.Sequence):
            raise TypeError("new: 'args' must be a sequence but is {} "
                            "instead.".format(args))

        iterator = []
        if num is None:
            iterator = range(1)
        elif isinstance(num, int):
            iterator = range(num)
        elif isinstance(num, collections.abc.Iterable):
            iterator = num
        else:
            raise TypeError("new: invalid value for 'num': {}".format(num))

        if isinstance(at, collections.abc.Set):
            at = {self.resolve(nameorid) for nameorid in at}
        else:
            at = self.resolve(at)
        if method is None:
            method = get_runtime_option('default_proc_impl')

        self._log.debug("Creating instances of %s using '%s'", pcls, method)
        seqno = self._create_cmd_seqno()
        self._register_async_event(Command.NewAck, seqno)
        if at is not None and at != self._id:
            self._register_async_event(Command.RPCReply, seqno)
            if self._send1(Command.New,
                           message=(pcls, iterator, method, seqno, props),
                           to=at,
                           flags=ChannelCaps.RELIABLEFIFO):
                res = self._sync_async_event(Command.RPCReply, seqno, at)
                if isinstance(at, set):
                    children = [pid for target in at for pid in res[target]]
                else:
                    children = res[at]
            else:
                self._deregister_async_event(Command.RPCReply, seqno)
                children = []
        else:
            children = self.__procimpl.spawn(pcls, iterator, self._id, props,
                                             seqno, container=method)
        self._sync_async_event(Command.NewAck, seqno, children)
        self._log.debug("%d instances of %s created.", len(children), pcls)

        if args is not None:
            tmp = []
            for cid in children:
                if self._setup(cid, args, seqno=seqno):
                    tmp.append(cid)
                else:
                    self._log.warning(
                        "`setup` failed for %r, terminating child.", cid)
                    self.end(cid)
            children = tmp
        if num is None and at is None:
            return children[0] if len(children) > 0 else None
        else:
            return set(children)

    @internal
    def _setup(self, procs, args, seqno=None):
        if not isinstance(args, collections.abc.Sequence):
            raise TypeError("setup: 'args' must be a sequence but is {} "
                            "instead.".format(args))
        res = True
        if seqno is None:
            seqno = self._create_cmd_seqno()
        self._register_async_event(msgtype=Command.SetupAck, seqno=seqno)
        if self._send1(msgtype=Command.Setup,
                       message=(seqno, args),
                       to=procs,
                       flags=ChannelCaps.RELIABLEFIFO,
                       retry_refused_connections=True):
            self._sync_async_event(msgtype=Command.SetupAck,
                                    seqno=seqno,
                                    srcs=procs)
        else:
            res = False
            self._deregister_async_event(msgtype=Command.SetupAck,
                                          seqno=seqno)
        return res

    @internal
    def _start(self, procs, args=None):
        res = True
        seqno = self._create_cmd_seqno()
        if args is not None:
            if not self._setup(procs, args, seqno=seqno):
                return False
        self._register_async_event(msgtype=Command.StartAck, seqno=seqno)
        if self._send1(msgtype=Command.Start, message=seqno, to=procs,
                       flags=ChannelCaps.RELIABLEFIFO):
            self._sync_async_event(msgtype=Command.StartAck,
                                    seqno=seqno,
                                    srcs=procs)
        else:
            res = False
            self._deregister_async_event(msgtype=Command.StartAck,
                                          seqno=seqno)
        return res

    @builtin
    def nameof(self, pid):
        """Returns the process name of `pid`, if any.

        """
        assert isinstance(pid, ProcessId)
        return pid.name

    @builtin
    def parent(self):
        """Returns the parent process id.

        The parent process is the process that called `new` to create this
        process.

        """
        return self.__parent

    @builtin
    def nodeof(self, pid):
        """Returns the id of the node process that's running `pid`.

        """
        assert isinstance(pid, ProcessId)
        if self._id == pid or len(pid.nodename) == 0:
            return self.__procimpl._nodeid
        else:
            return self.resolve(pid.nodename)

    @builtin
    def exit(self, code=0):
        """Terminates the current process.

        `code` specifies the exit code.

        """
        raise DistProcessExit(code)

    @builtin
    def output(self, *message, sep=' ', level=logging.INFO+1):
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
            msg = sep.join([str(v) for v in message])
            self._log.log(level, msg)

    @builtin
    def debug(self, *message, sep=' '):
        """Prints debugging output to the process log.

        This is the same as `output` except the message is logged at the
        'USRDBG' level.

        """
        self.output(*message, sep=sep, level=logging.DEBUG+1)

    @builtin
    def error(self, *message, sep=' '):
        """Prints error message to the process log.

        This is the same as `output` except the message is logged at the
        'USRERR' level.

        """
        self.output(*message, sep=sep, level=logging.INFO+2)

    @builtin
    def work(self):
        """Waste some random amount of time.

        This suspends execution of the process for a period of 0-2 seconds.

        """
        time.sleep(random.randint(0, 200) / 100)
        pass

    @builtin
    def end(self, target, exit_code=1):
        """Terminate the child processes specified by `target`.

        `target` can be a process id or a set of process ids, all of which must
        be a child process of this process.

        """
        self._send1(Command.End, exit_code, to=target,
                    flags=ChannelCaps.RELIABLEFIFO)

    @builtin
    def logical_clock(self):
        """Returns the current value of the logical clock.

        """
        return self._logical_clock

    @builtin
    def incr_logical_clock(self):
        """Increments the logical clock.

        For Lamport's clock, this increases the clock value by 1.

        """
        if isinstance(self._logical_clock, int):
            self._logical_clock += 1

    @builtin
    def send(self, message, to, channel=None, **rest):
        """Send a DistAlgo message.

        `message` can be any pickle-able Python object. `to` can be a process id
        or a set of process ids.

        """
        self.incr_logical_clock()
        if (self.__fails('send')):
            self._log.info("Dropped outgoing message due to lottery: %r", message)
            return False

        flags = None
        if channel is not None:
            flags = self.__get_channel_flags(channel)
        impersonate = rest.get('impersonate', None)
        res = self._send1(msgtype=Command.Message,
                          message=(self._logical_clock, message),
                          to=to,
                          flags=flags,
                          impersonate=impersonate)
        self.__trigger_event(pattern.SentEvent(
            (self._logical_clock, to, self._id), message))
        return res

    @builtin
    def hanged(self):
        """Hangs the current process.

        When a process enters the 'hanged' state, its main logic and all message
        handlers will no longer run.

        """
        self._register_async_event(Command.EndAck, seqno=0)
        self._sync_async_event(Command.EndAck, seqno=0, srcs=self._id)

    @builtin
    def resolve(self, name):
        """Returns the process id associated with `name`.

        """
        if name is None:
            return None
        elif isinstance(name, ProcessId):
            return name
        elif not isinstance(name, str):
            self._log.error("resolve: unsupported type %r", name)
            return None
        fullname, host, port = name_split_host(name)
        if fullname is None:
            self._log.error("Malformed name: %s", name)
            return None
        procname, nodename = name_split_node(fullname)
        if procname is None:
            self._log.error("Malformed name: %s", name)
            return None
        dest = ProcessId.lookup((procname, nodename))
        if dest is None:
            self._log.info("Waiting to resolve name %r...", name)
            seqno = self._create_cmd_seqno()
            self._register_async_event(Command.ResolveAck, seqno)
            if self._send1(Command.Resolve,
                           message=((procname, nodename), host, port, seqno),
                           to=self.__procimpl._nodeid,
                           flags=ChannelCaps.RELIABLEFIFO):
                res = self._sync_async_event(Command.ResolveAck, seqno,
                                             self.__procimpl._nodeid)
                dest = res[self.__procimpl._nodeid]
                self._log.debug("%r successfully resolved to %r.", name, dest)
            else:
                self._deregister_async_event(Command.ResolveAck, seqno)
                self._log.error("Unable to resolve %r: failed to send "
                                "request to node!", name)
        return dest

    @internal
    def _resolve_callback(self, pid, src, seqno):
        self._send1(Command.ResolveAck, message=(seqno, pid), to=src)

    @internal
    def _send1(self, msgtype, message, to, flags=None, impersonate=None,
               **params):
        """Internal send.

        Pack the message and forward to router.

        """
        if to is None:
            self._log.warning("send: 'to' is None!")
            return False
        if flags is None:
            flags = self.__default_flags
        protocol_message = (msgtype, message)
        res = True
        if isinstance(to, ProcessId) or isinstance(to, str):
            target = [to]
        else:
            # 'to' must be an iterable of `ProcessId`s:
            target = to
        if impersonate is None:
            impersonate = self._id
        for dest in target:
            if isinstance(dest, str):
                # This is a process name, try to resolve to an id
                dest = self.resolve(dest)
            if not self.__router.send(impersonate, dest, protocol_message,
                                      params, flags):
                res = False
        return res

    @internal
    def _timer_start(self):
        self.__local.timer = time.time()
        self.__local.timer_expired = False

    @internal
    def _timer_end(self):
        self.__local.timer = None

    @property
    @internal
    def _timer_expired(self):
        return self.__local.timer_expired

    def __fails(self, failtype):
        if failtype not in self.__properties:
            return False
        if (random.random() < self.__properties[failtype]):
            return True
        return False

    @internal
    def _label(self, name, block=False, timeout=None):
        """This simulates the controlled "label" mechanism.

        The number of pending events handled at each label is controlled by the
        'handling' configuration key -- if 'handling' is 'one' then `__do_label`
        will be set to `__label_one`, otherwise `__do_label` will be set to
        `_label_all`(see `__init__`).

        """
        if self.__fails('hang'):
            self._log.warning("Hanged(@label %s)", name)
            self.hanged()
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

    @internal
    def _create_cmd_seqno(self):
        cnt = self.__local.seqcnt = (self.__local.seqcnt + 1) % 1024
        return (threading.current_thread().ident << 10) | cnt

    @internal
    def _register_async_event(self, msgtype, seqno):
        self.__async_events[msgtype.value][seqno] = list()

    @internal
    def _deregister_async_event(self, msgtype, seqno):
        with self.__lock:
            del self.__async_events[msgtype.value][seqno]

    @internal
    def _sync_async_event(self, msgtype, seqno, srcs):
        if isinstance(srcs, ProcessId):
            remaining = {srcs}
        else:
            remaining = set(srcs)
        container = self.__async_events[msgtype.value][seqno]
        with self.__lock:
            results = dict(container)
            remaining.difference_update(results)
            self.__async_events[msgtype.value][seqno] = (remaining, results)
        self._wait_for(lambda: not remaining)
        self._deregister_async_event(msgtype, seqno)
        return results

    @internal
    def _wait_for(self, predicate, timeout=None):
        while not predicate():
            self.__process_event(block=True, timeout=timeout)

    def __cmd_handle_Ack(self, src, args, cmdtype):
        seqno, res = args
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
                    container.append((src, res))
                else:
                    # Otherwise, we can just mark the peer off the list:
                    container[0].discard(src)
                    container[1][src] = res

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
            handler = self.__command_dispatch_table[cmd.value]
            if handler is None:
                self._log.warning("No handler for %r.", message)
                return False
            else:
                handler(src, args)
                return True
        except Exception as e:
            self._log.error(
                "Exception while processing message %r: %r", message, e)
        return False

    @internal
    def _cmd_New(self, src, args):
        pcls, num, method, seqno, props = args
        children = self.__procimpl.spawn(pcls, num,
                                         parent=src, props=props,
                                         seqno=seqno, container=method)
        self._send1(msgtype=Command.RPCReply,
                    message=(seqno, children),
                    to=src,
                    flags=ChannelCaps.RELIABLEFIFO)

    @internal
    def _cmd_Start(self, src, seqno):
        if self.__running:
            self._log.warning("Process already started but got `start` again.")
        else:
            if not self.__setup_called:
                self._log.error("`start` received before `setup`!")
            else:
                self._log.debug("`start` command received, commencing...")
                self.__running = True
        self._send1(msgtype=Command.StartAck,
                    message=(seqno, None),
                    to=src,
                    flags=ChannelCaps.RELIABLEFIFO)

    @internal
    def _cmd_End(self, src, args):
        if src == self.__parent or src == self.__procimpl._nodeid:
            self._log.debug("`End(%r)` command received, terminating..", args)
            self.exit(args)
        else:
            self._log.warning("Ignoring `End(%r)` command from non-parent(%r)!",
                              args, src)

    @internal
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
        self._send1(msgtype=Command.SetupAck,
                    message=(seqno, True),
                    to=src,
                    flags=ChannelCaps.RELIABLEFIFO)

    @internal
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

    @internal
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
            if (p.match(event, bindings=bindings, ignore_bound_vars=True,
                        SELF_ID=self._id, **self._state.__dict__)):
                if p.record_history is True:
                    getattr(self, p.name).append(event.to_tuple())
                elif p.record_history is not None:
                    # Call the update stub:
                    p.record_history(getattr(self, p.name), event.to_tuple())
                for h in p.handlers:
                    self.__jobq.append((h, copy.deepcopy(bindings)))

    def __repr__(self):
        res = "<process {}#{}>"
        return res.format(self._id, self.__procimpl)

    __str__ = __repr__

class NodeProcess(DistProcess):

    AckCommands = DistProcess.AckCommands + [Command.NodeAck]

    def __init__(self, procimpl, props):
        super().__init__(procimpl, props)
        self._router = procimpl.router
        self._nodes = set()

    def bootstrap(self):
        target = self._router.bootstrap_peer
        if target is None:
            return
        self._nodes.add(target)
        seqno = self._create_cmd_seqno()
        self._register_async_event(Command.NodeAck, seqno)
        if self._send1(Command.NodeJoin,
                       message=(ProcessId.all_named_ids(), seqno),
                       to=target,
                       flags=ChannelCaps.RELIABLEFIFO):
            res = self._sync_async_event(Command.NodeAck, seqno, target)
            newnodes, _ = res[target]
            self._nodes.update(newnodes)
            self._log.debug("Bootstrap success.")
        else:
            self._deregister_async_event(Command.NodeAck, seqno)
            self._log.error("Bootstrap failed! Unable to join existing network.")

    @internal
    def _cmd_Resolve(self, src, args):
        procname, hostname, port, seqno = args
        pid = ProcessId.lookup_or_register_callback(
            procname, functools.partial(self._resolve_callback,
                                        src=src, seqno=seqno))
        if pid is not None:
            self._send1(Command.ResolveAck, message=(seqno, pid), to=src)
        elif hostname is not None:
            if port is None:
                port = get_runtime_option('default_master_port')
            self._router.bootstrap_node(hostname, port, timeout=3)
            self.bootstrap()

    @internal
    def _resolve_callback(self, pid, src, seqno):
        super()._resolve_callback(pid, src, seqno)
        # propagate name:
        self._send1(Command.NodePing, message=(seqno, pid), to=self._nodes)

    @internal
    def _cmd_NodeJoin(self, src, args):
        _, seqno = args
        self._send1(Command.NodeAck,
                    message=(seqno, (self._nodes, ProcessId.all_named_ids())),
                    to=src,
                    flags=ChannelCaps.RELIABLEFIFO)
        self._nodes.add(src)

    @internal
    def _cmd_NodeLeave(self, src, args):
        self._log.debug("%s terminated.", src)
        self._nodes.discard(src)

    @internal
    def _cmd_NodePing(self, src, args):
        self._log.debug("%s is alive.", src)
        self._nodes.add(src)

    def _delayed_start(self):
        common.set_global_config(self._config_object)
        if len(self._nodes) > 0:
            self.bootstrap()
        try:
            if (not get_runtime_option('idle')) and hasattr(self, 'run'):
                return self.run()
            else:
                self.hanged()
        except Exception as e:
            self._log.error("Unrecoverable error in node process: %r",
                            e, exc_info=1)
            return -1
        finally:
            self._send1(Command.NodeLeave,
                        message=self._id, to=self._nodes)

class RoutingException(Exception): pass
class CircularRoutingException(RoutingException): pass
class BootstrapException(RoutingException): pass
class NoAvailableTransportException(RoutingException): pass
class MessageTooBigException(RoutingException): pass
class InvalidMessageException(RoutingException): pass
class InvalidRouterStateException(RoutingException): pass

class RouterCommands(enum.Enum):
    """Control messages for the router."""
    HELLO    = 1
    PING     = 2
    BYE      = 3
    ACK      = 4
    SENTINEL = 10

class Router(threading.Thread):
    """The router thread.

    Creates an event object for each incoming message, and appends the event
    object to the target process' event queue.

    """
    def __init__(self, transport_manager):
        threading.Thread.__init__(self)
        self.log = logging.getLogger(__name__) \
                          .getChild(self.__class__.__qualname__)
        self.daemon = True
        self.running = False
        self.prestart_mesg_sink = []
        self.bootstrap_peer = None
        self.endpoint = transport_manager
        self.hostname = get_runtime_option('hostname')
        self.payload_size = get_runtime_option('message_buffer_size') - \
                            endpoint.HEADER_SIZE
        self.local_procs = dict()
        self.local = threading.local()
        self.local.buf = None
        self.lock = threading.Lock()
        self._init_dispatch_table()

    def register_local_process(self, pid):
        with self.lock:
            if pid in self.local_procs:
                self.log.warning("Registering duplicate process: %s.", pid)
            self.local_procs[pid] = common.WaitableQueue()
        self.log.debug("Process %s registered.", pid)

    def deregister_local_process(self, pid):
        if self.running:
            with self.lock:
                if pid in self.local_procs:
                    del self.local_procs[pid]

    def terminate_local_processes(self):
        with self.lock:
            for mq in self.local_procs.values():
                mq.append((common.pid_of_node(), (Command.End, 1)))

    def get_queue_for_process(self, pid):
        return self.local_procs.get(pid, None)

    def bootstrap_node(self, hostname, port, timeout=None):
        """Bootstrap the node.

        This function implements bootstrapping at the router level. The
        responsibility of `bootstrap_node` is to obtain the process id of a
        single existing node process, which is stored into
        `self.bootstrap_peer`. The rest will then be handled at the node level.

        """
        self.log.debug("boostrap_node to %s:%d...", hostname, port)
        self.bootstrap_peer = None
        nid = common.pid_of_node()
        hellocmd = (RouterCommands.HELLO, ProcessId.all_named_ids())
        dummyid = ProcessId(uid=0, seqno=1, pcls=DistProcess, name='',
                            nodename='', hostname=hostname,
                            transports=\
                            tuple(port for _ in range(len(nid.transports))))
        self.log.debug("Dummy id: %r", dummyid)
        for transport in self.endpoint.transports:
            self.log.debug("Attempting bootstrap using %r...", transport)
            try:
                self._send_remote(src=nid,
                                  dest=dummyid,
                                  mesg=hellocmd,
                                  transport=transport,
                                  flags=ChannelCaps.BROADCAST)
                self.mesgloop(until=(lambda: self.bootstrap_peer),
                              timeout=timeout)
                if self.bootstrap_peer is not None and \
                   self.bootstrap_peer != common.pid_of_node():
                    self.log.info("Bootstrap succeeded using %s.", transport)
                    return
                else:
                    self.log.debug(
                        "Bootstrap attempt to %s:%d with %r timed out. ",
                        hostname, port, transport)
                    self.bootstrap_peer = None
            except endpoint.AuthenticationException as e:
                # Abort immediately:
                raise e
            except (CircularRoutingException, endpoint.TransportException) as e:
                self.log.debug("Bootstrap attempt to %s:%d with %r failed "
                               ": %r", hostname, port, transport, e)
        if self.bootstrap_peer is None:
            raise BootstrapException("Unable to contact a peer node.")

    def _init_dispatch_table(self):
        self._dispatch_table = [None] * RouterCommands.SENTINEL.value
        for cmdname, cmd in RouterCommands.__members__.items():
            handlername = '_cmd_' + cmdname.casefold()
            if hasattr(self, handlername):
                self._dispatch_table[cmd.value] = getattr(self, handlername)

    def _cmd_hello(self, src, args):
        self.log.debug("HELLO from %r", src)
        self._send_remote(src=None,
                          dest=src,
                          mesg=(RouterCommands.ACK,
                                (common.pid_of_node(),
                                 ProcessId.all_named_ids())),
                          flags=(ChannelCaps.BROADCAST |
                                 ChannelCaps.RELIABLEFIFO))

    def _cmd_ack(self, src, args):
        self.bootstrap_peer, _ = args

    def _cmd_ping(self, src, args):
        self.log.debug("Pinged from %r: %r", src, args)

    def _cmd_bye(self, src, args):
        self.log.debug("%r signed off.", src)
        ProcessId.drop_entry(src)

    def run(self):
        try:
            self.running = True
            for item in self.prestart_mesg_sink:
                self._dispatch(*item)
            self.prestart_mesg_sink = []
            self.mesgloop(until=(lambda: False))
        except Exception as e:
            self.log.debug("Unhandled exception: %r.", e)
        self.terminate_local_processes()

    def stop(self):
        self.running = False
        self.terminate_local_processes()

    def send(self, src, dest, mesg, params=dict(), flags=0):
        return self._dispatch(src, dest, mesg, params, flags)

    def _send_remote(self, src, dest, mesg, flags=0, transport=None, **params):
        """Forward `mesg` to remote process `dest`.

        """
        self.log.debug("* Received forwarding request: %r to %s with flags=%d",
                       mesg, dest, flags)
        if dest.hostname != self.hostname:
            flags |= ChannelCaps.INTERHOST
        elif dest.transports == self.endpoint.transport_addresses:
            # dest is not in our local_procs but has same hostname and transport
            # address, so most likely dest is a process that has already
            # terminated. Do not attempt forwarding or else will cause infinite
            # loop:
            raise CircularRoutingException('destination: {}'.format(dest))

        if transport is None:
            transport = self.endpoint.get_transport(flags)
        if transport is None:
            raise NoAvailableTransportException()
        if not hasattr(self.local, 'buf') or self.local.buf is None:
            self.local.buf = bytearray(self.payload_size)

        if flags & ChannelCaps.BROADCAST:
            payload = (src, None, mesg)
        else:
            payload = (src, dest, mesg)
        wrapper = common.BufferIOWrapper(self.local.buf)
        try:
            pickle.dump(payload, wrapper)
        except TypeError as e:
            raise InvalidMessageException("Error pickling {}.".format(payload)) \
                from e
        except OSError as e:
            raise MessageTooBigException(
                "** Outgoing message object too big to fit in buffer, dropped.")
        self.log.debug("** Forwarding %r(%d bytes) to %s with flags=%d using %s.",
                       mesg, wrapper.fptr, dest, flags, transport)
        with memoryview(self.local.buf)[0:wrapper.fptr] as chunk:
            transport.send(chunk, dest, **params)

    def _dispatch(self, src, dest, payload, params=dict(), flags=0):
        if dest in self.local_procs:
            if flags & ChannelCaps.BROADCAST:
                return True
            self.log.debug("Local forward from %s to %s: %r", src, dest, payload)
            try:
                # Only needs to copy if message is from local to local:
                if src in self.local_procs:
                    payload = copy.deepcopy(payload)
                queue = self.local_procs.get(dest, None)
                # This extra test is needed in case the destination process
                # terminated and de-registered itself:
                if queue is not None:
                    queue.append((src, payload))
                return True
            except Exception as e:
                self.log.warning("Failed to deliver to local process %s: %r",
                                 dest, e)
                return False
        elif dest is not None:
            if not self.running:
                # We are still in bootstrap mode, which means this may be a
                # message destined for a process that has yet to register, so
                # save it in a sink to be dispatched later in run():
                self.prestart_mesg_sink.append((src, dest, payload))
                return True
            try:
                self._send_remote(src, dest, payload, flags, **params)
                return True
            except CircularRoutingException as e:
                # This is most likely due to stale process ids, so don't log
                # error, just debug:
                self.log.debug("Caught %r.", e)
                return False
            except Exception as e:
                self.log.error("Could not send message due to: %r", e)
                self.log.debug("Send failed: ", exc_info=1)
                return False
        else:
            # This is a router message
            try:
                cmd, args = payload
                self._dispatch_table[cmd.value](src, args)
                return True
            except Exception as e:
                self.log.warning(
                    "Caught exception while processing router message from "
                    "%s(%r): %r", src, payload, e)
                self.log.debug("Router dispatch failed: ", exc_info=1)
                return False

    def mesgloop(self, until, timeout=None):
        incomingq = self.endpoint.queue
        if timeout is not None:
            start = time.time()
            timeleft = timeout
        else:
            timeleft = None
        while True:
            try:
                transport, chunk, remote = incomingq.pop(block=True,
                                                         timeout=timeleft)
                if transport.data_offset > 0:
                    chunk = memoryview(chunk)[transport.data_offset:]
                src, dest, mesg = pickle.loads(chunk)
                chunk = None
                self._dispatch(src, dest, mesg)
            except common.QueueEmpty:
                pass
            except (ValueError, pickle.UnpicklingError) as e:
                self.log.warning("Dropped invalid message: %r", chunk)
            if until():
                break
            if timeout is not None:
                timeleft = timeout - (time.time() - start)
                if timeleft <= 0:
                    break


class ProcessContainer:
    """An abstract base class for process implementations.

    """
    def __init__(self, process_class, transport_manager, process_id, parent_id,
                 process_name="", cmd_seqno=None, props=None, router=None):
        assert issubclass(process_class, DistProcess)
        super().__init__()
        # Logger can not be serialized so it has to be instantiated in the child
        # proc's address space:
        self._log = None
        self._dacls = process_class
        self._daobj = None
        self._nodeid = common.pid_of_node()
        self._properties = props if props is not None else dict()
        self.dapid = process_id
        self.daparent = parent_id
        self.router = router
        self.seqno = cmd_seqno
        if len(process_name) > 0:
            self.name = process_name
        self.endpoint = transport_manager
        if multiprocessing.get_start_method() == 'spawn':
            setattr(self, '_spawn_process', self._spawn_process_spawn)
        else:
            setattr(self, '_spawn_process', self._spawn_process_fork)

    def init_router(self):
        if self.router is None:
            self.endpoint.start()
            self.router = Router(self.endpoint)

    def start_router(self):
        if not self.router.running:
            self.router.start()

    def end(self):
        if self.router is not None:
            self.router.stop()

    def is_node(self):
        return self.dapid == common.pid_of_node()

    def _spawn_process_spawn(self, pcls, name, parent, props, seqno=None):
        trman = None
        p = None
        cid = None
        parent_pipe = child_pipe = None
        try:
            trman = endpoint.TransportManager(cookie=self.endpoint.authkey)
            trman.initialize()
            cid = ProcessId._create(pcls, trman.transport_addresses, name)
            parent_pipe, child_pipe = multiprocessing.Pipe()
            p = OSProcessContainer(process_class=pcls,
                                   transport_manager=trman,
                                   process_id=cid,
                                   parent_id=parent,
                                   process_name=name,
                                   cmd_seqno=seqno,
                                   pipe=child_pipe,
                                   **props)
            p.start()
            child_pipe.close()
            trman.serialize(parent_pipe, p.pid)
            assert parent_pipe.recv() == 'done'
            if not p.is_alive():
                self._log.error("%r terminated prematurely.", cid)
                cid = None
        except Exception as e:
            cid = None
            self._log.error("Failed to create instance (%s) of %s: %r",
                            name, pcls, e)
            if p is not None and p.is_alive():
                p.terminate()
        finally:
            if trman is not None:
                trman.close()
            if parent_pipe:
                parent_pipe.close()
        return cid

    def _spawn_process_fork(self, pcls, name, parent, props, seqno=None):
        trman = None
        p = None
        cid = None
        try:
            trman = endpoint.TransportManager(cookie=self.endpoint.authkey)
            trman.initialize()
            cid = ProcessId._create(pcls, trman.transport_addresses, name)
            p = OSProcessContainer(process_class=pcls,
                                   transport_manager=trman,
                                   process_id=cid,
                                   parent_id=parent,
                                   process_name=name,
                                   cmd_seqno=seqno,
                                   **props)
            p.start()
            p.join(timeout=0.01)
            if not p.is_alive():
                self._log.error("%r terminated prematurely.", cid)
                cid = None
        except Exception as e:
            cid = None
            self._log.error("Failed to create instance (%s) of %s: %r",
                            name, pcls, e)
            if p is not None and p.is_alive():
                p.terminate()
        finally:
            if trman is not None:
                trman.close()
        return cid

    def _spawn_thread(self, pcls, name, parent, props, seqno=None):
        p = None
        cid = None
        try:
            cid = ProcessId._create(pcls,
                                    self.endpoint.transport_addresses,
                                    name)
            p = OSThreadContainer(process_class=pcls,
                                  transport_manager=self.endpoint,
                                  process_id=cid,
                                  parent_id=parent,
                                  process_name=name,
                                  cmd_seqno=seqno,
                                  router=self.router,
                                  **props)
            p.start()
            p.join(timeout=0.01)
            if not p.is_alive():
                self._log.error("%r terminated prematurely.", cid)
                cid = None
        except Exception as e:
            cid = None
            self._log.error("Failed to create instance (%s) of %s: %r",
                            name, pcls, e)
        return cid

    def spawn(self, pcls, names, parent, props, seqno=None, container='process'):
        children = []
        spawn_1 = getattr(self, '_spawn_' + container, None)
        if spawn_1 is None:
            self._log.error("Invalid process container: %r", container)
            return children
        newnamed = []
        for name in names:
            if not isinstance(name, str):
                name = ""
            elif not common.check_name(name):
                self._log.error("Name '%s' contains an illegal character(%r).",
                                name, common.ILLEGAL_NAME_CHARS)
                continue
            cid = spawn_1(pcls, name, parent, props, seqno)
            if cid is not None:
                children.append(cid)
                if len(name) > 0:
                    newnamed.append(cid)
        self._log.debug("%d instances of %s created.",
                        len(children), pcls.__name__)
        if len(newnamed) > 0 and not self.is_node():
            # Propagate names to node
            self.router.send(src=self.dapid, dest=self._nodeid,
                             mesg=(RouterCommands.PING, newnamed),
                             flags=(ChannelCaps.RELIABLEFIFO |
                                    ChannelCaps.BROADCAST))
        return children

class OSProcessContainer(ProcessContainer, multiprocessing.Process):
    """An implementation of processes using OS process.

    """
    def __init__(self, process_class, transport_manager, process_id, parent_id,
                 process_name="", cmd_seqno=None, daemon=False, pipe=None,
                 **rest):
        super().__init__(process_class, transport_manager, process_id,
                         parent_id, process_name, cmd_seqno, rest)
        self.daemon = daemon
        if multiprocessing.get_start_method() == 'spawn':
            self.pipe = pipe
            self._rtopts = (common.GlobalOptions, common.GlobalConfig)

    def _debug_handler(self, sig, frame):
        self._debugger.set_trace(frame)

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
                common.sysinit()
                common._restore_module_logging()
                assert self.pipe is not None
                self.endpoint.initialize(pipe=self.pipe)
                del self.pipe

            self.init_router()
            self.router.register_local_process(self.dapid)
            self.start_router()
            self._daobj = self._dacls(self, self._properties)
            self._log.debug("Process object initialized.")

            return self._daobj._delayed_start()

        except DistProcessExit as e:
            self._log.debug("Caught %r, exiting gracefully.", e)
            return e.exit_code
        except RoutingException as e:
            self._log.debug("Caught %r.", e)
            return 2
        except KeyboardInterrupt as e:
            self._log.debug("Received KeyboardInterrupt, exiting")
            return 1
        except Exception as e:
            self._log.error("Unexpected error: %r", e, exc_info=1)
        finally:
            self.end()

class OSThreadContainer(ProcessContainer, threading.Thread):
    """An implementation of processes using OS threads.

    """
    def __init__(self, process_class, transport_manager, process_id, parent_id,
                 process_name="", cmd_seqno=None, daemon=False, router=None,
                 **rest):
        super().__init__(process_class, transport_manager, process_id,
                         parent_id, process_name, cmd_seqno, rest, router)
        self.daemon = daemon

    def run(self):
        self._log = logger.getChild(self.__class__.__qualname__)
        if len(self.name) == 0:
            self.name = str(self.pid)
        try:
            self.init_router()
            self.router.register_local_process(self.dapid)
            self.start_router()
            self._daobj = self._dacls(self, self._properties)
            self._log.debug("Process object initialized.")

            return self._daobj._delayed_start()

        except DistProcessExit as e:
            self._log.debug("Caught %r, exiting gracefully.", e)
            return e.exit_code
        except RoutingException as e:
            self._log.debug("Caught %r.", e)
            return 2
        except KeyboardInterrupt as e:
            self._log.debug("Received KeyboardInterrupt, exiting")
            return 1
        except Exception as e:
            self._log.error("Unexpected error: %r", e, exc_info=1)
            return 5
        finally:
            if self.router is not None:
                self.router.deregister_local_process(self.dapid)
