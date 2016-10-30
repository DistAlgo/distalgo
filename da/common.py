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
import sys
import copy
import time
import os.path
import logging
import warnings
import importlib
import threading
import multiprocessing

from collections import abc, deque, namedtuple
from inspect import signature, Parameter
from functools import wraps

INCOQ_MODULE_NAME = "incoq.mars.runtime"
CONSOLE_LOG_FORMAT = \
    '[%(relativeCreated)d] %(name)s<%(processName)s>:%(levelname)s: %(message)s'
FILE_LOG_FORMAT = \
    '[%(asctime)s] %(name)s<%(processName)s>:%(levelname)s: %(message)s'

# a dict that contains the runtime configuration values:
GlobalOptions = None
# a dict containing configuration overrides set by the `config` API:
GlobalConfig = dict()
# Process id of the node process:
CurrentNode = None
# incoq.runtime.Type, only if using incoq:
IncOQBaseType = None

# Define custom levels for text output from user code using the `output`,
# `debug`, and `error` builtins, to differentiate output from user programs and
# log messages from the DistAlgo system:
logging.addLevelName(logging.INFO+1, "OUTPUT")
logging.addLevelName(logging.INFO+2, "USRERR")
logging.addLevelName(logging.DEBUG+1, "USRDBG")

log = logging.getLogger(__name__)

api_registry = dict()
builtin_registry = dict()

class InvalidStateException(RuntimeError): pass

def get_runtime_option(key, default=None):
    if GlobalOptions is None:
        if default is None:
            raise InvalidStateException("DistAlgo is not initialized.")
        else:
            return default

    return GlobalOptions.get(key, default)

def set_runtime_option(key, value):
    if GlobalOptions is None:
        raise InvalidStateException("DistAlgo is not initialized.")

    GlobalOptions[key] = value

def initialize_runtime_options(configs):
    global GlobalOptions
    if GlobalOptions is not None:
        raise InvalidStateException("DistAlgo is already initialized")

    GlobalOptions = dict(configs)

def _restore_runtime_options(params):
    global GlobalOptions
    GlobalOptions = params

def set_global_config(props):
    GlobalConfig.update(props)

def global_config():
    return GlobalConfig

def _set_node(node_id):
    global CurrentNode
    CurrentNode = node_id

def pid_of_node():
    return CurrentNode

def get_inc_module():
    if GlobalOptions is None:
        raise InvalidStateException("DistAlgo is not initialized.")
    if not hasattr(sys.modules, GlobalOptions['inc_module_name']):
        return None
    return sys.modules[GlobalOptions['inc_module_name']]

def sysinit():
    if get_runtime_option("long_form_ids"):
        ProcessId.__str__ = ProcessId.__repr__ = ProcessId._long_form_
    setup_logging_for_module("da")
    load_modules()

def setup_logging_for_module(modulename,
                             consolefmt=CONSOLE_LOG_FORMAT,
                             filefmt=FILE_LOG_FORMAT):
    """Configures package level logger.

    """
    rootlog = logging.getLogger(modulename)
    rootlog.handlers = []       # Clear all handlers

    if not GlobalOptions['no_log']:
        rootlog.setLevel(logging.DEBUG)
        consoleformatter = logging.Formatter(consolefmt)
        consolelvl = logging._nameToLevel[GlobalOptions['logconsolelevel'].upper()]
        ch = logging.StreamHandler()
        ch.setFormatter(consoleformatter)
        ch.setLevel(consolelvl)
        rootlog._consolelvl = consolelvl
        rootlog.addHandler(ch)

        if GlobalOptions['logfile']:
            filelvl = logging._nameToLevel[GlobalOptions['logfilelevel'].upper()]
            logfilename = GlobalOptions['logfilename'] \
                          if GlobalOptions['logfilename'] is not None else \
                             (os.path.basename(GlobalOptions['file']) + ".log")
            fh = logging.FileHandler(logfilename)
            formatter = logging.Formatter(filefmt)
            fh.setFormatter(formatter)
            fh.setLevel(filelvl)
            rootlog._filelvl = filelvl
            rootlog.addHandler(fh)

        if GlobalOptions['logdir'] is not None:
            os.makedirs(GlobalOptions['logdir'], exist_ok=True)
            rootlog._logdir = GlobalOptions['logdir']
        else:
            rootlog._logdir = None
    else:
        rootlog.addHandler(logging.NullHandler())

def load_modules():
    global IncOQBaseType
    if not GlobalOptions['load_inc_module']:
        return
    main = sys.modules[GlobalOptions['this_module_name']]
    inc = importlib.import_module(GlobalOptions['inc_module_name'])
    if inc.JbStyle:
        IncOQBaseType = importlib.import_module(INCOQ_MODULE_NAME) \
                        .IncOQType
    if GlobalOptions['control_module_name'] is not None:
        ctrl = importlib.import_module(GlobalOptions['control_module_name'])
        main.IncModule = ModuleIntrument(ctrl, inc)
    else:
        main.IncModule = inc

####################
# Process ID
####################
ThreadLocals = threading.local()
ThreadLocals.pid_counter = 0
class ProcessId(namedtuple("_ProcessId",
                           'uid, seqno, clsname, \
                           name, nodename, hostname, transports')):
    """An instance of `ProcessId` uniquely identifies a DistAlgo process instance.

    A `ProcessId` instance should contain all necessary information for any
    DistAlgo process in a distributed system to send messages to that process.
    This includes the network addresses of all ports the process listens on.

    There is a total ordering over the set of all `ProcessId`s. `ProcessId`s
    referring to the same process will always compare equal.

    From the point of view of DistAlgo programs, `ProcessId` instances are
    opaque objects -- no assumptions should be made about the internal structure
    of `ProcessId` instances.

    """
    __slots__ = ()

    @staticmethod
    def __gen_uid(hostname, pid):
        """Generate a globally unique 96-bit id.

        """
        # 54 bits of timestamp:
        tstamp = int(time.time() * 1000) & 0x3fffffffffffff
        # 16 bits of hostname hash
        hh = int(hash(hostname)) & 0xffff
        # 16 bits of os pid
        pid %= 0xffff
        # 10 bit thread-local counter
        cnt = ThreadLocals.pid_counter = (ThreadLocals.pid_counter + 1) % 1024
        return (tstamp << 42) | (hh << 26) | (pid << 10) | cnt

    @staticmethod
    def _create(pcls, transports, name=""):
        """Creates a new `ProcessId` instance.

        """
        hostname = get_runtime_option('hostname')
        nodename = get_runtime_option('nodename')
        uid = ProcessId.__gen_uid(hostname,
                                  pid=threading.current_thread().ident)
        return ProcessId(uid=uid, seqno=1,
                         clsname=pcls.__qualname__,
                         name=name, nodename=nodename,
                         hostname=hostname, transports=transports)

    def _short_form_(self):
        """Constructs a short string representation of this pid.

        This form is more suitable for use in output strings.

        """
        if len(self.nodename) > 0:
            if len(self.name) > 0:
                return "<{0.clsname}:{0.name}@{0.nodename}>".format(self)
            else:
                # otherwise, we use `uid` truncated to the last 5 hex digits:
                return "<{0.clsname}:{1:05x}@{0.nodename}>".format(
                    self, self.uid & 0xfffff)
        else:
            if len(self.name) > 0:
                return "<{0.clsname}:{0.name}>".format(self)
            else:
                return "<{0.clsname}:{1:05x}>".format(self, self.uid & 0xfffff)

    def _long_form_(self):
        """Constructs a full string representation of this pid.

        This form may be more useful in debugging.

        """
        fmt = "ProcessId(uid={0.uid:x}, seqno={0.seqno}, clsname={0.clsname}, " \
              "name='{0.name}', nodename='{0.nodename}', " \
              "hostname='{0.hostname}', transports={0.transports})"
        return fmt.format(self)

    __str__ = __repr__ = _short_form_

####################

warnings.simplefilter("default", DeprecationWarning)
def deprecated(func):
    """Declare 'func' as deprecated.

    This is a decorator which can be used to mark functions as deprecated. It
    will result in a warning being emmitted when the function is used.

    """
    def newFunc(*args, **kwargs):
        warnings.warn("Call to deprecated function %s." % func.__name__,
                      category=DeprecationWarning, stacklevel=2)
        return func(*args, **kwargs)
    newFunc.__name__ = func.__name__
    newFunc.__doc__ = func.__doc__
    newFunc.__dict__.update(func.__dict__)
    return newFunc

def api(func):
    """Declare 'func' as DistPy API.

    This wraps the function to perform basic type checking for type-annotated
    parameters and return value.

    """
    global api_registry
    funame = func.__name__
    if api_registry.get(funame) is not None:
        return api_registry[funame]

    sig = signature(func)

    @wraps(func)
    def _func_impl(*args, **kwargs):
        try:
            binding = sig.bind(*args, **kwargs)
        except TypeError as e:
            log.error(str(e))
            return None
        for argname in binding.arguments:
            atype = sig.parameters[argname].annotation
            if (atype is not Parameter.empty and
                    not isinstance(binding.arguments[argname], atype)):
                log.error(
                    "'%s' called with wrong type argument: "
                     "%s, expected %s, got %s.",
                    funame, argname, str(atype),
                    str(binding.arguments[argname].__class__))
                return None
        result = func(*args, **kwargs)
        if (sig.return_annotation is not Parameter.empty and
                not isinstance(result, sig.return_annotation)):
            log.warning(
                "Possible bug: API function '%s' return value type mismatch: "
                "declared %s, returned %s.",
                funame, sig.return_annotation, result.__class__)
        return result

    _func_impl.__name__ = func.__name__
    _func_impl.__doc__ = func.__doc__
    _func_impl.__dict__.update(func.__dict__)
    api_registry[funame] = _func_impl
    return _func_impl

def builtin(func):
    """Declare 'func' as DistPy builtin.

    Builtins are instance methods of da.DistProcess, and must be called with
    the process instance as first argument (self).

    """
    global builtin_registry
    funame = func.__name__
    if builtin_registry.get(funame) is not None:
        return builtin_registry[funame]
    else:
        builtin_registry[funame] = func
        return func

class Namespace(object):
    pass

class Null(object):
    def __init__(self, *args, **kwargs): pass
    def __call__(self, *args, **kwargs): return self
    def __getattribute__(self, attr): return self
    def __setattr__(self, attr, value): pass
    def __delattr__(self, attr): pass

class BufferIOWrapper:
    def __init__(self, barray):
        self.buffer = barray
        self.total_bytes = len(barray)
        self.fptr = 0

    def write(self, data):
        end = self.fptr + len(data)
        if end > self.total_bytes:
            raise IOError("buffer full.")
        self.buffer[self.fptr:end] = data
        self.fptr = end

class QueueEmpty(Exception): pass
class WaitableQueue:
    """This class implements a fast waitable queue based on a `deque`.

    The `Queue` class from the standard library `queue` module has a lot of
    unneccesary overhead due to synchronization on every `get` and `put`
    operation. We can avoid these synchronization overheads by piggy-backing off
    the CPython GIL when the queue is non-empty.

    """
    def __init__(self, iterable=[], maxlen=None):
        self._q = deque(iterable, maxlen)
        self._condition = threading.Condition()
        self._num_waiting = 0

    def append(self, item):
        self._q.append(item)
        if self._num_waiting > 0:
            with self._condition:
                self._condition.notify_all()

    def pop(self, block=True, timeout=None):
        # Opportunistically try to get the next item off the queue:
        try:
            return self._q.popleft()
        except IndexError:
            pass
        # The queue was empty, if we don't need to block then we're done:
        if not block or timeout == 0:
            raise QueueEmpty()
        # Otherwise, we have to acquire the condition object and block:
        try:
            with self._condition:
                self._num_waiting += 1
                self._condition.wait(timeout)
                self._num_waiting -= 1
            return self._q.popleft()
        except IndexError:
            # If the queue is still empty at this point, it means that the new
            # event was picked up by another thread, so it's ok for us to
            # return:
            raise QueueEmpty()
        # Other exceptions will be propagated

    def __len__(self):
        return self._q.__len__()


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


class IntrumentationError(Exception): pass
class FunctionInstrument(object):
    def __init__(self, control_func, subject_func):
        super().__setattr__('_control', control_func)
        super().__setattr__('_subject', subject_func)

    def __call__(self, *args, **kwargs):
        ctrl_result = self._control(*args, **kwargs)
        subj_result = self._subject(*args, **kwargs)
        if ctrl_result != subj_result:
            raise IntrumentationError("Result mismatch for %s: "
                                      "control returned %s; "
                                      "subject returned %s." %
                                      (self._control.__name__,
                                       str(ctrl_result),
                                       str(subj_result)))
        return subj_result

    def __setattr__(self, attr, value):
        super().__setattr__(attr, value)
        setattr(self._control, attr, value)
        setattr(self._subject, attr, value)

    def __delattr__(self, attr):
        super().__delattr__(attr)
        delattr(self._control, attr)
        delattr(self._subject, attr)

class ModuleIntrument(object):
    def __init__(self, control, subject):
        super().__setattr__('_control', control)
        super().__setattr__('_subject', subject)

    def __getattribute__(self, attr):
        ctrl_attr = getattr(super().__getattribute__('_control'), attr)
        subj_attr = getattr(super().__getattribute__('_subject'), attr)
        if type(ctrl_attr) is not type(subj_attr):
            raise IntrumentationError("Attribute mismatch for %s:"
                                      "control is type %s; "
                                      "subject is type %s." %
                                      (attr, str(type(ctrl_attr)),
                                       str(type(subj_attr))))
        if hasattr(ctrl_attr, '__call__') and \
           (ctrl_attr.__name__.startswith("Query_") or
            ctrl_attr.__name__ == "init"):
                return FunctionInstrument(ctrl_attr, subj_attr)
        else:
            return subj_attr

    def __setattr__(self, attr, value):
        super().__setattr__(attr, value)
        setattr(self._control, attr, value)
        setattr(self._subject, attr, value)

    def __delattr__(self, attr):
        super().__delattr__(attr)
        delattr(self._control, attr)
        delattr(self._subject, attr)

class frozendict(dict):
    """Hashable immutable dict implementation

    Copied from http://code.activestate.com/recipes/414283/

    """
    def _blocked_attribute(obj):
        raise AttributeError("A frozendict cannot be modified.")
    _blocked_attribute = property(_blocked_attribute)

    __delitem__ = __setitem__ = clear = _blocked_attribute
    pop = popitem = setdefault = update = _blocked_attribute

    def __new__(cls, *args, **kws):
        new = dict.__new__(cls)
        dict.__init__(new, *args, **kws)
        return new

    def __init__(self, *args, **kws):
        pass

    def __hash__(self):
        try:
            return self._cached_hash
        except AttributeError:
            h = self._cached_hash = hash(tuple(sorted(self.items())))
            return h

    def __repr__(self):
        return "frozendict(%s)" % dict.__repr__(self)

BuiltinImmutables = {int, float, complex, tuple, str, bytes, frozenset}

def freeze(obj):
    """Return a hashable version of `obj`.

    Contents of `obj` may be copied if necessary.

    """
    if any(isinstance(obj, itype) for itype in BuiltinImmutables):
        return obj

    if IncOQBaseType is not None:
        if isinstance(obj, IncOQBaseType):
            return copy.deepcopy(obj)

    if isinstance(obj, abc.MutableSequence):
        if isinstance(obj, abc.ByteString):
            # bytearray -> bytes
            return bytes(obj)
        else:
            # list -> tuple
            return tuple(freeze(elem) for elem in obj)
    elif isinstance(obj, abc.MutableSet):
        # set -> frozenset
        return frozenset(freeze(elem) for elem in obj)
    elif isinstance(obj, abc.MutableMapping):
        # dict -> frozendict
        return frozendict((freeze(k), freeze(v)) for k, v in obj.items())
    elif isinstance(obj, abc.Sequence):
        #NOTE: This part is fragile. For immutable sequence objects, we still
        # have to recursively freeze its elements, which means we have to create
        # a new instance of the same type. Here we just assume the class
        # `__init__` method takes a 'iterable' as argument. Otherwise,
        # everything falls apart.
        return type(obj)(freeze(e) for e in obj)
    else:
        # everything else just assume hashable & immutable, hahaha:
        return obj

if __name__ == "__main__":
    @api
    def testapi(a : int, b : list) -> dict:
        print (a, b)
        return []

    testapi(1, [2])
    testapi(1, {})
    print(api_registry)

    @deprecated
    def testdepre():
        print("deprecated function")

    t = ('a', 1)
    print("Freeze " + str(t) + "->" + str(freeze(t)))
    testdepre()
