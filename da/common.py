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
import os.path
import logging
import importlib
import threading
import warnings
import multiprocessing
import collections.abc

from collections import deque
from inspect import signature, Parameter
from functools import wraps

GlobalOptions = None
CurrentProcess = {'pid' : multiprocessing.current_process().pid}
IncOQBaseType = None           # incoq.runtime.Type, if using incoq
INCOQ_MODULE_NAME = "incoq.mars.runtime"

class DALogger(logging.getLoggerClass()):
    """Custom logger that attaches current process info.

    """
    def _log(self, level, msg, args, exc_info=None, extra=None):
        if extra is None:
            extra = CurrentProcess
        return super()._log(level, msg, args, exc_info, extra)

logging.setLoggerClass(DALogger)
# Define a custom level for text output from user code using the `output`
# builtin, to better differentiate user output from library output. This level
# should *only* be used by the `output` builtin. Its severity is set to one
# level above 'INFO':
logging.addLevelName(logging.INFO+1, "OUTPUT")

log = logging.getLogger(__name__)

api_registry = dict()
builtin_registry = dict()

def set_current_process():
    global CurrentProcess
    CurrentProcess['pid'] = multiprocessing.current_process().pid

def set_global_options(params):
    global GlobalOptions
    GlobalOptions = params

def global_options():
    return GlobalOptions

def current_process():
    return CurrentProcess

def get_inc_module():
    if not hasattr(sys.modules, GlobalOptions.inc_module_name):
        return None
    return sys.modules[GlobalOptions.inc_module_name]

def sysinit():
    setup_root_logger()
    load_modules()

def setup_root_logger():
    rootlog = logging.getLogger("")

    if not GlobalOptions.nolog:
        rootlog.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '[%(asctime)s] %(name)s<%(processName)s>:%(levelname)s: %(message)s')
        consoleformatter = logging.Formatter(
            '[%(relativeCreated)d] %(name)s<%(processName)s>:%(levelname)s: %(message)s')
        rootlog._formatter = formatter

        consolelvl = logging._nameToLevel[GlobalOptions.logconsolelevel.upper()]

        ch = logging.StreamHandler()
        ch.setFormatter(consoleformatter)
        ch.setLevel(consolelvl)
        rootlog._consolelvl = consolelvl
        rootlog.addHandler(ch)

        if GlobalOptions.logfile:
            filelvl = logging._nameToLevel[GlobalOptions.logfilelevel.upper()]
            logfilename = GlobalOptions.logfilename \
                          if GlobalOptions.logfilename is not None else \
                             (os.path.basename(GlobalOptions.file) + ".log")
            fh = logging.FileHandler(logfilename)
            fh.setFormatter(formatter)
            fh.setLevel(filelvl)
            rootlog._filelvl = filelvl
            rootlog.addHandler(fh)

        if GlobalOptions.logdir is not None:
            os.makedirs(GlobalOptions.logdir, exist_ok=True)
            rootlog._logdir = GlobalOptions.logdir
        else:
            rootlog._logdir = None
    else:
        rootlog.addHandler(logging.NullHandler())

def load_modules():
    global IncOQBaseType
    if not GlobalOptions.load_inc_module:
        return
    main = sys.modules[GlobalOptions.this_module_name]
    inc = importlib.import_module(GlobalOptions.inc_module_name)
    if inc.JbStyle:
        IncOQBaseType = importlib.import_module(INCOQ_MODULE_NAME) \
                        .IncOQType
    if GlobalOptions.control_module_name is not None:
        ctrl = importlib.import_module(GlobalOptions.control_module_name)
        main.IncModule = ModuleIntrument(ctrl, inc)
    else:
        main.IncModule = inc

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
            return None
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
            return None
        # Other exceptions will be propagated

    def __len__(self):
        return self._q.__len__()

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
    if type(obj) in BuiltinImmutables:
        return obj

    if IncOQBaseType is not None:
        if isinstance(obj, IncOQBaseType):
            return copy.deepcopy(obj)

    if isinstance(obj, collections.abc.MutableSequence):
        if isinstance(obj, collections.abc.ByteString):
            # bytearray -> bytes
            return bytes(obj)
        else:
            # list -> tuple
            return tuple(freeze(elem) for elem in obj)
    elif isinstance(obj, collections.abc.MutableSet):
        # set -> frozenset
        return frozenset(freeze(elem) for elem in obj)
    elif isinstance(obj, collections.abc.MutableMapping):
        # dict -> frozendict
        return frozendict((freeze(k), freeze(v)) for k, v in obj.items())
    elif isinstance(obj, collections.abc.Sequence):
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
