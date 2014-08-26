import os
import os.path
import logging

from inspect import signature, Parameter
from functools import wraps

log = logging.getLogger(__name__)
formatter = logging.Formatter(
    '[%(asctime)s]%(name)s:%(levelname)s: %(message)s')
log._formatter = formatter

api_registry = dict()
builtin_registry = dict()

def setup_root_logger(params):
    rootlog = logging.getLogger("")

    if not params.nolog:
        rootlog.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '[%(asctime)s]%(name)s:%(levelname)s: %(message)s')
        rootlog._formatter = formatter

        consolelvl = logging._nameToLevel[params.logconsolelevel.upper()]

        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        ch.setLevel(consolelvl)
        rootlog._consolelvl = consolelvl
        rootlog.addHandler(ch)

        if params.logfile:
            filelvl = logging._nameToLevel[params.logfilelevel.upper()]
            logfilename = params.logfilename \
                          if params.logfilename is not None else \
                             (os.path.basename(params.file) + ".log")
            fh = logging.FileHandler(logfilename)
            fh.setFormatter(formatter)
            fh.setLevel(filelvl)
            rootlog._filelvl = filelvl
            rootlog.addHandler(fh)

        if params.logdir is not None:
            os.makedirs(params.logdir, exist_ok=True)
            rootlog._logdir = params.logdir
        else:
            rootlog._logdir = None
    else:
        rootlog.addHandler(logging.NullHandler())

def load_inc_module(options, module_name):
    if options.loadincmodule:
        name = options.incmodulename if options.incmodulename is not None \
               else module_name + "_inc"
        return importlib.import_module(name)
    else:
        return None

def deprecated(func):
    """Declare 'func' as deprecated.

    This is a decorator which can be used to mark functions as deprecated. It
    will result in a warning being emmitted when the function is used.
    """

    def newFunc(*args, **kwargs):
        warnings.warn("Call to deprecated function %s." % func.__name__,
                      category=DeprecationWarning)
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
        #raise RuntimeError("Double definition of API function: %s" % funame)

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
                    ("'%s' called with wrong type argument: "
                     "%s, expected %s, got %s.") %
                    (funame, argname, str(atype),
                     str(binding.arguments[argname].__class__)))
                return None
        result = func(*args, **kwargs)
        if (sig.return_annotation is not Parameter.empty and
                not isinstance(result, sig.return_annotation)):
            log.warn(
                ("Possible bug: API function '%s' return value type mismatch: "
                 "declared %s, returned %s.") %
                (funame, sig.return_annotation, result.__class__))
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

class Null(object):
    def __init__(self, *args, **kwargs): pass
    def __call__(self, *args, **kwargs): return self
    def __getattribute__(self, attr): return self
    def __setattr__(self, attr, value): pass
    def __delattr__(self, attr): pass

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

def freeze(obj):
    if isinstance(obj, list):
        # list -> tuple
        return tuple(freeze(elem) for elem in obj)
    elif isinstance(obj, bytearray):
        # bytearray -> bytes
        return bytes(obj)
    elif isinstance(obj, set) and not isinstance(obj, frozenset):
        # set -> frozenset
        return frozenset(freeze(elem) for elem in obj)
    elif isinstance(obj, dict) and not isinstance(obj, frozendict):
        # dict -> frozendict
        return frozendict((freeze(k), freeze(v)) for k, v in obj.items())
    elif hasattr(obj, '__iter__'):
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
