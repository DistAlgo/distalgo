import logging

from inspect import signature, Parameter
from functools import wraps

log = logging.getLogger(__name__)
formatter = logging.Formatter(
    '[%(asctime)s]%(name)s:%(levelname)s: %(message)s')
log._formatter = formatter

api_registry = dict()

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

class Null(object):
    def __init__(self, *args, **kwargs): pass
    def __call__(self, *args, **kwargs): return self
    def __getattribute__(self, attr): return self
    def __setattr__(self, attr, value): pass
    def __delattr__(self, attr): pass
