"""Utility for making (best-effort) read-only copies of objects.

This is adapted from the `copy.deepcopy` standard library API. To freeze builtin
types, we create `frozenset`, `frozendict`, and `frozenlist` in place of `set`,
`dict`, and `list`, respectively. For user-defined types, if it has a
`__deepfreeze__` special method, then that method is called to create a
read-only deep-copy. Otherwise, we simply fall-back to making a deep copy, which
is better than nothing. Refer to the documentation for module `copy` on how deep
copy works.

"""

import types
import weakref
from copyreg import dispatch_table

__all__ = ['frozendict', 'frozenlist', 'deepfreeze']


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

    def _build_set_keyvalue_(self, key, val):
        """Backdoor updater for recursively building the frozendict."""
        if not hasattr(self, '_cached_hash'):
            return super().__setitem__(key, val)
        else:
            raise AttributeError("Attempting to update frozendict after "
                                 "hash value has been read.")


class frozenlist(list):
    """Hashable immutable list implementation

    Copied from http://code.activestate.com/recipes/414283/

    """

    def _blocked_attribute(obj):
        raise AttributeError("A frozenlist cannot be modified.")
    _blocked_attribute = property(_blocked_attribute)

    append = extend = insert = remove = sort = clear = pop = reverse = \
    __iadd__ = __imul__ = __delitem__ = __setitem__ =  _blocked_attribute

    def __new__(cls, *args, **kws):
        new = list.__new__(cls)
        list.__init__(new, *args, **kws)
        return new

    def __init__(self, *args, **kws):
        pass

    def __hash__(self):
        try:
            return self._cached_hash
        except AttributeError:
            h = self._cached_hash = hash(tuple(sorted(self)))
            return h

    def __repr__(self):
        return "frozenlist(%s)" % list.__repr__(self)

    def _build_add_elem_(self, elem):
        """Backdoor updater for recursively building the frozenlist."""
        if not hasattr(self, '_cached_hash'):
            return super().append(elem)
        else:
            raise AttributeError("Attempting to modify frozenlist after "
                                 "hash value has been read.")


def deepfreeze(x, memo=None, _nil=[]):
    """Deep freeze operation on arbitrary Python objects.

    See the module's __doc__ string for more info.

    """

    if memo is None:
        memo = {}

    d = id(x)
    y = memo.get(d, _nil)
    if y is not _nil:
        return y

    cls = type(x)

    copier = _deepfreeze_dispatch.get(cls)
    if copier:
        y = copier(x, memo)
    else:
        try:
            issc = issubclass(cls, type)
        except TypeError: # cls is not a class (old Boost; see SF #502085)
            issc = 0
        if issc:
            y = _deepfreeze_atomic(x, memo)
        else:
            copier = getattr(x, "__deepfreeze__", None)
            if copier:
                y = copier(memo)
            else:
                reductor = dispatch_table.get(cls)
                if reductor:
                    rv = reductor(x)
                else:
                    reductor = getattr(x, "__reduce_ex__", None)
                    if reductor:
                        rv = reductor(4)
                    else:
                        reductor = getattr(x, "__reduce__", None)
                        if reductor:
                            rv = reductor()
                        else:
                            raise Error(
                                "un(deep)copyable object of type %s" % cls)
                if isinstance(rv, str):
                    y = x
                else:
                    y = _reconstruct(x, memo, *rv)

    # If is its own copy, don't memoize.
    if y is not x:
        memo[d] = y
        _keep_alive(x, memo) # Make sure x lives at least as long as d
    return y

_deepfreeze_dispatch = d = {}

def _deepfreeze_atomic(x, memo):
    return x
d[type(None)] = _deepfreeze_atomic
d[type(Ellipsis)] = _deepfreeze_atomic
d[type(NotImplemented)] = _deepfreeze_atomic
d[int] = _deepfreeze_atomic
d[float] = _deepfreeze_atomic
d[bool] = _deepfreeze_atomic
d[complex] = _deepfreeze_atomic
d[bytes] = _deepfreeze_atomic
d[str] = _deepfreeze_atomic
try:
    d[types.CodeType] = _deepfreeze_atomic
except AttributeError:
    pass
d[type] = _deepfreeze_atomic
d[types.BuiltinFunctionType] = _deepfreeze_atomic
d[types.FunctionType] = _deepfreeze_atomic
d[weakref.ref] = _deepfreeze_atomic

def _deepfreeze_set(x, memo, deepfreeze=deepfreeze):
    # A set can not contain itself, so we can freeze its elements before putting
    # the set in the memo:
    y = frozenset(deepfreeze(a, memo) for a in x)
    # We still have to put the set on the memo because other mutable structures
    # might contain a reference to it:
    memo[id(x)] = y
    return y
d[set] = _deepfreeze_set

def _deepfreeze_bytearray(x, memo, deepfreeze=deepfreeze):
    y = bytes(x)
    memo[id(x)] = y
    return y
d[bytearray] = _deepfreeze_bytearray

def _deepfreeze_list(x, memo, deepfreeze=deepfreeze):
    y = frozenlist()
    memo[id(x)] = y
    append = y._build_add_elem_
    for a in x:
        append(deepfreeze(a, memo))
    return y
d[list] = _deepfreeze_list

def _deepfreeze_tuple(x, memo, deepfreeze=deepfreeze):
    y = [deepfreeze(a, memo) for a in x]
    # We're not going to put the tuple in the memo, but it's still important we
    # check for it, in case the tuple contains recursive mutable structures.
    try:
        return memo[id(x)]
    except KeyError:
        pass
    for k, j in zip(x, y):
        if k is not j:
            y = tuple(y)
            break
    else:
        y = x
    return y
d[tuple] = _deepfreeze_tuple

def _deepfreeze_dict(x, memo, deepfreeze=deepfreeze):
    y = frozendict()
    memo[id(x)] = y
    update = y._build_set_keyvalue_
    for key, value in x.items():
        update(deepfreeze(key, memo), deepfreeze(value, memo))
    return y
d[dict] = _deepfreeze_dict

def _deepfreeze_method(x, memo): # Copy instance methods
    return type(x)(x.__func__, deepfreeze(x.__self__, memo))
d[types.MethodType] = _deepfreeze_method

del d

def _keep_alive(x, memo):
    """Keeps a reference to the object x in the memo.

    Because we remember objects by their id, we have
    to assure that possibly temporary objects are kept
    alive by referencing them.
    We store a reference at the id of the memo, which should
    normally not be used unless someone tries to deepfreeze
    the memo itself...
    """
    try:
        memo[id(memo)].append(x)
    except KeyError:
        # aha, this is the first one :-)
        memo[id(memo)]=[x]

def _reconstruct(x, memo, func, args,
                 state=None, listiter=None, dictiter=None,
                 deepfreeze=deepfreeze):
    deep = memo is not None
    if deep and args:
        args = (deepfreeze(arg, memo) for arg in args)
    y = func(*args)
    if deep:
        memo[id(x)] = y

    if state is not None:
        if deep:
            state = deepfreeze(state, memo)
        if hasattr(y, '__setstate__'):
            y.__setstate__(state)
        else:
            if isinstance(state, tuple) and len(state) == 2:
                state, slotstate = state
            else:
                slotstate = None
            if state is not None:
                y.__dict__.update(state)
            if slotstate is not None:
                for key, value in slotstate.items():
                    setattr(y, key, value)

    if listiter is not None:
        if deep:
            for item in listiter:
                item = deepfreeze(item, memo)
                y.append(item)
        else:
            for item in listiter:
                y.append(item)
    if dictiter is not None:
        if deep:
            for key, value in dictiter:
                key = deepfreeze(key, memo)
                value = deepfreeze(value, memo)
                y[key] = value
        else:
            for key, value in dictiter:
                y[key] = value
    return y

del types, weakref
