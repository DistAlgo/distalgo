from .common import freeze

class PatternElement:
    """Tree structure representing a message pattern.
    """

    def __init__(self, value):
        self.value = value

    def filter(self, iterable, order=None, **context):
        if isinstance(iterable, set):
            return self._filter_set(iterable, order, **context)
        else:
            return self._filter_list(iterable, order, **context)

    def _filter_list(self, iterable, order=None, **context):
        for elt in iterable:
            bindings = dict()
            if self.match(elt, bindings=bindings, **context):
                if len(bindings) > 0 and order is not None:
                    yield tuple(bindings[name] for name in order)
                else:
                    yield True

    def _filter_set(self, iterable, order=None, **context):
        seen = set()
        for elt in iterable:
            bindings = dict()
            if self.match(elt, bindings=bindings, **context):
                if len(bindings) > 0 and order is not None:
                    res = tuple(bindings[name] for name in order)
                    if res not in seen:
                        seen.add(res)
                        yield res
                else:
                    if True not in seen:
                        seen.add(True)
                        yield True

    def match(self, message, bindings=None,
              ignore_bound_vars=False, **context):
        return False

    def match_iter(self, iterable, **context):
        for elt in iterable:
            bindings = dict()
            if self.match(elt, bindings=bindings, **context):
                return True
        return False

    def __str__(self):
        raise "<PatternElement " + str(self.value) + ">"

    def __repr__(self):
        return str(self)

class ConstantPattern(PatternElement):
    def match(self, message, bindings=None,
              ignore_bound_vars=False, **context):
        return self.value == message

    def __str__(self):
        return "=" + repr(self.value)

class SelfPattern(ConstantPattern):
    def __init__(self):
        super().__init__(None)

    def match(self, message, bindings=None,
              ignore_bound_vars=False, SELF_ID=None, **context):
        return message == SELF_ID

    def __str__(self):
        return "=SELF"

class BoundPattern(PatternElement):
    def match(self, message, bindings=None,
              ignore_bound_vars=False, **context):
        if ignore_bound_vars:
            return True
        try:
            return context[self.value] == message
        except KeyError:
            return False

    def __str__(self):
        return "=" + self.value

class FreePattern(PatternElement):
    def match(self, message, bindings=None,
              ignore_bound_vars=False, **context):
        if bindings is None:
            bindings = dict()
        if self.value is not None:
            try:
                if bindings[self.value] != message:
                    return False
            except KeyError:
                bindings[self.value] = message
        return True

    def __str__(self):
        if self.value is None:
            return "_"
        else:
            return self.value

class TuplePattern(PatternElement):
    def match(self, message, bindings=None,
              ignore_bound_vars=False, **context):
        if bindings is None:
            bindings = dict()
        if (type(message) is not tuple or
            len(message) != len(self.value)):
            return False
        # Fall through
        for pat, mem in zip(self.value, message):
            if not pat.match(mem, bindings, ignore_bound_vars, **context):
                return False
        return True

    def __str__(self):
        return "(" + ",".join([str(p) for p in self.value]) + ")"

class ListPattern(PatternElement):
    def match(self, message, bindings=None,
              ignore_bound_vars=False, **context):
        if bindings is None:
            bindings = dict()
        if (type(message) is not list or
            len(message) != len(self.value)):
            return False
        # Fall through
        for pat, mem in zip(self.value, message):
            if not pat.match(mem, bindings, ignore_bound_vars, **context):
                return False
        return True

    def __str__(self):
        return "[" + ",".join([str(p) for p in self.value]) + "]"


class Event:
    """ Describes a single event.

    Instances of Event are created by the backend thread and passed to the
    front end.
    """
    def __init__(self, envelope, message):
        (self.timestamp, self.destination, self.source) = envelope
        self.message = message

    def to_tuple(self):
        """Generates a tuple representation for this event."""
        return (type(self),
                (self.timestamp, self.destination, self.source),
                freeze(self.message))

    def __str__(self):
        buf = ["<", type(self).__name__,
               " time:", str(self.timestamp),
               " to:", repr(self.destination),
               " from:", repr(self.source),
               " msg:", repr(self.message),
               ">"]
        return "".join(buf)

    def __repr__(self):
        return str(self)

class ReceivedEvent(Event): pass
class SentEvent(Event): pass


class EventPattern:
    """ Describes an event "pattern" that can be used to match against Event
    instances.
    """

    def __init__(self, eventtype, name, pattern,
                 sources=None, destinations=None, timestamps=None,
                 record_history=False, handlers=[]):
        self.eventtype = eventtype
        self.name = name
        self.pattern = pattern
        self.sources = sources
        self.destinations = destinations
        self.timestamps = timestamps
        self.record_history = record_history
        self.handlers = handlers

    def filter(self, iterable, order=None, **context):
        for elt in iterable:
            bindings = dict()
            if self.match(elt, bindings=bindings, **context):
                if len(bindings) > 0 and order is not None:
                    yield tuple(bindings[name] for name in order)
                else:
                    yield True

    def match(self, event, bindings=None,
              ignore_bound_vars=False, **context):
        if isinstance(event, tuple):
            event = event[0](*event[1:])
        if type(event) is not self.eventtype:
            return False
        if bindings is None:
            bindings = dict()

        if self.sources is not None:
            for pat in self.sources:
                if pat.match(event.source, bindings,
                             ignore_bound_vars, **context):
                    break
            else:
                return False

        if self.destinations is not None:
            for pat in self.destinations:
                if pat.match(event.destination, bindings,
                             ignore_bound_vars, **context):
                    break
            else:
                return False

        if self.timestamps is not None:
            for pat in self.timestamps:
                if pat.match(event.timestamp, bindings,
                             ignore_bound_vars, **context):
                    break
            else:
                return False

        if (self.pattern is not None and
                not self.pattern.match(event.message, bindings,
                                       ignore_bound_vars, **context)):
            return False
        else:
            return True

    def __str__(self):
        buf = ["<", self.eventtype.__name__,
               " name:", self.name,
               " msg:", str(self.pattern)]
        if self.sources is not None:
            buf.extend([" from:{",
                        ",".join([str(s) for s in self.sources]),
                        "}"])
        if self.destinations is not None:
            buf.extend([" to:{",
                        ",".join([str(s) for s in self.destinations]),
                        "}"])
        if self.timestamps is not None:
            buf.extend([" time:{",
                        ",".join([str(s) for s in self.timestamps]),
                        "}"])
        buf.extend([">"])
        return "".join(buf)

    def __repr__(self):
        return str(self)


if __name__ == "__main__":
    # pattern = PatternElement(
    #     TupleVar,
    #     [PatternElement(ConstantVar, "xxx"),
    #      PatternElement(BoundVar, "foo1"),
    #      PatternElement(
    #          ListVar,
    #          [PatternElement(FreeVar, "bar1"),
    #           PatternElement(ConstantVar, 5)]
    #          )
    #  ])
    eventpat = EventPattern(ReceivedEvent,
                            'ReceivedEvent_1',
                            TuplePattern([ConstantPattern('Request'),
                                          FreePattern('reqts')]),
                            sources=[FreePattern('source')],
                            destinations=None,
                            timestamps=None,
                            record_history=False, handlers=[])
    # evtpat = EventPattern(ReceiveEvent,
    #                       "abc",
    #                       pattern,
    #                       sources={PatternElement(FreeVar, "src1")},
    #                       destinations={PatternElement(FreeVar, "self")},
    #                       timestamps=None
    #                       )
    message1 = ("Request", 0)
    evt = ReceivedEvent(message1, 0, 1, 2)
    b = dict()
    res = eventpat.match(evt, bindings=b, **{"foo1": 2})
    print(eventpat, evt)
