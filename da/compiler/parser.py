# Copyright (c) 2010-2014 Bo Lin
# Copyright (c) 2010-2014 Yanhong Annie Liu
# Copyright (c) 2010-2014 Stony Brook University
# Copyright (c) 2010-2014 The Research Foundation of SUNY
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

import builtins
import sys
from ast import *

from .. import common
from . import dast
from .utils import printe, printw, printd

# DistAlgo keywords
KW_PROCESS_DEF = "process"
KW_CONFIG = "config"
KW_RECV_QUERY = "received"
KW_SENT_QUERY = "sent"
KW_RECV_EVENT = "receive"
KW_SENT_EVENT = "sent"
KW_MSG_PATTERN = "msg"
KW_EVENT_SOURCE = "from_"
KW_EVENT_DESTINATION = "dst"
KW_EVENT_TIMESTAMP = "clk"
KW_EVENT_LABEL = "at"
KW_DECORATOR_LABEL = "labels"
KW_EXISTENTIAL_QUANT = "some"
KW_UNIVERSAL_QUANT = "each"
KW_AGGREGATE_SIZE = "lenof"
KW_AGGREGATE_MIN = "minof"
KW_AGGREGATE_MAX = "maxof"
KW_AGGREGATE_SUM = "sumof"
KW_COMP_SET = "setof"
KW_COMP_TUPLE = "tupleof"
KW_COMP_LIST = "listof"
KW_COMP_DICT = "dictof"
KW_AWAIT = "await"
KW_AWAIT_TIMEOUT = "timeout"
KW_SEND = "send"
KW_SEND_TO = "to"
KW_BROADCAST = "bcast"
KW_PRINT = "output"
KW_SELF = "self"
KW_TRUE = "True"
KW_FALSE = "False"
KW_NULL = "None"
KW_SUCH_THAT = "has"
KW_RESET = "reset"

def is_setup_func(node):
    """Returns True if this node defines a function named 'setup'."""

    return (isinstance(node, FunctionDef) and
            node.name == "setup")

def extract_label(node):
    """Returns the label name specified in 'node', or None if 'node' is not a
    label.
    """
    if (isinstance(node, UnaryOp) and
            isinstance(node.op, USub) and
            isinstance(node.operand, UnaryOp) and
            isinstance(node.operand.op, USub) and
            isinstance(node.operand.operand, Name)):
        return node.operand.operand.id
    else:
        return None

##########
# Operator mappings:
##########
NegatedOperators = {
    NotEq : dast.EqOp,
    IsNot : dast.IsOp,
    NotIn : dast.InOp
}

OperatorMap = {
    Add : dast.AddOp,
    Sub : dast.SubOp,
    Mult : dast.MultOp,
    Div : dast.DivOp,
    Mod : dast.ModOp,
    Pow : dast.PowOp,
    LShift : dast.LShiftOp,
    RShift : dast.RShiftOp,
    BitOr : dast.BitOrOp,
    BitXor : dast.BitXorOp,
    BitAnd : dast.BitAndOp,
    FloorDiv : dast.FloorDivOp,

    Eq : dast.EqOp,
    NotEq: dast.NotEqOp,
    Lt : dast.LtOp,
    LtE : dast.LtEOp,
    Gt : dast.GtOp,
    GtE : dast.GtEOp,
    Is : dast.IsOp,
    IsNot : dast.IsNotOp,
    In : dast.InOp,
    NotIn : dast.NotInOp,

    USub : dast.USubOp,
    UAdd : dast.UAddOp,
    Invert : dast.InvertOp,

    And : dast.AndOp,
    Or : dast.OrOp
}
# New matrix multiplication operator since 3.5:
if sys.version_info > (3, 5):
    OperatorMap[MatMult] = dast.MatMultOp

# FIXME: is there a better way than hardcoding these?
KnownUpdateMethods = {
    "add", "append", "extend", "update",
    "insert", "reverse", "sort",
    "delete", "remove", "pop", "clear", "discard"
}

ValidResetTypes = {"Received", "Sent", ""}

ApiMethods = common.api_registry.keys()

BuiltinMethods = common.builtin_registry.keys()

PythonBuiltins = dir(builtins)

ComprehensionTypes = {KW_COMP_SET, KW_COMP_TUPLE, KW_COMP_DICT, KW_COMP_LIST}

AggregateKeywords = {KW_AGGREGATE_MAX, KW_AGGREGATE_MIN,
                     KW_AGGREGATE_SIZE, KW_AGGREGATE_SUM}

Quantifiers = {KW_UNIVERSAL_QUANT, KW_EXISTENTIAL_QUANT}

##########
# Exceptions:
class MalformedStatementError(Exception): pass

##########
# Name context types:
class NameContext:
    def __init__(self, type=None):
        self.type = type

class Assignment(NameContext): pass
class Update(NameContext): pass
class Read(NameContext): pass
class IterRead(Read): pass
class FunCall(NameContext): pass
class Delete(NameContext): pass
class AttributeLookup(NameContext): pass
class SubscriptLookup(NameContext): pass
class PatternContext(NameContext): pass
class Existential(NameContext): pass
class Universal(NameContext): pass

##########

class PatternParser(NodeVisitor):
    """Parses a pattern.
    """

    def __init__(self, parser, literal=False):
        self._parser = parser
        if parser.current_query_scope is None:
            self.namescope = dast.NameScope(parser.current_scope)
        else:
            self.namescope = parser.current_query_scope
        self.parent_node = parser.current_parent
        self.current_query = parser.current_query
        self.use_object_style = parser.use_object_style
        self.literal = literal

    @property
    def outer_scope(self):
        return self.namescope.parent_scope

    def visit(self, node):
        if isinstance(node, Name):
            return self.visit_Name(node)
        elif isinstance(node, Tuple):
            return self.visit_Tuple(node)
        elif isinstance(node, List):
            return self.visit_List(node)

        # Parse general expressions:
        self._parser.current_context = Read()
        expr = self._parser.visit(node)
        if isinstance(expr, dast.ConstantExpr):
            return dast.ConstantPattern(self.parent_node, node, value=expr)
        else:
            return dast.BoundPattern(self.parent_node, node, value=expr)

    def is_bound(self, name):
        n = self.namescope.find_name(name)
        if n is not None:
            for r, _ in n.reads:
                if r.is_child_of(self.current_query):
                    return True
        return False

    def visit_Name(self, node):
        if self._parser.current_process is not None and \
           node.id == KW_SELF:
            return dast.ConstantPattern(
                self.parent_node, node,
                value=dast.SelfExpr(self.parent_node, node))
        elif node.id == KW_TRUE:
            return dast.ConstantPattern(
                self.parent_node, node,
                value=dast.TrueExpr(self.parent_node, node))
        elif node.id == KW_FALSE:
            return dast.ConstantPattern(
                self.parent_node, node,
                value=dast.FalseExpr(self.parent_node, node))
        elif node.id == KW_NULL:
            return dast.ConstantPattern(
                self.parent_node, node,
                value=dast.NoneExpr(self.parent_node, node))
        elif self.literal:
            name = node.id
            n = self.outer_scope.find_name(name)
            if n is None:
                n = self.namescope.add_name(name)
            pat = dast.BoundPattern(self.parent_node, node, value=n)
            n.add_read(pat)
            return pat

        name = node.id
        if name == "_":
            # Wild card
            return dast.FreePattern(self.parent_node, node)
        elif name.startswith("_"):
            # Bound variable:
            name = node.id[1:]
            n = self.outer_scope.find_name(name)
            if n is None:
                self._parser.warn(
                    ("new variable '%s' introduced by bound pattern." % name),
                    node)
                n = self.namescope.add_name(name)
            pat = dast.BoundPattern(self.parent_node, node, value=n)
            n.add_read(pat)
            return pat
        else:
            # Could be free or bound:
            name = node.id
            if self.is_bound(name):
                self._parser.debug("[PatternParser] reusing bound name " +
                                   name, node)
                n = self.namescope.find_name(name)
                pat = dast.BoundPattern(self.parent_node, node, value=n)
                n.add_read(pat)
            else:
                self._parser.debug("[PatternParser] free name " + name, node)
                n = self.namescope.add_name(name)
                pat = dast.FreePattern(self.parent_node, node, value=n)
                n.add_assignment(pat)
            return pat

    def visit_Str(self, node):
        return dast.ConstantPattern(
            self.parent_node, node,
            value=dast.ConstantExpr(self.parent_node, node, node.s))

    def visit_Bytes(self, node):
        return dast.ConstantPattern(
            self.parent_node, node,
            value=dast.ConstantExpr(self.parent_node, node, node.s))

    def visit_Num(self, node):
        return dast.ConstantPattern(
            self.parent_node, node,
            value=dast.ConstantExpr(self.parent_node, node, node.n))

    def visit_Tuple(self, node):
        return dast.TuplePattern(
            self.parent_node, node,
            value=[self.visit(e) for e in node.elts])

    def visit_List(self, node):
        return dast.ListPattern(
            self.parent_node, node,
            value=[self.visit(e) for e in node.elts])

    def visit_Call(self, node):
        if not self.use_object_style:
            return self.generic_visit(node)

        if not isinstance(node.func, Name): return None
        elts = [dast.ConstantPattern(
            self.parent_node, node,
            value=dast.ConstantExpr(self.parent_node,
                                    node.func,
                                    value=node.func.id))]
        for e in node.args:
            elts.append(self.visit(e))
        return dast.TuplePattern(self.parent_node, node,
                                 value=elts)

class Pattern2Constant(NodeVisitor):
    def __init__(self, parent):
        super().__init__()
        self.stack = [parent]

    @property
    def current_parent(self):
        return self.stack[-1]

    def visit_ConstantPattern(self, node):
        expr = node.value.clone()
        expr._parent = self.current_parent
        return expr

    visit_BoundPattern = visit_ConstantPattern

    def visit_TuplePattern(self, node):
        expr = TupleExpr(self.current_parent)
        self.stack.push(expr)
        expr.subexprs = [self.visit(e) for e in node.value]
        self.stack.pop()
        return expr

    def visit_ListPattern(self, node):
        expr = ListExpr(self.current_parent)
        self.stack.push(expr)
        expr.subexprs = [self.visit(e) for e in node.value]
        self.stack.pop()
        return expr

class PatternFinder(NodeVisitor):
    def __init__(self):
        self.found = False

    # It's a pattern if it has bound variables:
    def visit_Name(self, node):
        if node.id.startswith("_"):
            self.found = True

    # It's also a pattern if it contains constants:
    def visit_Constant(self, node):
        self.found = True

    visit_Num = visit_Constant
    visit_Str = visit_Constant
    visit_Bytes = visit_Constant
    visit_NameConstant = visit_Constant


class Parser(NodeVisitor):
    """The main parser class.
    """

    def __init__(self, filename="", options=None, execution_context=None):
        # used in error messages:
        self.filename = filename
        # used to construct statement tree, also used for symbol table:
        self.state_stack = []
        # new statements are appended to this list:
        self.current_block = None
        self.current_context = None
        self.current_label = None
        self.current_query_scope = None
        self.current_query = None
        self.errcnt = 0
        self.warncnt = 0
        self.program = execution_context if execution_context is not None \
                       else dast.Program() # Just in case

        self.full_event_pattern = (options.full_event_pattern
                                   if hasattr(options,
                                              'full_event_pattern')
                                   else False)
        self.use_object_style = (options.enable_object_pattern
                                 if hasattr(options,
                                            'enable_object_pattern')
                                 else False)
        self.enable_membertest_pattern = (options.enable_membertest_pattern
                                 if hasattr(options,
                                            'enable_membertest_pattern')
                                 else False)
        self.enable_iterator_pattern = (options.enable_iterator_pattern
                                 if hasattr(options,
                                            'enable_iterator_pattern')
                                 else False)


    def push_state(self, node):
        self.state_stack.append((node,
                                 self.current_context,
                                 self.current_label,
                                 self.current_query_scope,
                                 self.current_block))

    def pop_state(self):
        (_,
         self.current_context,
         self.current_label,
         self.current_query_scope,
         self.current_block) = self.state_stack.pop()

    def is_in_setup(self):
        if self.current_process is None:
            return False
        elif isinstance(self.current_scope, dast.Function):
            return self.current_scope.name == "setup"

    def enter_query(self):
        if self.current_query_scope is None:
            self.current_query_scope = dast.NameScope(self.current_scope)
            self.current_query = self.current_parent

    def leave_query(self, node=None):
        if self.current_parent is self.current_query:
            self.current_query = None
            self.current_scope.parent_scope.merge_scope(self.current_query_scope)
            if node is not None:
                self.audit_query(self.current_parent, node)

    @property
    def current_parent(self):
        return self.state_stack[-1][0]

    @property
    def current_process(self):
        for node, _, _, _, _ in reversed(self.state_stack):
            if isinstance(node, dast.Process):
                return node
        return None

    @property
    def current_scope(self):
        if self.current_query_scope is not None:
            return self.current_query_scope
        for node, _, _, _, _ in reversed(self.state_stack):
            if isinstance(node, dast.NameScope):
                return node
        return None

    @property
    def current_loop(self):
        for node, _, _, _, _ in reversed(self.state_stack):
            if isinstance(node, dast.ArgumentsContainer) or \
               isinstance(node, dast.ClassStmt):
                break
            elif isinstance(node, dast.LoopStmt):
                return node
        return None

    def visit_Module(self, node):
        self.program = dast.Program(None, node)
        # Populate global scope with Python builtins:
        for name in PythonBuiltins:
            self.program.add_name(name)
        self.push_state(self.program)
        self.current_block = self.program.body
        self.current_context = Read()
        self.body(node.body)
        self.pop_state()

    def visit_Interactive(self, node):
        self.program = dast.InteractiveProgram(None, node)
        # Populate global scope with Python builtins:
        for name in PythonBuiltins:
            self.program.add_name(name)
        self.push_state(self.program)
        contxtproc = dast.Process()
        self.push_state(contxtproc)


    # Helpers:

    def parse_bases(self, node):
        """Scans a ClassDef's bases list and checks whether the class defined by
           'node' is a DistProcess.

        A DistProcess is a class whose bases contain the name $KW_PROCESS_DEF.
        """
        isproc = False
        bases = []
        for b in node.bases:
            if (isinstance(b, Name) and b.id == KW_PROCESS_DEF):
                isproc = True
            else:
                bases.append(self.visit(b))
        return isproc, bases

    def parse_pattern_expr(self, node, literal=False):
        expr = self.create_expr(dast.PatternExpr, node)
        pp = PatternParser(self, literal)
        pattern = pp.visit(node)
        if pattern is None:
            self.error("invalid pattern", node)
            self.pop_state()
            return None
        expr.pattern = pattern
        self.pop_state()
        return expr

    def parse_decorators(self, node):
        assert hasattr(node, 'decorator_list')
        labels = set()
        notlabels = set()
        decorators = []
        for exp in node.decorator_list:
            if isinstance(exp, Call) and exp.func.id == KW_DECORATOR_LABEL:
                for arg in exp.args:
                    l, negated = self.parse_label_spec(arg)
                    if negated:
                        notlabels |= l
                    else:
                        labels |= l
            else:
                decorators.append(self.visit(exp))
        return decorators, labels, notlabels

    def parse_label_spec(self, expr):
        negated = False
        if (type(expr) is UnaryOp and
                type(expr.operand) in {Set, Tuple, List}):
            names = expr.operand.elts
            negated = True
        elif type(expr) in {Set, Tuple, List}:
            names = expr.elts
        else:
            self.error("invalid label spec.", expr)
            names = []

        result = set()
        for elt in names:
            if type(elt) is not Name:
                self.error("invalid label spec.", elt)
            else:
                result.add(elt.id)
        return result, negated

    def parse_event_handler(self, node):
        if node.name == KW_RECV_EVENT:
            eventtype = dast.ReceivedEvent
        elif node.name == KW_SENT_EVENT:
            eventtype = dast.SentEvent
        else:
            # Impossible
            return None

        extras = []
        args = node.args
        if len(args.defaults) < len(args.args):
            extras.append(args.args[:(len(args.defaults) - len(args.args))])
            args.args = args.args[(len(args.defaults) - len(args.args)):]
        if args.vararg:
            extras.append(args.vararg)
        if args.kwonlyargs:
            extras.append(args.kwonlyargs)
        if args.kwarg:
            extras.append(args.kwarg)
        if len(extras) > 0:
            for node in extras:
                self.warn("extraneous arguments in event spec ignored.", node)

        events = []
        labels = set()
        notlabels = set()
        self.enter_query()
        for key, patexpr in zip(args.args, args.defaults):
            if key.arg == KW_EVENT_LABEL:
                ls, neg = self.parse_label_spec(patexpr)
                if neg:
                    notlabels |= ls
                else:
                    labels |= ls
                continue
            pat = self.parse_pattern_expr(patexpr)
            if key.arg == KW_MSG_PATTERN:
                events.append(dast.Event(self.current_process, ast=node,
                                         event_type=eventtype, pattern=pat))
                continue
            if len(events) == 0:
                self.error("invalid event spec: missing 'msg' argument.", node)
                # Add a phony event so we can recover as much as possible:
                events.append(dast.Event(self.current_process))
            if key.arg == KW_EVENT_SOURCE:
                events[-1].sources.append(pat)
            elif key.arg == KW_EVENT_DESTINATION:
                events[-1].destinations.append(pat)
            elif key.arg == KW_EVENT_TIMESTAMP:
                events[-1].timestamps.append(pat)
            else:
                self.warn("unrecognized event parameter '%s'" % key.arg, node)
        self.leave_query()
        return events, labels, notlabels

    def body(self, statements):
        """Process a block of statements.
        """
        for stmt in statements:
            self.current_context = Read()
            self.visit(stmt)
        if self.current_label is not None:
            # Create a noop statement to hold the last label:
            self.create_stmt(dast.NoopStmt, statements[-1])

    def proc_body(self, statements):
        """Process the body of a process definition.

        Process bodies differs from normal ClassDef bodies in that the names
        defined in this scope are visible to the whole process.
        """
        for stmt in statements:
            if (isinstance(stmt, FunctionDef) and stmt.name not in
                    {KW_RECV_EVENT, KW_SENT_EVENT}):
                self.debug("Adding function %s to process scope." % stmt.name,
                           stmt)
                self.current_scope.add_name(stmt.name)
            elif isinstance(stmt, ClassDef):
                self.debug("Adding class %s to process scope." % stmt.name,
                           stmt)
                self.current_scope.add_name(stmt.name)
            elif isinstance(stmt, Assign):
                for expr in stmt.targets:
                    if isinstance(expr, Name):
                        self.debug(
                            "Adding variable %s to process scope." % expr.id,
                            stmt)
                        self.current_scope.add_name(expr.id)
            elif isinstance(stmt, AugAssign):
                if isinstance(target, Name):
                    self.current_scope.add_name(target.id)
        for stmt in statements:
            self.visit(stmt)
        if self.current_label is not None:
            # Create a noop statement to hold the last label:
            self.create_stmt(dast.NoopStmt, statements[-1])

    def signature(self, node):
        """Process the argument lists."""
        assert isinstance(self.current_parent, dast.ArgumentsContainer)
        padding = len(node.args) - len(node.defaults)
        container = self.current_parent.args
        for arg in node.args[:padding]:
            container.add_arg(arg.arg)
        for arg, val in zip(node.args[padding:], node.defaults):
            container.add_defaultarg(arg.arg, self.visit(val))
        if node.vararg is not None:
            # Python 3.4 compatibility:
            if type(node.vararg) is str:
                container.add_vararg(node.vararg)
            else:
                container.add_vararg(node.vararg.arg)
        if node.kwarg is not None:
            # Python 3.4 compatibility:
            if type(node.kwarg) is str:
                container.add_kwarg(node.kwarg)
            else:
                container.add_vararg(node.kwarg.arg)
        for kwarg, val in zip(node.kwonlyargs, node.kw_defaults):
            container.add_kwonlyarg(kwarg.arg, self.visit(val))


    # Top-level blocks:

    def visit_ClassDef(self, node):
        isproc, bases = self.parse_bases(node)
        if isproc:
            if type(self.current_parent) is not dast.Program:
                self.error("Process definition must be at top level.", node)

            initfun = None
            bodyidx = None
            for idx, s in enumerate(node.body):
                if is_setup_func(s):
                    if initfun is None:
                        initfun = s
                        bodyidx = idx
                    else:
                        self.error("Duplicate setup() definition.", s)
            if initfun is None:
                self.error("Process missing 'setup()' definition.", node)
                return

            n = self.current_scope.add_name(node.name)
            proc = dast.Process(self.current_parent, node,
                                name=node.name, bases=bases)
            n.add_assignment(proc)
            proc.decorators, _, _ = self.parse_decorators(node)
            self.push_state(proc)
            self.program.processes.append(proc)
            self.program.body.append(proc)

            self.signature(initfun.args)
            self.current_block = proc.body
            # setup() has to be parsed first:
            self.proc_body([node.body[bodyidx]] +
                           node.body[:bodyidx] + node.body[(bodyidx+1):])
            proc.setup = proc.body[0]
            self.pop_state()

        else:
            clsobj = dast.ClassStmt(self.current_parent, node,
                                    name=node.name, bases=bases)
            if self.current_block is None or self.current_parent is None:
                self.error("Statement not allowed in this context.", ast)
            else:
                self.current_block.append(clsobj)
                n = self.current_scope.add_name(node.name)
                n.add_assignment(clsobj)
            self.current_context = Read()
            clsobj.decorators, _, _ = self.parse_decorators(node)
            self.push_state(clsobj)
            self.current_block = clsobj.body
            self.body(node.body)
            self.pop_state()

    def visit_FunctionDef(self, node):
        if (self.current_process is None or
                node.name not in {KW_SENT_EVENT, KW_RECV_EVENT}):
            # This is a normal method
            n = self.current_scope.add_name(node.name)
            s = self.create_stmt(dast.Function, node,
                                 params={"name" : node.name})
            n.add_assignment(s)
            s.process = self.current_process
            if type(s.parent) is dast.Process:
                if s.name == "main":
                    self.current_process.entry_point = s
                else:
                    self.current_process.methods.append(s)
            elif (type(s.parent) is dast.Program and
                  s.name == "main"):
                self.current_parent.entry_point = s
            # Ignore the label decorators:
            s.decorators, _, _ = self.parse_decorators(node)
            self.current_block = s.body
            self.signature(node.args)
            self.body(node.body)
            self.pop_state()

        else:
            # This is an event handler:
            h = dast.EventHandler(self.current_parent, node)
            # Parse decorators before adding h to node_stack, since decorators
            # should belong to the outer scope:
            h.decorators, h.labels, h.notlabels = self.parse_decorators(node)
            self.push_state(h)
            events, labels, notlabels = self.parse_event_handler(node)
            events = self.current_process.add_events(events)
            h.events = events
            h.labels |= labels
            h.notlabels |= notlabels
            if len(h.labels) == 0:
                h.labels = None
            if len(h.notlabels) == 0:
                h.notlabels = None
            for evt in events:
                evt.handlers.append(h)
            for v in evt.freevars:
                if v is not None:
                    self.debug("adding event argument %s" % v)
                    h.args.add_arg(v.name)
            self.current_block = h.body
            self.body(node.body)
            self.pop_state()

    def check_await(self, node):
        if (isinstance(node, Call) and
            isinstance(node.func, Name) and
            node.func.id == KW_AWAIT):
            if len(node.args) <= 2:
                return True
            else:
                self.error("malformed await statement.", node)
                return None
        else:
            return False


    # Statements:
    #
    # The visit_* method for statements appends generated dast AST statements
    # to self.current_block.

    def create_stmt(self, stmtcls, ast, params=None, nopush=False):
        """Convenience method to instantiate a statement node and append to
        'current_block'.

        """
        if params is None:
            stmtobj = stmtcls(parent=self.current_parent, ast=ast)
        else:
            stmtobj = stmtcls(parent=self.current_parent, ast=ast, **params)
        stmtobj.label = self.current_label
        self.current_label = None

        if self.current_block is None or self.current_parent is None:
            self.error("Statement not allowed in this context.", ast)
        else:
            self.current_block.append(stmtobj)
        if not nopush:
            self.push_state(stmtobj)
        self.current_context = Read()
        return stmtobj

    def create_expr(self, exprcls, ast, params=None, nopush=False):
        """Convenience method to instantiate an expression node.
        """
        if params is None:
            expr = exprcls(self.current_parent, ast=ast)
        else:
            expr = exprcls(self.current_parent, ast=ast, **params)
        if not nopush:
            self.push_state(expr)
        return expr


    def visit_Assign(self, node):
        stmtobj = self.create_stmt(dast.AssignmentStmt, node)
        self.current_context = Read()
        stmtobj.value = self.visit(node.value)
        self.current_context = Assignment(stmtobj.value)
        for target in node.targets:
            stmtobj.targets.append(self.visit(target))
        self.pop_state()

    def visit_AugAssign(self, node):
        stmtobj = self.create_stmt(dast.OpAssignmentStmt, node,
                                   params={'op':OperatorMap[type(node.op)]})
        self.current_context = Read()
        valexpr = self.visit(node.value)
        self.current_context = Assignment(valexpr)
        tgtexpr = self.visit(node.target)
        stmtobj.target = tgtexpr
        stmtobj.value = valexpr
        self.pop_state()

    def visit_ImportFrom(self, node):
        if type(self.current_parent) is not dast.Program:
            self.error("'import' statement is only allowed at the top level.",
                       node)
            return
        stmtobj = self.create_stmt(dast.PythonStmt, node)
        for alias in node.names:
            if alias.asname is not None:
                name = alias.asname
            else:
                name = alias.name
            nobj = self.current_scope.add_name(name)
            nobj.add_assignment(stmtobj)
        self.pop_state()

    visit_Import = visit_ImportFrom

    def expr_check(self, name, minargs, maxargs, node,
                   keywords={}, optional_keywords={}):
        if not (isinstance(node, Call) and
                isinstance(node.func, Name) and
                node.func.id == name):
            return False
        errmsg = None
        if len(node.args) >= minargs and len(node.args) <= maxargs:
            if keywords is None:
                return True
            for kw in node.keywords:
                if kw.arg in keywords:
                    keywords -= {kw.arg}
                elif kw.arg not in optional_keywords:
                    errmsg = "unrecognized keyword in %s statement." % name
                    break
            if errmsg is None:
                if len(keywords) > 0:
                    errmsg = ("missing required keywords: " + keywords +
                              " in " + name + " statement.")
                else:
                    return True
        else:
            errmsg = "Malformed %s statement." % name

        self.error(errmsg, node)
        raise MalformedStatementError

    def kw_check(self, node, names):
        if not isinstance(node, Name):
            return False
        if node.id not in names:
            return False
        return True

    def parse_message(self, node):
        expr = dast.TupleExpr(self.current_parent, node)
        if type(node) is Call:
            assert type(node.func) is Name
            elem = dast.ConstantExpr(self.current_parent, node.func)
            elem.value = node.func.id
            expr.subexprs.append(elem)
            elts = node.args
        else:
            elts = node.elts
        for elt in elts:
            expr.subexprs.append(self.visit(elt))
        return expr

    def visit_Expr(self, node):
        l = extract_label(node.value)
        if l is not None and self.current_process is not None:
            self.current_label = l
            return
        stmtobj = None
        try:
            e = node.value
            if self.expr_check(KW_AWAIT, 1, 2, e,
                               keywords={},
                               optional_keywords={KW_AWAIT_TIMEOUT}):
                stmtobj = self.create_stmt(dast.AwaitStmt, node)
                branch = dast.Branch(stmtobj, node,
                                     condition=self.visit(e.args[0]))
                stmtobj.branches.append(branch)
                if len(e.args) == 2:
                    stmtobj.timeout = self.visit(e.args[1])
                    if len(e.keywords) > 0:
                        if stmtobj.timeout is not None:
                            self.warn(
                                "duplicate timeout value in await statement.",
                                e)
                            stmtobj.timeout = self.visit(kw.value)

            elif self.expr_check(KW_SEND, 1, 1, e, keywords={KW_SEND_TO}):
                stmtobj = self.create_stmt(dast.SendStmt, node)
                stmtobj.message = self.parse_message(e.args[0])
                stmtobj.target = self.visit(e.keywords[0].value)

            elif self.expr_check(KW_BROADCAST, 1, 1, e, keywords={KW_SEND_TO}):
                stmtobj = self.create_stmt(dast.SendStmt, node)
                stmtobj.message = self.parse_message(e.args[0])
                stmtobj.target = self.visit(e.keywords[0].value)

            elif self.expr_check(KW_PRINT, 1, 2, e):
                stmtobj = self.create_stmt(dast.OutputStmt, node)
                stmtobj.message = self.visit(e.args[0])
                if len(e.args) == 2:
                    stmtobj.level = self.visit(e.args[1])

            elif self.current_process is not None and \
                 self.expr_check(KW_RESET, 0, 1, e):
                stmtobj = self.create_stmt(dast.ResetStmt, node)
                if len(e.args) > 0:
                    stmtobj.expr = self.visit(e.args[0])
                    if not isinstance(stmtobj.expr, dast.ConstantExpr):
                        self.error("Invalid argument in reset statement.", e)
                    elif stmtobj.expr.value not in ValidResetTypes:
                        self.error("Unknown argument in reset statement. "
                                   "Valid arguments are: " +
                                   str(ValidResetTypes), node)

            elif (isinstance(self.current_parent, dast.Process) and
                  self.expr_check(KW_CONFIG, 0, 0, e, keywords=None)):
                self.current_process.configurations.extend(
                    self.parse_config_section(e))

            # 'yield' and 'yield from' should be statements, handle them here:
            elif type(e) is Yield:
                stmtobj = self.create_stmt(dast.YieldStmt, node)
                stmtobj.expr = self.visit(e)
            elif type(e) is YieldFrom:
                # 'yield' should be a statement, handle it here:
                stmtobj = self.create_stmt(dast.YieldFromStmt, node)
                stmtobj.expr = self.visit(e)

            else:
                stmtobj = self.create_stmt(dast.SimpleStmt, node)
                stmtobj.expr = self.visit(node.value)

        except MalformedStatementError:
            # already errored in expr_check so just ignore:
            pass
        finally:
            if stmtobj is not None:
                self.pop_state()

    # ~~~

    def visit_If(self, node):
        stmtobj = None
        try:
            if self.expr_check(KW_AWAIT, 1, 1, node.test):
                stmtobj = self.create_stmt(dast.AwaitStmt, node)
                branch = dast.Branch(stmtobj, node.test,
                                     condition=self.visit(node.test.args[0]))
                self.current_block = branch.body
                self.body(node.body)
                stmtobj.branches.append(branch)
                while True:
                    else_ = node.orelse
                    if len(else_) == 1 and isinstance(else_[0], If):
                        node = else_[0]
                        if self.expr_check(KW_AWAIT_TIMEOUT, 1 ,1, node.test):
                            stmtobj.timeout = self.visit(node.test.args[0])
                            self.current_block = stmtobj.orelse
                            self.body(node.body)
                            if len(node.orelse) > 0:
                                self.error("timeout branch must be the last"
                                           " branch of await statement", node)
                        else:
                            branch = dast.Branch(stmtobj, node,
                                                 condition=self.visit(node.test))
                            self.current_block = branch.body
                            self.body(node.body)
                            stmtobj.branches.append(branch)
                    elif len(else_) == 0:
                        break
                    else:
                        self.current_block = stmtobj.orelse
                        self.body(else_)
                        break

            else:
                stmtobj = self.create_stmt(dast.IfStmt, node)
                stmtobj.condition = self.visit(node.test)
                self.current_block = stmtobj.body
                self.body(node.body)
                self.current_block = stmtobj.elsebody
                self.body(node.orelse)

        except MalformedStatementError:
            pass
        finally:
            if stmtobj is not None:
                self.pop_state()

    def visit_For(self, node):
        s = self.create_stmt(dast.ForStmt, node)
        self.current_context = Assignment()
        s.domain = self.parse_domain_spec(node)
        self.current_context = Read()
        self.current_block = s.body
        self.body(node.body)
        self.current_block = s.elsebody
        self.body(node.orelse)
        self.pop_state()

    def visit_While(self, node):
        if self.expr_check(KW_AWAIT, 1, 2, node.test,
                           optional_keywords={KW_AWAIT_TIMEOUT}):
            s = self.create_stmt(dast.LoopingAwaitStmt, node)
            s.condition = self.visit(node.test.args[0])
            if len(node.test.args) == 2:
                s.timeout = self.visit(node.test.args[1])

        else:
            s = self.create_stmt(dast.WhileStmt, node)
            s.condition = self.visit(node.test)
        self.current_block = s.body
        self.body(node.body)
        if hasattr(s, "elsebody"):
            self.current_block = s.elsebody
            self.body(node.orelse)
        self.pop_state()

    def visit_With(self, node):
        s = self.create_stmt(dast.WithStmt, node)
        for item in node.items:
            self.current_context = Read()
            ctxexpr = self.visit(item.context_expr)
            if item.optional_vars is not None:
                self.current_context = Assignment(ctxexpr)
                s.items.append((ctxexpr, self.visit(item.optional_vars)))
            else:
                s.items.append((ctxexpr, None))
        self.current_context = Read()
        self.current_block = s.body
        self.body(node.body)
        self.pop_state()

    def visit_Pass(self, node):
        self.create_stmt(dast.PassStmt, node, nopush=True)

    def visit_Break(self, node):
        loop = self.current_loop
        if loop is None:
            self.warn("Possible use of 'break' outside loop.", node)
        self.create_stmt(dast.BreakStmt, node, nopush=True,
                         params={"loopstmt": loop})

    def visit_Continue(self, node):
        loop = self.current_loop
        if loop is None:
            self.warn("Possible use of 'continue' outside loop.", node)
        self.create_stmt(dast.ContinueStmt, node, nopush=True,
                         params={"loopstmt": loop})

    def visit_Delete(self, node):
        s = self.create_stmt(dast.DeleteStmt, node)
        self.current_context = Delete()
        for target in node.targets:
            s.targets.append(self.visit(target))
        self.pop_state()

    def visit_Try(self, node):
        s = self.create_stmt(dast.TryStmt, node)
        self.current_block = s.body
        self.body(node.body)
        self.current_context = Read()
        for handler in node.handlers:
            h = dast.ExceptHandler(s, handler)
            h.name = handler.name
            if h.name is not None:
                n = self.current_scope.find_name(h.name)
                if n is None:
                    self.current_scope.add_name(h.name)
                n.add_assignment(s)
            if handler.type is not None:
                h.type = self.visit(handler.type)
            self.current_block = h.body
            self.body(handler.body)
            s.excepthandlers.append(h)
        self.current_block = s.elsebody
        self.body(node.orelse)
        self.current_block = s.finalbody
        self.body(node.finalbody)
        self.pop_state()

    def visit_Assert(self, node):
        s = self.create_stmt(dast.AssertStmt, node)
        s.expr = self.visit(node.test)
        if node.msg is not None:
            s.msg = self.visit(node.msg)
        self.pop_state()

    def visit_Global(self, node):
        if self.current_process is not None:
            self.warn("'global' statement inside process is redundant and "
                      "ignored.", node)
        else:
            self.create_stmt(dast.GlobalStmt, node,
                             {"names": list(node.names)})
            for name in node.names:
                localname = self.current_scope.find_name(name, local=True)
                if localname is not None:
                    self.warn("name '%s' used before declared 'global'." %
                              name, node)
                nobj = self.program.find_name(name)
                if nobj is None:
                    nobj = self.program.add_name(name)
                self.debug("Linking global name '%s'" % name)
                self.current_scope.link_name(nobj)
            self.pop_state()

    def visit_Nonlocal(self, node):
        self.create_stmt(dast.NonlocalStmt, node, {"names": list(node.names)})
        if self.current_scope.parent_scope is None:
            self.error("No nonlocal scope found.", node)
        else:
            for name in node.names:
                nobj = self.current_scope.find_name(name, local=True)
                if nobj is not None:
                    self.warn("Variable '%s' used before declared 'nonlocal'." %
                              name, node)
                nobj = self.current_scope.parent_scope.find_name(name, local=False)
                if nobj is None:
                    self.warn("Unable to determine scope for nonlocal var %s" %
                              name, node)
                else:
                    self.debug("Linking nonlocal name '%s'" % name)
                    self.current_scope.link_name(nobj)
        self.pop_state()

    def visit_Return(self, node):
        s = self.create_stmt(dast.ReturnStmt, node)
        if node.value is not None:
            s.value = self.visit(node.value)
        self.pop_state()

    def visit_Raise(self, node):
        s = self.create_stmt(dast.RaiseStmt, node)
        if node.exc is not None:
            s.expr = self.visit(node.exc)
        if node.cause is not None:
            s.cause = self.visit(node.cause)
        self.pop_state()


    # Expressions:
    #
    # The visit_* methods for expressions return the newly
    # constructed dast AST node

    def visit_Attribute(self, node):
        if (isinstance(self.current_context, FunCall) and
                node.attr in KnownUpdateMethods):
            # Calling a method that is known to update an object's state is an
            # Update operation:
            self.current_context = Update()
        expr = self.create_expr(dast.AttributeExpr, node)
        if type(self.current_context) is Assignment:
            # Assigning to an attribute of an object updates that object:
            self.current_context = Update()
        expr.value = self.visit(node.value)
        expr.attr = node.attr
        self.pop_state()
        if isinstance(expr.value, dast.SelfExpr):
            # Need to update the namedvar object
            n = self.current_process.find_name(expr.attr)
            if n is None:
                if (self.is_in_setup() and
                        isinstance(self.current_context, Assignment)):
                    self.debug("Adding name '%s' to process scope"
                               " from setup()." % expr.attr, node)
                    n = self.current_process.add_name(expr.attr)
                    n.add_assignment(expr)
                    n.set_scope(self.current_process)
                else:
                    self.error("Undefined process state variable: " +
                               str(expr.attr), node)
            else:
                if isinstance(self.current_context, Assignment):
                    self.debug("Assignment to variable '%s'" % str(n), node)
                    n.add_assignment(expr)
                elif isinstance(self.current_context, Update) or \
                     isinstance(self.current_context, Delete):
                    self.debug("Update to process variable '%s'" % str(n), node)
                    n.add_update(expr)
                else:
                    n.add_read(expr)
        return expr

    def ensure_one_arg(self, name, node):
        l = len(node.args)
        if l != 1:
            self.error("'%s' takes exactly one argument (%d given)" % (name, l),
                       node)
            return False
        return True

    def ensure_sequence_arg(self, name, node):
        l = len(node.args)
        if l > 1:
            self.error("'%s' takes zero or one argument (%d given)" % (name, l),
                       node)
            return False
        if l == 1 and not hasattr(node.args[0], "elts"):
            return False
        return True

    def parse_event_expr(self, node, literal=False):
        if (node.starargs is not None or node.kwargs is not None):
            self.warn("extraneous arguments in event expression.", node)
        pattern = self.parse_pattern_expr(node.args[0], literal)
        if node.func.id == KW_RECV_QUERY:
            event = dast.Event(self.current_process,
                               event_type=dast.ReceivedEvent,
                               pattern=pattern)
        elif node.func.id == KW_SENT_QUERY:
            event = dast.Event(self.current_process,
                               event_type=dast.SentEvent,
                               pattern=pattern)
        else:
            self.error("unknown event specifier", node)
            return None
        for kw in node.keywords:
            pat = self.parse_pattern_expr(kw.value, literal)
            if kw.arg == KW_EVENT_SOURCE:
                event.sources.append(pat)
            elif kw.arg == KW_EVENT_DESTINATION:
                event.destinations.append(pat)
            elif kw.arg == KW_EVENT_TIMESTAMP:
                event.timestamps.append(pat)
            else:
                self.warn("unknown keyword in query.", node)
        return self.current_process.add_event(event)

    def event_from_pattern(self, node, event_type):
        assert isinstance(node, dast.PatternExpr)
        pattern = node.pattern
        assert isinstance(pattern, dast.TuplePattern)
        event = dast.Event(self.current_process,
                           event_type=event_type)
        if self.full_event_pattern:
            if len(pattern.value) != 3:
                self.error("malformed event pattern.", node)
            else:
                event.pattern = dast.PatternExpr(node.parent,
                                                 pattern=pattern.value[2])
                envpat = pattern.value[1]
                if isinstance(envpat, dast.TuplePattern):
                    if len(envpat.value) != 3:
                        self.warn("possible malformed envelope pattern.", node)
                    else:
                        event.timestamps.append(
                            dast.PatternExpr(node.parent,
                                             pattern=envpat.value[0]))
                        event.destinations.append(
                            dast.PatternExpr(node.parent,
                                             pattern=envpat.value[1]))
                        event.sources.append(
                            dast.PatternExpr(node.parent,
                                             pattern=envpat.value[2]))
        else:
            if len(pattern.value) != 2:
                self.error("malformed event pattern.", node)
            else:
                event.pattern = dast.PatternExpr(node.parent,
                                                 pattern=pattern.value[0])
                event.sources.append(
                    dast.PatternExpr(node.parent, pattern=pattern.value[1]))
        return self.current_process.add_event(event)

    def pattern_from_event(self, node, literal=False):
        if not isinstance(node, dast.Event):
            return None
        expr = self.create_expr(dast.PatternExpr if not literal else
                                dast.LiteralPatternExpr,
                                node.ast)
        pattern = dast.TuplePattern(node.parent)

        # Pattern structure:
        # (TYPE, ENVELOPE, MESSAGE)
        # ENVELOPE: (TIMESTAMP, DESTINATION, SOURCE)
        if isinstance(node.type, dast.EventType):
            pattern.value.append(
                dast.ConstantPattern(
                    pattern,
                    value=self.current_scope.add_name(
                        node.type.__name__)))
        else:
            pattern.value.append(dast.FreePattern(pattern))

        env = dast.TuplePattern(pattern)
        if (len(node.timestamps) == 0):
            env.value.append(dast.FreePattern(env))
        elif len(node.timestamps) == 1:
            env.value.append(node.timestamps[0].pattern.clone())
            env.value[-1]._parent = env
        else:
            self.error("multiple timestamp spec in event pattern.", node)
        if (len(node.destinations) == 0):
            env.value.append(dast.FreePattern(env))
        elif len(node.destinations) == 1:
            env.value.append(node.destinations[0].pattern.clone())
            env.value[-1]._parent = env
        else:
            self.error("multiple destination spec in event pattern.", node)
        if (len(node.sources) == 0):
            env.value.append(dast.FreePattern(env))
        elif len(node.sources) == 1:
            env.value.append(node.sources[0].pattern.clone())
            env.value[-1]._parent = env
        else:
            self.error("multiple source spec in event pattern.", node)

        pattern.value.append(env)
        if node.pattern is None:
            msgpat = dast.FreePattern(pattern)
        else:
            msgpat = node.pattern.pattern.clone()
            msgpat._parent = pattern
        pattern.value.append(msgpat)
        expr.pattern = pattern
        self.pop_state()
        return expr

    def call_check(self, names, minargs, maxargs, node):
        if (isinstance(node.func, Name) and node.func.id in names):
            if ((minargs is not None and len(node.args) < minargs) or
                    (maxargs is not None and len(node.args) > maxargs)):
                self.error("Malformed %s expression." % node.func.id, node)
                return False
            else:
                return True
        return False

    def parse_domain_spec(self, node):
        if (self.current_process is not None and
                isinstance(node, Call) and
                self.call_check({KW_RECV_QUERY, KW_SENT_QUERY}, 1, 1, node)):
            # As a short hand, "sent" and "rcvd" can be used as a domain spec:
            # some(rcvd(EVENT_PATTERN) | PRED) is semantically equivalent to
            # some(EVENT_PATTERN in rcvd | PRED).
            expr = self.create_expr(dast.DomainSpec, node)
            event = self.parse_event_expr(node, literal=False)
            if event is not None:
                event.record_history = True
                expr.pattern = self.pattern_from_event(event)
                if node.func.id == KW_RECV_QUERY:
                    expr.domain = self.create_expr(dast.ReceivedExpr, node)
                else:
                    expr.domain = self.create_expr(dast.SentExpr, node)
                expr.domain.event = event
                self.pop_state()
            self.pop_state()
            return expr
        elif (isinstance(node, Compare) and len(node.ops) == 1 and
              type(node.ops[0]) is In):
            expr = self.create_expr(dast.DomainSpec, node)
            self.current_context = Assignment()
            expr.pattern = self.parse_pattern_expr(node.left)
            self.current_context = IterRead(expr.pattern)
            expr.domain = self.visit(node.comparators[0])
            self.pop_state()
            return expr
        elif isinstance(node, comprehension) or isinstance(node, For):
            expr = self.create_expr(dast.DomainSpec, node)
            self.current_context = Assignment()
            if self.enable_iterator_pattern:
                expr.pattern = self.parse_pattern_expr(node.target)
            else:
                expr.pattern = self.visit(node.target)
            self.current_context = IterRead(expr.pattern)
            expr.domain = self.visit(node.iter)
            if isinstance(expr.domain, dast.HistoryExpr):
                expr.pattern = self.pattern_from_event(expr.domain.event)
            self.pop_state()
            return expr
        else:
            raise MalformedStatementError("malformed domain specifier.")

    def parse_quantified_expr(self, node):
        if node.func.id == KW_EXISTENTIAL_QUANT:
            context = dast.ExistentialOp
        elif node.func.id == KW_UNIVERSAL_QUANT:
            context = dast.UniversalOp
        else:
            raise MalformedStatementError("Unknown quantifier.")

        expr = self.create_expr(dast.QuantifiedExpr, node, {'op': context})
        self.enter_query()
        try:
            expr.domains, predicates = self.parse_domains_and_predicate(node)
            if len(predicates) > 1:
                self.warn("Multiple predicates in quantified expression, "
                          "first one is used, the rest are ignored.", node)
            expr.predicate = predicates[0]
        finally:
            self.leave_query(node)
            self.pop_state()
        return expr

    def parse_comprehension(self, node):
        if node.func.id == KW_COMP_SET:
            expr_type = dast.SetCompExpr
        elif node.func.id == KW_COMP_LIST:
            expr_type = dast.ListCompExpr
        elif node.func.id == KW_COMP_DICT:
            expr_type = dast.DictCompExpr
        elif node.func.id == KW_COMP_TUPLE:
            expr_type = dast.TupleCompExpr

        expr = self.create_expr(expr_type, node)
        self.enter_query()

        first_arg = node.args[0]
        node.args = node.args[1:]
        try:
            expr.domains, expr.conditions = self.parse_domains_and_predicate(node)
            if expr_type is dast.DictCompExpr:
                if not (isinstance(first_arg, Tuple) and
                        len(first_arg.elts) == 2):
                    self.error("Malformed element in dict comprehension.",
                               first_arg)
                else:
                    kv = dast.KeyValue(expr)
                    kv.key = self.visit(node.key)
                    kv.value = self.visit(node.value)
                    expr.elem = kv
            else:
                expr.elem = self.visit(first_arg)
        finally:
            self.leave_query(node)
            self.pop_state()
        return expr

    def audit_query(self, expr, node):
        self.debug("auditing " + str(expr), node)
        self.debug("...freevars: " + str(expr.freevars), node)
        self.debug("...boundvars: " + str(expr.boundvars), node)
        intersect = {v.name for v in expr.ordered_freevars} & \
                    {v.name for v in expr.ordered_boundvars}
        if intersect:
            msg = ("query variables " +
                   " ".join(["'" + n + "'" for n in intersect]) +
                   " are both free and bound.")
            self.error(msg, node)

    def parse_aggregates(self, node):
        if node.func.id == KW_AGGREGATE_SUM:
            expr_type = dast.SumExpr
        elif node.func.id == KW_AGGREGATE_SIZE:
            expr_type = dast.SizeExpr
        elif node.func.id == KW_AGGREGATE_MIN:
            expr_type = dast.MinExpr
        elif node.func.id == KW_AGGREGATE_MAX:
            expr_type = dast.MaxExpr

        expr = self.create_expr(expr_type, node)
        first_arg = node.args[0]
        node.args = node.args[1:]
        try:
            expr.domains, expr.conditions = self.parse_domains_and_predicate(node)
            expr.elem = self.visit(first_arg)
        finally:
            self.pop_state()
        return expr

    def parse_domains_and_predicate(self, node):
        preds = []
        # Find predicate:
        for kw in node.keywords:
            if kw.arg == KW_SUCH_THAT:
                preds.append(kw.value)
            else:
                self.error("Unknown keyword '%s' in comprehension expression." %
                           kw.arg, node)
        # ..if no predicate found, then default to True:
        if len(preds) == 0:
            preds= [NameConstant(True)]
        domains = node.args
        if len(domains) == 0:
            self.warn("No domain specifiers in comprehension expression.", node)
        dadomains = [self.parse_domain_spec(node) for node in domains]
        self.current_context = Read()
        dapredicates = [self.visit(pred) for pred in preds]
        return dadomains, dapredicates

    def parse_config_section(self, node):
        res = []
        for kw in node.keywords:
            key = kw.arg
            vnode = kw.value
            value = None
            if isinstance(vnode, Name):
                value = vnode.id
            elif isinstance(vnode, Num):
                value = vnode.n
            elif isinstance(vnode, Str) or isinstance(vnode, Bytes):
                value = vnode.s
            elif isinstance(vnode, NameConstant):
                value = vnode.value
            else:
                self.error("Invalid configuration value.", vnode)
            if value is not None:
                res.append((key, value))
        return res

    def visit_Call(self, node):
        if self.call_check(Quantifiers, 1, None, node):
            try:
                return self.parse_quantified_expr(node)
            except MalformedStatementError as e:
                self.error("Malformed quantification expression: " + str(e),
                           node)
                return dast.SimpleExpr(self.current_parent, node)

        if self.call_check(ComprehensionTypes, 2, None, node):
            try:
                return self.parse_comprehension(node)
            except MalformedStatementError as e:
                self.error("Malformed comprehension expression: " + str(e),
                           node)
                return dast.SimpleExpr(self.current_parent, node)

        if (self.current_process is not None and
                self.call_check({KW_RECV_QUERY, KW_SENT_QUERY}, 1, 1, node)):
            if isinstance(self.current_context, IterRead):
                if node.func.id == KW_RECV_QUERY:
                    expr = self.create_expr(dast.ReceivedExpr, node)
                else:
                    expr = self.create_expr(dast.SentExpr, node)
                expr.context = self.current_context.type
                expr.event = self.parse_event_expr(
                    node, literal=(not self.enable_iterator_pattern))
                self.pop_state()
                if expr.event is not None:
                    expr.event.record_history = True
                return expr
            else:
                outer = self.create_expr(dast.ComparisonExpr, node)
                outer.comparator = dast.InOp
                if node.func.id == KW_RECV_QUERY:
                    expr = self.create_expr(dast.ReceivedExpr, node)
                else:
                    expr = self.create_expr(dast.SentExpr, node)
                if self.current_context is not None:
                    expr.context = self.current_context.type
                event = self.parse_event_expr(
                    node, literal=(not self.enable_membertest_pattern))
                self.pop_state()
                expr.event = event
                outer.right = expr
                if event is not None:
                    outer.left = self.pattern_from_event(
                        event, literal=(not self.enable_membertest_pattern))
                    event.record_history = True
                self.pop_state()
                return outer

        if self.call_check(ApiMethods, None, None, node):
            self.debug("Api method call: " + node.func.id, node)
            expr = self.create_expr(dast.ApiCallExpr, node)
            expr.func = node.func.id
        elif self.call_check(BuiltinMethods, None, None, node):
            self.debug("Builtin method call: " + node.func.id, node)
            expr = self.create_expr(dast.BuiltinCallExpr, node)
            expr.func = node.func.id
        else:
            if isinstance(node.func, Name):
                self.debug("Method call: " + str(node.func.id), node)
            expr = self.create_expr(dast.CallExpr, node)
            self.current_context = FunCall()
            expr.func = self.visit(node.func)

        self.current_context = Read()
        expr.args = [self.visit(a) for a in node.args]
        expr.keywords = [(kw.arg, self.visit(kw.value))
                         for kw in node.keywords]
        expr.starargs = self.visit(node.starargs) \
                        if node.starargs is not None else None
        expr.kwargs = self.visit(node.kwargs) \
                      if node.kwargs is not None else None
        self.pop_state()
        return expr

    def visit_Name(self, node):
        if node.id in {KW_TRUE, KW_FALSE, KW_NULL}:
            if type(self.current_context) in {Assignment, Update, Delete}:
                self.warn("Constant expression in update context.", node)

            if node.id == KW_TRUE:
                return self.create_expr(dast.TrueExpr, node, nopush=True)
            elif node.id == KW_FALSE:
                return self.create_expr(dast.FalseExpr, node, nopush=True)
            elif node.id == KW_NULL:
                return self.create_expr(dast.NoneExpr, node, nopush=True)
        if self.current_process is not None and node.id == KW_SELF:
            return self.create_expr(dast.SelfExpr, node, nopush=True)

        if (self.current_process is not None and
                (node.id in {KW_RECV_QUERY, KW_SENT_QUERY})):
            if node.id == KW_RECV_QUERY:
                expr = self.create_expr(dast.ReceivedExpr, node)
                event_type = dast.ReceivedEvent
            else:
                expr = self.create_expr(dast.SentExpr, node)
                event_type = dast.SentEvent

            if (isinstance(self.current_context, Read) and
                    isinstance(self.current_context.type, dast.PatternExpr)):
                expr.context = self.current_context.type
                event = self.event_from_pattern(expr.context, event_type)
                expr.event = event
                event.record_history = True
            else:
                self.error("Invalid context for '%s'" % node.id, node)
            self.pop_state()
            return expr

        # NamedVar is not by itself an Expression, we'll have to wrap it in a
        # SimpleExpr:
        expr = self.create_expr(dast.SimpleExpr, node)
        if isinstance(self.current_context, Assignment):
            n = self.current_scope.find_name(node.id, local=False)
            if n is None:
                self.debug("Adding name %s to %s" % (node.id,
                                                     self.current_scope), node)
                n = self.current_scope.add_name(node.id)
            n.add_assignment(expr)
        elif isinstance(self.current_context, Update) or\
             isinstance(self.current_context, Delete):
            n = self.current_scope.find_name(node.id, local=False)
            if n is None:
                self.warn("Possible use of uninitialized variable '%s'" %
                          node.id, node)
                self.debug(str(self.current_scope.parent_scope), node)
                n = self.current_scope.add_name(node.id)
            n.add_update(expr)
        elif isinstance(self.current_context, Read) or \
             isinstance(self.current_context, FunCall):
            n = self.current_scope.find_name(node.id, local=False)
            if n is None:
                self.warn("Possible use of uninitialized variable '%s'" %
                          node.id, node)
                self.debug(str(self.current_scope.parent_scope), node)
                if self.current_scope.parent_scope is not None:
                    self.debug(self.current_scope.parent_scope._names, node)
                else:
                    self.debug(self.current_scope._names, node)
                n = self.current_scope.add_name(node.id)
            n.add_read(expr)
        expr.value = n
        self.pop_state()
        return expr

    def visit_Str(self, node):
        expr = self.create_expr(dast.ConstantExpr, node)
        expr.value = node.s
        self.pop_state()
        return expr

    def visit_Bytes(self, node):
        expr = self.create_expr(dast.ConstantExpr, node)
        expr.value = node.s
        self.pop_state()
        return expr

    def visit_Num(self, node):
        expr = self.create_expr(dast.ConstantExpr, node)
        expr.value = node.n
        self.pop_state()
        return expr

    # Since Python 3.4:
    def visit_NameConstant(self, node):
        if node.value == True:
            return self.create_expr(dast.TrueExpr, node, nopush=True)
        elif node.value == False:
            return self.create_expr(dast.FalseExpr, node, nopush=True)
        elif node.value == None:
            return self.create_expr(dast.NoneExpr, node, nopush=True)
        else:
            raise NotImplementedError("Unrecognized NameConstant %s." % repr(node.value))

    def visit_Tuple(self, node):
        expr = self.create_expr(dast.TupleExpr, node)
        for item in node.elts:
            expr.subexprs.append(self.visit(item))
        self.pop_state()
        return expr

    def visit_List(self, node):
        expr = self.create_expr(dast.ListExpr, node)
        for item in node.elts:
            expr.subexprs.append(self.visit(item))
        self.pop_state()
        return expr

    def visit_Set(self, node):
        expr = self.create_expr(dast.SetExpr, node)
        for item in node.elts:
            expr.subexprs.append(self.visit(item))
        self.pop_state()
        return expr

    def visit_Dict(self, node):
        expr = self.create_expr(dast.DictExpr, node)
        for key in node.keys:
            expr.keys.append(self.visit(key))
        for value in node.values:
            expr.values.append(self.visit(value))
        self.pop_state()
        return expr

    def visit_BinOp(self, node):
        e = self.create_expr(dast.BinaryExpr, node,
                             {"op": OperatorMap[type(node.op)]})
        e.left = self.visit(node.left)
        e.right = self.visit(node.right)
        self.pop_state()
        return e

    def visit_BoolOp(self, node):
        e = self.create_expr(dast.LogicalExpr, node,
                             {"op": OperatorMap[type(node.op)]})
        for v in node.values:
            e.subexprs.append(self.visit(v))
        self.pop_state()
        return e

    def visit_Compare(self, node):
        if len(node.ops) > 1:
            self.error("Explicit parenthesis required in comparison expression",
                       node)
            return None
        outer = None
        # We make all negation explicit:
        if type(node.ops[0]) in NegatedOperators:
            outer = self.create_expr(dast.LogicalExpr, node)
            outer.operator = dast.NotOp

        expr = self.create_expr(dast.ComparisonExpr, node)

        if self.enable_membertest_pattern:
            # DistAlgo: overload "in" to allow pattern matching
            if isinstance(node.ops[0], In) or \
                   isinstance(node.ops[0], NotIn):
                # Backward compatibility: only assume pattern if containing free
                # var
                pf = PatternFinder()
                pf.visit(node.left)
                if pf.found:
                    expr.left = self.parse_pattern_expr(node.left)
        if expr.left is None:
            expr.left = self.visit(node.left)
        self.current_context = Read(expr.left)
        expr.right = self.visit(node.comparators[0])
        if (isinstance(expr.right, dast.HistoryExpr) and
                expr.right.event is not None):
            # Must replace short pattern format with full pattern here:
            expr.left = self.pattern_from_event(expr.right.event)

        if outer is not None:
            expr.comparator = NegatedOperators[type(node.ops[0])]
            outer.subexprs.append(expr)
            self.pop_state()
            self.pop_state()
            return outer
        else:
            expr.comparator = OperatorMap[type(node.ops[0])]
            self.pop_state()
            return expr

    def visit_UnaryOp(self, node):
        if type(node.op) is Not:
            expr = self.create_expr(dast.LogicalExpr, node, {"op": dast.NotOp})
            expr.subexprs.append(self.visit(node.operand))
        else:
            expr = self.create_expr(dast.UnaryExpr, node,
                                    {"op": OperatorMap[type(node.op)]})
            expr.right = self.visit(node.operand)
        self.pop_state()
        return expr

    def visit_Subscript(self, node):
        expr = self.create_expr(dast.SubscriptExpr, node)
        expr.value = self.visit(node.value)
        self.current_context = Read()
        expr.index = self.visit(node.slice)
        self.pop_state()
        return expr

    def visit_Index(self, node):
        return self.visit(node.value)

    def visit_Slice(self, node):
        expr = self.create_expr(dast.SliceExpr, node)
        if node.lower is not None:
            expr.lower = self.visit(node.lower)
        if node.upper is not None:
            expr.upper = self.visit(node.upper)
        if node.step is not None:
            expr.step = self.visit(node.step)
        self.pop_state()
        return expr

    def visit_ExtSlice(self, node):
        self.warn("ExtSlice in subscript not supported.", node)
        return self.context_expr(dast.PythonExpr, node, nopush=True)

    def visit_Yield(self, node):
        # Should not get here: 'yield' statements should have been handles by
        # visit_Expr
        self.error("unexpected 'yield' expression.", node)
        return self.create_expr(dast.PythonExpr, node, nopush=True)

    def visit_YieldFrom(self, node):
        # Should not get here: 'yield from' statements should have been
        # handles by visit_Expr
        self.error("unexpected 'yield from' expression.", node)
        return self.create_expr(dast.PythonExpr, node, nopush=True)

    def visit_Lambda(self, node):
        expr = self.create_expr(dast.LambdaExpr, node)
        self.signature(node.args)
        expr.body = self.visit(node.body)
        self.pop_state()
        return expr

    def visit_Ellipsis(self, node):
        return self.create_expr(dast.EllipsisExpr, node, nopush=True)

    def generator_visit(self, node):
        if isinstance(node, SetComp):
            expr = self.create_expr(dast.SetCompExpr, node)
        elif isinstance(node, ListComp):
            expr = self.create_expr(dast.ListCompExpr, node)
        elif isinstance(node, DictComp):
            expr = self.create_expr(dast.DictCompExpr, node)
        else:
            expr = self.create_expr(dast.GeneratorExpr, node)

        for g in node.generators:
            expr.unlock()
            self.current_context = Assignment()
            # DistAlgo: overload 'in' to allow pattern matching:
            expr.domains.append(self.parse_domain_spec(g))
            expr.lock()
            self.current_context = Read()
            expr.conditions.extend([self.visit(i) for i in g.ifs])
        if isinstance(node, DictComp):
            kv = dast.KeyValue(expr)
            kv.key = self.visit(node.key)
            kv.value = self.visit(node.value)
            expr.elem = kv
        else:
            expr.elem = self.visit(node.elt)
        self.pop_state()
        return expr

    visit_ListComp = generator_visit
    visit_GeneratorExp = generator_visit
    visit_SetComp = generator_visit
    visit_DictComp = generator_visit
    del generator_visit

    def visit_IfExp(self, node):
        expr = self.create_expr(dast.IfExpr, node)
        expr.condition = self.visit(node.test)
        expr.body = self.visit(node.body)
        expr.orbody = self.visit(node.orelse)
        self.pop_state()
        return expr

    def visit_Starred(self, node):
        expr = self.create_expr(dast.StarredExpr, node)
        expr.value = self.visit(node.value)
        self.pop_state()
        return expr

    # Helper Nodes

    def error(self, mesg, node):
        self.errcnt += 1
        if node is not None:
            printe(mesg, node.lineno, node.col_offset, self.filename)
        else:
            printe(mesg, 0, 0, self.filename)

    def warn(self, mesg, node):
        self.warncnt += 1
        if node is not None:
            printw(mesg, node.lineno, node.col_offset, self.filename)
        else:
            printw(mesg, 0, 0, self.filename)

    def debug(self, mesg, node=None):
        if node is not None:
            printd(mesg, node.lineno, node.col_offset, self.filename)
        else:
            printd(mesg, 0, 0, self.filename)

if __name__ == "__main__":
    pass
