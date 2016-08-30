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

import ast
import sys
from da.compiler.dast import *

BOOLOP_SYMBOLS = {
    AndOp:        '∧',
    OrOp:         '∨',
    NotOp:        '¬'
}

QUANTOP_SYMBOLS = {
    ExistentialOp:  '∃',
    UniversalOp:    '∀'
}

BINOP_SYMBOLS = {
    AddOp:        '+',
    SubOp:        '-',
    MultOp:       '*',
    DivOp:        '/',
    FloorDivOp:   '//',
    ModOp:        '%',
    LShiftOp:     '<<',
    RShiftOp:     '>>',
    BitOrOp:      '|',
    BitAndOp:     '&',
    BitXorOp:     '^',
    PowOp:        '**'
}

CMPOP_SYMBOLS = {
    EqOp:         '==',
    GtOp:         '>',
    GtEOp:        '>=',
    InOp:         '∈',
    IsOp:         'is',
    IsNotOp:      'is not',
    LtOp:         '<',
    LtEOp:        '<=',
    NotEqOp:      '≠',
    NotInOp:      '∉'
}

UNARYOP_SYMBOLS = {
    InvertOp:     '~',
    NotOp:        'not',
    UAddOp:       '+',
    USubOp:       '-'
}

EVENT_TYPES = {
    ReceivedEvent: 'receive',
    SentEvent:     'send'
}

# Large float and imaginary literals get turned into infinities in the AST.
# We unparse those infinities to INFSTR.
INFSTR = "1e" + repr(sys.float_info.max_10_exp + 1)

def to_pseudo(tree):
    import io
    textbuf = io.StringIO(newline='')
    DastUnparser(tree, textbuf)
    return textbuf.getvalue()

def interleave(inter, f, seq):
    """Call f on each item in seq, calling inter() in between.
    """
    seq = iter(seq)
    try:
        f(next(seq))
    except StopIteration:
        pass
    else:
        for x in seq:
            inter()
            f(x)

class DastUnparser:
    """Methods in this class recursively traverse an AST and
    output source code for the abstract syntax; original formatting
    is disregarded. """

    def __init__(self, tree, file=sys.stdout, indent_width=4):
        """Unparser(tree, file=sys.stdout) -> None.
         Print the source for tree to file."""
        self.f = file
        self.counter = 0
        self._indent = 0
        self._indent_width = indent_width
        self.dispatch(tree)
        print("", file=self.f)
        self.f.flush()

    def fill(self, text = ""):
        "Indent a piece of text, according to the current indentation level"
        text = ("\n"+" "*self._indent_width*self._indent + text)
        self.f.write(text)
        self.counter += len(text)

    def write(self, text):
        "Append a piece of text to the current line."
        self.f.write(text)
        self.counter += len(text)

    def enter(self):
        "Print ':', and increase the indentation."
        self.write(":")
        self._indent += 1

    def leave(self):
        "Decrease the indentation level."
        self._indent -= 1

    def label(self, label):
        "Print 'label' followed by a ':', indented one space from the previous level."
        offset = (self._indent_width * (self._indent - 1)) + 1
        self.write("\n" + " " * offset + label + ':')

    def dispatch(self, tree):
        "Dispatcher function, dispatching tree type T to method _T."
        if isinstance(tree, list):
            for t in tree:
                self.dispatch(t)
            return
        if isinstance(tree, Statement) and tree.label:
            self.label(tree.label)
        meth = getattr(self, "_"+tree.__class__.__name__)
        meth(tree)


    ############### Unparsing methods ######################
    # There should be one method per concrete grammar type #
    # Constructors should be grouped by sum type. Ideally, #
    # this would follow the order in the grammar, but      #
    # currently doesn't.                                   #
    ########################################################

    def _Program(self, tree):
        # for stmt in tree.processes:
        #     self.dispatch(stmt)
        # if tree.entry_point:
        #     self.dispatch(tree.entry_point)
        self.dispatch(tree.body)

    # stmt
    def _SimpleStmt(self, tree):
        self.fill()
        self.dispatch(tree.expr)

    def _ImportStmt(self, t):
        self.fill("import ")
        interleave(lambda: self.write(", "), self.dispatch, t.items)

    def _ImportFromStmt(self, t):
        self.fill("from ")
        self.write("." * t.level)
        if t.module:
            self.write(t.module)
        self.write(" import ")
        interleave(lambda: self.write(", "), self.dispatch, t.items)

    def _AssignmentStmt(self, t):
        self.fill()
        for target in t.targets:
            self.dispatch(target)
            self.write(" = ")
        self.dispatch(t.value)

    def _OpAssignmentStmt(self, t):
        self.fill()
        self.dispatch(t.target)
        self.write(" " + BINOP_SYMBOLS[t.operator] + "= ")
        self.dispatch(t.value)

    def _ReturnStmt(self, t):
        self.fill("return")
        if t.value:
            self.write(" ")
            self.dispatch(t.value)

    def _NoopStmt(self, t):
        self.fill("pass")

    def _BreakStmt(self, t):
        self.fill("break")

    def _ContinueStmt(self, t):
        self.fill("continue")

    def _DeleteStmt(self, t):
        self.fill("del ")
        interleave(lambda: self.write(", "), self.dispatch, t.targets)

    def _AssertStmt(self, t):
        self.fill("assert ")
        self.dispatch(t.expr)
        if t.msg:
            self.write(", ")
            self.dispatch(t.msg)

    def _GlobalStmt(self, t):
        self.fill("global ")
        interleave(lambda: self.write(", "), self.write, t.names)

    def _NonlocalStmt(self, t):
        self.fill("nonlocal ")
        interleave(lambda: self.write(", "), self.write, t.names)

    def _AwaitStmt(self, t):
        self.fill("await")
        self._do_await_branches(t)

    def _LoopingAwaitStmt(self, t):
        self.fill("awhile")
        self._do_await_branches(t)

    def _do_await_branches(self, t):
        if len(t.branches) == 1 and not t.branches[0].body and not t.orelse:
            # single-line await
            self.write(" ")
            self.dispatch(t.branches[0].condition)
        if t.timeout:
            self.write(" until ")
            self.dispatch(t.timeout)
        if len(t.branches) > 1 or t.branches[0].body or t.orelse:
            self.enter()
            for b in t.branches:
                self.fill("if ")
                self.dispatch(b)
            if t.orelse:
                self.fill("if timeout")
                self.enter()
                self.dispatch(t.orelse)
                self.leave()
            self.leave()


    def _Branch(self, t):
        self.dispatch(t.condition)
        self.enter()
        self.dispatch(t.body)
        self.leave()

    def _SendStmt(self, t):
        self.fill("send ")
        self.dispatch(t.message)
        self.write(" to ")
        self.dispatch(t.target)

    def _OutputStmt(self, t):
        self.fill("output ")
        interleave(lambda: self.write(', '), self.dispatch, t.message)
        if t.level:
            self.write(" at level ")
            self.dispatch(t.level)

    def _ResetStmt(self, t):
        self.fill("reset ")
        self.dispatch(t.expr)

    def _YieldStmt(self, t):
        self.write("(")
        self.write("yield")
        if t.value:
            self.write(" ")
            self.dispatch(t.value)
        self.write(")")

    def _YieldFrom(self, t):
        self.write("(")
        self.write("yield from")
        if t.value:
            self.write(" ")
            self.dispatch(t.value)
        self.write(")")

    def _RaiseStmt(self, t):
        self.fill("raise")
        if not t.expr:
            assert not t.cause
            return
        self.write(" ")
        self.dispatch(t.expr)
        if t.cause:
            self.write(" from ")
            self.dispatch(t.cause)

    def _PassStmt(self, t):
        self.fill("pass")

    def _TryStmt(self, t):
        self.fill("try")
        self.enter()
        self.dispatch(t.body)
        self.leave()
        for ex in t.excepthandlers:
            self.dispatch(ex)
        if t.elsebody:
            self.fill("else")
            self.enter()
            self.dispatch(t.elsebody)
            self.leave()
        if t.finalbody:
            self.fill("finally")
            self.enter()
            self.dispatch(t.finalbody)
            self.leave()

    def _ExceptHandler(self, t):
        self.fill("except")
        if t.type:
            self.write(" ")
            self.dispatch(t.type)
        if t.name:
            self.write(" as ")
            self.write(t.name)
        self.enter()
        self.dispatch(t.body)
        self.leave()

    def _ClassStmt(self, t):
        self.write("\n")
        for deco in t.decorators:
            self.fill("@")
            self.dispatch(deco)
        self.fill("class " + t.name)
        self.write("(")
        comma = False
        for e in t.bases:
            if comma: self.write(", ")
            else: comma = True
            self.dispatch(e)
        for e in t.keywords:
            if comma: self.write(", ")
            else: comma = True
            self.dispatch(e)
        if t.starargs:
            if comma: self.write(", ")
            else: comma = True
            self.write("*")
            self.dispatch(t.starargs)
        if t.kwargs:
            if comma: self.write(", ")
            else: comma = True
            self.write("**")
            self.dispatch(t.kwargs)
        self.write(")")

        self.enter()
        self.dispatch(t.body)
        self.leave()

    def _Process(self, t):
        self.write("\n")
        for deco in t.decorators:
            self.fill("@")
            self.dispatch(deco)
        self.fill("process " + t.name)
        if t.bases:
            self.write(" extends ")
            comma = False
            for e in t.bases:
                if comma: self.write(", ")
                else: comma = True
                self.dispatch(e)
        self.enter()
        self.label("#PARAMS")
        self.write(" ")
        self.dispatch(t.args)
        self.write("\n")
        self.label('#STATES')
        self.write(" ")
        for name in t.ordered_local_names:
            self.write(name)
            self.write(" ")
        self.write("\n")
        if t.configurations:
            self.label("#CONFIG")
            self.write("\n")
            for key, value in t.configurations:
                self.fill(key + " : ")
                self.dispatch(value)
        if t.setup:
            self.label("#INIT")
            self.dispatch(t.setup.body)
            self.write("\n")
        if t.entry_point:
            self.label("#ENTRY_POINT")
            self.dispatch(t.entry_point)
            self.write("\n")
        if t.events:
            self.label("#EVENTS")
            self.dispatch(t.event_handlers)
            self.write("\n")
        if t.methods:
            self.label("#METHODS")
            self.dispatch(t.methods)
        self.leave()

    def _Function(self, t):
        self.write("\n")
        for deco in t.decorators:
            self.fill("@")
            self.dispatch(deco)
        self.fill("def "+t.name + "(")
        self.dispatch(t.args)
        self.write(")")
        self.enter()
        self.dispatch(t.body)
        self.leave()

    def _EventHandler(self, t):
        self.write("\n")
        self.fill("on ")
        first = True
        for evt in t.events:
            if first: first = False
            else: self.write(" or ")
            self.write(EVENT_TYPES[evt.type] + " ")
            self.dispatch(evt)
        if t.labels:
            self.write("at labels ")
            self.write(t.labels)
        if t.notlabels:
            self.write("but not labels ")
            self.write(t.notlabels)
        self.enter()
        self.dispatch(t.body)
        self.leave()

    def _ForStmt(self, t):
        self.fill("for ")
        self.dispatch(t.domain)
        self.enter()
        self.dispatch(t.body)
        self.leave()
        if t.elsebody:
            self.fill("else")
            self.enter()
            self.dispatch(t.orelse)
            self.leave()

    def _IfStmt(self, t):
        self.fill("if ")
        self.dispatch(t.condition)
        self.enter()
        self.dispatch(t.body)
        self.leave()
        # collapse nested ifs into equivalent elifs.
        while (t.elsebody and len(t.elsebody) == 1 and
               isinstance(t.elsebody[0], IfStmt)):
            t = t.elsebody[0]
            self.fill("elif ")
            self.dispatch(t.condition)
            self.enter()
            self.dispatch(t.body)
            self.leave()
        # final else
        if t.elsebody:
            self.fill("else")
            self.enter()
            self.dispatch(t.elsebody)
            self.leave()

    def _WhileStmt(self, t):
        self.fill("while ")
        self.dispatch(t.condition)
        self.enter()
        self.dispatch(t.body)
        self.leave()
        if t.elsebody:
            self.fill("else")
            self.enter()
            self.dispatch(t.elsebody)
            self.leave()

    def _WithStmt(self, t):
        self.fill("with ")
        first = True
        for item, alias in t.items:
            if first:
                first = False
            else:
                self.write(", ")
            self.dispatch(item)
            if alias:
                self.write(" as ")
                self.dispatch(alias)
        self.enter()
        self.dispatch(t.body)
        self.leave()

    # expr
    def _SimpleExpr(self, t):
        if isinstance(t.value, DistNode):
            self.dispatch(t.value)
        elif t.value:
            self.write(repr(t.value))

    def _ConstantExpr(self, t):
        if isinstance(t.value, int) or isinstance(t.value, float):
            # Substitute overflowing decimal literal for AST infinities.
            self.write(repr(t.value).replace("inf", INFSTR))
        else:
            self.write(repr(t.value))

    def _SelfExpr(self, t):
        self.write("self")

    def _TrueExpr(self, t):
        self.write("True")

    def _FalseExpr(self, t):
        self.write("False")

    def _NoneExpr(self, t):
        self.write("None")

    def _NamedVar(self, t):
        s = t.scope
        if isinstance(s, Process):
            self.write("self.")
        self.write(t.name)

    def _ListExpr(self, t):
        self.write("[")
        interleave(lambda: self.write(", "), self.dispatch, t.subexprs)
        self.write("]")

    def _TupleExpr(self, t):
        self.write("(")
        if len(t.subexprs) == 1:
            (elt,) = t.subexprs
            self.dispatch(elt)
            self.write(",")
        else:
            interleave(lambda: self.write(", "), self.dispatch, t.subexprs)
        self.write(")")

    def _SetExpr(self, t):
        assert(t.subexprs) # should be at least one element
        self.write("{")
        interleave(lambda: self.write(", "), self.dispatch, t.subexprs)
        self.write("}")

    def _DictExpr(self, t):
        assert len(t.keys) == len(t.values)
        self.write("{")
        def write_pair(pair):
            (k, v) = pair
            self.dispatch(k)
            self.write(": ")
            self.dispatch(v)
        interleave(lambda: self.write(", "), write_pair, zip(t.keys, t.values))
        self.write("}")

    def _IfExpr(self, t):
        self.write("(")
        self.dispatch(t.body)
        self.write(" if ")
        self.dispatch(t.condition)
        self.write(" else ")
        self.dispatch(t.orbody)
        self.write(")")

    def _GeneratorExpr(self, t):
        self.write("(")
        self.dispatch(t.elem)
        self.write(" | ")
        interleave(lambda: self.write(", "), self.dispatch, t.conditions)
        self.write(")")

    def _ListCompExpr(self, t):
        self.write("[")
        self.dispatch(t.elem)
        self.write(" | ")
        interleave(lambda: self.write(", "), self.dispatch, t.conditions)
        self.write("]")

    def _SetCompExpr(self, t):
        self.write("{")
        self.dispatch(t.elem)
        self.write(" | ")
        interleave(lambda: self.write(", "), self.dispatch, t.conditions)
        self.write("}")

    def _TupleCompExpr(self, t):
        self.write("()")
        self.dispatch(t.elem)
        self.write(" | ")
        interleave(lambda: self.write(", "), self.dispatch, t.conditions)
        self.write(")")

    def _DictCompExpr(self, t):
        self.write("{")
        self.dispatch(t.elem)
        self.write(" | ")
        interleave(lambda: self.write(", "), self.dispatch, t.conditions)
        self.write("}")

    def _KeyValue(self, t):
        self.dispatch(t.key)
        self.write(": ")
        self.dispatch(t.value)

    def _MaxExpr(self, t):
        self.write("max(")
        self._callargs(t)
        self.write(")")

    def _MinExpr(self, t):
        self.write("min(")
        self._callargs(t)
        self.write(")")

    def _SizeExpr(self, t):
        self.write("size(")
        self._callargs(t)
        self.write(")")

    def _SumExpr(self, t):
        self.write("sum(")
        self._callargs(t)
        self.write(")")

    def _DomainSpec(self, t):
        if not isinstance(t.domain, HistoryExpr):
            self.dispatch(t.pattern)
            self.write("∈")
        self.dispatch(t.domain)

    def _QuantifiedExpr(self, t):
        prefix = QUANTOP_SYMBOLS[t.operator]
        self.write("{")
        for dom in t.domains:
            self.write(prefix)
            self.dispatch(dom)
        self.write(" | ")
        self.dispatch(t.predicate)
        self.write("}")

    def _LogicalExpr(self, t):
        opsym = BOOLOP_SYMBOLS[t.operator]
        if t.operator is NotOp:
            self.write(opsym)
            self.dispatch(t.left)
        else:
            s = " " + opsym + " "
            interleave(lambda: self.write(s), self.dispatch, t.subexprs)

    def _UnaryExpr(self, t):
        self.write("(")
        self.write(UNARYOP_SYMBOLS[t.operator])
        self.write(" ")
        self.dispatch(t.right)
        self.write(")")

    def _BinaryExpr(self, t):
        self.write("(")
        self.dispatch(t.left)
        self.write(" " + BINOP_SYMBOLS[t.operator] + " ")
        self.dispatch(t.right)
        self.write(")")

    def _ComparisonExpr(self, t):
        self.write("(")
        # XXX: Hack! if RHS is HistoryExpr, ignore LHS
        if not isinstance(t.right, HistoryExpr):
            self.dispatch(t.left)
            self.write(" " + CMPOP_SYMBOLS[t.comparator] + " ")
        self.dispatch(t.right)
        self.write(")")

    def _PatternExpr(self, t):
        self.dispatch(t.pattern)

    _LiteralPatternExpr = _PatternExpr

    def _ReceivedExpr(self, t):
        self.write("received(")
        self.dispatch(t.event)
        self.write(")")

    def _SentExpr(self, t):
        self.write("sent(")
        self.dispatch(t.event)
        self.write(")")

    def _CallExpr(self, t):
        if isinstance(t.func, DistNode):
            self.dispatch(t.func)
        else:
            self.write(t.func)
        self.write("(")
        self._callargs(t)
        self.write(")")

    _BuiltinCallExpr = _CallExpr
    _ApiCallExpr = _CallExpr

    def _AttributeExpr(self,t):
        self.dispatch(t.value)
        # Special case: 3.__abs__() is a syntax error, so if t.value
        # is an integer literal then we need to either parenthesize
        # it or add an extra space to get 3 .__abs__().
        if isinstance(t.value, ConstantExpr) and \
           isinstance(t.value.value, int):
            self.write(" ")
        self.write(".")
        self.write(t.attr)

    def _SubscriptExpr(self, t):
        self.dispatch(t.value)
        self.write("[")
        self.dispatch(t.index)
        self.write("]")

    def _SliceExpr(self, t):
        self.write("[")
        if t.lower:
            self.dispatch(t.lower)
        self.write(":")
        if t.upper:
            self.dispatch(t.upper)
        if t.step:
            self.write(":")
            self.dispatch(t.step)
        self.write("]")

    def _StarredExpr(self, t):
        self.write("*")
        self.dispatch(t.value)

    # slice
    def _EllipsisExpr(self, t):
        self.write("...")

    # argument
    def _arg(self, t):
        self.write(t.arg)
        if t.annotation:
            self.write(": ")
            self.dispatch(t.annotation)

    # pattern
    def _ConstantPattern(self, t):
        self.dispatch(t.value)

    def _FreePattern(self, t):
        if t.value:
            self.dispatch(t.value)
        else:
            self.write('_')

    def _BoundPattern(self, t):
        self.write("=")
        self.dispatch(t.value)

    _TuplePattern = _TupleExpr
    _ListPattern = _ListExpr

    # others
    def _Arguments(self, t):
        first = True
        # normal arguments
        defaults = [None] * (len(t.args) - len(t.defaults)) + t.defaults
        for a, d in zip(t.args, defaults):
            if first: first = False
            else: self.write(", ")
            self.dispatch(a)
            if d:
                self.write("=")
                self.dispatch(d)

        # varargs, or bare '*' if no varargs but keyword-only arguments present
        if t.vararg or t.kwonlyargs:
            if first:first = False
            else: self.write(", ")
            self.write("*")
            if t.vararg:
                self.dispatch(t.vararg)

        # keyword-only arguments
        if t.kwonlyargs:
            for a, d in zip(t.kwonlyargs, t.kw_defaults):
                if first:first = False
                else: self.write(", ")
                self.dispatch(a),
                if d:
                    self.write("=")
                    self.dispatch(d)

        # kwargs
        if t.kwarg:
            if first:first = False
            else: self.write(", ")
            self.write("**")
            self.dispatch(t.kwarg.arg)

    def _Event(self, t):
        self.dispatch(t.pattern)
        if t.sources:
            self.write(" from=(")
            self.dispatch(t.sources)
            self.write(")")
        if t.destinations:
            self.write(" to=(")
            self.dispatch(t.destinations)
            self.write(")")
        if t.timestamps:
            self.write(" clk=(")
            self.dispatch(t.timestamps)
            self.write(")")

    def _LambdaExpr(self, t):
        self.write("(")
        self.write("lambda ")
        self.dispatch(t.args)
        self.write(": ")
        self.dispatch(t.body)
        self.write(")")

    def _Alias(self, t):
        if t.asname:
            self.write(t.name + " as ")
            self.dispatch(t.asname)
        else:
            self.dispatch(t.name)

    def _callargs(self, t):
        comma = False
        for e in t.args:
            if comma: self.write(", ")
            else: comma = True
            self.dispatch(e)
        for key, value in t.keywords:
            if comma: self.write(", ")
            else: comma = True
            self.write(key)
            self.write("=")
            self.dispatch(value)
        if t.starargs:
            if comma: self.write(", ")
            else: comma = True
            self.write("*")
            self.dispatch(t.starargs)
        if t.kwargs:
            if comma: self.write(", ")
            else: comma = True
            self.write("**")
            self.dispatch(t.kwargs)

    def _withitem(self, t):
        self.dispatch(t.context_expr)
        if t.optional_vars:
            self.write(" as ")
            self.dispatch(t.optional_vars)

if __name__ == '__main__':
    from da.compiler.ui import daast_from_file
    from da.compiler.utils import to_pseudo
    ast = daast_from_file("../test/await.da")
    print(to_pseudo(ast))
