# Copyright (c) 2010-2017 Bo Lin
# Copyright (c) 2010-2017 Yanhong Annie Liu
# Copyright (c) 2010-2017 Stony Brook University
# Copyright (c) 2010-2017 The Research Foundation of SUNY
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
from ast import *
from itertools import chain
from . import dast, symtab
from .utils import printd, printw, printe

from pprint import pprint

OperatorMap = {
    dast.AddOp      : Add,
    dast.SubOp      : Sub,
    dast.MultOp     : Mult,
    dast.MatMultOp  : MatMult,
    dast.DivOp      : Div,
    dast.ModOp      : Mod,
    dast.PowOp      : Pow,
    dast.LShiftOp   : LShift,
    dast.RShiftOp   : RShift,
    dast.BitOrOp    : BitOr,
    dast.BitXorOp   : BitXor,
    dast.BitAndOp   : BitAnd,
    dast.FloorDivOp : FloorDiv,

    dast.EqOp       : Eq,
    dast.NotEqOp    : NotEq,
    dast.LtOp       : Lt,
    dast.LtEOp      : LtE,
    dast.GtOp       : Gt,
    dast.GtEOp      : GtE,
    dast.IsOp       : Is,
    dast.IsNotOp    : IsNot,
    dast.InOp       : In,
    dast.NotInOp    : NotIn,

    dast.USubOp     : USub,
    dast.UAddOp     : UAdd,
    dast.InvertOp   : Invert,

    dast.AndOp      : And,
    dast.OrOp       : Or
}

AggregateMap = {
    dast.MaxExpr : "max",
    dast.MinExpr : "min",
    dast.SizeExpr : "len",
    dast.SumExpr : "sum"
}


GenCompMap = {
    dast.TupleCompExpr: "tuple",
    dast.MinCompExpr: "min",
    dast.MaxCompExpr: "max",
    dast.SumCompExpr: "sum",
}

CONFIG_OBJECT_NAME = "_config_object"
STATE_ATTR_NAME = "_state"
ENTRYPOINT_NAME = "run"
CATCHALL_PARAM_NAME = "rest_%d"

# FIXME: is there a better way than hardcoding these?
KnownUpdateMethods = {
    "add", "append", "extend", "update",
    "insert", "reverse", "sort",
    "delete", "remove", "pop", "clear", "discard"
}


########## Convenience methods for creating AST nodes: ##########

def pyCall(func, args=[], keywords=[], starargs=None, kwargs=None):
    if isinstance(func, str):
        func = pyName(func)
    ast = Call(func,
               list(args),
               [keyword(arg, val) for arg, val in keywords])
    propagate_attributes(func, ast)
    propagate_attributes(args, ast)
    propagate_attributes([val for _, val in keywords], ast)
    return ast

def pyName(name, ctx=None):
    return Name(name, Load() if ctx is None else ctx)

def pyNone():
    return NameConstant(None)

def pyTrue():
    return NameConstant(True)

def pyFalse():
    return NameConstant(False)

def pyNot(expr):
    ast = UnaryOp(Not(), expr)
    return propagate_attributes(expr, ast)

def pyList(elts, ctx=None):
    ast = List(elts, Load() if ctx is None else ctx)
    return propagate_attributes(elts, ast)

def pySet(elts, ctx=None):
    ast = Set(elts)
    return propagate_attributes(elts, ast)

def pyTuple(elts, ctx=None):
    ast = Tuple(elts, Load() if ctx is None else ctx)
    return propagate_attributes(elts, ast)

def pySetC(elts):
    return pyCall("set", args=elts)

def pySubscr(value, index, ctx=None):
    ast = Subscript(value, index, Load() if ctx is None else ctx)
    return propagate_attributes((value, index), ast)

def pySize(value):
    return pyCall("len", [value])

def pyMin(value):
    return pyCall("min", [value])

def pyMax(value):
    return pyCall("max", [value])

def pyAttr(name, attr, ctx=None):
    if isinstance(name, str):
        ast = Attribute(Name(name, Load()), attr,
                         Load() if ctx is None else ctx)
    else:
        ast = Attribute(name, attr, Load() if ctx is None else ctx)
    return propagate_attributes(ast.value, ast)

def pyCompare(left, op, right):
    ast = Compare(left, [op()], [right])
    return propagate_fields(ast)

def pyLabel(name, block=False, timeout=None):
    kws = [("block", pyTrue() if block else pyFalse())]
    if timeout is not None:
        kws.append(("timeout", timeout))
    return Expr(pyCall(func=pyAttr(pyCall("super"), "_label"),
                       args=[Str(name)],
                       keywords=kws))

def pycomprehension(target, iter, ifs, is_async=0):
    if sys.version_info < (3, 6):
        return comprehension(target, iter, ifs)
    else:
        return comprehension(target, iter, ifs, is_async)

def pyAssign(targets, value):
    ast = Assign(targets, value)
    return propagate_fields(ast)

def pyAugAssign(target, op, value):
    ast = AugAssign(target, op(), value)
    return propagate_fields(ast)

def pyFor(target, iter, body, orelse):
    ast = For(target, iter, body, orelse)
    return propagate_attributes([target, iter], ast)

def pyIf(test, body, orelse):
    ast = If(test, body, orelse)
    return propagate_attributes(test, ast)

def pyWhile(test, body, orelse):
    ast = While(test, body, orelse)
    return propagate_attributes(test, ast)

def pyExpr(value):
    ast = Expr(value)
    return propagate_fields(ast)

def pyReturn(value):
    ast = Return(value)
    return propagate_fields(ast)

def pyClassDef(name, bases=[], keywords=[], starargs=None,
               kwargs=None, body=[], decorator_list=[]):
    return ClassDef(name,
                    list(bases),
                    [keyword(arg, val) for arg, val in keywords],
                    list(body),
                    list(decorator_list))

def pyFunctionDef(name, args=[], kwarg=None, body=[], decorator_list=[],
                  returns=None):
    arglist = arguments(args=[arg(n, None) for n in args],
                        vararg=None,
                        varargannotation=None,
                        kwonlyargs=[],
                        kwarg=(arg(arg=kwarg, annotation=None)
                               if kwarg is not None else None),
                        kwargannotation=None,
                        defaults=[],
                        kw_defaults=[])
    ast = FunctionDef(name, arglist, list(body), list(decorator_list), returns)
    return ast

def propagate_attributes(from_nodes, to_node):
    """Propagates the 'prebody' and 'postbody' attributes.

    These attributes carry auxiliary function definitions/cleanup statements,
    which need to be propagated upward in the AST until they reach a point (i.e.
    a statement block) where injection is possible.

    """
    if isinstance(to_node, AST):
        if not (isinstance(from_nodes, list) or
                isinstance(from_nodes, tuple) or
                isinstance(from_nodes, set)):
            from_nodes = [from_nodes]
        for fro in from_nodes:
            if (hasattr(fro, "prebody") and isinstance(fro.prebody, list)):
                if not hasattr(to_node, "prebody"):
                    to_node.prebody = []
                to_node.prebody.extend(fro.prebody)
            if (hasattr(fro, "postbody") and isinstance(fro.postbody, list)):
                if not hasattr(to_node, "postbody"):
                    to_node.postbody = []
                to_node.postbody.extend(fro.postbody)
    return to_node

def propagate_fields(node):
    """Propagate attributes from `node`'s fields to `node`.

    """
    if hasattr(node, '_fields'):
        for f in node._fields:
            propagate_attributes(getattr(node, f), node)
    return node

class MaxLineAndColFinder(NodeVisitor):
    """Find the number of the last line and its maximum column offset under a
    given tree."""

    def __init__(self):
        super().__init__()
        self.max_lineno = None
        self.max_col_offset = None

    def visit(self, node):
        super().visit(node)
        if hasattr(node, 'lineno'):
            assert isinstance(node.lineno, int)
            assert (hasattr(node, 'col_offset') and
                    isinstance(node.col_offset, int))
            if self.max_lineno is None or node.lineno > self.max_lineno:
                self.max_lineno = node.lineno
                self.max_col_offset = node.col_offset
            elif node.lineno == self.max_lineno:
                self.max_col_offset = max(self.max_col_offset, node.col_offset)

def _find_max_line_and_col(node):
    finder = MaxLineAndColFinder()
    finder.visit(node)
    return finder.max_lineno, finder.max_col_offset

def fixup_locations_in_block(block, last_lineno=None, last_col_offset=None):
    """Make sure '.lineno' attributes are sane within 'block'

    `compile` expects .lineno attributes to be always monotonically increasing
    within a code block, or else the lineno goes haywire in the generated
    bytecode. `ast.fix_missing_locations` does not ensure this property (since
    generated statements would get their lineno from the parent node, which
    might have a smaller value than existing statements in the block), so we
    have to fix it up ourselves.

    """
    for node in block:
        assert isinstance(node, stmt)
        if last_lineno is not None:
            if not hasattr(node, 'lineno') or node.lineno < last_lineno:
                node.lineno = last_lineno
                node.col_offset = last_col_offset
        last_lineno, last_col_offset = _find_max_line_and_col(node)
    return last_lineno, last_col_offset

class _LocationAttrRemoverCls(NodeVisitor):
    def visit(self, node):
        super().visit(node)
        for attr in 'lineno', 'col_offset':
            if hasattr(node, attr):
                delattr(node, attr)

LocationAttrRemover = _LocationAttrRemoverCls()
def clear_location_attrs(nodes):
    """Remove .lineno and .col_offset attributes from all nodes in the subtrees
    rooted at 'nodes'.

    """
    for node in nodes:
        LocationAttrRemover.visit(node)
    return nodes

def is_all_wildcards(targets):
    """True if 'targets' contain only wildcards."""

    for elt in targets:
        if not (isinstance(elt, Name) and elt.id == '_'):
            return False
    return True

class PythonGeneratorException(Exception): pass

def translate(distalgo_ast, filename="", options=None):
    pg = PythonGenerator(filename, options)
    try:
        return pg.visit(distalgo_ast)
    except Exception as ex:
        raise PythonGeneratorException(str(pg.current_node)) from ex

# List of arguments needed to initialize a process:
PROC_INITARGS = ["procimpl", "forwarder"]

PREAMBLE = parse(
    """
import da
    """).body
POSTAMBLE = parse("""
if __name__ == "__main__":
    da.init(config)
""").body


class PythonGenerator(NodeVisitor):
    """Transforms DistPy AST into Python AST.

    """

    def __init__(self, filename="", options=None):
        self.filename = filename
        self.processed_patterns = set()
        self.preambles = list(PREAMBLE)
        self.postambles = list()
        # One instance of PatternComprehensionGenerator for each query.
        # This is needed so free vars with the same name in a query can be
        # properly unified:
        self.pattern_generator = None
        self.cmdline_args = options
        self.module_args = None
        # Used by incgen to avoid expanding 'pre/postbody' in the inc module:
        self.disable_body_expansion = False
        self.current_context = Load

        self.current_node = None
        self.fromImportSet = set()  # a tuple of 2 or 3 elements, from _ import _[ as _]
        self.importSet = set()      # a tuple of 1 or 2 elements, import _[ as _]


    def get_option(self, option, default=None):
        if hasattr(self.cmdline_args, option):
            return getattr(self.cmdline_args, option)
        elif hasattr(self.module_args, option):
            return getattr(self.module_args, option)
        else:
            return default

    def reset(self):
        """Resets internal states.

        Call this before compiling a new file or code segment, if you don't
        want to create a new instance.

        """
        self.processed_patterns = set()
        self.preambles = list(PREAMBLE)
        self.postambles = list()
        self.pattern_generator = None

    def visit(self, node):
        """Generic visit method.

        If the Incrementalization interface generated code for this node, as
        indicated by the 'ast_override' attribute, then return the generated
        code. Otherwise, call the normal visit method.

        """
        if node is None:
            return None

        assert isinstance(node, dast.DistNode)
        self.current_node = node
        if hasattr(node, "ast_override"):
            res = node.ast_override
        else:
            res = super().visit(node)

        if isinstance(node, dast.Statement):
            assert isinstance(res, list)
            # This is a statement block, propagate line number info:
            if len(res) > 0:
                copy_location(res[0], node)
                propagate_attributes(node, res[0])
            return res
        else:
            assert isinstance(res, AST)
            # This is an expression, pass on pre and post bodies:
            copy_location(res, node)
            return propagate_attributes(node, res)

    def _expand_block_attr(self, attr, from_block, to_block):
        if self.disable_body_expansion:
            to_block.extend(from_block)
        else:
            for stmt in from_block:
                if hasattr(stmt, attr):
                    new_block = clear_location_attrs(getattr(stmt, attr))
                    copy_location(new_block[0], stmt)
                    to_block.extend(new_block)
        return to_block

    def body(self, body, res=None):
        """Process a block of statements."""
        if res is None:
            res = []
        for stmt in body:
            if stmt.label is not None:
                res.append(copy_location(pyLabel(stmt.label), stmt))
            block = self.visit(stmt)
            if block is not None:
                self._expand_block_attr('prebody', block, res)
                res.extend(block)
                self._expand_block_attr('postbody', block, res)
            else:
                printe("None result from %s" % str(stmt))
        fixup_locations_in_block(res)
        return res

    def visit_Program(self, node):
        self.module_args = node._compiler_options
        mainbody = self.body(node.body)
        if node.nodecls is not None:
            # `nodecls` is the `Node_` process:
            nodeproc = self.visit(node.nodecls)
        body = list(self.preambles)
        body.append(self.generate_config(node))
        for stmt in body:
            stmt.lineno = 1
        body.extend(mainbody)
        if node.nodecls is not None:
            body.extend(nodeproc)
        body.extend(self.postambles)

        importList = [Import([alias(t[0], t[1] if len(t)>1 else None)]) for t in self.importSet]
        fromImportList = [ImportFrom(t[0], [alias(t[1], t[2] if len(t)>2 else None)], 0) for t in self.fromImportSet]
        # print(importList,fromImportList)

        return [Module(importList+fromImportList+body)]

    def generate_config(self, node):
        return Assign([pyName(CONFIG_OBJECT_NAME, Store())],
                      Dict([Str(key) for key, _ in node.configurations],
                           [self.visit(val) for _, val in node.configurations]))

    def generate_event_def(self, node):
        evtype = pyAttr(pyAttr("da", "pat"), node.type.__name__)
        name = Str(node.name)
        history = self.history_stub(node)
        pattern = self.visit(node.pattern)
        sources = pyNone()
        destinations = pyNone()
        timestamps = pyNone()
        if len(node.sources) > 0:
            sources = pyList([self.visit(s) for s in node.sources])
        if len(node.destinations) > 0:
            destinations = pyList([self.visit(s) for s in node.destinations])
        if len(node.timestamps) > 0:
            timestamps = pyList([self.visit(s) for s in node.timestamps])
        handlers = pyList([pyAttr("self", h.name) for h in node.handlers])
        return pyCall(func=pyAttr(pyAttr("da", "pat"), "EventPattern"),
                      args=[evtype, name, pattern],
                      keywords=[("sources", sources),
                                ("destinations", destinations),
                                ("timestamps", timestamps),
                                ("record_history", history),
                                ("handlers", handlers)])

    def history_initializers(self, node):
        return [pyAssign(targets=[pyAttr("self", evt.name, Store())],
                         value=pyList([]))
                for evt in node.events if evt.record_history]

    def generate_init(self, node):
        body = [
            pyExpr(pyCall(func=pyAttr(pyCall(pyName("super")), "__init__"),
                          args=[pyName(n) for n in PROC_INITARGS],
                          keywords=[(None, pyName('props'))]))
        ]
        body.extend(self.history_initializers(node))
        body.extend([
            pyExpr(pyCall(func=pyAttr(pyAttr("self", "_events"), "extend"),
                          args=[pyList([self.generate_event_def(evt)
                                        for evt in node.events])]))
        ])
        return pyFunctionDef(name="__init__",
                             args=(["self"] + PROC_INITARGS),
                             kwarg='props',
                             body=body)

    def generate_handlers(self, node):
        """Generate the message handlers of a process."""
        body = []
        for evt in node.events:
            for handler in evt.handlers:
                body.extend((self.visit(handler)))
        return body

    def visit_Arguments(self, node):
        """Generates the argument lists for functions and lambdas."""
        self.current_context = Param
        args = [arg(ident.name, None) for ident in node.args]
        kwonlyargs = [arg(ident.name, None) for ident in node.kwonlyargs]
        kw_defaults = [self.visit(expr) for expr in node.kw_defaults]
        defaults = [self.visit(expr) for expr in node.defaults]
        vararg = arg(node.vararg.name, None) \
                 if node.vararg is not None else None
        kwarg = arg(node.kwarg.name, None) \
                if node.kwarg is not None else None
        self.current_context = Load
        return arguments(
            args=args,
            vararg=vararg,
            kwonlyargs=kwonlyargs,
            kwarg=kwarg,
            defaults=defaults,
            kw_defaults=kw_defaults)

    def visit_Process(self, node):
        printd("Compiling process %s" % node.name)
        printd("has methods:%r" % node.methods)
        cd = ClassDef()
        cd.name = node.name
        cd.bases = [self.visit(e) for e in node.bases]
        if node is node.immediate_container_of_type(dast.Program).nodecls:
            cd.bases.append(pyAttr("da", "NodeProcess"))
        else:
            cd.bases.append(pyAttr("da", "DistProcess"))
        if node.ast is not None and hasattr(node.ast, 'keywords'):
            # ########################################
            # TODO: just pass these through until we figure out a use for them:
            cd.keywords = node.ast.keywords
            # ########################################
        else:
            cd.keywords = []
            cd.starargs = []
            cd.kwargs = []
        cd.body = [self.generate_init(node)]
        if node.configurations:
            cd.body.append(self.generate_config(node))
        if node.setup is not None:
            cd.body.extend(self.visit(node.setup))
        if node.entry_point is not None:
            cd.body.extend(self._entry_point(node.entry_point))
        cd.decorator_list = [self.visit(d) for d in node.decorators]
        cd.body.extend(self.body(node.staticmethods))
        cd.body.extend(self.body(node.methods))
        cd.body.extend(self.generate_handlers(node))
        return [cd]

    def _entry_point(self, node):
        stmts = self.visit(node)
        stmts[0].name = ENTRYPOINT_NAME
        stmts[0].args.args = [arg("self", None)]
        return stmts

    def _generate_setup(self, node, fd):
        fd.args = self.visit(node.parent.args)
        kwargname = CATCHALL_PARAM_NAME % node._index
        fd.args.kwarg = arg(kwargname, None)
        superargs = [(argname.name, pyName(argname.name))
                     for argname in node.parent.args.args]
        superargs.append((None, pyName(kwargname)))
        fd.body.append(Expr(
            pyCall(func=pyAttr(pyCall("super"), "setup"),
                   keywords=superargs)))
        fd.body.extend([
            Assign(targets=[pyAttr(pyAttr("self", STATE_ATTR_NAME),
                                   name, Store())],
                   value=pyName(name))
            for name in node.parent.ordered_names
        ])

    def visit_Function(self, node):
        fd = FunctionDef()
        fd.name = node.name
        fd.args = self.visit(node.args)
        fd.body = []
        if isinstance(node.parent, dast.Process):
            if node.name == "setup":
                self._generate_setup(node, fd)
            if node not in node.parent.staticmethods:
                fd.args.args.insert(0, arg("self", None))
        fd.body = self.body(node.body, fd.body)
        fd.decorator_list = [self.visit(d) for d in node.decorators]
        fd.returns = None
        return [fd]

    def visit_ClassStmt(self, node):
        cd = pyClassDef(name=node.name,
                        bases=[self.visit(e) for e in node.bases],
                        body=self.body(node.body))
        # ########################################
        # TODO: just pass these through until we figure out a use for them:
        cd.keywords = node.ast.keywords
        # ########################################
        cd.decorator_list = [self.visit(d) for d in node.decorators]
        return [cd]

    def visit_NameExpr(self, node):
        # `visit()` will overwrite the location attributes of the generated AST
        # using the location of `node`:
        return self.visit(node.value)

    def visit_AttributeExpr(self, node):
        ctx = self.current_context
        self.current_context = Load
        sub = self.visit(node.value)
        self.current_context = ctx
        return pyAttr(sub, node.attr, ctx())

    def visit_SubscriptExpr(self, node):
        ctx = self.current_context
        self.current_context = Load
        val = self.visit(node.value)
        if isinstance(node.index, dast.SliceExpr):
            idx = self.visit(node.index)
        else:
            idx = Index(self.visit(node.index))
            propagate_attributes([idx.value], idx)
        self.current_context = ctx
        return pySubscr(val, idx, ctx())

    def visit_SliceExpr(self, node):
        l = self.visit(node.lower) if node.lower is not None else None
        u = self.visit(node.upper) if node.upper is not None else None
        s = self.visit(node.step) if node.step is not None else None
        ast = Slice(l, u, s)
        return propagate_attributes((l, u, s), ast)

    def visit_StarredExpr(self, node):
        ctx = self.current_context
        self.current_context = Load
        val = self.visit(node.value)
        self.current_context = ctx
        ast = Starred(val, ctx())
        return propagate_attributes([val], ast)

    def visit_EllipsisExpr(self, node):
        return Ellipsis()

    def visit_ConstantExpr(self, node):
        if isinstance(node.value, str):
            return Str(node.value)
        elif isinstance(node.value, bytes):
            return Bytes(node.value)
        else:
            return Num(node.value)

    def visit_SelfExpr(self, node):
        return pyAttr("self", "_id")

    def visit_TrueExpr(self, node):
        return pyTrue()

    def visit_FalseExpr(self, node):
        return pyFalse()

    def visit_NoneExpr(self, node):
        return pyNone()

    def visit_FormattedValueExpr(self, node):
        return FormattedValue(self.visit(node.value), node.conversion,
                              self.visit(node.format_spec)
                              if node.format_spec else None)

    def visit_TupleExpr(self, node):
        elts = [self.visit(e) for e in node.subexprs]
        return pyTuple(elts, self.current_context())

    def visit_ListExpr(self, node):
        ctx = self.current_context
        self.current_context = Load
        elts = [self.visit(e) for e in node.subexprs]
        self.current_context = ctx
        return pyList(elts, ctx())

    def visit_SetExpr(self, node):
        return pySet([self.visit(e) for e in node.subexprs])

    def visit_FormattedStrExpr(self, node):
        ast = JoinedStr([self.visit(value) for value in node.subexprs])
        return propagate_attributes(ast.values, ast)

    def visit_DictExpr(self, node):
        ast = Dict([self.visit(e) for e in node.keys],
                   [self.visit(e) for e in node.values])
        return propagate_attributes(ast.keys + ast.values, ast)

    def visit_IfExpr(self, node):
        ast = IfExp(self.visit(node.condition),
                    self.visit(node.body),
                    self.visit(node.orbody))
        return propagate_attributes((ast.test, ast.body, ast.orelse), ast)

    def visit_CallExpr(self, node):
        return pyCall(self.visit(node.func),
                     [self.visit(a) for a in node.args],
                     [(key, self.visit(value)) for key, value in node.keywords],
                     self.visit(node.starargs)
                     if node.starargs is not None else None,
                     self.visit(node.kwargs)
                     if node.kwargs is not None else None)

    def visit_ApiCallExpr(self, node):
        return pyCall(pyAttr("da", node.func),
                     [self.visit(a) for a in node.args],
                     [(key, self.visit(value)) for key, value in node.keywords],
                     self.visit(node.starargs)
                     if node.starargs is not None else None,
                     self.visit(node.kwargs)
                     if node.kwargs is not None else None)

    def visit_BuiltinCallExpr(self, node):
        return pyCall(pyAttr("self", node.func),
                     [self.visit(a) for a in node.args],
                     [(key, self.visit(value)) for key, value in node.keywords],
                     self.visit(node.starargs)
                     if node.starargs is not None else None,
                     self.visit(node.kwargs)
                     if node.kwargs is not None else None)

    visit_SetupExpr = visit_StartExpr = visit_ConfigExpr = visit_BuiltinCallExpr

    def visit_AggregateExpr(self, node):
        return pyCall(AggregateMap[type(node)],
                     [self.visit(a) for a in node.args])

    visit_MaxExpr = visit_AggregateExpr
    visit_MinExpr = visit_AggregateExpr
    visit_SumExpr = visit_AggregateExpr
    visit_SizeExpr = visit_AggregateExpr

    def visit_ProdExpr(self, node):
        #1. functools.reduce(operator.mul,lis)
        #2. eval('*'.join(str(item) for item in list))
        self.importSet.add(('functools',))
        self.importSet.add(('operator',))
        prod = pyCall(func=pyAttr("functools", "reduce"), args=[pyAttr("operator","mul"), self.visit(node.args[0])])
        if len(node.args) > 1:
            return propagate_fields(BinOp(self.visit(node.args[1]), Mult(), prod))
        else:
            return prod

    def visit_LogicalExpr(self, node):
        if node.operator is dast.NotOp:
            ast = UnaryOp(Not(), self.visit(node.left))
            return propagate_attributes([ast.operand], ast)
        else:
            ast = BoolOp(OperatorMap[node.operator](),
                         [self.visit(e) for e in node.subexprs])
            return propagate_attributes(ast.values, ast)

    def visit_DomainSpec(self, node):
        domain = self.visit(node.domain)
        if not isinstance(node.pattern, dast.PatternExpr):
            self.current_context = Store
            result = pycomprehension(self.visit(node.pattern), domain, [])
            self.current_context = Load
        else:
            if self.pattern_generator is None:
                # Legacy pattern
                target = PatternComprehensionGenerator(Store).visit(node.pattern)
            else:
                ctx = self.pattern_generator.current_context
                self.pattern_generator.current_context = Store
                target = self.pattern_generator.visit(node.pattern)
                self.pattern_generator.current_context = ctx
            result = pycomprehension(target, domain, target.conditions)
        return propagate_fields(result)

    def visit_QuantifiedExpr(self, node):
        if self.pattern_generator is None:
            self.pattern_generator = PatternComprehensionGenerator()
            is_top_level_query = True
        else:
            is_top_level_query = False
            if not self.get_option('use_top_semantic', default=False):
                self.pattern_generator.push_state()
                self.pattern_generator.reset_state()

        body = funcbody = []
        for domspec in node.domains:
            comp = self.visit(domspec)
            body.append(pyFor(comp.target, comp.iter, [], []))
            body = body[0].body
            for cond in comp.ifs:
                body.append(pyIf(cond, [], []))
                body = body[0].body
        postbody = []
        ifcond = self.visit(node.predicate)
        if hasattr(ifcond, "prebody"):
            body.extend(ifcond.prebody)
        if hasattr(ifcond, "postbody"):
            postbody.extend(cnode.postbody)

        if node.operator is dast.UniversalOp:
            ifcond = pyNot(ifcond)
            ifbody = [pyReturn(pyFalse())]
        else:                   # ExistentialExpr
            ifbody = [pyReturn(pyTrue())]
        body.append(pyIf(ifcond, ifbody, []))
        body.extend(postbody)
        if node.operator is dast.UniversalOp:
            funcbody.append(pyReturn(pyTrue()))
        else:
            funcbody.append(pyReturn(pyFalse()))

        # names that should be unified with a containing query need to be
        # explicitly passed in:
        curnode = node
        params = set()
        while curnode is not node.top_level_query:
            curnode = curnode.parent
            if isinstance(curnode, dast.QueryExpr):
                params |= set(curnode.ordered_local_freevars)
        params &= node.nameobjs
        ast = pyCall(func=pyName(node.name),
                     keywords=[(v.name, self.visit(v)) for v in params])
        funast = pyFunctionDef(name=node.name,
                               args=[v.name for v in params],
                               body=funcbody)
        ast.prebody = [funast]

        nameset = node.freevars - params
        if len(nameset) > 0:
            # Back patch nonlocal statement
            if not isinstance(node.scope, dast.ComprehensionExpr):
                if not isinstance(node.statement.parent, dast.Program):
                    decl = Nonlocal([nv.name for nv in nameset])
                else:
                    decl = Global([nv.name for nv in nameset])
                funast.body.insert(0, decl)

            # Assignment needed to ensure all vars are bound at this point
            if is_top_level_query:
                ast.prebody.insert(
                    0, Assign(targets=[pyName(nv.name, Store())
                                       for nv in nameset],
                              value=pyNone()))

        if is_top_level_query:
            self.pattern_generator = None
        elif not self.get_option('use_top_semantic', default=False):
            self.pattern_generator.pop_state()
        return ast

    def visit_ComprehensionExpr(self, node):
        printd("Entering comprehension " + str(node))
        if self.pattern_generator is None:
            self.pattern_generator = PatternComprehensionGenerator()
            is_top_level_query = True
        else:
            self.pattern_generator.push_state()
            is_top_level_query = False
            if not self.get_option('use_top_semantic', default=False):
                self.pattern_generator.reset_state()

        generators = []
        dangling = []
        for dom in node.conditions:
            comp = self.visit(dom)
            if isinstance(comp, comprehension):
                # Tuck any dangling conditions here:
                comp.ifs.extend(dangling)
                dangling = []
                generators.append(comp)
            else:
                if len(generators) > 0:
                    generators[-1].ifs.append(comp)
                    propagate_attributes(generators[-1].ifs, generators[-1])
                else:
                    dangling.append(comp)
        if len(dangling) == 0:
            test = pyTrue()
        elif len(dangling) == 1:
            test = dangling[0]
        else:
            test = propagate_fields(BoolOp(And(), dangling))
        try:
            if type(node) is dast.DictCompExpr:
                key = self.visit(node.elem.key)
                value = self.visit(node.elem.value)
                if len(generators) > 0:
                    ast = DictComp(key, value, generators)
                else:
                    # No generators, degenerate to IfExp:
                    ast = IfExp(test,
                                propagate_fields(Dict([key], [value])),
                                Dict([], []))
                return propagate_fields(ast)
            else:
                elem = self.visit(node.elem)
                if len(generators) > 0:
                    if isinstance(node, dast.SetCompExpr):
                        ast = SetComp(elem, generators)
                    elif isinstance(node, dast.ListCompExpr):
                        ast = ListComp(elem, generators)
                    elif isinstance(node, dast.LenCompExpr):
                        ast = pyCall("len", args=[ListComp(elem, generators)])
                    elif type(node) in GenCompMap:
                        ast = pyCall(GenCompMap[type(node)], args=[GeneratorExp(elem, generators)])
                    elif isinstance(node, dast.PrdCompExpr):
                        #1. functools.reduce(operator.mul,lis)
                        #2. eval('*'.join(str(item) for item in list))
                        self.importSet.add(('functools',))
                        self.importSet.add(('operator',))
                        ast = pyCall(func=pyAttr("functools", "reduce"), args=[pyAttr("operator","mul"), ListComp(elem, generators)])
                    elif isinstance(node, dast.GeneratorExpr):
                        ast = GeneratorExp(elem, generators)
                    else:
                        self.error("Unknown expression", node)
                        return None
                else:
                    # No generators, degenerate to IfExp:
                    if isinstance(node, dast.SetCompExpr):
                        ast = IfExp(test,
                                    propagate_fields(pySet([elem])),
                                    pySetC([]))
                    elif isinstance(node, dast.ListCompExpr):
                        ast = IfExp(test,
                                    propagate_fields(pyList([elem])),
                                    pyList([]))
                    elif isinstance(node, dast.TupleCompExpr):
                        ast = IfExp(test,
                                    propagate_fields(pyTuple([elem])),
                                    pyTuple([]))
                    elif isinstance(node, dast.GeneratorExpr):
                        # Impossible:
                        self.error("Illegal generator expression.", node)
                        return None
                    else:
                        self.error("Illegal unknown expression.", node)
                        return None
                return propagate_fields(ast)

        finally:
            if is_top_level_query:
                printd("Leaving toplevel " + str(node))
                self.pattern_generator = None
            else:
                # We need to restore the pattern state because comprehensions
                # does not bind witness values outside its scope:
                self.pattern_generator.pop_state()
                printd("Leaving comprehension " + str(node))

    visit_GeneratorExpr = visit_ComprehensionExpr
    visit_SetCompExpr = visit_ComprehensionExpr
    visit_ListCompExpr = visit_ComprehensionExpr
    visit_DictCompExpr = visit_ComprehensionExpr
    visit_TupleCompExpr = visit_ComprehensionExpr

    visit_MinCompExpr = visit_ComprehensionExpr
    visit_MaxCompExpr = visit_ComprehensionExpr
    visit_SumCompExpr = visit_ComprehensionExpr
    visit_PrdCompExpr = visit_ComprehensionExpr
    visit_LenCompExpr = visit_ComprehensionExpr

    def visit_ComparisonExpr(self, node):
        left = self.visit(node.left)
        right = self.visit(node.right)
        if isinstance(node.left, dast.PatternExpr):
            # 'PATTERN in DOMAIN'
            context = [(v.unique_name, self.visit(v.value))
                       for v in node.left.ordered_boundpatterns]
            if node.immediate_container_of_type(dast.Process) is not None:
                # Propagate value of current process id to `SelfPattern`:
                context.append(("SELF_ID", pyAttr("self", "_id")))
            ast = pyCall(func=pyAttr(left, "match_iter"),
                         args=[right], keywords=context)
        else:
            ast = pyCompare(left, OperatorMap[node.comparator], right)
        return ast

    def visit_ArithmeticExpr(self, node):
        op = OperatorMap[node.operator]()
        if issubclass(node.operator, dast.UnaryOperator):
            ast = UnaryOp(op, self.visit(node.right))
            return propagate_fields(ast)
        else:
            ast = BinOp(self.visit(node.left), op, self.visit(node.right))
            return propagate_fields(ast)

    visit_BinaryExpr = visit_ArithmeticExpr
    visit_UnaryExpr = visit_ArithmeticExpr

    def visit_PatternElement(self, node):
        if type(node) is dast.FreePattern:
            val = Str(node.value.name) if node.value is not None else pyNone()
        elif type(node) is dast.BoundPattern:
            val = Str(node.unique_name)
        elif type(node) is dast.ConstantPattern:
            if isinstance(node.value, dast.SelfExpr):
                # We have to special case the 'self' expr here:
                return pyCall(func=pyAttr(pyAttr("da", "pat"), "SelfPattern"))
            else:
                val = self.visit(node.value)
        else:
            val = pyList([self.visit(v) for v in node.value])

        return pyCall(func=pyAttr(pyAttr("da", "pat"), type(node).__name__),
                      args=[val])

    visit_FreePattern = visit_PatternElement
    visit_BoundPattern = visit_PatternElement
    visit_ConstantPattern = visit_PatternElement
    visit_TuplePattern = visit_PatternElement
    visit_ListPattern = visit_PatternElement

    def visit_PatternExpr(self, node):
        if node.name not in self.processed_patterns:
            patast = self.visit(node.pattern)
            ast = pyAssign([pyName(node.name, Store())], patast)
            self.preambles.append(ast)
            self.processed_patterns.add(node.name)
        return pyName(node.name)

    visit_LiteralPatternExpr = visit_PatternExpr

    def visit_HistoryExpr(self, node):
        assert node.event is not None
        return pyAttr("self", node.event.name)

    visit_ReceivedExpr = visit_HistoryExpr
    visit_SentExpr = visit_HistoryExpr

    def visit_LambdaExpr(self, node):
        args = self.visit(node.args)
        return Lambda(args, self.visit(node.body))

    def visit_NamedVar(self, node):
        if isinstance(node.scope, dast.Process):
            if node.name in node.scope.methodnames:
                return pyAttr("self", node.name,
                              self.current_context())
            elif node.name in node.scope.staticnames:
                return pyAttr(node.scope.name, node.name,
                              self.current_context())
            else:
                return pyAttr(pyAttr("self", STATE_ATTR_NAME), node.name,
                              self.current_context())
        else:
            return pyName(node.name, self.current_context())


    ########## Statements ##########

    def visit_NoopStmt(self, node):
        return [Pass()]

    def visit_PassStmt(self, node):
        return [Pass()]

    def visit_AssignmentStmt(self, node):
        if node.value is None:
            # This is a "pure" annotation (since Python 3.6), don't generate
            # anything:
            return []
        self.current_context = Store
        targets = [self.visit(tgt) for tgt in node.targets]
        self.current_context = Load
        val = self.visit(node.value)
        return [pyAssign(targets, val)]

    def visit_OpAssignmentStmt(self, node):
        self.current_context = Store
        target = self.visit(node.target)
        self.current_context = Load
        val = self.visit(node.value)
        return [pyAugAssign(target, OperatorMap[node.operator], val)]

    def visit_IfStmt(self, node):
        test = self.visit(node.condition)
        body = self.body(node.body)
        orelse = self.body(node.elsebody)
        return [pyIf(test, body, orelse)]

    def visit_WhileStmt(self, node):
        test = self.visit(node.condition)
        body = self.body(node.body)
        orelse = self.body(node.elsebody)
        return [pyWhile(test, body, orelse)]

    def visit_ForStmt(self, node):
        comp = self.visit(node.domain)
        body = self.body(node.body)
        orelse = self.body(node.elsebody)
        return [pyFor(comp.target, comp.iter, body, orelse)]

    def visit_TryStmt(self, node):
        body = self.body(node.body)
        handlers = [self.visit(eh) for eh in node.excepthandlers]
        orelse = self.body(node.elsebody)
        finalbody = self.body(node.finalbody)
        return [Try(body, handlers, orelse, finalbody)]

    def visit_ExceptHandler(self, node):
        type = self.visit(node.type)
        body = self.body(node.body)
        return ExceptHandler(type, node.name, body)

    def visit_TryFinallyStmt(self, node):
        body = self.body(node.body)
        finalbody = self.body(node.finalbody)
        return [TryFinally(body, finalbody)]

    # 'await' and 'if await':
    def visit_AwaitStmt(self, node):
        def INCGRD():
            return pyAugAssign(pyName(node.unique_label, Store()), Add, Num(1))
        def DEDGRD():
            return pyAugAssign(pyName(node.unique_label, Store()), Sub, Num(1))
        conds = []
        body = [INCGRD()]       # body of the main while loop
        last = body
        last_lineno, max_colno = None, None
        timeout_branches = []
        whilenode = pyWhile(pyCompare(pyName(node.unique_label), Eq, Num(0)),
                            body, [])
        main = [pyAssign([pyName(node.unique_label, Store())], Num(0))]
        main.append(whilenode)
        for br in node.branches:
            if br.condition is not None:
                # Normal branch:
                cond = self.visit(br.condition)
                conds.append(cond)
            else:
                # timeout branch:
                cond = pyAttr("self", "_timer_expired")
                timeout_branches.append(br)
            ifbody = self.body(br.body)
            ifbody.append(INCGRD())
            last_lineno, max_colno = fixup_locations_in_block(ifbody)
            brnode = pyIf(cond, ifbody, [])
            copy_location(brnode, br)
            last.append(brnode)
            last = brnode.orelse
        if node.timeout is not None:
            main.append(pyExpr(pyCall(pyAttr("self", "_timer_start"))))
            if not timeout_branches:
                # If there are no explicit timeout branches, we have to create
                # one here:
                cond = pyAttr("self", "_timer_expired")
                ifbody = [INCGRD()]
                brnode = pyIf(cond, ifbody, [])
                if last_lineno is not None:
                    ifbody[0].lineno, ifbody[0].col_offset = last_lineno, max_colno
                fixup_locations_in_block(ifbody)
                last.append(brnode)
                last = brnode.orelse
        # Label call must come after the If tests:
        last.append(pyLabel(node.label, block=True,
                        timeout=(self.visit(node.timeout)
                                 if node.timeout is not None else None)))
        last.append(DEDGRD())
        if last_lineno is not None:
            last[0].lineno, last[0].col_offset = last_lineno, max_colno
        fixup_locations_in_block(last)
        if node.is_in_loop:
            propagate_continue \
                = pyIf(test=pyCompare(pyName(node.unique_label), NotEq, Num(2)),
                       body=[Continue()], orelse=[])
            propagate_break \
                = pyIf(test=pyCompare(pyName(node.unique_label), NotEq, Num(2)),
                       body=[Break()], orelse=[])
            if last_lineno is not None:
                propagate_continue.lineno, propagate_continue.col_offset \
                    = propagate_break.lineno, propagate_break.col_offset \
                    = last_lineno, max_colno
            whilenode.orelse.append(propagate_continue)
            main.append(propagate_break)
        propagate_attributes(conds, main[0])
        return main

    # 'while await':
    def visit_LoopingAwaitStmt(self, node):
        def INCGRD():
            return pyAugAssign(pyName(node.unique_label, Store()), Add, Num(1))
        def DEDGRD():
            return pyAugAssign(pyName(node.unique_label, Store()), Sub, Num(1))
        conds = []
        timeout_branches = []
        body = [INCGRD()]       # body of the main while loop
        last = body
        last_lineno, max_colno = None, None
        for br in node.branches:
            if br.condition is None:
                # timeout branch:
                timeout_branches.append(br)
            else:
                cond = self.visit(br.condition)
                conds.append(cond)
                ifbody = self.body(br.body)
                ifbody.append(DEDGRD())
                last_lineno, max_colno = fixup_locations_in_block(ifbody)
                brnode = pyIf(cond, ifbody, [])
                copy_location(brnode, br)
                last.append(brnode)
                last = brnode.orelse
        last = body
        if timeout_branches:
            cond = pyAttr("self", "_timer_expired")
            ifbody = []
            for br in timeout_branches:
                ifbody.extend(self.body(br.body))
            ifbody.append(INCGRD())
            brnode = pyIf(cond, ifbody, [])
            if last_lineno:
                brnode.lineno, brnode.col_offset = last_lineno, max_colno
                ifbody[0].lineno, ifbody[0].col_offset = last_lineno, max_colno
            last_lineno, max_colno = fixup_locations_in_block(ifbody)
            last.append(brnode)
            last = brnode.orelse
        # Label call must come after the If tests:
        labelnode = pyIf(pyCompare(pyName(node.unique_label), Eq, Num(0)),
                         [pyLabel(node.label, block=True,
                                  timeout=(self.visit(node.timeout)
                                        if node.timeout is not None else None))
                         ], [])
        last.append(labelnode)
        if last_lineno is not None:
            last[0].lineno, last[0].col_offset = last_lineno, max_colno
        fixup_locations_in_block(last)
        whilenode = pyWhile(pyCompare(pyName(node.unique_label), Eq, Num(0)),
                            body, [])
        main = [pyAssign([pyName(node.unique_label, Store())], Num(0))]
        if node.timeout is not None:
            main.append(pyExpr(pyCall(pyAttr("self", "_timer_start"))))
        main.append(whilenode)
        propagate_attributes(conds, main[0])
        return main

    def visit_ReturnStmt(self, node):
        if node.value is not None:
            value = self.visit(node.value)
        else:
            value = None
        return [pyReturn(value)]

    def visit_DeleteStmt(self, node):
        self.current_context = Del
        targets = [self.visit(tgt) for tgt in node.targets]
        self.current_context = Load
        return [propagate_fields(Delete(targets))]

    def visit_YieldStmt(self, node):
        if node.value is not None:
            value = self.visit(node.value)
            return [pyExpr(Yield(value))]
        else:
            return [pyExpr(Yield(None))]

    def visit_YieldFromStmt(self, node):
        if node.value is not None:
            value = self.visit(node.value)
            return [pyExpr(YieldFrom(value))]
        else:
            return [pyExpr(YieldFrom(None))]

    def visit_WithStmt(self, node):
        items = []
        for item in node.items:
            context_expr = self.visit(item[0])
            if item[1] is not None:
                self.current_context = Store
                optional_vars = self.visit(item[1])
                self.current_context = Load
            else:
                optional_vars = None
            items.append(withitem(context_expr, optional_vars))
        body = self.body(node.body)
        ast = With(items, body)
        return [propagate_attributes([e.context_expr for e in items], ast)]

    def visit_RaiseStmt(self, node):
        ast = Raise(self.visit(node.expr), self.visit(node.cause))
        return [propagate_fields(ast)]

    def visit_SimpleStmt(self, node):
        value = self.visit(node.expr)
        return [pyExpr(value)]

    def visit_BreakStmt(self, node):
        return [Break()]

    def visit_ContinueStmt(self, node):
        return [Continue()]

    def visit_ImportStmt(self, node):
        names = []
        for item in node.items:
            if item.asname is None:
                names.append(alias(item.name, None))
            else:
                names.append(alias(item.name, item.asname))
        return [Import(names)]

    def visit_ImportFromStmt(self, node):
        names = []
        for item in node.items:
            if item.asname is None:
                names.append(alias(item.name, None))
            else:
                names.append(alias(item.name, item.asname.name))
        return [ImportFrom(node.module, names, node.level)]

    def visit_AssertStmt(self, node):
        expr = self.visit(node.expr)
        msg = self.visit(node.msg) if node.msg is not None else None
        ast = Assert(expr, msg)
        return [propagate_fields(ast)]

    def visit_GlobalStmt(self, node):
        return [Global(node.names)]

    def visit_NonlocalStmt(self, node):
        return [Nonlocal(node.names)]

    def visit_ResetStmt(self, node):
        # XXX: Gross Hack!!!
        blueprint = """
for attr in dir(self):
    if attr.find("{0}Event_") != -1:
        getattr(self, attr).clear()
"""
        src = blueprint.format(node.target)
        return clear_location_attrs(parse(src).body)

    def history_stub(self, node):
        if node.record_history:
            return pyTrue()
        else:
            return pyNone()

    def visit_Event(self, node):
        return pyAttr(pyAttr("da", "pat"), node.type.__name__)

    def visit_EventHandler(self, node):
        stmts = self.visit_Function(node)
        stmts.append(pyAssign(
            [pyAttr(node.name, "_labels", Store())],
            (pyNone() if node.labels is None else
               pyCall(pyName("frozenset"),
                      [pySet([Str(l) for l in node.labels])]))))
        stmts.append(pyAssign(
            [pyAttr(node.name, "_notlabels", Store())],
            (pyNone() if node.notlabels is None else
               pyCall(pyName("frozenset"),
                      [pySet([Str(l) for l in node.notlabels])]))))
        return stmts

class PatternComprehensionGenerator(PythonGenerator):
    def __init__(self, ctx=Load):
        super().__init__()
        # Set of freevars seen so far. Freevars after the first occurrence
        # needs to be unified:
        self.freevars = set()
        self.state_stack = []
        self.current_context = ctx

    def push_state(self):
        self.state_stack.append(frozenset(self.freevars))

    def pop_state(self):
        s = self.state_stack.pop()
        self.freevars = set(s)

    def reset_state(self):
        self.freevars = set()

    def visit_FreePattern(self, node):
        ctx = self.current_context
        conds = []
        if node.value is None:
            target = pyName("_", ctx())
        elif node.value in self.freevars:
            target = pyName(node.unique_name, ctx())
            self.current_context = Load
            conds = [pyCompare(pyName(node.unique_name), Eq,
                               self.visit(node.value))]
        else:
            target = self.visit(node.value)
            self.freevars.add(node.value)
        target.conditions = conds
        self.current_context = ctx
        return target

    def visit_BoundPattern(self, node):
        ctx = self.current_context
        boundname = pyName(node.unique_name, ctx())
        self.current_context = Load
        targetname = self.visit(node.value)
        conast = pyCompare(pyName(node.unique_name), Eq, targetname)
        boundname.conditions = [conast]
        self.current_context = ctx
        return boundname

    def visit_ConstantPattern(self, node):
        ctx = self.current_context
        target = pyName(node.unique_name, ctx())
        self.current_context = Load
        compval = self.visit(node.value)
        target.conditions = [pyCompare(pyName(node.unique_name), Eq, compval)]
        self.current_context = ctx
        return target

    #### todo add extra condition to condition_list to fix number of components error
    #### length of pattern and length of elements
    #### when recursive visiting
    #### remember the freevar seen.
    ####    first time seen a variable: free, see it later: bound
    def visit_TuplePattern(self, node):
        condition_list = []
        targets = []
        for elt in node.value:
            tgt = self.visit(elt)
            targets.append(tgt)
            condition_list.extend(tgt.conditions)
        if is_all_wildcards(targets):
            # Optimization: combine into one '_'
            target = pyName('_', self.current_context())
        else:
            target = pyTuple(targets, self.current_context())
        target.conditions = condition_list
        return target

    def visit_ListPattern(self, node):
        raise NotImplementedError(
            "Can not compile list pattern to comprehension.")

    def visit_PatternExpr(self, node):
        return self.visit(node.pattern)

    visit_LiteralPatternExpr = visit_PatternExpr
