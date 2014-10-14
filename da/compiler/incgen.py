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

import io
import sys
from da.tools.unparse import Unparser

from ast import *
from . import dast
from .pygen import *
from .utils import printd, printw, OptionsManager

INC_MODULE_VAR = "IncModule"

QUERY_STUB_FORMAT = "Query_%d"
ASSIGN_STUB_FORMAT = "Assign_%s"
UPDATE_STUB_FORMAT = "Update_%s_%d"
INIT_STUB_FORMAT = "Init_Event_%s"
RESET_STUB_FORMAT = "Reset_%s_Events"

SELF_ID_NAME = "SELF_ID"

NegatedOperatorMap = {
    dast.EqOp : NotEq,
    dast.NotEqOp : Eq,
    dast.LtOp : Gt,
    dast.LtEOp : GtE,
    dast.GtOp : Lt,
    dast.GtEOp : LtE,
    dast.IsOp : IsNot,
    dast.IsNotOp : Is,
    dast.InOp : NotIn,
    dast.NotInOp : In
}

Options = None                  # Command line options
ModuleFilename = ""

Counter = 0                     # For unique names

##########
# Auxiliary methods:

def to_source(tree):
    textbuf = io.StringIO(newline='')
    Unparser(tree, textbuf)
    return textbuf.getvalue()

def iprintd(message):
    printd(message, filename=ModuleFilename)

def iprintw(message):
    printw(message, filename=ModuleFilename)

def combine_not_comparison(node):
    if (isinstance(node, dast.LogicalExpr) and
            node.operator is dast.NotOp and
            isinstance(node.left, dast.ComparisonExpr)):
        exp = dast.ComparisonExpr(parent=node.parent, ast=node.ast)
        exp.left = node.left.left
        exp.right = node.left.right
        exp.comparator = NegatedOperatorMap[node.left.comparator]
        return exp
    else:
        return node

def apply_demorgan_rule(node):
    if (isinstance(node, dast.LogicalExpr) and
            node.operator is dast.NotOp and
            isinstance(node.left, dast.LogicalExpr) and
            node.left.operator in {dast.AndOp, dast.OrOp}):
        exp = dast.LogicalExpr(parent=node.parent, ast=node.ast,
                               op=NegatedOperatorMap[node.left.operator])
        exp.subexprs = [dast.LogicalExpr(parent=exp, ast=None,
                                         op=dast.NotOp,
                                         subexprs=[e])
                        for e in node.left.subexprs]
        iprintd("demorgan applied to " + str(node))
        return exp
    else:
        return node

def domain_for_condition(domainspec, condition):
    expr = dast.SetCompExpr(domainspec.parent)
    expr.elem = dast.TupleExpr(expr)
    expr.elem.subexprs = [dast.SimpleExpr(expr, value=v)
                          for v in domainspec.pattern.ordered_freevars]
    ds = domainspec.clone()
    expr.domains.append(ds)
    expr.conditions.append(condition)
    ds.pattern = dast.PatternExpr(domainspec)
    ds.pattern.pattern=dast.TuplePattern(
        ds.pattern,
        value=[dast.FreePattern(ds.pattern, value=fv)
               for fv in domainspec.pattern.ordered_freevars])
    iprintd("domain_for_condition: " + str(expr))
    return expr

def ast_eq(left, right):
    """Structural equality of AST nodes."""

    if type(left) is not type(right):
        return False
    if isinstance(left, list):
        if len(left) != len(right):
            return False
        for litem, ritem in zip(left, right):
            if not ast_eq(litem, ritem):
                return False
        return True
    if not isinstance(left, AST):
        return left == right
    for (left_fieldname, left_value), (right_fieldname, right_value) in \
            zip(iter_fields(left), iter_fields(right)):
        if not (left_fieldname == right_fieldname):
            break
        if not ast_eq(left_value, right_value):
            break
    else:
        return True
    return False

def is_all_wildcards(targets):
    """True if 'targets' contain only wildcards."""

    for elt in targets:
        if not (isinstance(elt, Name) and elt.id == '_'):
            return False
    return True

def mangle_name(nameobj):
    if isinstance(nameobj, dast.Event):
        # Event.name is already unique
        return nameobj.name

    namelist = []
    scope = nameobj.scope
    proc = scope.immediate_container_of_type(dast.Process)
    if proc is not None:
        namelist.append(proc.name)
        namelist.append('_')
    if scope is not proc and hasattr(scope, 'name'):
        namelist.append(scope.name)
        namelist.append('_')
    namelist.append(nameobj.name)
    return ''.join(namelist)

PREAMBLE = """
import da
ReceivedEvent = da.pat.ReceivedEvent
SentEvent = da.pat.SentEvent
{0} = None
def init(procobj):
    global {0}
    {0} = procobj.id
""".format(SELF_ID_NAME)

GLOBAL_READ = "globals()['{0}']"
GLOBAL_WRITE = "globals()['{0}'] = {1}"

def gen_query_stub(query):
    global Counter
    assert isinstance(query, dast.Expression)
    iprintd("Processing %r" % query)

    qname = QUERY_STUB_FORMAT % Counter
    Counter += 1
    params = list(set(nobj for nobj in query.ordered_nameobjs
                      if query.is_child_of(nobj.scope)
                      if not nobj.is_assigned_in(query)))
    evtex = EventExtractor()
    evtex.visit(query)
    events = list(evtex.events)
    incqu = IncInterfaceGenerator(params).visit(query)
    qrydef = FunctionDef(
        name=qname,
        args=arguments(args=([arg(mangle_name(nobj), None) for nobj in params] +
                             [arg(evt.name, None) for evt in events]),
                       vararg=None,
                       varargannotation=None,
                       kwonlyargs=[],
                       kwarg=None,
                       kwargannotation=None,
                       defaults=[],
                       kw_defaults=[]),
        decorator_list=[],
        returns=None,
        body=[Expr(Str(to_source(query.ast))),
              Return(incqu)])
    qrycall = pyCall(
        func=pyAttr(INC_MODULE_VAR, qname),
        args=[],
        keywords=([(mangle_name(arg), PythonGenerator().visit(arg))
                   for arg in params] +
                  [(evt.name, pyAttr("self", evt.name)) for evt in events]))

    return qrydef, qrycall, params, events

def gen_update_stub(nameobj, updnode):
    """Generate update stub and call node for 'nameobj'."""

    global Counter
    vname = mangle_name(nameobj)
    uname = UPDATE_STUB_FORMAT % (vname, Counter)
    Counter += 1

    params = [nobj for nobj in updnode.nameobjs
              if updnode.is_child_of(nobj.scope)]
    iprintd("".join(["*** params:"] +
                    [mangle_name(p) for p in chain(params, [nameobj])]))
    astval = IncInterfaceGenerator(params + [nameobj]).visit(updnode)
    updfun = FunctionDef(name=uname,
                         args=arguments(
                             args=([arg(mangle_name(nobj), None)
                                    for nobj in params]),
                             vararg=None,
                             kwonlyargs=[],
                             kw_defaults=[],
                             kwarg=None,
                             defaults=[]),
                         decorator_list=[],
                         returns=None,
                         # Don't add the 'return' for jb_style:
                         body=[Expr(astval) if Options.jb_style
                               else Return(astval)])
    updcall = pyCall(
        func=pyAttr(INC_MODULE_VAR, uname),
        args=[],
        keywords=[(mangle_name(arg), PythonGenerator().visit(arg))
                  for arg in params])
    return updfun, updcall

def gen_assign_stub(nameobj, stub_only=False):
    """Generate assignment stub and call node for 'nameobj'."""

    stub = """
def {1}({0}):
    if type({0}) is set:
        res = set()
        for elt in {0}:
            res.add(elt)
        {0} = res
    {0} = {0}
    return {0}
    """ if not Options.jb_style else """
def {1}({0}):
    globals()['{0}'] = {0}
    return {0}
"""
    vname = mangle_name(nameobj)
    fname = ASSIGN_STUB_FORMAT % mangle_name(nameobj)
    stubast = parse(stub.format(vname, fname)).body[0]
    if stub_only:
        return stubast
    else:
        vnode = PythonGenerator().visit(nameobj)
        stubcallast = Assign(targets=[vnode],
                             value=pyCall(func=pyAttr(INC_MODULE_VAR, fname),
                                          keywords=[(vname, vnode)]))
        return stubast, stubcallast

def gen_reset_stub(process, events):
    """Generate the event reset stub."""

    args = [evt.name for evt in process.events if evt in events]
    body = [Expr(pyCall(func=pyAttr(evt.name, "clear")))
            for evt in process.events if evt in events]
    if len(body) > 0:
        return [pyFunctionDef(name=RESET_STUB_FORMAT % process.name,
                              args=args, body=body)]
    else:
        return []

def gen_init_event_stub(event):
    """Generate the event init stub."""

    blueprint = """
def {0}():
    globals()['{1}'] = set()
    return globals()['{1}']
""" if not Options.jb_style else """
def {0}():
    globals()['{1}'] = runtimelib.Set()
    return globals()['{1}']
"""

    src = blueprint.format(INIT_STUB_FORMAT % event.name, event.name)
    return parse(src).body[0]

def gen_inc_module(daast, cmdline_args, filename=""):
    """Generates the interface file from a DistPy AST."""

    assert isinstance(daast, dast.Program)

    global Options
    global ModuleFilename
    global Counter
    Options = OptionsManager(cmdline_args, daast._compiler_options)
    ModuleFilename = filename

    module = parse(PREAMBLE)
    if Options.jb_style:
        # Additional import for jb_style
        module.body.insert(0, parse("import runtimelib").body[0])
    quex = QueryExtractor()
    quex.visit(daast)

    # Generate the query functions and accumulate set of parameters:
    all_params = set()
    all_events = set()        # Events have to be handled separately

    # Generate query stubs:
    for query in quex.queries:
        qrydef, qrycall, params, events = gen_query_stub(query)
        all_params.update(params)
        all_events.update(events)
        module.body.append(qrydef)
        query.ast_override = qrycall

    # Generate assignments and updates:
    for vobj in all_params:
        # We only need one assignment stub per variable:
        stub, stubcall = gen_assign_stub(vobj)
        module.body.append(stub)

        # Inject call to stub at all assignment points:
        for node, ctx in vobj.assignments:
            if isinstance(node, dast.Arguments):
                # this is a function or process argument
                node = node.parent
                if isinstance(node, dast.Process):
                    body = node.setup.body
                else:
                    body = node.body
                if not hasattr(body[0], "prebody"):
                    body[0].prebody = []
                assert len(body) > 0
                body[0].prebody.insert(0, stubcall)
            elif not (isinstance(node.parent, dast.Program) or
                      (isinstance(node, dast.Function) and
                       isinstance(node.parent, dast.Process))):
                # This is a normal assignment
                if not hasattr(node, "postbody"):
                    node.postbody = []
                node.postbody.append(stubcall)

        for node, attr, attrtype in vobj.updates:
            node = node.last_parent_of_type(dast.Expression)
            for query in quex.queries:
                if node.is_child_of(query):
                    printw("Update %s inside query %s is ignored!" %
                           (node, query))
                    break
                elif query is node or query.is_child_of(node):
                    printw("Query %s inside update %s is ignored!" %
                           (query, node))
                    break
            else:
                updfun, updcall = gen_update_stub(vobj, node)
                module.body.append(updfun)
                node.ast_override = updcall

    # Generate stubs for events:
    for event in all_events:
        uname = UPDATE_STUB_FORMAT % (event.name, 0)
        aname = ASSIGN_STUB_FORMAT % event.name
        updfun = pyFunctionDef(name=uname,
                               args=[event.name, "element"],
                               body=[Expr(pyCall(
                                   func=pyAttr(event.name, "add"),
                                   args=[pyName("element")]))])
        module.body.append(gen_assign_stub(event, stub_only=True))
        module.body.append(gen_init_event_stub(event))
        module.body.append(updfun)

    # Inject calls to stub init for each process:
    for proc in daast.processes:
        module.body.extend(gen_reset_stub(proc, all_events))
        setup_body = proc.setup.body
        if not hasattr(setup_body[0], "prebody"):
            setup_body[0].prebody = []
        setup_body[0].prebody.insert(
            0, Expr(pyCall(
                func=pyAttr(INC_MODULE_VAR, "init"),
                args=[pyName("self")])))

    # Generate the main python file:
    pyast = StubcallGenerator(all_events).visit(daast)
    return module, pyast

class StubcallGenerator(PythonGenerator):
    """Transforms DistPy AST into Python AST with calls to incrementalization
    stubs injected.

    """

    def __init__(self, events):
        super().__init__()
        self.events = events
        self.preambles.append(Assign([pyName(INC_MODULE_VAR)], pyNone()))

    def history_initializers(self, node):
        return [Assign(
            targets=[pyAttr("self", evt.name)],
            value=(pyCall(func=pyAttr(INC_MODULE_VAR,
                                      INIT_STUB_FORMAT % evt.name))
                   if evt in self.events else pyList([])))
                for evt in node.events if evt.record_history]


    def history_stub(self, node):
        if node.record_history and node in self.events:
            return pyAttr(INC_MODULE_VAR,
                          UPDATE_STUB_FORMAT %
                          (node.name, 0))
        else:
            return super().history_stub(node)

    def visit_ResetStmt(self, node):
        proc = node.first_parent_of_type(dast.Process)
        # Call the inc reset stub for all events used in queries:
        stmts = [Expr(pyCall(func=pyAttr(INC_MODULE_VAR,
                                         RESET_STUB_FORMAT % proc.name),
                             args=[pyAttr("self", evt.name)
                                   for evt in proc.events
                                   if evt.record_history
                                   if evt in self.events]))]
        # Clear all remaining events:
        stmts.extend([Expr(pyCall(func=pyAttr(pyAttr("self", evt.name),
                                              "clear")))
                      for evt in proc.events
                      if evt.record_history if evt not in self.events])
        return stmts

class EventExtractor(NodeVisitor):
    """Extracts event specs from queries.

    HistoryDomainSpec and HistoryExpr contain events, these need to be
    extracted in order to generate the correct query parameters.
    """

    def __init__(self):
        self.events = set()

    def visit_Event(self, node):
        self.events.add(node)

class QueryExtractor(NodeVisitor):
    """Extracts expensive queries.

    'Expensive' queries are quantified expressions and comprehensions.

    """

    def __init__(self):
        self.queries = []
        self.processes = []

    def visit_QuantifiedExpr(self, node):
        if node.first_parent_of_type(dast.Process) is None:
            return
        # We try to find the largest node that contains the query:
        par = node.last_parent_of_type(dast.BooleanExpr)
        if par is None:
            par = node
        if par not in self.queries:
            self.queries.append(par)

    def visit_ComplexExpr(self, node):
        if node.first_parent_of_type(dast.Process) is None:
            return
        par = node.last_parent_of_type(dast.Expression)
        if par is None:
            par = node
        if par not in self.queries:
            self.queries.append(par)

    visit_GeneratorExpr = visit_ComplexExpr
    visit_SetCompExpr = visit_ComplexExpr
    visit_ListCompExpr = visit_ComplexExpr
    visit_DictCompExpr = visit_ComplexExpr
    visit_TupleCompExpr = visit_ComplexExpr
    visit_ReceivedExpr = visit_ComplexExpr
    visit_SentExpr = visit_ComplexExpr


class IncInterfaceGenerator(PatternComprehensionGenerator):
    """Transforms DistPy patterns to Python comprehension.

    """

    def __init__(self, mangle_names=None, last_freevars=None):
        super().__init__()
        self.mangle_names = mangle_names if mangle_names is not None else set()
        # Set of free vars seens so far in this pattern.
        # This is needed so free vars with the same name in the pattern can be
        # properly unified:
        self.freevars = set() if last_freevars is None else last_freevars

    def reset_pattern_state(self):
        """Resets pattern related parser states.

        Call this before starting to parse a new pattern.

        """
        self.freevars.clear()

    def reset(self):
        super().reset()
        self.reset_pattern_state()

    def jb_tuple_optimize(self, elt):
        """Eliminate single element tuples for jb_style."""
        if Options.jb_style:
            if type(elt) is Tuple and len(elt.elts) == 1:
                elt = elt.elts[0]
        return elt

    def visit_NamedVar(self, node):
        if node in self.mangle_names:
            return pyName(mangle_name(node))
        else:
            return pyName(node.name)

    def visit_AttributeExpr(self, node):
        if isinstance(node.value, dast.SelfExpr):
            if node.attr == 'id':
                return pyName(SELF_ID_NAME)
            else:
                nameobj = node.first_parent_of_type(dast.Process).\
                          find_name(node.attr)
                return self.visit_NamedVar(nameobj)
        else:
            return super().visit_AttributeExpr(node)

    def visit_TuplePattern(self, node):
        condition_list = []
        targets = []
        for elt in node.value:
            tgt, conds = self.visit(elt)
            targets.append(tgt)
            condition_list.extend(conds)
        if is_all_wildcards(targets):
            # Optimization: combine into one '_'
            return pyName('_'), []
        target = pyTuple(targets)
        if Options.jb_style:
            # XXX: Hack!
            if len(targets) == 1:
                target = targets[0]
                if len(condition_list) == 1:
                    assert isinstance(condition_list[0], Compare) and \
                        len(condition_list[0].comparators) == 1
                    condition_list[0].comparators[0] = pyTuple(
                        [condition_list[0].comparators[0]])
        return target, condition_list

    def visit_PatternExpr(self, node):
        target, conds = self.visit(node.pattern)
        return target, conds
    visit_LiteralPatternExpr = visit_PatternExpr

    def visit_ComparisonExpr(self, node):
        if isinstance(node.left, dast.PatternExpr):
            # 'PATTERN in DOMAIN'
            target, conds = self.visit(node.left)
            right = self.visit(node.right)
            gen = comprehension(target, right, conds)
            elem = self.jb_tuple_optimize(pyTuple(node.left.ordered_freevars))
            ast = pySize(ListComp(elem, [gen])) if not Options.jb_style \
                  else pySize(SetComp(elem, [gen]))
            return ast
        else:
            return super().visit_ComparisonExpr(node)

    def visit_ComprehensionExpr(self, node):
        generators = [self.visit(dom) for dom in node.domains]
        generators[-1].ifs.extend([self.visit(cond) for cond in
                                   node.conditions
                                   # Don't add redundant 'if True's:
                                   if not isinstance(cond, dast.TrueExpr)])

        if type(node) is dast.DictCompExpr:
            key = self.visit(node.elem.key)
            value = self.visit(node.elem.value)
            ast = DictComp(key, value, generators)
        else:
            elem = self.visit(node.elem)
            if isinstance(node, dast.SetCompExpr):
                ast = SetComp(elem, generators)
            elif isinstance(node, dast.ListCompExpr):
                ast = ListComp(elem, generators)
            elif isinstance(node, dast.TupleCompExpr):
                ast = pyCall("tuple", args=[GeneratorExp(elem, generators)])
            elif isinstance(node, dast.GeneratorExpr):
                ast = GeneratorExp(elem, generators)
            else:
                iprintw("Warning: unknown comprehension type!")
                ast = SetComp(elem, generators)
        return ast

    visit_GeneratorExpr = visit_ComprehensionExpr
    visit_SetCompExpr = visit_ComprehensionExpr
    visit_ListCompExpr = visit_ComprehensionExpr
    visit_DictCompExpr = visit_ComprehensionExpr
    visit_TupleCompExpr = visit_ComprehensionExpr

    def visit_Event(self, node):
        assert node.type is not None
        self.reset_pattern_state()
        typ = pyName("_EventType_")
        evtconds = [pyCompare(typ, Is,
                              pyName(node.type.__name__))]
        msg, msgconds = self.visit(node.pattern)
        evtconds += msgconds
        srcconds = []
        if len(node.sources) > 0:
            if len(node.sources) > 1:
                raise NotImplementedError(
                    "Multiple sources in event spec not supported.")
            src, cond = self.visit(node.sources[0])
            evtconds += cond
        else:
            src = pyName("_Source_")
        if len(node.destinations) > 0:
            if len(node.destinations) > 1:
                raise NotImplementedError(
                    "Multiple destinations in event spec not supported.")
            dst, cond = self.visit(node.destinations[0])
            evtconds += cond
        else:
            dst = pyName("_Destination_")
        if len(node.timestamps) > 0:
            if len(node.timestamps) > 1:
                raise NotImplementedError(
                    "Multiple timestamps in event spec not supported.")
            clk, cond = self.visit(node.timestamps[0])
            evtconds += cond
        else:
            clk = pyName("_Timestamp_")
        env = pyTuple([clk, dst, src])
        return pyTuple([typ, env, msg]), evtconds

    def visit_DomainSpec(self, node):
        if isinstance(node.pattern, dast.PatternExpr):
            target, ifs = self.visit(node.pattern)
            domain = self.visit(node.domain)
        else:
            target = self.visit(node.pattern)
            ifs = []
            domain = self.visit(node.domain)
        return comprehension(target, domain, ifs)

    def visit_HistoryExpr(self, node):
        return pyName(node.event.name)

    visit_ReceivedExpr = visit_HistoryExpr
    visit_SentExpr = visit_HistoryExpr

    def visit_QuantifiedExpr(self, node):
        assert node.predicate is not None

        if not (Options.no_table4 or Options.no_all_tables):
            iprintd("Trying table4...")
            res = self.do_table4_transformation(node)
            if res is not None:
                return res
        if not (Options.no_table3 or Options.no_all_tables):
            iprintd("Trying table3...")
            res = self.do_table3_transformation(node)
            if res is not None:
                return res
        if not (Options.no_table2 or Options.no_all_tables):
            iprintd("Trying table2...")
            res = self.do_table2_transformation(node)
            if res is not None:
                return res
        if not (Options.no_table1 or Options.no_all_tables):
            iprintd("Trying table1...")
            res = self.do_table1_transformation(node)
            if res is not None:
                return res

        # Fallback to 'any' and 'all'
        generators = [self.visit(dom) for dom in node.domains]
        elt = self.jb_tuple_optimize(self.visit(node.predicate))
        gexp = GeneratorExp(elt, generators)
        if node.operator is dast.UniversalOp:
            func = "all"
        else:
            func = "any"
        return pyCall(func, [gexp])

    def do_table1_transformation(self, node):
        """Transformation defined in Table 1 of OOPSLA paper.

        This transformation converts quantificactions into aggregates.

        """
        cond = self.visit(node.predicate)
        for dom in reversed(node.domains):
            elt = self.jb_tuple_optimize(
                pyTuple([self.visit(name) for name in dom.freevars]))
            domast = self.visit(dom)
            gexp = SetComp(elt, [domast])
            if node.operator is dast.UniversalOp:
                domast.ifs.append(pyNot(cond))
                cond = pyCompare(pySize(gexp), Eq, Num(0))
            else:
                # Existential
                domast.ifs.append(cond)
                cond = pyCompare(pySize(gexp), Gt, Num(0))
        return cond


    def do_table2_transformation(self, node):
        """Transformation defined in Table 2 of OOPSLA paper.

        This transformation converts nested quantificactions into aggregates.
        It supports up to one level of alternation; the non-alternating case
        is simply a generalization of Table 1.

        """
        res = None
        outer_quantifier = node.operator
        inner_quantifier = None
        outer_generators = []
        inner_generators = []
        elements = []
        while (isinstance(node, dast.QuantifiedExpr) and
               (inner_quantifier is None or
                node.operator is inner_quantifier)):
            if node.operator is not outer_quantifier:
                inner_quantifier = node.operator
            if inner_quantifier is None:
                outer_generators += [self.visit(dom) for dom in node.domains]
                elements += [self.visit(name)
                             for dom in node.domains
                             for name in dom.freevars]
            else:
                inner_generators += [self.visit(dom) for dom in node.domains]
            node = node.predicate

        assert node is not None
        elements = self.jb_tuple_optimize(pyTuple(elements))
        generators = outer_generators + inner_generators
        bexp = self.visit(node)
        if inner_quantifier is None:
            # Non-alternating:
            if outer_quantifier is dast.UniversalOp:
                bexp = pyNot(bexp)
                generators[-1].ifs.append(bexp)
                res = pyCompare(pySize(SetComp(elements, generators)), Eq, Num(0))
            else:
                generators[-1].ifs.append(bexp)
                res = pyCompare(pySize(SetComp(elements, generators)), Gt, Num(0))

        else:
            # One-level alternation:
            if len(outer_generators) == 1 and len(outer_generators[0].ifs) == 0:
                # Special case: if only one set with no conditions then no
                # need to wrap it in a comprehension:
                right = pySize(outer_generators[0].iter)
            else:
                right = pySize(SetComp(elements, outer_generators))
            if outer_quantifier is dast.UniversalOp:
                generators[-1].ifs.append(bexp)
                left = pySize(SetComp(elements, generators))
                res = pyCompare(left, Eq, right)
            else:
                generators[-1].ifs.append(pyNot(bexp))
                left = pySize(SetComp(elements, generators))
                res = pyCompare(left, NotEq, right)

        return res

    def do_table4_transformation(self, node):
        """Transformation defined in Table 4 of OOPSLA paper.

        This transformation breaks up quantification conditions involving
        logical operators (and/or) in the hopes of creating opportunities to
        apply table 3.

        Note 1: unlike the other 3 transformations which generate Python ASTs,
        this transformation generates DistPy AST (which must then be further
        passed through other transformations or the default translator to
        obtain a Python AST)

        Note 2: since Python lacks a "implies" logical operator, rules 3 and 6
        of table 4 are ignored.

        """
        pred = apply_demorgan_rule(node.predicate)
        if isinstance(pred, dast.LogicalExpr):
            # Rule 1:
            if (pred.operator is dast.AndOp and
                    node.operator is dast.ExistentialOp):
                iprintd("do_table4_transformation: using rule 1")
                for i, e in enumerate(pred.subexprs):
                    newnode = node.clone()
                    newnode.domains[-1].domain = domain_for_condition(
                        newnode.domains[-1], e)
                    newnode.subexprs = pred.subexprs[0:i] + pred.subexprs[(i+1):]
                    res = self.do_table3_transformation(newnode)
                    if res is not None:
                        return res
            # Rule 2:
            elif (pred.operator is dast.OrOp and
                    node.operator is dast.ExistentialOp):
                iprintd("do_table4_transformation: using rule 2")
                expr = dast.LogicalExpr(node.parent)
                expr.operator = dast.OrOp
                for cond in pred.subexprs:
                    expr.subexprs.append(node.clone())
                    expr.subexprs[-1].subexprs = [cond]
                return self.visit(expr)
            # Rule 4:
            elif (pred.operator is dast.AndOp and
                    node.operator is dast.UniversalOp):
                iprintd("do_table4_transformation: using rule 4")
                expr = dast.LogicalExpr(node.parent)
                expr.operator = dast.AndOp
                for cond in pred.subexprs:
                    expr.subexprs.append(node.clone())
                    expr.subexprs[-1].subexprs = [cond]
                return self.visit(expr)
            # Rule 5:
            elif (pred.operator is dast.OrOp and
                    node.operator is dast.UniversalOp):
                iprintd("do_table4_transformation: using rule 5")
                for idx, e in enumerate(pred.subexprs):
                    e = e.clone()
                    outer = dast.LogicalExpr(e.parent,
                                             op=dast.NotOp,
                                             subexprs=[e])
                    e._parent = outer
                    newnode = node.clone()
                    newnode.domains[-1].domain = domain_for_condition(
                        newnode.domains[-1], e)
                    newnode.subexprs = [s for i, s in enumerate(pred.subexprs)
                                        if i != idx]
                    res = self.do_table3_transformation(newnode)
                    if res is not None:
                        return res

        return None

    def do_table3_transformation(self, node):
        """Transformation defined in Table 3 of OOPSLA paper.

        This transforms single quantifications over comparisons into min/max
        aggregates. Returns None if table 3 can not be applied to 'node'.

        """
        # We can only handle comparisons:
        pred = combine_not_comparison(node.predicate)
        if not (isinstance(pred, dast.ComparisonExpr) and
                pred.comparator in
                {dast.LtOp, dast.LtEOp, dast.GtOp, dast.GtEOp}):
            iprintd("Table 3 can not be applied to %s: not comparison" % node)
            return None

        # All free variables must appear on one side of the comparison:
        x = y = None
        left = {name for name in pred.left.nameobjs if name in node.freevars}
        right = {name for name in pred.right.nameobjs if name in node.freevars}
        if len(left) > 0 and len(right) == 0:
            x = pred.left
            y = pred.right
        elif len(left) == 0 and len(right) > 0:
            x = pred.right
            y = pred.left
        else:
            iprintd("Table 3 can not be applied to %s: free var distribution." %
                   node)
            return None

        pyx = self.visit(x)
        pyy = self.visit(y)
        generators = [self.visit(dom) for dom in node.domains]
        if (len(generators) == 1 and len(generators[0].ifs) == 0 and
                ast_eq(pyx, generators[0].target)):
            s = sp = generators[0].iter
        else:
            pyx = self.jb_tuple_optimize(pyx)
            s = SetComp(pyx, generators)
            if Options.jb_style:
                # jb_style can not handle generator expressions:
                sp = s
            else:
                sp = GeneratorExp(pyx, generators)

        if y is pred.right:
            y_comp_op = NegatedOperatorMap[pred.comparator]
        else:
            y_comp_op = OperatorMap[pred.comparator]
        if node.operator is dast.UniversalOp:
            set_comp_op = Eq
            logical_op = Or
            if y_comp_op in {Gt, GtE}:
                y_comp_right = pyMax(sp)
            else:
                y_comp_right = pyMin(sp)
        else:
            set_comp_op = NotEq
            logical_op = And
            if y_comp_op in {Lt, LtE}:
                y_comp_right = pyMax(sp)
            else:
                y_comp_right = pyMin(sp)

        return BoolOp(op=logical_op(),
                      values=[Compare(left=pySize(s),
                                      ops=[set_comp_op()],
                                      comparators=[Num(0)]),
                              Compare(left=pyy,
                                      ops=[y_comp_op()],
                                      comparators=[y_comp_right])])

if __name__ == "__main__":
    ast1 = parse("[(c2, p2)]")
    ast2 = parse("[(c1, p2)]")
    print(ast_eq(ast1, ast2))
