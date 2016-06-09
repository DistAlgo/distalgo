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

import sys

from ast import *
from collections import OrderedDict
from . import dast
from .pygen import *
from .parser import Pattern2Constant
from .utils import printe, printd, printw, OptionsManager, to_source

INC_MODULE_VAR = "IncModule"

QUERY_STUB_FORMAT = "Query_%d"
ASSIGN_STUB_FORMAT = "Assign_%s"
DEL_STUB_FORMAT = "Delete_%s"
RESET_STUB_FORMAT = "Reset_%s_Events"
INIT_STUB_PREFIX = "Init_"
UPDATE_STUB_PREFIX = "Update_"
LOCAL_WITNESS_SET = "__witness_set__"
LOCAL_RESULT_VAR = "__result__"
GLOBAL_WITNESS_VAR = "Witness"
SELF_ID_NAME = "SELF_ID"
JB_STYLE_MODULE = "incoq.runtime"
JB_STYLE_SET = "Set"
JB_STYLE_MAP = "Map"

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

##################################################
# Global states:

Options = None                  # Command line options
ModuleFilename = ""

##########
# Auxiliary methods:

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
        exp.left._parent = exp
        exp.right._parent = exp
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
        for e in exp.subexprs:
            e.left._parent = e
        return exp
    else:
        return node

def domain_for_condition(domainspec, condition):
    expr = dast.SetCompExpr(domainspec.parent)
    expr.conditions.append(domainspec.clone())
    expr.conditions.append(condition)
    if len(domainspec.pattern.ordered_freevars) == 1:
        expr.elem = dast.SimpleExpr(expr)
        expr.elem.value = domainspec.pattern.ordered_freevars[0]
        domainspec.pattern = dast.PatternExpr(domainspec)
        domainspec.pattern.pattern = dast.FreePattern(
            domainspec.pattern, value=expr.elem.value)
    else:
        expr.elem = dast.TupleExpr(expr)
        expr.elem.subexprs = [dast.SimpleExpr(expr.elem, value=v)
                              for v in domainspec.pattern.ordered_freevars]
        domainspec.pattern = dast.PatternExpr(domainspec)
        domainspec.pattern.pattern = dast.TuplePattern(
            domainspec.pattern,
            value = [dast.FreePattern(domainspec.pattern, value=fv.value)
                     for fv in expr.elem.subexprs])
    domainspec.domain = expr
    iprintd("domain_for_condition: " + str(expr))
    return expr

def optimize_tuple(elt):
    """Expand single element tuples."""

    if type(elt) is Tuple and len(elt.elts) == 1:
        elt = elt.elts[0]
    return elt

def append_unique(alist, newelts):
    """Append unique new elements from 'newelts' to 'alist'.

    Order is preserved.
    """
    for elt in newelts:
        if elt not in alist:
            alist.append(elt)

def uniquify(alist):
    res = []
    append_unique(res, alist)
    return res

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
    """Generate a name for 'nameobj' that is unique in the flat namespace of the
    inc module.

    """

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
{jbstyle_import}
"""
# Inc directives goes between `PREAMBLE' and `DEFINITIONS'
DEFINITIONS = """
ReceivedEvent = da.pat.ReceivedEvent
SentEvent = da.pat.SentEvent
{self_name} = None
{witness_var} = None
JbStyle = {is_jbstyle}
"""

# Additional directives needed by jbstyle:
JBSTYLE_DIRECTIVES = """""".format(JB_STYLE_MODULE, GLOBAL_WITNESS_VAR)

GLOBAL_READ = "globals()['{0}']"
GLOBAL_WRITE = "globals()['{0}'] = {1}"

def extract_all_queries(distalgo_ast):
    """Return a list of all queries in given ast node."""

    quex = QueryExtractor()
    quex.visit(distalgo_ast)
    return uniquify(quex.queries)

def extract_query_parameters(query):
    """Return list of parameters of 'query'."""

    return uniquify(nobj for nobj in query.ordered_nameobjs
                    if query.is_child_of(nobj.scope)
                    # NOTE: this assumes a name can not both be a parameter
                    # and assigned to inside the query, which is not true
                    # for Python in general, but is true for DistAlgo
                    # queries that we can handle:
                    if not nobj.is_assigned_in(query))

def extract_query_events(query):
    """Return set of events used in 'query'."""

    ev = EventExtractor()
    ev.visit(query)
    return uniquify(ev.events)

def process_query(query, state):
    """Generates stub and hook for 'query'."""

    assert isinstance(query, dast.Expression)

    iprintd("Processing %r" % query)
    qname = QUERY_STUB_FORMAT % state.counter
    state.counter += 1
    params = extract_query_parameters(query)
    events = extract_query_events(query)
    append_unique(state.parameters, params)
    append_unique(state.events, events)
    iig = IncInterfaceGenerator(params)
    incqu = iig.visit(query)
    if iig.witness_set is None:
        return _process_nonwitness_query(qname, params, events, query, incqu)
    else:
        return _process_witness_query(qname, params, events,
                                      query, incqu, iig.witness_set)

def _process_nonwitness_query(stub_name, params, events, query, inc_query):
    qrydef = pyFunctionDef(
        name=stub_name,
        args=([mangle_name(nobj) for nobj in params] +
              [evt.name for evt in events]),
        decorator_list=[],
        returns=None,
        body=[Expr(Str(to_source(query.ast))),
              Return(inc_query)])
    # Replace the query node in the main module with the hook:
    qryhook = pyCall(
        func=pyAttr(INC_MODULE_VAR, stub_name),
        args=[],
        keywords=([(mangle_name(arg), PythonGenerator().visit(arg))
                   for arg in params] +
                  [(evt.name, pyAttr("self", evt.name)) for evt in events]))
    query.ast_override = qryhook
    # Replace the query node in the inc module with the hook:
    inchook = pyCall(
        func=stub_name,
        args=[],
        keywords=([(mangle_name(arg), pyName(mangle_name(arg)))
                   for arg in params] +
                  [(evt.name, pyName(evt.name)) for evt in events]))
    query.inc_query_override = inchook
    return qrydef

def _process_witness_query(stub_name, params, events,
                           query, inc_query, witness_set):
    # Query stub in inc:
    qrydef = pyFunctionDef(
        name=stub_name,
        args=([mangle_name(nobj) for nobj in params] +
              [evt.name for evt in events]),
        body=[Expr(Str(to_source(query.ast))),
              Global([GLOBAL_WITNESS_VAR]),
              Try([Assign([pyName(GLOBAL_WITNESS_VAR)],
                          pyCall("next", [pyCall("iter", [witness_set])]))],
                  [ExceptHandler(pyName("StopIteration"), None,
                                 [Assign([pyName(GLOBAL_WITNESS_VAR)],
                                         pyNone())])],
                  [], []),
              Return(inc_query)])

    # Witness propagation function in main:
    qryfunc = pyFunctionDef(
        name=query.name,
        # We know 'query' must be top-level, so no need for any arguments:
        args=[],
        body=[
            # IncInterfaceGenerator checks query has non-empty
            # ordered_local_freevars before assigning witness, so we are
            # guaranteed non-empty here:
            Nonlocal([fv.name for fv in query.ordered_local_freevars]),
            Assign([pyName(LOCAL_RESULT_VAR)],
                   pyCall(
                       func=pyAttr(INC_MODULE_VAR, stub_name),
                       args=[],
                       keywords=([(mangle_name(arg),
                                   PythonGenerator().visit(arg))
                                  for arg in params] +
                                 [(evt.name, pyAttr("self", evt.name))
                                  for evt in events]))),
            If(pyName(LOCAL_RESULT_VAR),
               [Assign([optimize_tuple(pyTuple(
                   [pyName(fv.name) for fv in query.ordered_local_freevars]))],
                       pyAttr(INC_MODULE_VAR, GLOBAL_WITNESS_VAR)),
                Return(pyTrue())],
               [Return(pyFalse())])])
    # Replace query with call to witness propagation function:
    query.ast_override = pyCall(func=query.name)
    if not hasattr(query, 'prebody'):
        query.prebody = []
    query.prebody.extend([Assign(
        targets=[pyName(nv.name) for nv in query.ordered_local_freevars],
        value=pyNone()),
                          qryfunc])

    # Replace the query node in the inc module with the hook:
    inchook = pyCall(
        func=stub_name,
        args=[],
        keywords=([(mangle_name(arg), pyName(mangle_name(arg)))
                   for arg in params] +
                  [(evt.name, pyName(evt.name)) for evt in events]))
    query.inc_query_override = inchook
    return qrydef


def process_all_queries(queries, state):
    """Generates query stubs for given queries."""

    query_stubs = [process_query(query, state) for query in queries]
    state.module.body.extend(query_stubs)

def gen_update_stub_name_for_node(updnode, state):
    """Generates an update stub name for the given update node.

    Generated name is based on the query parameters updated in 'updnode'. A
    counter is appended to the end to ensure uniqueness.

    """

    name_comps = UPDATE_STUB_PREFIX + \
                 "".join([mangle_name(nameobj)
                          for nameobj in state.updates[updnode]] +
                         [str(state.counter)])
    state.counter += 1
    return name_comps

def generate_update_stub(updnode, state):
    """Generate update stub and hook for 'updnode'."""

    uname = gen_update_stub_name_for_node(updnode, state)
    params = uniquify(nobj for nobj in updnode.ordered_nameobjs
                      if updnode.is_contained_in(nobj.scope))
    astval = IncInterfaceGenerator(params).visit(updnode)
    # the body depends on the syntactic type of update we're handling:
    if isinstance(updnode, dast.Expression):
        if Options.jb_style and type(updnode.parent) is dast.SimpleStmt:
            # jbstyle doesn't like updates as expressions, so just don't
            # return anything and hope for the best:
            body = [Expr(astval)]
        else:
            body = [Return(astval)]
    elif isinstance(updnode, dast.AssignmentStmt):
        body = astval
    elif isinstance(updnode, dast.DeleteStmt):
        body = astval

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
                         body=body)
    updhook = pyCall(
        func=pyAttr(INC_MODULE_VAR, uname),
        args=[],
        keywords=[(mangle_name(arg), PythonGenerator().visit(arg))
                  for arg in params])
    if isinstance(updnode, dast.Expression):
        updnode.ast_override = updhook
    else:
        updnode.ast_override = [Expr(updhook)]
    return updfun

def process_updates(state):
    # Accumulate all updates:
    for vobj in state.parameters:
        for node, _, _ in vobj.updates:
            if state.updates.get(node) is None:
                state.updates[node] = list()
            state.updates[node].append(vobj)

    for node in state.updates:
        for query in state.queries:
            if node.is_child_of(query):
                # We can not handle queries with side-effects:
                iprintw("Update %s inside query %s is ignored!" %
                        (node, query))
                break
        else:
            state.module.body.append(generate_update_stub(node, state))

STUB_ASSIGN = """
def {1}(_{0}):
    global {0}
    if type(_{0}) is set:
        {0} = set()
        for elt in {0}:
            {0}.add(elt)
    {0} = _{0}
    return {0}
"""
STUB_ASSIGN_JB = """
def {1}(_{0}):
    global {0}
    {0} = _{0}
    return {0}
"""
STUB_ASSIGN_JB_SET = """
def {1}(_{0}):
    {0}.copy_update(_{0})
    return {0}
"""
STUB_ASSIGN_JB_MAP = """
def {1}(_{0}):
    {0}.dictcopy_update(_{0})
    return {0}
"""
STUB_DELETE = """
def {1}():
    global {0}
    del {0}
"""

def generate_assignment_stub_and_hook(nameobj):
    """Generate assignment stub and hook node for 'nameobj'.

    Assignment stubs notifies the incrementalizer of changes to value of
    variables, and gives the incrementalizer a chance to perform necessary
    housekeeping.

    """

    assert isinstance(nameobj, dast.NamedVar)
    vname = mangle_name(nameobj)
    fname = ASSIGN_STUB_FORMAT % vname
    if Options.jb_style:
        if nameobj.is_a("set"):
            stub = STUB_ASSIGN_JB_SET
        elif nameobj.is_a("dict"):
            stub = STUB_ASSIGN_JB_MAP
        else:
            stub = STUB_ASSIGN_JB
    else:
        stub = STUB_ASSIGN
    stubast = parse(stub.format(vname, fname)).body[0]
    vnode = PythonGenerator().visit(nameobj)
    stubcallast = Assign(targets=[vnode],
                         value=pyCall(func=pyAttr(INC_MODULE_VAR, fname),
                                      keywords=[("_" + vname, vnode)]))
    return stubast, stubcallast

def generate_deletion_stub_and_hook(nameobj):
    """Generate delete stub and hook node for 'nameobj'."""

    vname = mangle_name(nameobj)
    fname = DEL_STUB_FORMAT % mangle_name(nameobj)
    stub = parse(STUB_DELETE.format(vname, fname)).body[0]
    hook = Expr(value=pyCall(func=pyAttr(INC_MODULE_VAR, fname)))
    return stub, hook

def process_assignments_and_deletions(state):
    """Generate stub and hook for assignments and deletions.

    This should be called after the query parameters have been accumulated.

    """

    for vobj in state.parameters:
        # We only need one assignment or delete stub per variable:
        stub, hook = generate_assignment_stub_and_hook(vobj)
        del_stub, del_hook = generate_deletion_stub_and_hook(vobj)
        has_assign, has_del = False, False

        # Inject call to stub at all assignments to vobj:
        for node, ctx in vobj.assignments:
            if (isinstance(node.parent, dast.Program) or
                (isinstance(node, dast.Function) and
                 isinstance(node.parent, dast.Process))):
                continue

            if isinstance(node, dast.Arguments):
                # this is a function or process argument
                node = node.parent
                if isinstance(node, dast.Process):
                    body = node.setup.body
                else:
                    body = node.body
                assert len(body) > 0
                if not hasattr(body[0], "prebody"):
                    body[0].prebody = []
                body[0].prebody.insert(0, hook)
                has_assign = True
            elif isinstance(node, dast.AssignmentStmt):
                # This is a normal assignment
                if not hasattr(node, "postbody"):
                    node.postbody = []
                node.postbody.append(hook)
                has_assign = True
            elif isinstance(node, dast.DeleteStmt):
                # This is a del assignment
                if not hasattr(node, "prebody"):
                    node.prebody = []
                node.prebody.append(del_hook)
                has_del = True
            elif isinstance(node, dast.ForStmt) or \
                 isinstance(node, dast.WithStmt):
                first = node.body[0]
                if not hasattr(first, "prebody"):
                    first.prebody = []
                first.prebody.append(hook)
                has_assign = True
            elif isinstance(node, dast.TryStmt):
                # We need to find the except handler that binds this name
                for handler in node.excepthandlers:
                    if handler.name == vobj.name:
                        first = handler.body[0]
                        if not hasattr(first, "prebody"):
                            first.prebody = []
                        first.prebody.append(hook)
                        has_assign = True
            state.assignments.append(node)

        if has_assign:
            state.module.body.append(stub)
        if has_del:
            state.module.body.append(del_stub)

def generate_event_reset_stub(process, state):
    """Generate the event reset stub for 'process'."""

    finder = NodetypeFinder(dast.ResetStmt)
    # Only generate the stub if the process contains a 'reset' statement:
    finder.visit(process)
    if finder.found:
        args = [evt.name for evt in process.events if evt in state.events]
        body = [Expr(pyCall(func=pyAttr(evt.name, "clear")))
                for evt in process.events if evt in state.events]
        if len(body) > 0:
            return [pyFunctionDef(name=RESET_STUB_FORMAT % process.name,
                                  args=args, body=body)]
    return []

def process_events(state):
    """Generate stubs for events."""

    for event in state.events:
        uname = UPDATE_STUB_PREFIX + event.name
        aname = ASSIGN_STUB_FORMAT % event.name
        updfun = pyFunctionDef(name=uname,
                               args=[event.name, "element"],
                               body=[Expr(pyCall(
                                   func=pyAttr(event.name, "add"),
                                   args=[pyName("element")]))])
        state.module.body.append(updfun)
    for proc in state.input_ast.processes:
        state.module.body.extend(generate_event_reset_stub(proc, state))

STUB_INIT = """
def {name}():
    global {var}
    {var} = {type}()
    return {var}
"""

def generate_initializer_stub(varname, typename):
    """Generate initializer stub for given name."""

    src = STUB_INIT.format(name=(INIT_STUB_PREFIX + varname),
                           var=varname, type=typename)
    return parse(src).body[0]

INIT_FUN = """
def init(procobj):
    global {self_name}
    {self_name} = procobj.id
"""

def process_initializers(state):
    """Generate initializer stubs for parameters."""

    for event in state.events:
        state.module.body.append(generate_initializer_stub(
            varname=event.name,
            typename=JB_STYLE_SET if Options.jb_style else "set"))

    initnames = []
    if Options.jb_style:
        for nameobj in state.parameters:
            stub = None
            if nameobj.is_a("set"):
                stub = generate_initializer_stub(
                    varname=mangle_name(nameobj),
                    typename=JB_STYLE_SET)
            elif nameobj.is_a("dict"):
                stub = generate_initializer_stub(
                    varname=mangle_name(nameobj),
                    typename=JB_STYLE_MAP)
            if stub is not None:
                state.module.body.append(stub)
                initnames.append(mangle_name(nameobj))

    init = parse(INIT_FUN.format(self_name=SELF_ID_NAME)).body[0]
    if Options.jb_style:
        # For jb_style: inject calls to initalizers. This sets up the set
        # and map objects in the inc module namespace:
        init.body.extend([Expr(pyCall(INIT_STUB_PREFIX + name))
                          for name in initnames])
    state.module.body.append(init)

def process_setups(state):
    """Inject calls to stub initializers from 'setup'."""

    for proc in state.input_ast.processes:
        setup_body = proc.setup.body
        if not hasattr(setup_body[0], "prebody"):
            setup_body[0].prebody = []

        his_init = [Assign(
            targets=[pyAttr("self", evt.name)],
            value=(pyCall(func=pyAttr(INC_MODULE_VAR,
                                      INIT_STUB_PREFIX + evt.name))
                   if evt in state.events else pyList([])))
                    for evt in proc.events if evt.record_history]
        incmodule_init = [Expr(pyCall(
            func=pyAttr(INC_MODULE_VAR, "init"),
            args=[pyName("self")]))]
        setup_body[0].prebody = incmodule_init + his_init + \
                                setup_body[0].prebody


class CompilerState:
    """States shared by different parts of the compiler."""

    def __init__(self, input):
        self.counter = 0        # For unique names
        self.input_ast = input
        self.queries = []
        self.parameters = []
        self.events = []
        self.assignments = []
        self.updates = OrderedDict()
        self.module = None

class StubfileGenerationException(Exception): pass

def flatten_opassignments(state):
    transformer = OpAssignmentTransformer()
    transformer.visit(state.input_ast)

def generate_header(state):
    module = parse(
        PREAMBLE.format(jbstyle_import=("from " + JB_STYLE_MODULE + " import *"
                                        if Options.jb_style else "")))
    module.body.extend(state.input_ast.directives)
    module.body.extend(parse(
        DEFINITIONS.format(self_name=SELF_ID_NAME,
                           witness_var=GLOBAL_WITNESS_VAR,
                           is_jbstyle=str(Options.jb_style))).body)
    state.module = module

def translate_with_stubs(state):
    sg = StubcallGenerator(state)
    try:
        return sg.visit(state.input_ast)
    except Exception as ex:
        raise StubfileGenerationException(sg.current_node) from ex

def gen_inc_module(daast, cmdline_args, filename=""):
    """Generates the interface file from a DistPy AST."""

    assert isinstance(daast, dast.Program)

    global Options, ModuleFilename

    Options = OptionsManager(cmdline_args, daast._compiler_options)
    ModuleFilename = filename

    state = CompilerState(daast)

    flatten_opassignments(state)
    generate_header(state)
    process_all_queries(extract_all_queries(daast), state)
    process_assignments_and_deletions(state)
    process_updates(state)
    process_events(state)
    process_initializers(state)
    process_setups(state)

    # Generate the main python file:
    pyast = translate_with_stubs(state)
    return state.module, pyast

class NodetypeFinder(NodeVisitor):
    """Looks a for specific type of node starting from a given root node."""
    def __init__(self, nodetype):
        self.target_type = nodetype
        self.found = False

    def visit(self, node):
        if isinstance(node, self.target_type):
            self.found = True
        else:
            super().visit(node)

class OpAssignmentTransformer(NodeTransformer):
    """Transforms operator assignment statements into plain assignments."""

    def visit_OpAssignmentStmt(self, node):
        if node.first_parent_of_type(dast.Process) is None:
            # Ignore if not in Process
            return node

        newstmt = dast.AssignmentStmt(parent=node.parent, ast=node.ast)
        expr = dast.BinaryExpr(parent=newstmt, ast=node.ast, op=node.operator)
        expr.left = node.target
        expr.right = node.value
        expr.left._parent = expr.right._parent = expr
        newstmt.value = expr
        newstmt.targets = [node.target.clone()]
        newstmt.targets[0]._parent = newstmt
        for nobj in newstmt.nameobjs:
            nobj.replace_node(node, newstmt)
        return newstmt

class StubcallGenerator(PythonGenerator):
    """Transforms DistPy AST into Python AST with calls to incrementalization
    stubs injected.

    """

    def __init__(self, compiler_state):
        super().__init__()
        self.compiler_state = compiler_state
        self.preambles.append(Assign([pyName(INC_MODULE_VAR)], pyNone()))

    def history_initializers(self, node):
        # When using inc, event histories are initialized in 'setup', so do
        # nothing here:
        return []

    def history_stub(self, node):
        if node.record_history and node in self.compiler_state.events:
            return pyAttr(INC_MODULE_VAR,
                          UPDATE_STUB_PREFIX + node.name)
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
                                   if evt in self.compiler_state.events]))]
        # Clear all remaining events:
        stmts.extend([Expr(pyCall(func=pyAttr(pyAttr("self", evt.name),
                                              "clear")))
                      for evt in proc.events
                      if evt.record_history
                      if evt not in self.compiler_state.events])
        return stmts

class EventExtractor(NodeVisitor):
    """Extracts event specs from queries.

    HistoryDomainSpec and HistoryExpr contain events, these need to be
    extracted in order to generate the correct query parameters.
    """

    def __init__(self):
        self.events = []

    def visit_Event(self, node):
        self.events.append(node)

class QueryExtractor(NodeVisitor):
    """Extracts expensive queries.

    'Expensive' queries are quantifications, comprehensions, aggregates, and
    history queries.

    """

    def __init__(self):
        self.queries = []
        self.processes = []

    def visit_ComplexExpr(self, node):
        if node.first_parent_of_type(dast.Process) is None:
            return

        if node not in self.queries:
            self.queries.append(node)

    visit_QuantifiedExpr = visit_ComplexExpr
    visit_GeneratorExpr = visit_ComplexExpr
    visit_SetCompExpr = visit_ComplexExpr
    visit_ListCompExpr = visit_ComplexExpr
    visit_DictCompExpr = visit_ComplexExpr
    visit_TupleCompExpr = visit_ComplexExpr
    visit_MaxExpr = visit_ComplexExpr
    visit_MinExpr = visit_ComplexExpr
    visit_SumExpr = visit_ComplexExpr
    visit_SizeExpr = visit_ComplexExpr

    def visit_HistoryExpr(self, node):
        if node.first_parent_of_type(dast.Process) is None:
            return

        par = node.parent
        if (isinstance(par, dast.ComparisonExpr) and
                par.comparator is dast.InOp):
            node = par
        if node not in self.queries:
            self.queries.append(node)

    visit_ReceivedExpr = visit_HistoryExpr
    visit_SentExpr = visit_HistoryExpr


class IncInterfaceGenerator(PatternComprehensionGenerator):
    """Transforms DistPy patterns to Python comprehension.

    """

    def __init__(self, mangle_names=None, last_freevars=None):
        super().__init__()
        self.disable_body_expansion = True
        self.mangle_names = mangle_names if mangle_names is not None else set()
        # Set of free vars seen so far in this pattern.
        # This is needed so free vars with the same name in the pattern can be
        # properly unified:
        self.freevars = set() if last_freevars is None else last_freevars
        # The primary set for extracting witness for quantifications:
        self.witness_set = None

    def reset_pattern_state(self):
        """Resets pattern related parser states.

        Call this before starting to parse a new pattern.

        """
        self.freevars.clear()

    def reset(self):
        super().reset()
        self.reset_pattern_state()

    def visit(self, node):
        """Generic visit method.

        If the node is a query, then 'process_query' would have attached a
        call to the query method on the 'inc_query_override' attribute, so we
        simply return that node. Otherwise, pass along to the parent 'visit'.

        """

        if hasattr(node, 'inc_query_override'):
            return node.inc_query_override
        else:
            return super().visit(node)

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
            elem = optimize_tuple(pyTuple(node.left.ordered_freevars))
            ast = pySize(ListComp(elem, [gen])) if not Options.jb_style \
                  else pySize(SetComp(elem, [gen]))
            return ast
        else:
            return super().visit_ComparisonExpr(node)

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
        domain = self.visit(node.domain)
        if isinstance(node.pattern, dast.PatternExpr):
            target, ifs = self.visit(node.pattern)
        else:
            target = self.visit(node.pattern)
            ifs = []
        return comprehension(target, domain, ifs)

    def visit_HistoryExpr(self, node):
        return pyName(node.event.name)

    visit_ReceivedExpr = visit_HistoryExpr
    visit_SentExpr = visit_HistoryExpr

    def visit_ComprehensionExpr(self, node):
        # We'll just trick the PythonGenerator to use this instance as the
        # pattern generator:
        self.pattern_generator = self
        res = super().visit_ComprehensionExpr(node)
        return res

    visit_GeneratorExpr = visit_ComprehensionExpr
    visit_SetCompExpr = visit_ComprehensionExpr
    visit_ListCompExpr = visit_ComprehensionExpr
    visit_DictCompExpr = visit_ComprehensionExpr
    visit_TupleCompExpr = visit_ComprehensionExpr

    def visit_QuantifiedExpr(self, node):
        assert node.predicate is not None
        if node is node.top_level_query:
            self.reset_pattern_state()

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
        elt = optimize_tuple(self.visit(node.predicate))
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
        primary = None
        cond = self.visit(node.predicate)
        for dom in reversed(node.domains):
            elt = optimize_tuple(pyTuple([self.visit(name)
                                          for name in dom.ordered_freevars]))
            domast = self.visit(dom)
            primary = SetComp(elt, [domast])
            left = pySize(primary)
            if node.operator is dast.UniversalOp:
                domast.ifs.append(pyNot(cond))
                cond = pyCompare(left, Eq, Num(0))
            else:
                # Existential
                domast.ifs.append(cond)
                cond = pyCompare(left, Gt, Num(0))

        # Extract witness set. Note: for table1, only freevars bound in the
        # first domain can be witnessed:
        if node is node.top_level_query and \
           node.operator is dast.ExistentialOp and \
           len(node.ordered_local_freevars) > 0:
            self.witness_set = primary

        return cond


    def do_table2_transformation(self, node):
        """Transformation defined in Table 2 of OOPSLA paper.

        This transformation converts nested quantificactions into aggregates.
        It supports up to one level of alternation; the non-alternating case
        is simply a generalization of Table 1.

        """
        res = None
        query_node = node
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
                             for name in node.ordered_local_freevars]
            else:
                inner_generators += [self.visit(dom) for dom in node.domains]
            node = node.predicate

        assert node is not None
        elements = optimize_tuple(pyTuple(elements))
        generators = outer_generators + inner_generators
        bexp = self.visit(node)

        primary = SetComp(elements, generators)
        left = pySize(primary)

        if inner_quantifier is None:
            # Non-alternating:
            if outer_quantifier is dast.UniversalOp:
                bexp = pyNot(bexp)
                generators[-1].ifs.append(bexp)
                res = pyCompare(left, Eq, Num(0))
            else:
                generators[-1].ifs.append(bexp)
                res = pyCompare(left, Gt, Num(0))
                # Extract witness set if there are free variables:
                if query_node is query_node.top_level_query and \
                   len(query_node.ordered_local_freevars) > 0:
                    self.witness_set = primary

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
                res = pyCompare(left, Eq, right)
            else:
                generators[-1].ifs.append(pyNot(bexp))
                res = pyCompare(left, NotEq, right)
                # Extract witness set if there are free variables:
                if query_node is query_node.top_level_query and \
                   len(query_node.ordered_local_freevars) > 0:
                    self.witness_set = BinOp(SetComp(elements, outer_generators),
                                             Sub(),
                                             primary)

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
        current_pattern_state = frozenset(self.freevars)
        pred = apply_demorgan_rule(node.predicate)
        if isinstance(pred, dast.LogicalExpr):
            # Rule 1:
            if (pred.operator is dast.AndOp and
                    node.operator is dast.ExistentialOp):
                iprintd("do_table4_transformation: using rule 1")
                for i, e in enumerate(pred.subexprs):
                    self.freevars = set(current_pattern_state)
                    newnode = node.clone()
                    domain_for_condition(newnode.domains[-1], e)
                    newnode.subexprs = pred.subexprs[0:i] + \
                                       pred.subexprs[(i+1):]
                    iprintd("do_table4_transformation: newnode: " + str(newnode))
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
                    self.freevars = set(current_pattern_state)
                    e = dast.LogicalExpr(e.parent,
                                         op=dast.NotOp,
                                         subexprs=[e])
                    newnode = node.clone()
                    domain_for_condition(newnode.domains[-1], e)
                    newnode.subexprs = pred.subexprs[0:idx] + \
                                       pred.subexprs[(idx+1):]
                    res = self.do_table3_transformation(newnode)
                    if res is not None:
                        return res
        iprintd("do_table4_transformation: unable to apply.")
        self.freevars = set(current_pattern_state)
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
        if len(left) > 0 and len(right) == 0 and \
           (isinstance(pred.left, dast.SimpleExpr) or
            isinstance(pred.left, dast.TupleExpr)):
            x = pred.left
            y = pred.right
        elif len(left) == 0 and len(right) > 0 and \
             (isinstance(pred.right, dast.SimpleExpr) or
              isinstance(pred.right, dast.TupleExpr)):
            x = pred.right
            y = pred.left
        else:
            iprintd("Table 3 can not be applied to %s: free var distribution."
                    "left %s : right %s" %
                   (node, left, right))
            return None

        generators = [self.visit(dom) for dom in node.domains]
        iprintd("do_table3_transformation: generators = " + str(generators))
        # Need to normalize tuples here, for comparison:
        pyx = optimize_tuple(self.visit(x))
        pyy = optimize_tuple(self.visit(y))
        if (len(generators) == 1 and len(generators[0].ifs) == 0 and
                ast_eq(pyx, optimize_tuple(generators[0].target))):
            s = sp = generators[0].iter
        else:
            s = SetComp(pyx, generators)
            iprintd("table3 s: " + to_source(s))
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

        # Extract witness:
        if node is node.top_level_query and \
           node.operator is dast.ExistentialOp and \
           len(node.ordered_local_freevars) > 0:
            self.witness_set = s

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
