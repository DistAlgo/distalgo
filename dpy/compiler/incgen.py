import sys
from ast import *
from . import dast
from .pygen import *
from .utils import printd

QUERY_STUB_FORMAT = "Query_%d"
ASSIGN_STUB_FORMAT = "Assign_%s"
UPDATE_STUB_FORMAT = "Update_%s_%d"

SELF_ID_NAME = "SELF_ID"

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

PREAMBLE = """import dpy
ReceivedEvent = dpy.pat.ReceivedEvent
SentEvent = dpy.pat.SentEvent
{0} = None
def init(procid):
    global {0}
    {0} = procid
""".format(SELF_ID_NAME)

def gen_inc_module(dpyast, module_name, args=dict()):
    """Generates the interface file from a DistPy AST."""

    assert isinstance(dpyast, dast.Program)
    module = parse(PREAMBLE)
    quex = QueryExtractor()
    quex.visit(dpyast)

    # Generate the query functions and accumulate set of parameters:
    all_params = set()
    all_events = set()
    # Use the IncInterfaceGenerator for the inc module:
    iig = IncInterfaceGenerator(**args)
    pg = PythonGenerator()

    # Generate query stubs:
    for idx, query in enumerate(quex.queries):
        assert isinstance(query, dast.Expression)
        printd("Processing %r" % query)
        evtex = EventExtractor()
        evtex.visit(query)
        events = evtex.events
        all_events |= events

        qname = QUERY_STUB_FORMAT % idx
        params = set()
        for nobj in query.nameobjs:
            if query.is_child_of(nobj.scope):
                # Ignore if this variable is assigned to inside the query
                # (i.e. free var):
                for place, ctx in nobj.assignments:
                    if place.is_child_of(query):
                        break
                else:
                    params.add(nobj)
        all_params |= params
        iig.reset()
        incqu = iig.visit(query)
        assert isinstance(incqu, AST)
        qrydef = FunctionDef(
            name=qname,
            args=arguments(args=([arg(nobj.name, None) for nobj in params] +
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
            body=[Return(incqu)])
        qrycall = pyCall(
            func=pyAttr(module_name, qname),
            args=[],
            keywords=([(arg.name, pg.visit(arg)) for arg in params] +
                      [(evt.name, pyAttr("self", evt.name)) for evt in events]))
        module.body.append(qrydef)
        query.ast_override = qrycall

    # Generate assignments and updates:
    for vobj in all_params:
        # We only need one assignment stub per variable:
        aname = ASSIGN_STUB_FORMAT % vobj.name
        assdef = FunctionDef(
            name=aname,
            args=arguments(args=([arg(vobj.name, None)]),
                           vararg=None,
                           varargannotation=None,
                           kwonlyargs=[],
                           kwarg=None,
                           kwargannotation=None,
                           defaults=[],
                           kw_defaults=[]),
            decorator_list=[],
            returns=None,
            body=[Return(pyName(vobj.name))])
        module.body.append(assdef)
        pg.reset()
        asscall = Assign(targets=[pg.visit(vobj)],
                         value=pyCall(
                             func=pyAttr(module_name, aname),
                             keywords=[(vobj.name, pg.visit(vobj))]))

        # Inject call to stub at all assignment points:
        for node, ctx in vobj.assignments:
            if isinstance(node, dast.Arguments):
                # this is a function or process argument
                node = node.parent
                if isinstance(node, dast.Process):
                    if not hasattr(node.initializers[0], "prebody"):
                        node.initializers[0].prebody = []
                    assert len(node.initializers) > 0
                    node.initializers[0].prebody.insert(0, asscall)
                else:
                    if not hasattr(node.body[0], "prebody"):
                        node.body[0].prebody = []
                    assert len(node.body) > 0
                    node.body[0].prebody.insert(0, asscall)
            else:
                # This is a normal assignment
                if not hasattr(node, "postbody"):
                    node.postbody = []
                node.postbody.append(asscall)

        idx = 0
        for node, attr, attrtype in vobj.updates:
            node = node.last_parent_of_type(dast.Expression)
            for query in quex.queries:
                if node.is_child_of(query):
                    printw("Update %s inside query %s is ignored!" %
                           (node, query))
                    break
                elif query.is_child_of(node):
                    printw("Query %s inside update %s is ignored!" %
                           (query, node))
                    break
            else:
                uname = UPDATE_STUB_FORMAT % (vobj.name, idx)
                idx += 1
                iig.reset()
                for n in node.nameobjs:
                    printd("%s, %s" % (n, n.scope))
                params = [nobj for nobj in node.nameobjs
                          if node.is_child_of(nobj.scope)]
                assert vobj in params
                updfun = FunctionDef(
                    name=uname,
                    args=arguments(
                        args=([arg(nobj.name, None) for nobj in params]),
                        vararg=None,
                        kwonlyargs=[],
                        kw_defaults=[],
                        kwarg=None,
                        defaults=[]),
                    decorator_list=[],
                    returns=None,
                    body=[Return(iig.visit(node))])
                updcall = pyCall(
                    func=pyAttr(module_name, uname),
                    args=[],
                    keywords=[(arg.name, pg.visit(arg)) for arg in params])
                module.body.append(updfun)
                node.ast_override = updcall

    for event in all_events:
        uname = UPDATE_STUB_FORMAT % (event.process.name + event.name, 0)
        aname = ASSIGN_STUB_FORMAT % (event.process.name + event.name)
        assfun = pyFunctionDef(name=aname,
                               args=["event"],
                               body=[Return(pyName("event"))])
        updfun = pyFunctionDef(name=uname,
                               args=["event", "element"],
                               body=[pyCall(
                                   func=pyAttr("event", "add"),
                                   args=[pyName("element")])])
        module.body.append(assfun)
        module.body.append(updfun)

    # Inject calls to stub init for each process:
    for proc in dpyast.processes:
        if not hasattr(proc.initializers[0], "prebody"):
            proc.initializers[0].prebody = []
        proc.initializers[0].prebody.insert(
            0, pyCall(
                func=pyAttr(module_name, "init"),
                args=[pyAttr("self", "_id")]))

    # Generate the main python file:
    pyast = StubcallGenerator(all_events, module_name).visit(dpyast)
    return module, pyast

class StubcallGenerator(PythonGenerator):
    """Transforms DistPy AST into Python AST with calls to incrementalization
    stubs injected.

    """

    def __init__(self, events, module_name):
        super().__init__()
        self.events = events
        self.module_name = module_name
        self.preambles.append(Import([alias(module_name, None)]))

    def generate_init(self, node):
        supercall = pyCall(func=pyAttr(pyCall(pyName("super")),
                                       "__init__"),
                           args=[pyName(n) for n in PROC_INITARGS])
        evtconstructors = [self.visit(evt) for evt in node.events]
        historyinit = [Assign(
            targets=[pyAttr("self", evt.name)],
            value=(pyCall(func=pyAttr(self.module_name,
                                      ASSIGN_STUB_FORMAT % (evt.process.name +
                                                            evt.name)),
                          args=[pySet([])])
                   if evt in self.events else pyList([])))
                       for evt in node.events if evt.record_history]
        evtdef = Assign(targets=[pyAttr("self", "_events")],
                        value=pyList(evtconstructors))
        return pyFunctionDef(name="__init__",
                             args=(["self"] + PROC_INITARGS),
                             body=([Expr(supercall), evtdef] + historyinit))


    def generate_history(self, node):
        if node.record_history and node in self.events:
            return pyAttr(self.module_name,
                          UPDATE_STUB_FORMAT %
                          (node.process.name + node.name, 0))
        else:
            return super().generate_history(node)

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

    def visit_QuantifiedExpr(self, node):
        # We try to find the largest node that contains the query:
        par = node.last_parent_of_type(dast.BooleanExpr)
        if par is None:
            par = node
        if par not in self.queries:
            self.queries.append(par)

    def visit_ComprehensionExpr(self, node):
        par = node.last_parent_of_type(dast.Expression)
        if par is None:
            par = node
        if par not in self.queries:
            self.queries.append(par)

    visit_GeneratorExpr = visit_ComprehensionExpr
    visit_SetCompExpr = visit_ComprehensionExpr
    visit_ListCompExpr = visit_ComprehensionExpr
    visit_DictCompExpr = visit_ComprehensionExpr

NegatedOperatorMap = {
    dast.EqOp : NotEq,
    dast.NotEqOp : Eq,
    dast.LtOp : Gt,
    dast.LtEOp : GtE,
    dast.GtOp : Lt,
    dast.GtEOp : LtE
}

class IncInterfaceGenerator(PythonGenerator):
    """Transforms DistPy patterns to Python comprehension.

    """

    def __init__(self, **args):
        super().__init__()
        self.notable3 = args['notable3'] if 'notable3' in args else False
        self.notable2 = args['notable2'] if 'notable2' in args else False
        self.notable1 = args['notable1'] if 'notable1' in args else False
        self.noalltables = args['noalltables'] \
                           if 'noalltables' in args else False
        # For unique names:
        self.counter = 0
        # Set of free vars seens so far in this pattern.
        # This is needed so free vars with the same name in the pattern can be
        # properly unified:
        self.freevars = set()

    def reset_pattern_state(self):
        """Resets pattern related parser states.

        Call this before starting to parse a new pattern.

        """
        self.freevars.clear()

    def reset(self):
        super().reset()
        self.reset_pattern_state()

    def visit_NamedVar(self, node):
        return pyName(node.name)

    def visit_SelfExpr(self, node):
        return pyName(SELF_ID_NAME)

    def visit_FreePattern(self, node):
        conds = []
        if node.value is None:
            target = pyName("_")
        elif node.value in self.freevars:
            target = pyName("_Free_" + str(self.counter))
            conds = [pyCompare(target, Eq, pyName(node.value.name))]
        else:
            target = pyName(node.value.name)
            self.freevars.add(node.value)
        self.counter += 1
        return target, conds

    def visit_BoundPattern(self, node):
        boundname = pyName("_Bound_" + str(self.counter))
        targetname = pyName(node.value.name)
        conast = pyCompare(boundname, Eq, targetname)
        self.counter += 1
        return boundname, [conast]

    def visit_ConstantPattern(self, node):
        target = pyName("_Constant_" + str(self.counter))
        compval = self.visit(node.value)
        self.counter += 1
        return target, [pyCompare(target, Eq, compval)]

    def visit_TuplePattern(self, node):
        condition_list = []
        targets = []
        for elt in node.value:
            tgt, conds = self.visit(elt)
            targets.append(tgt)
            condition_list.extend(conds)
        self.counter += 1
        return pyTuple(targets), condition_list

    def visit_ListPattern(self, node):
        raise NotImplementedError(
            "Can not compile list pattern to comprehension.")

    def visit_PatternExpr(self, node):
        target, conds = self.visit(node.pattern)
        return target, conds

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
        return pyTuple([typ, msg, clk, dst, src]), evtconds

    def visit_PatternDomainSpec(self, node):
        self.reset_pattern_state()
        target, ifs = self.visit(node.pattern)
        domain = self.visit(node.domain)
        return comprehension(target, domain, ifs)

    def visit_HistoryDomainSpec(self, node):
        assert node.event is not None
        self.reset_pattern_state()
        target, ifs = self.visit(node.event)
        domain = pyName(node.event.name)
        return comprehension(target, domain, ifs)

    def visit_HistoryExpr(self, node):
        assert node.event is not None
        domain = pyName(node.event.name)
        if len(node.boundvars) == 0 and node.context is None:
            return domain
        else:
            if node.context is not None:
                elt = pyTuple([self.visit(n)
                               for n in node.context.ordered_nameobjs])
            else:
                elt = pyTrue()
            self.reset_pattern_state()
            target, ifs = self.visit(node.event)
            gen = GeneratorExp(
                elt=elt,
                generators=[comprehension(target, domain, ifs)])
            return pySetC([gen])

    visit_ReceivedExpr = visit_HistoryExpr
    visit_SentExpr = visit_HistoryExpr

    def visit_QuantifiedExpr(self, node):
        assert node.predicate is not None

        if not (self.notable3 or self.noalltables):
            res = self.do_table3_transformation(node)
            if res is not None:
                return res
        if not (self.notable2 or self.noalltables):
            res = self.do_table2_transformation(node)
            if res is not None:
                return res
        if not (self.notable1 or self.noalltables):
            res = self.do_table1_transformation(node)
            if res is not None:
                return res

        # Fallback to 'any' and 'all'
        generators = [self.visit(dom) for dom in node.domains]
        elt = self.visit(node.predicate)
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
            elt = pyTuple([self.visit(name) for name in dom.freevars])
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
        elements = None
        while (isinstance(node, dast.QuantifiedExpr) and
               (inner_quantifier is None or
                node.operator is inner_quantifier)):
            if node.operator is not outer_quantifier:
                inner_quantifier = node.operator
            if inner_quantifier is None:
                outer_generators += [self.visit(dom) for dom in node.domains]
            else:
                inner_generators += [self.visit(dom) for dom in node.domains]
            node = node.predicate

        assert node is not None
        elements = pyTuple([gen.target for gen in outer_generators])
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

    @staticmethod
    def combine_logical_comparison(node):
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

    def do_table3_transformation(self, node):
        """Transformation defined in Table 3 of OOPSLA paper.

        This transforms single quantifications over comparisons into min/max
        aggregates.

        """
        # We can only handle comparisons:
        pred = IncInterfaceGenerator.combine_logical_comparison(node.predicate)
        if not (isinstance(pred, dast.ComparisonExpr) and
                pred.comparator in
                {dast.LtOp, dast.LtEOp, dast.GtOp, dast.GtEOp}):
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
            printd("Table 3 can not be applied to %s" % node)
            return None

        pyx = self.visit(x)
        pyy = self.visit(y)
        generators = [self.visit(dom) for dom in node.domains]
        printd(generators)
        printd(dump(pyx))
        if (len(generators) == 1 and len(generators[0].ifs) == 0 and
            ast_eq(pyx, generators[0].target)):
            s = sp = generators[0].iter
        else:
            s = SetComp(pyx, generators)
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
