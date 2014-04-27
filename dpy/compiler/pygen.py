from ast import *
from itertools import chain
from . import dast

OperatorMap = {
    dast.AddOp : Add,
    dast.SubOp : Sub,
    dast.MultOp : Mult,
    dast.DivOp : Div,
    dast.ModOp : Mod,
    dast.PowOp : Pow,
    dast.LShiftOp : LShift,
    dast.RShiftOp : RShift,
    dast.BitOrOp : BitOr,
    dast.BitXorOp : BitXor,
    dast.BitAndOp : BitAnd,
    dast.FloorDivOp : FloorDiv,

    dast.EqOp : Eq,
    dast.NotEqOp : NotEq,
    dast.LtOp : Lt,
    dast.LtEOp : LtE,
    dast.GtOp : Gt,
    dast.GtEOp : GtE,
    dast.IsOp : Is,
    dast.IsNotOp : IsNot,
    dast.InOp : In,
    dast.NotInOp : NotIn,

    dast.USubOp : USub,
    dast.UAddOp : UAdd,
    dast.InvertOp : Invert,

    dast.AndOp : And,
    dast.OrOp : Or
}

PATTERN_EXPR_NAME = "_PatternExpr_%d"
QUATIFIED_EXPR_NAME = "_QuantifiedExpr_%d"

########## Conveniece methods for creating AST nodes: ##########

def call_noarg_ast(name):
    return Call(Name(name, Load()), [], [], None, None)

def pyCall(func, args=[], keywords=[], starargs=None, kwargs=None):
    if isinstance(func, str):
        func = pyName(func)
    return Call(func,
                list(args),
                [keyword(arg, val) for arg, val in keywords],
                starargs,
                kwargs)

def pyName(name, ctx=None):
    return Name(name, Load() if ctx is None else ctx)

def pyNone():
    return pyName("None")

def pyTrue():
    return pyName("True")

def pyFalse():
    return pyName("False")

def pyList(elts, ctx=None):
    return List(elts, Load() if ctx is None else ctx)

def pySet(elts, ctx=None):
    return Set(elts)

def pyTuple(elts, ctx=None):
    return Tuple(elts, Load() if ctx is None else ctx)

def pySetC(elts):
    return pyCall("set", args=elts)

def pySubscr(value, index, ctx=None):
    return Subscript(value, Index(index),
                     Load() if ctx is None else ctx)

def pyAttr(name, attr, ctx=None):
    if isinstance(name, str):
        return Attribute(Name(name, Load()), attr,
                         Load() if ctx is None else ctx)
    else:
        return Attribute(name, attr, Load() if ctx is None else ctx)

def pyLabel(name, block=False, timeout=None):
    kws = [("block", pyTrue() if block else pyFalse())]
    if timeout is not None:
        kws.append(("timeout", timeout))
    return pyCall(func=pyAttr(pyCall("super"), "_label"),
                  args=[Str(name)],
                  keywords=kws)

def pyClassDef(name, bases=[], keywords=[], starargs=None,
               kwargs=None, body=[], decorator_list=[]):
    return ClassDef(name,
                    list(bases),
                    [keyword(arg, val) for arg, val in keywords],
                    starargs,
                    kwargs,
                    list(body),
                    list(decorator_list))

def pyFunctionDef(name, args=[], body=[], decorator_list=[], returns=None):
    arglist = arguments(args=[arg(n, None) for n in args],
                        vararg=None,
                        varargannotation=None,
                        kwonlyargs=[],
                        kwarg=None,
                        kwargannotation=None,
                        defaults=[],
                        kw_defaults=None)
    return FunctionDef(name,
                       arglist,
                       list(body),
                       list(decorator_list),
                       returns)

def propagate_attributes(from_nodes, to_node):
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

def propagate_subexprs(node):
    for e in node.subexprs:
        propagate_attributes(e, node)

def concat_bodies(subexprs, body):
    prebody = []
    postbody = []
    for e in subexprs:
        if hasattr(e, "prebody"):
            prebody.extend(e.prebody)
        if hasattr(e, "postbody"):
            postbody.extend(e.postbody)
    return prebody + body + postbody

PREAMBLE = parse(
    """
import dpy
    """).body
POSTAMBLE = parse("""
if __name__ == "__main__":
    dpy.init(config)
""").body


class PythonGenerator(NodeVisitor):
    """Transforms DistPy AST into Python AST.
    """

    def __init__(self, filename=""):
        self.filename = filename
        self.processed_patterns = set()
        self.active_target = None
        self.preambles = list(PREAMBLE)
        self.postambles = list()

    def visit(self, node):
        if isinstance(node, dast.DistNode):
            if hasattr(node, "incr_override"):
                return node.incr_override
        return super().visit(node)

    def body(self, body):
        res = []
        for stmt in body:
            if stmt.label is not None:
                res.append(pyLabel(stmt.label))
            ast = self.visit(stmt)
            if ast is not None:
                res.extend(self.visit(stmt))
            else:
                print("None result from %s" % str(stmt))
        return res

    def bases(self, bases):
        res = []
        for expr in bases:
            res.append(self.visit(expr))
        return res

    def visit_Program(self, node):
        body = []
        for p in node.processes:
            body.extend(self.visit(p))
        body.extend(self.body(node.body))
        if node.entry_point is not None:
            body.extend(self.visit(node.entry_point))
        return Module(self.preambles + body + self.postambles)

    def generate_init(self, node):
        InitArgs = ["parent", "initq", "channel", "log"]
        initfun = pyFunctionDef("__init__", ["self"] + InitArgs)
        supercall = pyCall(pyAttr(pyCall(pyName("super")),
                                  "__init__"),
                           [pyName(n) for n in InitArgs])
        evtconstructors = [self.visit(evt) for evt in node.events]
        historyinit = []
        for evt in node.events:
            if evt.record_history:
                historyinit.append(
                    Assign(targets=[pyAttr("self", evt.name)],
                           value=pyList([])))
        evtdef = Assign(targets=[pyAttr("self", "_events")],
                        value=pyList(evtconstructors))
        initfun.body = [Expr(supercall), evtdef] + historyinit
        return initfun

    def generate_setup(self, node):
        args = self.generate_args(node)
        args.args.insert(0, arg("self", None))
        body = []
        for name in node.names:
            body.append(Assign(targets=[pyAttr("self", name, Store())],
                                        value=pyName(name)))
        for stmt in node.initializers:
            body.extend(self.visit(stmt))
        return FunctionDef("setup", args, body, [], None)

    def generate_handlers(self, node):
        body = []
        for evt in node.events:
            for handler in evt.handlers:
                body.extend((self.visit(handler)))
        return body

    def generate_args(self, node):
        assert isinstance(node, dast.ArgumentsContainer)
        args = [arg(ident.name, None) for ident in node.args]
        vararg = node.vararg
        kwonlyargs = [arg(ident.name, None) for ident in node.kwonlyargs]
        kw_defaults = [self.visit(expr) for expr in node.kw_defaults]
        kwarg = node.kwarg
        defaults = [self.visit(expr) for expr in node.defaults]
        return arguments(args, vararg, None, kwonlyargs, kwarg,
                         None, defaults, kw_defaults)

    def visit_Process(self, node):
        cd = ClassDef()
        cd.name = node.name
        cd.bases = [pyAttr("dpy", "DistProcess")]
        cd.bases.extend(self.bases(node.bases))
        # ########################################
        # TODO: just pass these through until we figure out a use for them:
        cd.keywords = node.ast.keywords
        cd.starargs = node.ast.starargs
        cd.kwargs = node.ast.kwargs
        # ########################################
        cd.body = [self.generate_init(node),
                   self.generate_setup(node)]
        if node.entry_point is not None:
            cd.body.extend(self.visit(node.entry_point))
        cd.decorator_list = [self.visit(d) for d in node.decorators]
        cd.body.extend(self.body(node.methods))
        cd.body.extend(self.generate_handlers(node))
        return [cd]

    def visit_Function(self, node):
        fd = FunctionDef()
        fd.name = node.name
        fd.args = self.generate_args(node)
        if type(node.parent) is dast.Process:
            fd.args.args.insert(0, arg("self", None))
        fd.body = self.body(node.body)
        fd.decorator_list = [self.visit(d) for d in node.decorators]
        fd.returns = None
        return [fd]

    def visit_ClassStmt(self, node):
        cd = pyClassDef(name=node.name,
                        bases=self.bases(node.bases),
                        body=self.body(node.body))
        # ########################################
        # TODO: just pass these through until we figure out a use for them:
        cd.keywords = node.ast.keywords
        cd.starargs = node.ast.starargs
        cd.kwargs = node.ast.kwargs
        # ########################################
        cd.decorator_list = [self.visit(d) for d in node.decorators]
        return [cd]

    def visit_PythonExpr(self, node):
        return node.ast

    def visit_SimpleExpr(self, node):
        return self.visit(node.value)

    def visit_AttributeExpr(self, node):
        sub = self.visit(node.value)
        ast = Attribute(sub, node.attr, None)
        return propagate_attributes([sub], ast)

    def visit_SubscriptExpr(self, node):
        val = self.visit(node.value)
        if isinstance(node.index, dast.SliceExpr):
            idx = self.visit(node.index)
        else:
            idx = Index(self.visit(node.index))
            propagate_attributes([idx.value], idx)
        ast = Subscript(val, idx, Load())
        return propagate_attributes((val, idx), ast)

    def visit_SliceExpr(self, node):
        l = self.visit(node.lower) if node.lower is not None else None
        u = self.visit(node.upper) if node.upper is not None else None
        s = self.visit(node.step) if node.step is not None else None
        ast = Slice(l, u, s)
        return propagate_attributes((l, u, s), ast)

    def visit_StarredExpr(self, node):
        val = self.visit(node.value)
        ast = Starred(val, None)
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

    def visit_TupleExpr(self, node):
        ast = Tuple([self.visit(e) for e in node.subexprs], None)
        return propagate_attributes(ast.elts, ast)

    def visit_ListExpr(self, node):
        ast = List([self.visit(e) for e in node.subexprs], None)
        return propagate_attributes(ast.elts, ast)

    def visit_SetExpr(self, node):
        ast = Set([self.visit(e) for e in node.subexprs])
        return propagate_attributes(ast.elts, ast)

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
        ast = pyCall(self.visit(node.func),
                     [self.visit(a) for a in node.args],
                     [(key, self.visit(value)) for key, value in node.keywords],
                     self.visit(node.starargs)
                     if node.starargs is not None else None,
                     self.visit(node.kwargs)
                     if node.kwargs is not None else None)
        return propagate_attributes([ast.func] + ast.args, ast)

    def visit_ApiCallExpr(self, node):
        ast = pyCall(pyAttr(pyAttr("dpy", "api"), node.func),
                     [self.visit(a) for a in node.args])
        return propagate_attributes(ast.args, ast)

    def visit_BuiltinCallExpr(self, node):
        ast = pyCall(pyAttr("self", node.func),
                     [self.visit(a) for a in node.args])
        return propagate_attributes(ast.args, ast)

    def visit_LogicalExpr(self, node):
        if node.operator is dast.NotOp:
            ast = UnaryOp(Not(), self.visit(node.left))
            return propagate_attributes([ast.operand], ast)
        else:
            ast = BoolOp(OperatorMap[node.operator](),
                         [self.visit(e) for e in node.subexprs])
            return propagate_attributes(ast.values, ast)

    def visit_PatternDomainSpec(self, node):
        assert node.pattern is not None
        assert node.domain is not None
        pat = self.visit(node.pattern)
        domain = self.visit(node.domain)
        context = [(v.name, self.visit(v)) for v in node.pattern.boundvars]
        # FIXME: variable ordering?????:
        order = pyTuple([Str(v.name) for v in node.pattern.freevars])
        varelts = [self.visit(v) for v in node.pattern.freevars]
        iterater = pyCall(pyAttr(pat, "filter"), [domain, order],
                          keywords=context)
        if len(varelts) > 0:
            target = pyTuple(varelts)
        else:
            target = pyName("_dummyvar")
        body = []
        orelse = []
        ast = For(target, iterater, body, orelse)
        return propagate_attributes([ast.iter], ast)

    def visit_HistoryDomainSpec(self, node):
        assert node.event is not None
        ctx = [(v.name, self.visit(v)) for v in node.event.boundvars]
        pat = pySubscr(pyAttr("self", "_events"), Num(node.event.index))
        # FIXME: variable ordering?????:
        order = pyTuple([Str(v.name) for v in node.event.freevars])
        varelts = [self.visit(v) for v in node.event.freevars]
        iterater = pyCall(pyAttr(pat, "filter"),
                          args=[pyAttr("self", node.event.name), order],
                          keywords=ctx)
        if len(varelts) > 0:
            target = pyTuple(varelts)
        else:
            target = pyName("_dummyvar")
        body = []
        orelse = []
        ast = For(target, iterater, body, orelse)
        return propagate_attributes([ast.iter], ast)

    def visit_QuantifiedExpr(self, node):
        nameset = node.freevars
        body = funcbody = []

        for domspec in node.domains:
            body.append(self.visit(domspec))
            body = body[0].body
        postbody = []
        ifcond = self.visit(node.predicate)
        if hasattr(ifcond, "prebody"):
            body.extend(ifcond.prebody)
        if hasattr(ifcond, "postbody"):
            postbody.extend(cnode.postbody)

        if node.operator is dast.UniversalOp:
            ifcond = UnaryOp(Not(), ifcond)
            ifbody = [Return(pyFalse())]
        else:                   # ExistentialExpr
            ifbody = [Return(pyTrue())]
        body.append(If(ifcond, ifbody, []))
        body.extend(postbody)
        if node.operator is dast.UniversalOp:
            funcbody.append(Return(pyTrue()))
        else:
            funcbody.append(Return(pyFalse()))

        if len(nameset) > 0:
            # Back patch nonlocal statement
            if not type(node.parent_statement.parent) is dast.Program:
                decl = Nonlocal([nv.name for nv in nameset
                                 if type(nv.scope) is not dast.Process])
            else:
                decl = Global([nv.name for nv in nameset
                               if type(nv.scope) is not dast.Process])
            funcbody.insert(0, decl)

        # Bound values that are defined in a containing comprehension need to
        # be explicitly passed in
        params = set(nobj for nobj in chain(node.boundvars,
                                            node.predicate.nameobjs)
                     if isinstance(nobj.scope, dast.ComprehensionExpr)
                     if nobj not in nameset)

        ast = pyCall(func=pyName(node.name),
                     keywords=[(v.name, self.visit(v)) for v in params])
        funast = pyFunctionDef(name=node.name,
                               args=[v.name for v in params],
                               body=funcbody)
        # Assignment needed to ensure all vars are bound at this point
        if len(nameset) > 0:
            ast.prebody = [Assign(targets=[pyName(nv.name) for nv in nameset],
                                  value=pyNone()),
                           funast]
        else:
            ast.prebody = [funast]
        return ast

    def visit_ComprehensionExpr(self, node):
        generators = []
        for tgt, it in zip(node.targets, node.iters):
            tast = self.visit(tgt)
            self.active_target = tgt
            iast = self.visit(it)
            self.active_target = None
            comp = comprehension(tast, iast, [])
            generators.append(propagate_attributes((tast, iast), comp))
        generators[-1].ifs = [self.visit(cond) for cond in node.conditions]
        propagate_attributes(generators[-1].ifs, generators[-1])

        if type(node) is dast.DictCompExpr:
            key = self.visit(node.elem.key)
            value = self.visit(node.elem.value)
            ast = propagate_attributes((key, value),
                                       DictComp(key, value, generators))
        else:
            elem = self.visit(node.elem)
            if type(node) is dast.SetCompExpr:
                ast = SetComp(elem, generators)
            elif type(node) is dast.ListCompExpr:
                ast = ListComp(elem, generators)
            elif type(node) is dast.GeneratorExpr:
                ast = GeneratorExp(elem, generators)
            else:
                self.error("Unknown expression", node)
                return None
            ast = propagate_attributes([elem], ast)
        return propagate_attributes(ast.generators, ast)

    visit_GeneratorExpr = visit_ComprehensionExpr
    visit_SetCompExpr = visit_ComprehensionExpr
    visit_ListCompExpr = visit_ComprehensionExpr
    visit_DictCompExpr = visit_ComprehensionExpr

    def visit_MaxExpr(self, node):
        ast = pyCall(func=pyName("max"),
                     args=[self.visit(node.value)])
        return propagate_attributes(ast.args, ast)

    def visit_MinExpr(self, node):
        ast = pyCall(func=pyName("min"),
                     args=[self.visit(node.value)])
        return propagate_attributes(ast.args, ast)

    def visit_SizeExpr(self, node):
        ast = pyCall(func=pyName("len"),
                     args=[self.visit(node.value)])
        return propagate_attributes(ast.args, ast)

    def visit_SumExpr(self, node):
        ast = pyCall(func=pyName("sum"),
                     args=[self.visit(node.value)])
        return propagate_attributes(ast.args, ast)

    def visit_ComparisonExpr(self, node):
        left = self.visit(node.left)
        right = self.visit(node.right)
        op = OperatorMap[node.comparator]()
        ast = Compare(left, [op], [right])
        return propagate_attributes((left, right), ast)

    def visit_ArithmeticExpr(self, node):
        op = OperatorMap[node.operator]()
        if issubclass(node.operator, dast.UnaryOperator):
            ast = UnaryOp(op, self.visit(node.right))
            return propagate_attributes([ast.operand], ast)
        else:
            ast = BinOp(self.visit(node.left), op, self.visit(node.right))
            return propagate_attributes((ast.left, ast.right), ast)

    def visit_PatternElement(self, node):
        if node.type is dast.FreeVar:
            val = Str(node.value.name) if node.value is not None else pyNone()
        elif node.type is dast.BoundVar:
            val = Str(node.value.name)
        elif node.type is dast.ConstantVar:
            val = self.visit(node.value)
        else:
            val = pyList([self.visit(v) for v in node.value])

        return pyCall(func=pyAttr(pyAttr("dpy", "pat"), "PatternElement"),
                      args=[pyAttr(pyAttr("dpy", "pat"), node.type.__name__), val])

    def visit_PatternExpr(self, node):
        if node.name not in self.processed_patterns:
            patast = self.visit(node.pattern)
            ast = Assign([pyName(node.name)], patast)
            self.preambles.append(ast)
            self.processed_patterns.add(node.name)
        return pyName(node.name)

    def visit_HistoryExpr(self, node):
        assert node.event is not None
        if len(node.boundvars) == 0 and self.active_target is None:
            return pyAttr("self", node.event.name)
        else:
            ctx = [(v.name, self.visit(v)) for v in node.boundvars]
            if self.active_target is not None:
                order = pyTuple([Str(n)
                                 for n in self.active_target.ordered_names])
            else:
                order = pyNone()
            pat = pySubscr(pyAttr("self", "_events"),
                                  Num(node.evtidx))
            gen = pyCall(pyAttr(pat, "filter"),
                         args=[pyAttr("self", node.event.name), order],
                         keywords=ctx)
            return pySetC([gen])

    visit_ReceivedExpr = visit_HistoryExpr
    visit_SentExpr = visit_HistoryExpr

    def visit_LambdaExpr(self, node):
        args = self.generate_args(node)
        return Lambda(args, self.visit(node.body))

    def visit_NamedVar(self, node):
        if type(node.scope) is dast.Process:
            return pyAttr("self", node.name)
        else:
            return pyName(node.name)


    ########## Statements ##########

    def visit_NoopStmt(self, node):
        return [Pass()]

    def visit_PassStmt(self, node):
        return [Pass()]

    def visit_AssignmentStmt(self, node):
        targets = [self.visit(tgt) for tgt in node.targets]
        val = self.visit(node.value)
        ast = Assign(targets, val)
        return concat_bodies(targets + [val], [ast])

    def visit_OpAssignmentStmt(self, node):
        target = self.visit(node.target)
        val = self.visit(node.value)
        ast = AugAssign(target, OperatorMap[node.operator](), val)
        return concat_bodies([target, val], [ast])

    def visit_IfStmt(self, node):
        test = self.visit(node.condition)
        body = self.body(node.body)
        orelse = self.body(node.elsebody)
        ast = If(test, body, orelse)
        return concat_bodies([test], [ast])

    def visit_WhileStmt(self, node):
        test = self.visit(node.condition)
        body = self.body(node.body)
        orelse = self.body(node.elsebody)
        ast = While(test, body, orelse)
        return concat_bodies([test], [ast])

    def visit_ForStmt(self, node):
        target = self.visit(node.target)
        self.active_target = node.target
        it = self.visit(node.iter)
        self.active_target = None
        body = self.body(node.body)
        orelse = self.body(node.elsebody)
        ast = For(target, it, body, orelse)
        return concat_bodies((target, it), [ast])

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

    def visit_AwaitStmt(self, node):
        conds = []
        body = []
        if node.timeout is None:
            label = pyLabel(node.unique_label, block=True)
        else:
            label = pyLabel(node.unique_label, block=True,
                            timeout=self.visit(node.timeout))
        for br in node.branches:
            cond = self.visit(br.condition)
            conds.append(cond)
            ifbody = self.body(br.body)
            body.append(If(cond, ifbody + [Break()], []))
        if len(node.orelse) > 0:
            cond = pyAttr("self", "_timer_expired")
            ifbody = self.body(node.orelse)
            body.append(If(cond, ifbody + [Break()], []))
        # Label call must come after the If tests:
        body.append(label)
        if node.timeout is not None:
            main = [pyCall(pyAttr("self", "_timer_start")),
                    While(pyTrue(), body, []),
                    pyCall(pyAttr("self", "_timer_end"))]
        else:
            main = [While(pyTrue(), body, [])]
        return concat_bodies(conds, main)

    def visit_BranchingAwaitStmt(self, node):
        orlist = []
        branches = ifbody = []
        for branch in node.branches:
            if branch.condition is None:
                ifbody.extend(self.body(branch.body))
            else:
                test = self.visit(branch.condition)
                orlist.append(test)
                body = self.body(branch.body)
                orelse = []
                ifbody.append(If(test, body, orelse))
                ifbody = orelse
        cond = BoolOp(Or(), orlist)
        test = UnaryOp(Not(), cond)
        condfunc = pyFunctionDef(name="await_cond_%d" % hash(node),
                                 body=[Return(test)])
        awaitcall = pyCall(func=pyAttr(pyCall("super"), "_await_"),
                           args=[pyName(condfunc.name)])
        return concat_bodies(orlist, [condfunc, awaitcall, branches[0]])

    def visit_LoopingAwaitStmt(self, node):
        cond = self.visit(node.condition)
        test = UnaryOp(Not(), cond)
        condfunc = pyFunctionDef(name="await_cond_%d" % hash(node),
                                 body=[Return(test)])
        awaitcall = pyCall(func=pyAttr(pyCall("super"), "_await"),
                           args=[pyName(condfunc.name)])

        mainbody = self.body(node.body)
        orelse = [Break()]
        ifcheck = If(cond, mainbody, orelse)
        body = concat_bodies([cond], [condfunc, awaitcall, ifcheck])
        ast = While(pyTrue(), body, [])
        return [ast]

    def visit_ReturnStmt(self, node):
        value = self.visit(node.value)
        ast = Return(value)
        return concat_bodies([value], [ast])

    def visit_DeleteStmt(self, node):
        targets = [self.visit(tgt) for tgt in node.targets]
        ast = Delete(tgt)
        return concat_bodies(targets, [ast])

    def visit_YieldStmt(self, node):
        if node.value is not None:
            value = self.visit(node.value)
            ast = Expr(Yield(value))
            return concat_bodies([value], [ast])
        else:
            return [Expr(Yield(None))]

    def visit_YieldFromStmt(self, node):
        if node.value is not None:
            value = self.visit(node.value)
            ast = Expr(YieldFrom(value))
            return concat_bodies([value], [ast])
        else:
            return [Expr(YieldFrom(None))]

    def visit_WithStmt(self, node):
        items = []
        for item in node.items:
            context_expr = self.visit(item[0])
            if item[1] is not None:
                optional_vars = self.visit(item[1])
            else:
                optional_vars = None
            items.append(withitem(context_expr, optional_vars))
        body = self.body(node.body)
        ast = With(items, body)
        return concat_bodies([e[0] for e in items], [ast])

    def visit_SimpleStmt(self, node):
        value = self.visit(node.expr)
        ast = Expr(value)
        return concat_bodies([value], [ast])

    def visit_PythonStmt(self, node):
        return [node.ast]

    def visit_SendStmt(self, node):
        mesg = self.visit(node.message)
        tgt = self.visit(node.target)
        ast = pyCall(func=pyAttr("self", "_send"),
                     args=[mesg, tgt])
        return concat_bodies(ast.args, [ast])

    def visit_OutputStmt(self, node):
        args = [self.visit(node.message)]
        if node.level is not None:
            args.append(self.visit(node.level))
        ast = pyCall(func=pyAttr("self", "output"), args=args)
        return concat_bodies(ast.args, [ast])

    def visit_Event(self, node):
        evtype = pyAttr(pyAttr("dpy", "pat"), node.type.__name__)
        name = Str(node.name)
        if node.record_history:
            history = pyTrue()
        else:
            history = pyFalse()
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
        return pyCall(func=pyAttr(pyAttr("dpy", "pat"), "EventPattern"),
                      args=[evtype, name, pattern],
                      keywords=[("sources", sources),
                                ("destinations", destinations),
                                ("timestamps", timestamps),
                                ("record_history", history),
                                ("handlers", handlers)])

    def visit_EventHandler(self, node):
        stmts = self.visit_Function(node)
        stmts.append(Assign([pyAttr(node.name, "_labels")],
                            (pyNone() if node.labels is None else
                             pyCall(pyName("frozenset"),
                                    [Set([Str(l) for l in node.labels])]))))
        stmts.append(Assign([pyAttr(node.name, "_notlabels")],
                            (pyNone() if node.notlabels is None else
                             pyCall(pyName("frozenset"),
                                    [Set([Str(l) for l in node.notlabels])]))))
        return stmts
