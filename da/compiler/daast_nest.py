#Visitor in DastNest is borrowed wholesale from
#Bo Lin and Yanhong Annie Liu
#distalgo/da/compiler/psuedo.py

'''
The DastNest visitor will print the class name of each node that it visits in
the order in which they are visited.

In addition, in order to better represent the nested structure (or parent-child)
structure of the DistAlgo AST, the indent is increased by 2 at each call to
dispatch. The result is the child nodes names are indented from their parent 
nodes names in the print.

In order to more full understand the DistAlgo AST's we are working with the 
output of daast_nest.py should be used in combination with the output of 
daast_dict.py.
'''

import sys, ast
from da.compiler.dast import *
from da.compiler.parser import daast_from_file
from da.compiler.ui import parse_all_args

EVENT_TYPES = {
    ReceivedEvent: 'receive',
    SentEvent:     'send'
}


class DastNest:
    """Methods in this class recursively traverse an AST and
    output source code for the abstract syntax; original formatting
    is disregarded. """

    def __init__(self, tree, file=sys.stdout):
        """Unparser(tree, file=sys.stdout) -> None.
         Print the source for tree to file."""
        self.indent_level = 0
        self.indent_width = 2
        self.f = file
        self.dispatch(tree)
        self.f.flush()

    def fill(self):
        return (" " * self.indent_width * self.indent_level)
        
    def dispatch(self, tree):
        self.indent_level += 1
        "Dispatcher function, dispatching tree type T to method _T."
        if isinstance(tree, str):
            print(self.fill() + 'STRING:', tree, file = self.f, flush = True)
            self.indent_level -= 1
            return
        if isinstance(tree, list):
            for t in tree:
                self.dispatch(t)
            self.indent_level -= 1
            return
        if isinstance(tree, Statement) and tree.label:
            print(self.fill() + 'LABEL:', tree.label, file = self.f, flush = True)
        meth = getattr(self, "_"+tree.__class__.__name__)
        meth(tree)
        self.indent_level -= 1


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
        print(self.fill() + 'PROGRAM:', file = self.f, flush = True)
        self.dispatch(tree.body)

    # stmt
    def _SimpleStmt(self, tree):
        print(self.fill() + 'SIMPLESTMT:', file = self.f, flush = True)
        self.dispatch(tree.expr)

    def _ImportStmt(self, t):
        print(self.fill() + 'IMPORTSTMT:', file = self.f, flush = True)
        for target in t.items:
           self.dispatch(target)

    def _ImportFromStmt(self, t):
        print(self.fill() + 'IMPORTFROMSTMT:', file = self.f, flush = True)
        for target in t.items:
            self.dispatch(target)

    def _AssignmentStmt(self, t):
        print('\n----------BEGIN ASSIGNMENT----------', file = self.f, flush = True)
        print(self.fill() + 'ASSIGMMENTSTMT:', file = self.f, flush = True)
        for target in t.targets:
            self.dispatch(target)
        self.dispatch(t.value)
        print('----------END ASSIGNMENT----------\n', file = self.f, flush = True)

    def _OpAssignmentStmt(self, t):
        print(self.fill() + 'OPASSIGNMENTSTMT:', file = self.f, flush = True)
        self.dispatch(t.target)
        self.dispatch(t.value)

    def _ReturnStmt(self, t):
        print(self.fill() + 'RETURNSTMT:', file = self.f, flush = True)
        if t.value:
            self.dispatch(t.value)

    def _NoopStmt(self, t):
        print(self.fill() + 'NOOPSTMT:', file = self.f, flush = True)

    def _BreakStmt(self, t):
        print(self.fill() + 'BREAKSTMT:', file = self.f, flush = True)

    def _ContinueStmt(self, t):
        print(self.fill() + 'CONTINUESTMT:', file = self.f, flush = True)

    def _DeleteStmt(self, t):
        print(self.fill() + 'DELETESTMT:', file = self.f, flush = True)
        for target in t.targets:
            self.dispatch(target)

    def _AssertStmt(self, t):
        print(self.fill() + 'ASSERTSTMT:', file = self.f, flush = True)
        self.dispatch(t.expr)
        if t.msg:
            self.dispatch(t.msg)

    def _GlobalStmt(self, t):
        print(self.fill() + 'GLOBALSTMT:', file = self.f, flush = True)
        for name in t.names:
            self.dispatch(name)
            #may want to just print the names,
            #might just be strings not ast nodes

    def _NonlocalStmt(self, t):
        print(self.fill() + 'NONLOCALSTMT:', file = self.f, flush = True)
        interleave(lambda: self.write(", "), self.write, t.names)

    def _AwaitStmt(self, t):
        print('\n----------BEGIN AWAIT----------', file = self.f, flush = True)
        print(self.fill() + 'AWAITSTMT:', file = self.f, flush = True)
        self._do_await_branches(t)
        print('----------END AWAIT----------\n', file = self.f, flush = True)

    def _LoopingAwaitStmt(self, t):
        print(self.fill() + 'LOOPINGAWAITSTMT:', file = self.f, flush = True)
        self._do_await_branches(t)

    def _do_await_branches(self, t):
        if len(t.branches) == 1 and not t.branches[0].body:
            # single-line await
            self.dispatch(t.branches[0].condition)
        if t.timeout:
            self.dispatch(t.timeout)
        if len(t.branches) > 1 or t.branches[0].body:
            for b in t.branches:
                self.dispatch(b)

    def _Branch(self, t):
        print(self.fill() + 'BRANCH:', file = self.f, flush = True)
        self.dispatch(t.condition)
        self.dispatch(t.body)

    def _SendStmt(self, t):
        print(self.fill() + 'SEND STMT:', file = self.f, flush = True)
        self.dispatch(t.message)
        self.dispatch(t.target)

    def _OutputStmt(self, t):
        print(self.fill() + 'OUTPUTSTMT:', file = self.f, flush = True)
        for m in t.message:
            self.dispatch(m)
        if t.level:
            self.dispatch(t.level)

    def _ResetStmt(self, t):
        print(self.fill() + 'RESETSTMT:', file = self.f, flush = True)
        self.dispatch(t.expr)

    def _YieldStmt(self, t):
        print(self.fill() + 'YIELDSTMT:', file = self.f, flush = True)
        if t.value:
            self.dispatch(t.value)

    def _YieldFrom(self, t):
        print(self.fill() + 'YIELDFROM:', file = self.f, flush = True)
        if t.value:
            self.dispatch(t.value)

    def _RaiseStmt(self, t):
        print(self.fill() + 'RAISESTMT:', file = self.f, flush = True)
        if not t.expr:
            assert not t.cause
            return
        self.dispatch(t.expr)
        if t.cause:
            self.dispatch(t.cause)

    def _PassStmt(self, t):
        print(self.fill() + 'PASSTSTMT:', file = self.f, flush = True)

    def _TryStmt(self, t):
        print(self.fill() + 'TRYSTMT:', file = self.f, flush = True)
        self.dispatch(t.body)
        for ex in t.excepthandlers:
            self.dispatch(ex)
        if t.elsebody:
            self.dispatch(t.elsebody)
        if t.finalbody:
            self.dispatch(t.finalbody)

    def _ExceptHandler(self, t):
        print(self.fill() + 'EXCEPTHANDLER:', file = self.f, flush = True)
        if t.type:
            self.dispatch(t.type)
        if t.name:
            self.dispatch(t.name)
        self.dispatch(t.body)

    def _ClassStmt(self, t):
        print(self.fill() + 'CLASSSTMT:', file = self.f, flush = True)
        for deco in t.decorators:
            self.dispatch(deco)
        for e in t.bases:
            self.dispatch(e)
        for e in t.keywords:
            self.dispatch(e)
        if t.starargs:
            self.dispatch(t.starargs)
        if t.kwargs:
            self.dispatch(t.kwargs)
        self.dispatch(t.body)

    def _Process(self, t):
        print('\n----------BEGIN PROCESS----------', file = self.f, flush = True)
        print(self.fill() + 'PROCESS:', file = self.f, flush = True)
        for deco in t.decorators:
            self.dispatch(deco)
        if t.bases:
            for e in t.bases:
                self.dispatch(e)
        self.dispatch(t.args)
        for name in t.ordered_local_names:
            self.dispatch(name)
        if t.configurations:
            for key, value in t.configurations:
                self.dispatch(value)
        if t.setup:
            self.dispatch(t.setup.body)
        if t.entry_point:
            self.dispatch(t.entry_point)
        if t.events:
            self.dispatch(t.event_handlers)
        if t.methods:
            self.dispatch(t.methods)
        print('----------END PROCESS----------\n', file = self.f, flush = True)

    def _Function(self, t):
        print(self.fill() + 'FUNCTION:', file = self.f, flush = True)
        for deco in t.decorators:
            self.dispatch(deco)
        self.dispatch(t.args)
        self.dispatch(t.body)

    def _EventHandler(self, t):
        print(self.fill() + 'EVENTHANDLER:', file = self.f, flush = True)
        for evt in t.events:
            self.dispatch(evt)
        if t.labels:
            self.dispatch(t.labels)
        if t.notlabels:
            self.dispatch(t.notlabels)
        self.dispatch(t.body)

    def _ForStmt(self, t):
        print(self.fill() + 'FORSTMT:', file = self.f, flush = True)
        self.dispatch(t.domain)
        self.dispatch(t.body)
        if t.elsebody:
            self.dispatch(t.orelse)

    def _IfStmt(self, t):
        print(self.fill() + 'IFSTMT:', file = self.f, flush = True)
        self.dispatch(t.condition)
        self.dispatch(t.body)
        # collapse nested ifs into equivalent elifs.
        while (t.elsebody and len(t.elsebody) == 1 and
               isinstance(t.elsebody[0], IfStmt)):
            t = t.elsebody[0]
            self.dispatch(t.condition)
            self.dispatch(t.body)
        # final else
        if t.elsebody:
            self.dispatch(t.elsebody)
 
    def _WhileStmt(self, t):
        print(self.fill() + 'WHILESTMT:', file = self.f, flush = True)
        self.dispatch(t.condition)
        self.dispatch(t.body)
        if t.elsebody:
            self.dispatch(t.elsebody)

    def _WithStmt(self, t):
        print(self.fill() + 'WITHSTMT:', file = self.f, flush = True)
        for item, alias in t.items:
            self.dispatch(item)
            if alias:
                self.dispatch(alias)
        self.dispatch(t.body)

    # expr
    def _SimpleExpr(self, t):
        print(self.fill() + 'SIMPLEEXPR:', file = self.f, flush = True)
        if isinstance(t.value, DistNode):
            self.dispatch(t.value)
        elif t.value:
            print(self.fill() + 'VALUE:', repr(t.value), file = self.f, flush = True)

    def _ConstantExpr(self, t):
        print(self.fill() + 'CONSTANTEXPR:', file = self.f, flush = True)

    def _SelfExpr(self, t):
        print(self.fill() + 'SELFEXPR:', file = self.f, flush = True)

    def _TrueExpr(self, t):
        print(self.fill() + 'TRUEEXPR:', file = self.f, flush = True)

    def _FalseExpr(self, t):
        print(self.fill() + 'FALSEEXPR:', file = self.f, flush = True)

    def _NoneExpr(self, t):
        print(self.fill() + 'NONEEXPR:', file = self.f, flush = True)

    def _NameExpr(self, t):
        print(self.fill() + 'NAME_EXPR:', file = self.f, flush = True)
        self.dispatch(t.subexprs)
        
    def _NamedVar(self, t):
        print(self.fill() + 'NAMEDVAR:', file = self.f, flush = True)

    def _ListExpr(self, t):
        print(self.fill() + 'LISTEXPR:', file = self.f, flush = True)
        for subexpr in t.subexprs:
            self.dispatch(subexpr)

    def _TupleExpr(self, t):
        print(self.fill() + 'TUPLEEXPR:', file = self.f, flush = True)
        if len(t.subexprs) == 1:
            (elt,) = t.subexprs
            self.dispatch(elt)
        else:
            for subexpr in t.subexprs:
                self.dispatch(subexpr)

    def _SetExpr(self, t):
        print(self.fill() + 'SETEXPR:', file = self.f, flush = True)
        assert(t.subexprs) # should be at least one element
        for subexpr in t.subexprs:
            self.dispatch(subexpr)
            
    def _DictExpr(self, t):
        print(self.fill() + 'DICTEXPR:', file = self.f, flush = True)
        assert len(t.keys) == len(t.values)
        def write_pair(pair):
            (k, v) = pair
            self.dispatch(k)
            self.dispatch(v)
        for p in zip(t.keys, t.values):
            write_pair(p)

    def _IfExpr(self, t):
        print(self.fill() + 'IFEXPR:', file = self.f, flush = True)
        self.dispatch(t.body)
        self.dispatch(t.condition)
        self.dispatch(t.orbody)

    def _GeneratorExpr(self, t):
        print(self.fill() + 'GENERATOREXPR:', file = self.f, flush = True)
        self.dispatch(t.elem)
        for c in t.conditions:
            self.dispatch(c)

    def _ListCompExpr(self, t):
        print(self.fill() + 'LISTCOMPEXPR:', file = self.f, flush = True)
        self.dispatch(t.elem)
        for c in t.conditions:
            self.dispatch(c)
            
    def _SetCompExpr(self, t):
        print(self.fill() + 'SETCOMPEXPR:', file = self.f, flush = True)
        self.dispatch(t.elem)
        for c in t.conditions:
            self.dispatch(c)

    def _TupleCompExpr(self, t):
        print(self.fill() + 'TUPLECOMPEXPR:', file = self.f, flush = True)
        self.dispatch(t.elem)
        for c in t.conditions:
            self.dispatch(c)

    def _DictCompExpr(self, t):
        print(self.fill() + 'DICTCOMPEXPR:', file = self.f, flush = True)
        self.dispatch(t.elem)
        for c in t.conditions:
            self.dispatch(c)

    def _KeyValue(self, t):
        print(self.fill() + 'KEYVALUE:', file = self.f, flush = True)
        self.dispatch(t.key)
        self.dispatch(t.value)

    def _MaxExpr(self, t):
        print(self.fill() + 'MAXEXPR:', file = self.f, flush = True)
        self._callargs(t)

    def _MinExpr(self, t):
        print(self.fill() + 'MINEXPR:', file = self.f, flush = True)
        self._callargs(t)

    def _SizeExpr(self, t):
        print(self.fill() + 'SIZEEXPR:', file = self.f, flush = True)
        self._callargs(t)

    def _SumExpr(self, t):
        print(self.fill() + 'SUMEXPR:', file = self.f, flush = True)
        self._callargs(t)

    def _DomainSpec(self, t):
        print(self.fill() + 'DOMAINSPEC:', file = self.f, flush = True)
        if not isinstance(t.domain, HistoryExpr):
            self.dispatch(t.pattern)
        self.dispatch(t.domain)

    def _QuantifiedExpr(self, t):
        print(self.fill() + 'QUANTIFIEDEXPR:', file = self.f, flush = True)
        for dom in t.domains:
            self.dispatch(dom)
        self.dispatch(t.predicate)

    def _LogicalExpr(self, t):
        print(self.fill() + 'LOGICALEXPR:', file = self.f, flush = True)
        if t.operator is NotOp:
            self.dispatch(t.left)
        else:
            for s in t.subexprs:
                delf.dispatch(s)
                
    def _UnaryExpr(self, t):
        print(self.fill() + 'UNARYEXPR:', file = self.f, flush = True)
        self.dispatch(t.right)

    def _BinaryExpr(self, t):
        print(self.fill() + 'BINARYEXPR:', file = self.f, flush = True)
        self.dispatch(t.left)
        self.dispatch(t.right)

    def _ComparisonExpr(self, t):
        print(self.fill() + 'COMPARISONEXPR:', file = self.f, flush = True)
        # XXX: Hack! if RHS is HistoryExpr, ignore LHS
        if not isinstance(t.right, HistoryExpr):
            self.dispatch(t.left)
        self.dispatch(t.right)

    def _PatternExpr(self, t):
        print(self.fill() + 'PATTERNEXPR:', file = self.f, flush = True)
        self.dispatch(t.pattern)

    _LiteralPatternExpr = _PatternExpr

    def _ReceivedExpr(self, t):
        print(self.fill() + 'RECEIVEDEXPR:', file = self.f, flush = True)
        self.dispatch(t.event)

    def _SentExpr(self, t):
        print(self.fill() + 'SENTEXPR:', file = self.f, flush = True)
        self.dispatch(t.event)

    def _CallExpr(self, t):
        print(self.fill() + 'CALLEXPR:', file = self.f, flush = True)
        if isinstance(t.func, DistNode):
            self.dispatch(t.func)
        else:
            self.dispatch(t.func)
        self._callargs(t)

    _BuiltinCallExpr = _CallExpr
    _ApiCallExpr = _CallExpr

    def _AttributeExpr(self,t):
        print(self.fill() + 'ATTRIBUTEEXPR:', file = self.f, flush = True)
        self.dispatch(t.value)
        # Special case: 3.__abs__() is a syntax error, so if t.value
        # is an integer literal then we need to either parenthesize
        # it or add an extra space to get 3 .__abs__().
        self.dispatch(t.attr)

    def _SubscriptExpr(self, t):
        print(self.fill() + 'SUBSCRIPTEXPR:', file = self.f, flush = True)
        self.dispatch(t.value)
        self.dispatch(t.index)

    def _SliceExpr(self, t):
        print(self.fill() + 'SLICEEXPR:', file = self.f, flush = True)
        if t.lower:
            self.dispatch(t.lower)
        if t.upper:
            self.dispatch(t.upper)
        if t.step:
            self.dispatch(t.step)

    def _StarredExpr(self, t):
        print(self.fill() + 'STARREDEXPR:', file = self.f, flush = True)
        self.dispatch(t.value)

    # slice
    def _EllipsisExpr(self, t):
        print(self.fill() + 'ELLIPSISEXPR:', file = self.f, flush = True)

    # argument
    def _arg(self, t):
        print(self.fill() + 'ARG:', file = self.f, flush = True)
        self.dispatch(t.arg)
        if t.annotation:
            self.dispatch(t.annotation)

    # pattern
    def _ConstantPattern(self, t):
        print(self.fill() + 'CONSTANTPATTERN:', file = self.f, flush = True)
        self.dispatch(t.value)

    def _FreePattern(self, t):
        print(self.fill() + 'FREEPATTERN:', file = self.f, flush = True)
        print(self.fill() + '*****FreePatternValue:', t.value, file = self.f, flush = True)
        if t.value:
            self.dispatch(t.value)
        else:
            self.dispatch('_')

    def _BoundPattern(self, t):
        print(self.fill() + 'BOUNDPATTERN:', file = self.f, flush = True)
        self.dispatch(t.value)

    _TuplePattern = _TupleExpr
    _ListPattern = _ListExpr

    # others
    def _Arguments(self, t):
        print(self.fill() + 'ARGUMENTS:', file = self.f, flush = True)
        # normal arguments
        defaults = [None] * (len(t.args) - len(t.defaults)) + t.defaults
        for a, d in zip(t.args, defaults):
            self.dispatch(a)
            if d:
                self.dispatch(d)

        # varargs, or bare '*' if no varargs but keyword-only arguments present
        if t.vararg or t.kwonlyargs:
            if t.vararg:
                self.dispatch(t.vararg)

        # keyword-only arguments
        if t.kwonlyargs:
            for a, d in zip(t.kwonlyargs, t.kw_defaults):
                self.dispatch(a),
                if d:
                    self.dispatch(d)

        # kwargs
        if t.kwarg:
            self.dispatch(t.kwarg.arg)

    def _Event(self, t):
        print(self.fill() + 'EVENT:', file = self.f, flush = True)
        self.dispatch(t.pattern)
        if t.sources:
            self.dispatch(t.sources)
        if t.destinations:
            self.dispatch(t.destinations)
        if t.timestamps:
            self.dispatch(t.timestamps)

    def _LambdaExpr(self, t):
        print(self.fill() + 'LAMBDAEXPR:', file = self.f, flush = True)
        self.dispatch(t.args)
        self.dispatch(t.body)

    def _Alias(self, t):
        print(self.fill() + 'ALIAS:', file = self.f, flush = True)
        if t.asname:
            self.dispatch(t.asname)
        else:
            self.dispatch(t.name)

    def _callargs(self, t):
        print(self.fill() + 'CALLARGS:', file = self.f, flush = True)
        for e in t.args:
            self.dispatch(e)
        for key, value in t.keywords:
            self.dispatch(key)
            self.dispatch(value)
        if t.starargs:
            self.dispatch(t.starargs)
        if t.kwargs:
            self.dispatch(t.kwargs)

    def _withitem(self, t):
        print(self.fill() + 'WITHITEM:', file = self.f, flush = True)
        self.dispatch(t.context_expr)
        if t.optional_vars:
            self.dispatch(t.optional_vars)

if __name__ == '__main__':
    #recurse_count = 20
    if len(sys.argv) > 1:
        in_fn = sys.argv[1]
        the_daast = daast_from_file(in_fn, parse_all_args([]))
        dp = DastNest(the_daast)
    else:
        print('An input file name must be provided.', flush = True)
#end main()
