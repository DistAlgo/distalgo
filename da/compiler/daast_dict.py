#Visitor in DastDict is borrowed wholesale from
#Bo Lin and Yanhong Annie Liu
#distalgo/da/compiler/psuedo.py

"""
For each attributed listed in node.__dict__ for the visited note, this visitor
will print out the name of the field, the value of the field, and the type of
the value of the field (the type of the field name is always string.

Dict entries are printed in the order in which nodes are visited, thus the order
matches the nested structure of the output of daast_nest.py.
"""

import sys, ast
from da.compiler.dast import *
from da.compiler.parser import daast_from_file
from da.compiler.ui import parse_all_args

EVENT_TYPES = {
    ReceivedEvent: 'receive',
    SentEvent:     'send'
}



class DastDict:
    """Methods in this class recursively traverse an AST and
    output source code for the abstract syntax; original formatting
    is disregarded. """


    def __init__(self, tree):
        """Unparser(tree, file=sys.stdout) -> None.
         Print the source for tree to file."""
        print('********** BEGIN', tree, '**********', flush = True)
        self.dispatch(tree)
        print('********** END', tree, '**********', flush = True)

    def print_dict(self, t):
        print(t.__class__.__name__, flush = True)
        for d in t.__dict__:
            print('\t', d, '||=>', t.__dict__[d], ':', type(t.__dict__[d]), flush = True)
        print('\n', flush = True)


    def dispatch(self, tree):
        "Dispatcher function, dispatching tree type T to method _T."
        if isinstance(tree, list):
            for t in tree:
                self.dispatch(t)
            return
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
        self.print_dict(tree)
        self.dispatch(tree.body)

    # stmt
    def _SimpleStmt(self, tree):
        self.print_dict(tree)
        self.dispatch(tree.expr)

    def _ImportStmt(self, t):
        self.print_dict(t)
        for target in t.items:
           self.dispatch(target)

    def _ImportFromStmt(self, t):
        self.print_dict(t)
        for target in t.items:
            self.dispatch(target)

    def _AssignmentStmt(self, t):
        self.print_dict(t)
        for target in t.targets:
            self.dispatch(target)
        self.dispatch(t.value)

    def _OpAssignmentStmt(self, t):
        self.print_dict(t)
        self.dispatch(t.target)
        self.dispatch(t.value)

    def _ReturnStmt(self, t):
        self.print_dict(t)
        if t.value:
            self.dispatch(t.value)

    def _NoopStmt(self, t):
        self.print_dict(t)
        pass

    def _BreakStmt(self, t):
        self.print_dict(t)
        pass

    def _ContinueStmt(self, t):
        self.print_dict(t)
        pass

    def _DeleteStmt(self, t):
        self.print_dict(t)
        for target in t.targets:
            self.dispatch(target)

    def _AssertStmt(self, t):
        self.print_dict(t)
        self.dispatch(t.expr)
        if t.msg:
            self.dispatch(t.msg)

    def _GlobalStmt(self, t):
        self.print_dict(t)
        #names
        pass

    def _NonlocalStmt(self, t):
        self.print_dict(t)
        #names
        pass

    def _AwaitStmt(self, t):
        self.print_dict(t)
        self._do_await_branches(t)

    def _LoopingAwaitStmt(self, t):
        self.print_dict(t)
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
        self.print_dict(t)
        self.dispatch(t.condition)
        self.dispatch(t.body)

    def _SendStmt(self, t):
        self.print_dict(t)
        self.dispatch(t.message)
        self.dispatch(t.target)

    def _OutputStmt(self, t):
        self.print_dict(t)
        for m in t.message:
            self.dispatch(m)
        if t.level:
            self.dispatch(t.level)

    def _ResetStmt(self, t):
        self.print_dict(t)
        self.dispatch(t.expr)

    def _YieldStmt(self, t):
        self.print_dict(t)
        if t.value:
            self.dispatch(t.value)

    def _YieldFrom(self, t):
        self.print_dict(t)
        if t.value:
            self.dispatch(t.value)

    def _RaiseStmt(self, t):
        self.print_dict(t)
        if not t.expr:
            assert not t.cause
            return
        self.dispatch(t.expr)
        if t.cause:
            self.dispatch(t.cause)

    def _PassStmt(self, t):
        self.print_dict(t)
        pass

    def _TryStmt(self, t):
        self.print_dict(t)
        self.dispatch(t.body)
        for ex in t.excepthandlers:
            self.dispatch(ex)
        if t.elsebody:
            self.dispatch(t.elsebody)
        if t.finalbody:
            self.dispatch(t.finalbody)

    def _ExceptHandler(self, t):
        self.print_dict(t)
        if t.type:
            self.dispatch(t.type)
        if t.name:
            self.dispatch(t.name)
        self.dispatch(t.body)

    def _ClassStmt(self, t):
        self.print_dict(t)
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
        self.print_dict(t)
        for deco in t.decorators:
            self.dispatch(deco)
        if t.bases:
            for e in t.bases:
                self.dispatch(e)
        self.dispatch(t.args)
        #t.ordered_local_names
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

    def _Function(self, t):
        self.print_dict(t)
        for deco in t.decorators:
            self.dispatch(deco)
        self.dispatch(t.args)
        self.dispatch(t.body)

    def _EventHandler(self, t):
        self.print_dict(t)
        for evt in t.events:
            self.dispatch(evt)
        if t.labels:
            self.dispatch(t.labels)
        if t.notlabels:
            self.dispatch(t.notlabels)
        self.dispatch(t.body)

    def _ForStmt(self, t):
        self.print_dict(t)
        self.dispatch(t.domain)
        self.dispatch(t.body)
        if t.elsebody:
            self.dispatch(t.orelse)

    def _IfStmt(self, t):
        self.print_dict(t)
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
        self.print_dict(t)
        self.dispatch(t.condition)
        self.dispatch(t.body)
        if t.elsebody:
            self.dispatch(t.elsebody)

    def _WithStmt(self, t):
        for item, alias in t.items:
            self.dispatch(item)
            if alias:
                self.dispatch(alias)
        self.dispatch(t.body)

    # expr
    def _SimpleExpr(self, t):
        self.print_dict(t)
        if isinstance(t.value, DistNode):
            self.dispatch(t.value)
        #t.value may not be a DistNode
        elif t.value:
            pass

    def _NameExpr(self, t):
        self.print_dict(t)
        self.dispatch(t.subexprs)
                                                        
    def _ConstantExpr(self, t):
        self.print_dict(t)
        pass
    
    def _SelfExpr(self, t):
        self.print_dict(t)
        pass

    def _TrueExpr(self, t):
        self.print_dict(t)
        pass

    def _FalseExpr(self, t):
        self.print_dict(t)
        pass

    def _NoneExpr(self, t):
        self.print_dict(t)
        pass

    def _NamedVar(self, t):
        self.print_dict(t)
        pass

    def _ListExpr(self, t):
        self.print_dict(t)
        for subexpr in t.subexprs:
            self.dispatch(subexpr)

    def _TupleExpr(self, t):
        self.print_dict(t)
        if len(t.subexprs) == 1:
            (elt,) = t.subexprs
            self.dispatch(elt)
        else:
            for subexpr in t.subexprs:
                self.dispatch(subexpr)

    def _SetExpr(self, t):
        self.print_dict(t)
        assert(t.subexprs) # should be at least one element
        for subexpr in t.subexprs:
            self.dispatch(subexpr)
            
    def _DictExpr(self, t):
        assert len(t.keys) == len(t.values)
        self.print_dict(t)
        def write_pair(pair):
            (k, v) = pair
            self.dispatch(k)
            self.dispatch(v)
        for p in zip(t.keys, t.values):
            write_pair(p)

    def _IfExpr(self, t):
        self.print_dict(t)
        self.dispatch(t.body)
        self.dispatch(t.condition)
        self.dispatch(t.orbody)

    def _GeneratorExpr(self, t):
        self.print_dict(t)
        self.dispatch(t.elem)
        for c in t.conditions:
            self.dispatch(c)

    def _ListCompExpr(self, t):
        self.print_dict(t)
        self.dispatch(t.elem)
        for c in t.conditions:
            self.dispatch(c)
            
    def _SetCompExpr(self, t):
        self.print_dict(t)
        self.dispatch(t.elem)
        for c in t.conditions:
            self.dispatch(c)

    def _TupleCompExpr(self, t):
        self.print_dict(t)
        self.dispatch(t.elem)
        for c in t.conditions:
            self.dispatch(c)

    def _DictCompExpr(self, t):
        self.print_dict(t)
        self.dispatch(t.elem)
        for c in t.conditions:
            self.dispatch(c)

    def _KeyValue(self, t):
        self.print_dict(t)
        self.dispatch(t.key)
        self.dispatch(t.value)

    def _MaxExpr(self, t):
        self.print_dict(t)
        self._callargs(t)

    def _MinExpr(self, t):
        self.print_dict(t)
        self._callargs(t)

    def _SizeExpr(self, t):
        self.print_dict(t)
        self._callargs(t)

    def _SumExpr(self, t):
        self.print_dict(t)
        self._callargs(t)

    def _DomainSpec(self, t):
        self.print_dict(t)
        if not isinstance(t.domain, HistoryExpr):
            self.dispatch(t.pattern)
        self.dispatch(t.domain)

    def _QuantifiedExpr(self, t):
        self.print_dict(t)
        for dom in t.domains:
            self.dispatch(dom)
        self.dispatch(t.predicate)

    def _LogicalExpr(self, t):
        self.print_dict(t)
        if t.operator is NotOp:
            self.dispatch(t.left)
        else:
            for s in t.subexprs:
                delf.dispatch(s)
                
    def _UnaryExpr(self, t):
        self.print_dict(t)
        self.dispatch(t.right)

    def _BinaryExpr(self, t):
        self.print_dict(t)
        self.dispatch(t.left)
        self.dispatch(t.right)

    def _ComparisonExpr(self, t):
        self.print_dict(t)
        # XXX: Hack! if RHS is HistoryExpr, ignore LHS
        if not isinstance(t.right, HistoryExpr):
            self.dispatch(t.left)
        self.dispatch(t.right)

    def _PatternExpr(self, t):
        self.print_dict(t)
        self.dispatch(t.pattern)

    _LiteralPatternExpr = _PatternExpr

    def _ReceivedExpr(self, t):
        self.print_dict(t)
        self.dispatch(t.event)

    def _SentExpr(self, t):
        self.print_dict(t)
        self.dispatch(t.event)

    def _CallExpr(self, t):
        self.print_dict(t)
        if isinstance(t.func, DistNode):
            self.dispatch(t.func)
        else:
            #func may not be a DistNode (may be a string instead)
            pass
        self._callargs(t)

    _BuiltinCallExpr = _CallExpr
    _ApiCallExpr = _CallExpr

    def _AttributeExpr(self,t):
        self.print_dict(t)
        self.dispatch(t.value)
        # Special case: 3.__abs__() is a syntax error, so if t.value
        # is an integer literal then we need to either parenthesize
        # it or add an extra space to get 3 .__abs__().
        self.dispatch(t.attr)

    def _SubscriptExpr(self, t):
        self.print_dict(t)
        self.dispatch(t.value)
        self.dispatch(t.index)

    def _SliceExpr(self, t):
        self.print_dict(t)
        if t.lower:
            self.dispatch(t.lower)
        if t.upper:
            self.dispatch(t.upper)
        if t.step:
            self.dispatch(t.step)

    def _StarredExpr(self, t):
        self.print_dict(t)
        self.dispatch(t.value)

    # slice
    def _EllipsisExpr(self, t):
        self.print_dict(t)
        pass

    # argument
    def _arg(self, t):
        self.print_dict(t)
        self.dispatch(t.arg)
        if t.annotation:
            self.dispatch(t.annotation)

    # pattern
    def _ConstantPattern(self, t):
        self.print_dict(t)
        self.dispatch(t.value)

    def _FreePattern(self, t):
        self.print_dict(t)
        if t.value:
            self.dispatch(t.value)
        else:
            #if t.value == None, then this is a wildcard expression '_'
            pass

    def _BoundPattern(self, t):
        self.print_dict(t)
        self.dispatch(t.value)

    _TuplePattern = _TupleExpr
    _ListPattern = _ListExpr

    # others
    def _Arguments(self, t):
        self.print_dict(t)
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
        self.print_dict(t)
        self.dispatch(t.pattern)
        if t.sources:
            self.dispatch(t.sources)
        if t.destinations:
            self.dispatch(t.destinations)
        if t.timestamps:
            self.dispatch(t.timestamps)

    def _LambdaExpr(self, t):
        self.print_dict(t)
        self.dispatch(t.args)
        self.dispatch(t.body)

    def _Alias(self, t):
        self.print_dict(t)
        #.name and .asname are both strings now
        pass

    def _callargs(self, t):
        self.print_dict(t)
        for e in t.args:
            self.dispatch(e)
        for key, value in t.keywords:
            #key will not be a DistNode
            #self.dispatch(key)
            self.dispatch(value)
        if t.starargs:
            self.dispatch(t.starargs)
        if t.kwargs:
            self.dispatch(t.kwargs)

    def _withitem(self, t):
        self.print_dict(t)
        self.dispatch(t.context_expr)
        if t.optional_vars:
            self.dispatch(t.optional_vars)

            
if __name__ == '__main__':
    #recurse_count = 20
    if len(sys.argv) > 1:
        in_fn = sys.argv[1]
        the_daast = daast_from_file(in_fn, parse_all_args([]))
        dp = DastDict(the_daast)
    else:
        print('An input file name must be provided.', flush = True)
#end main()
