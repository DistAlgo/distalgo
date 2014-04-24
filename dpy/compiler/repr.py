from ast import AST, dump
from itertools import chain

class DistNode(AST):
    """Base class for the DistAlgo AST.

    This is a subclass of ast.AST, so the same AST 'visitor' mechanism can be
    used to traverse DistPy ASTs. However care must be taken when using an AST
    'transformer' to change a DistPy AST tree, since a DistPy AST may not be a
    strict tree structure, due to NamedVar instances having non-unique parents.

    """

    _fields = []
    _attributes = ['lineno', 'col_offset']

    def __init__(self, parent=None, ast=None):
        self._ast = ast
        self._parent = parent
        self.lineno = 0
        self.col_offset = 0
        if ast is not None:
            self.copy_location(ast)

    def copy_location(self, ast):
        if hasattr(ast, "lineno"):
            self.lineno = ast.lineno
        if hasattr(ast, "col_offset"):
            self.col_offset = ast.col_offset

    def replace_child(self, oldnode, newnode):
        """Replace all occurances of 'oldnode' with 'newnode' in the tree
        rooted at this node.
        """
        pass

    @property
    def scope(self):
        return None

    @property
    def parent(self):
        return self._parent

    @property
    def ast(self):
        return self._ast

    @ast.setter
    def ast(self, ast):
        assert ast is not None and isinstance(ast, AST)
        self._ast = ast
        self.copy_location(ast)


class NameScope(DistNode):
    """Implements a binding scope for variable names.
    """

    _fields = []

    def __init__(self, parent=None, ast=None):
        super().__init__(parent, ast)
        self._names = dict()

    def find_name(self, name, local=False):
        """Looks up a name from this scope.
        """
        assert isinstance(name, str)

        entity = self._names.get(name)
        if (entity is not None) or local:
            return entity
        elif self.parent_scope is not None:
            return self.parent_scope.find_name(name)
        else:
            return None

    def add_name(self, name, override=True):
        """Adds a name to the local scope.

        Return the NamedVar object for this name.
        """
        if not override:
            obj = self.find_name(name)
            if obj is not None:
                return obj
        obj = NamedVar(name)
        self._names[name] = obj
        return obj

    def link_name(self, namedvar):
        """Links a name object into this scope.

        If the name already exists in the current scope, the info from the
        existing name object is merged into the new object and all pointers to
        the old name is updated to the new name.
        """
        assert isinstance(namedvar, NamedVar)
        oldname = self.find_name(namedvar.name, local=True)
        if oldname is not None and oldname is not namedvar:
            namedvar.merge(oldname)
            for node in chain(oldname.assignments,
                              oldname.updates,
                              oldname.reads):
                node.replace_child(oldname, namedvar)
        self._names[namedvar.name] = namedvar
        return oldname

    @property
    def skip(self):
        """True if this scope should be skipped in the parent-child scope hierachy.
        """
        return False

    @property
    def scope(self):
        return self

    @property
    def parent_scope(self):
        p = self.parent
        while p is not None:
            if isinstance(p, NameScope) and not p.skip:
                return p
            else:
                p = p.parent
        return None

    @property
    def local_names(self):
        return self._names.keys()

    @property
    def local_nameobjs(self):
        return self._names.values()

class LockableNameScope(NameScope):
    def __init__(self, parent=None, ast=None):
        super().__init__(parent, ast)
        self.locked = False

    def get_name(self, name):
        if not self.locked:
            entity = NamedVar(name)
            self._names[name] = entity
            return entity
        else:
            return super().get_name(name)

    def lock(self):
        self.locked = True

    def unlock(self):
        self.locked = False


class ArgumentsContainer(NameScope):

    _fields = ["args", "defaults", "vararg", "kwonlyargs", "kw_defaults",
               "kwarg"]

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.args = []
        self.defaults = []
        self.vararg = None
        self.kwonlyargs = []
        self.kw_defaults = []
        self.kwarg = None

    def add_arg(self, name):
        assert isinstance(name, str)
        e = self.add_name(name)
        e.add_assignment(self)
        self.args.append(e)

    def add_defaultarg(self, name, value):
        assert isinstance(name, str) and isinstance(value, DistNode)
        e = self.add_name(name)
        e.add_assignment(self, typectx=value)
        self.args.append(e)
        self.defaults.append(value)

    def add_vararg(self, name):
        assert self.vararg is None and isinstance(name, str)
        e = self.add_name(name)
        e.add_assignment(self, typectx=list)
        self.vararg = e

    def add_kwonlyarg(self, name, value):
        assert isinstance(name, str) and isinstance(value, DistNode)
        e = self.add_name(name)
        e.add_assignment(self, typectx=value)
        self.kwonlyargs.append(e)
        self.kw_defaults.append(value)

    def add_kwarg(self, name):
        assert self.kwarg is None and isinstance(name, str)
        e = self.add_name(name)
        e.add_assignment(self, typectx=dict)
        self.kwarg = name

    @property
    def argnames(self):
        res = list(self.args)
        if self.vararg is not None:
            res.append(self.vararg)
        if self.kwarg is not None:
            res.append(self.kwarg)
        res.extend(self.kwonlyargs)
        return res


class NamedVar(DistNode):
    """Node representing a named variable.

    Unlike other node types, a NamedVar instance may have multiple parents.
    This is because the same names that refer to the same variable (those that
    occur in the same naming scope) in the program are represented by a shared
    NamedVar instance.

    """

    _fields = ['name']

    def __init__(self, name, ast=None):
        super().__init__(ast)
        self.name = name
        self.assignments = []
        self.updates = []
        self.reads = []
        self.aliases = []

    def merge(self, target):
        """Merges all info from 'target'.
        """
        assert isinstance(target, NamedVar)
        self.assignments.extend(target.assignments)
        self.updates.extend(target.updates)
        self.reads.extend(target.reads)
        self.aliases.extend(target.aliases)

    def add_assignment(self, node, typectx=object):
        """Add a node where this variable is being assigned to.

        An 'assignment' is a point in the program where a new value is
        assigned to this variable. This includes assignment statements and
        argument definitions.

        """
        self.assignments.append((node, typectx))

    def add_update(self, node, attr=None, attrtype=object):
        """Add a node where this variable is being updated.

        An 'update' is a point in the program where the state of the object
        referred to by this variable is changed (in Python, primitive types
        such as int and float can not be updated). This includes assigning to
        attributes and calling methods which potentially change object state.

        """
        self.updates.append((node, attr, attrtype))

    def add_read(self, node, typectx=object):
        """Add a node where the value of this variable is being read.
        """
        self.reads.append((node, typectx))

    def purge_reads(self, scope):
        """Purges all read references of this NamedVar from given scope.

        Return list of purged read references.
        """
        removed = []
        remain = []
        for ref in self.reads:
            if ref[0].scope is scope:
                removed.append(ref)
            else:
                remain.append(ref)
        self.reads = remain
        return removed

    @property
    def scope(self):
        if len(self.assignments) > 0:
            return self.assignments[0][0].scope
        elif len(self.updates) > 0:
            return self.updates[0][0].scope
        elif len(self.reads) > 0:
            return self.reads[0][0].scope
        else:
            return None

    @property
    def is_arg(self):
        """True if this variable is an argument of a function or lamba expression.
        """
        if (len(self.updates) > 0 and
            isinstance(self.updates[0], ArgumentsContainer)):
            return True
        else:
            return False


# Expressions:

class Expression(DistNode):
    """Base class for expressions.
    """

    _fields = ['subexprs']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.subexprs = []

    def replace_child(self, oldnode, newnode):
        """Replace all occurances of 'oldnode' with 'newnode' in the tree
        rooted at this node.
        """
        for idx, node in enumerate(self.subexprs):
            if node is oldnode:
                self.subexprs[idx] = newnode
            elif node is not None:
                node.replace_child(oldnode, newnode)

    @property
    def scope(self):
        if isinstance(self, NameScope):
            return self
        else:
            assert self.parent is not None
            return self.parent.scope

    @property
    def names(self):
        res = set()
        for e in self.subexprs:
            res |= e.names
        return res

    @property
    def parent_statement(self):
        p = self.parent
        while p is not None and not isinstance(p, Statement):
            p = p.parent
        return p

class PythonExpr(Expression):
    """This is placeholder for unsupported Python expressions."""
    _fields = []

class SimpleExpr(Expression):

    _fields = ['value']

    def __init__(self, parent, ast=None, value=None):
        super().__init__(parent, ast)
        self.subexprs = [value]

    @property
    def value(self):
        return self.subexprs[0]

    @value.setter
    def value(self, val):
        self.subexprs[0] = val

    @property
    def names(self):
        if type(self.value) is NamedVar:
            return {self.value}
        else:
            return set()

class AttributeExpr(SimpleExpr):

    _fields = ['value', 'attr']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.attr = None

class SubscriptExpr(SimpleExpr):

    _fields = ['value', 'index']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.subexprs.append(None)

    @property
    def index(self):
        return self.subexprs[1]

    @index.setter
    def index(self, idx):
        self.subexprs[1] = idx

class SliceExpr(Expression):

    _fields = ['lower', 'upper', 'step']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.subexprs = [None, None, None]

    @property
    def lower(self):
        return self.subexprs[0]

    @lower.setter
    def lower(self, idx):
        self.subexprs[0] = idx

    @property
    def upper(self):
        return self.subexprs[1]

    @upper.setter
    def upper(self, idx):
        self.subexprs[1] = idx

    @property
    def step(self):
        return self.subexprs[2]

    @step.setter
    def step(self, idx):
        self.subexprs[2] = idx


class StarredExpr(SimpleExpr): pass
class EllipsisExpr(SimpleExpr): pass
class ConstantExpr(SimpleExpr): pass
class SelfExpr(ConstantExpr): pass
class TrueExpr(ConstantExpr): pass
class FalseExpr(ConstantExpr): pass
class NoneExpr(ConstantExpr): pass

class SequenceExpr(Expression): pass
class TupleExpr(SequenceExpr): pass
class ListExpr(SequenceExpr): pass
class SetExpr(SequenceExpr): pass

class DictExpr(Expression):

    _fields = ['keys', 'values']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.keys = self.subexprs
        self.values = []

    @property
    def names(self):
        res = super().names()
        for v in self.values:
            res |= v.names
        return res

class IfExpr(Expression):

    _fields = ['condition', 'body', 'orbody']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.subexprs = [None, None, None]

    @property
    def condition(self):
        return self.subexprs[0]

    @property
    def body(self):
        return self.subexprs[1]

    @property
    def orbody(self):
        return self.subexprs[2]

    @condition.setter
    def condition(self, expr):
        self.subexprs[0] = expr

    @body.setter
    def body(self, expr):
        self.subexprs[1] = expr

    @orbody.setter
    def orbody(self, expr):
        self.subexprs[2] = expr

class CallExpr(Expression):

    _fields = ['func', 'args']

    def __init__(self, statement, ast=None):
        super().__init__(statement, ast)
        self.func = None
        self.args = []

    @property
    def names(self):
        res = set()
        if self.func is not None:
            res |= self.func.names
        for a in self.args:
            res |= a.names
        return res

class ApiCallExpr(CallExpr):
    @property
    def names(self):
        res = set()
        for a in self.args:
            res |= a.names
        return res

class BuiltinCallExpr(CallExpr):
    @property
    def names(self):
        res = set()
        for a in self.args:
            res |= a.names
        return res

class BooleanOperator(DistNode): pass
class AndOp(BooleanOperator): pass
class OrOp(BooleanOperator): pass
class NotOp(BooleanOperator): pass

class BooleanExpr(Expression): pass

class LogicalExpr(BooleanExpr):

    _fields = ['operator', 'subexprs']

    def __init__(self, statement, ast=None, op=None):
        super().__init__(statement, ast)
        self.operator = op

    @property
    def left(self):
        if len(self.subexprs) > 0:
            return self.subexprs[0]
        else:
            return None

    @left.setter
    def left(self, val):
        if len(self.subexprs) > 0:
            self.subexprs[0] = val
        else:
            self.subexprs.append(val)

class KeyValue(DistNode):
    def __init__(self, parent, ast=None):
        super().__init__(ast)
        self.expr = parent
        self.key = None
        self.value = None

class QuantifiedExpr(BooleanExpr, LockableNameScope):

    _index = 0
    _fields = ['elem', 'iters', 'targets', 'conditions']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.elem = None
        self.targets = []
        self.iters = []
        self.conditions = self.subexprs
        self.index = QuantifiedExpr._index
        QuantifiedExpr._index += 1

    @property
    def names(self):
        res = set()
        for e in self.targets:
            res |= e.names
        return res

class ExistentialExpr(QuantifiedExpr): pass
class UniversalExpr(QuantifiedExpr): pass

class ComprehensionExpr(Expression, LockableNameScope):

    _fields = ['elem', 'iters', 'targets', 'conditions']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.elem = None
        self.targets = []
        self.iters = []
        self.conditions = self.subexprs

    @property
    def names(self):
        res = set()
        for e in self.targets:
            res |= e.names
        return res

class GeneratorExpr(ComprehensionExpr): pass
class SetCompExpr(ComprehensionExpr): pass
class ListCompExpr(ComprehensionExpr): pass
class DictCompExpr(ComprehensionExpr): pass

class AggregateExpr(Expression):

    _fields = ['value']

    def __init__(self, statement, ast=None):
        super().__init__(statement, ast)
        self.subexprs = [None]

    @property
    def value(self):
        return self.subexprs[0]

    @value.setter
    def value(self, val):
        self.subexprs[0] = val

class MaxExpr(AggregateExpr): pass
class MinExpr(AggregateExpr): pass
class SizeExpr(AggregateExpr): pass
class SumExpr(AggregateExpr): pass

class ComparisonOperator(DistNode): pass
class EqOp(ComparisonOperator): pass
class NotEqOp(ComparisonOperator): pass
class LtOp(ComparisonOperator): pass
class LtEOp(ComparisonOperator): pass
class GtOp(ComparisonOperator): pass
class GtEOp(ComparisonOperator): pass
class IsOp(ComparisonOperator): pass
class IsNotOp(ComparisonOperator): pass
class InOp(ComparisonOperator): pass
class NotInOp(ComparisonOperator): pass

class ComparisonExpr(BooleanExpr):

    _fields = ['left', 'right', 'comparator']

    def __init__(self, statement, ast=None):
        super().__init__(statement, ast)
        self.comparator = None
        self.subexprs = [None, None]

    @property
    def left(self):
        return self.subexprs[0]

    @property
    def right(self):
        return self.subexprs[1]

    @left.setter
    def left(self, val):
        self.subexprs[0] = val

    @right.setter
    def right(self, val):
        self.subexprs[1] = val

class Operator(DistNode): pass
class AddOp(Operator):pass
class SubOp(Operator): pass
class MultOp(Operator): pass
class DivOp(Operator): pass
class ModOp(Operator): pass
class PowOp(Operator): pass
class LShiftOp(Operator): pass
class RShiftOp(Operator): pass
class BitOrOp(Operator): pass   # Also set union
class BitXorOp(Operator): pass  # Also set Xor
class BitAndOp(Operator): pass  # Also set intersect
class FloorDivOp(Operator): pass

class UnaryOperator(DistNode): pass
class InvertOp(UnaryOperator): pass
class UAddOp(UnaryOperator): pass
class USubOp(UnaryOperator): pass

class ArithmeticExpr(Expression):

    _fields = ['operator', 'left', 'right']

    def __init__(self, statement, ast=None, op=None):
        super().__init__(statement, ast)
        self.operator = op
        self.subexprs = [None, None]

    @property
    def left(self):
        return self.subexprs[0]

    @property
    def right(self):
        return self.subexprs[1]

    @left.setter
    def left(self, val):
        self.subexprs[0] = val

    @right.setter
    def right(self, val):
        self.subexprs[1] = val

class PatternElementType: pass
class ConstantVar(PatternElementType): pass
class BoundVar(PatternElementType): pass
class FreeVar(PatternElementType): pass
class TupleVar(PatternElementType): pass
class ListVar(PatternElementType): pass

class PatternElement(DistNode):
    """An Element represents a sub-component of a pattern.
    """

    _fields = ['type', 'value']

    def __init__(self, elemtype, value, ast=None):
        super().__init__(ast)
        self.type = elemtype
        self.value = value

    def replace_child(self, oldnode, newnode):
        """Replace all occurances of 'oldnode' with 'newnode' in the tree
        rooted at this node.
        """
        if self.value is oldnode:
            self.value = newnode
        elif self.value is not None:
            self.value.replace_child(oldnode, newnode)


    @property
    def boundvars(self):
        if self.type is BoundVar:
            return {self.value}
        elif self.type in {FreeVar, ConstantVar}:
            return set()
        else:
            result = set()
            for v in self.value:
                result |= v.boundvars
            return result

    @property
    def freevars(self):
        if self.type is FreeVar:
            return {self.value}
        elif self.type in {BoundVar, ConstantVar}:
            return set()
        else:
            result = set()
            for v in self.value:
                result |= v.freevars
            return result

    def match(self, target):
        """Compare two Elements to see if they describe the same pattern.
        """

        assert isinstance(target, PatternElement)

        if self.type is not target.type:
            return False
        elif self.type is FreeVar or self.type is BoundVar :
            return True
        elif self.type is ConstantVar:
            return target.value == self.value
        elif self.type is TupleVar or self.type is ListVar:
            if len(self.value) != len(target.value):
                return False
            for v, t in zip(self.value, target.value):
                if not v.match(t):
                    return False
            return True

class HistoryExpr(Expression):

    _fields = ['subexprs']

    def __init__(self, statement, event, ast=None):
        super().__init__(statement, ast)
        self.event = event

    @property
    def evtidx(self):
        if self.event is None: return None
        return self.event.index

    @property
    def names(self):
        return self.boundvars

    @property
    def boundvars(self):
        if self.pattern is not None:
            return self.pattern.boundvars
        else:
            return set()

    @property
    def freevars(self):
        if self.pattern is not None:
            return self.pattern.freevars
        else:
            return set()


class ReceivedExpr(HistoryExpr): pass
class SentExpr(HistoryExpr): pass

class LambdaExpr(Expression, ArgumentsContainer):

    _fields = ["subexprs"] + ArgumentsContainer._fields

    def __init__(self, parent, ast=None):
        super().__init__(parent ,ast)
        self.subexprs[0] = None

    @property
    def body(self):
        return self.subexprs[0]

    @body.setter
    def body(self, expr):
        assert isinstance(expr, Expression)
        self.subexprs[0] = expr


# Statements:

class Statement(DistNode):
    """Base class for statements.
    """

    _index = 0
    _fields = ['label', 'ast']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.label = None
        self.index = Statement._index
        Statement._index += 1

    @property
    def statement(self):
        return self

    @property
    def unique_label(self):
        if self.label is not None:
            return self.label
        else:
            return "_st_label_%d" % self.index

    @property
    def scope(self):
        if isinstance(self, NameScope):
            return self
        else:
            assert self._parent is not None
            return self._parent.scope

class BlockStatement(Statement):

    _fields = ["body"]

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.body = []

    def replace_child(self, oldnode, newnode):
        """Replace all occurances of 'oldnode' with 'newnode' in the tree
        rooted at this node.
        """
        for idx, node in enumerate(self.body):
            if node is oldnode:
                self.body[idx] = newnode
            elif node is not None:
                node.replace_child(oldnode, newnode)

class Program(BlockStatement, NameScope):
    """The global NameScope.
    """

    _fields = ['processes', 'entry_point', 'body']

    def __init__(self, ast=None):
        super().__init__(None, ast)
        self.processes = []
        self.entry_point = None

    @property
    def skip(self):
        return False

class InteractiveProgram(BlockStatement, NameScope):
    """For compiling interactively entered code.
    """

    _fields = ['processes', 'entry_point', 'body']

    def __init__(self, ast=None):
        super().__init__(None, ast)
        self.processes = []
        self.entry_point = None

    @property
    def skip(self):
        return False

class Function(BlockStatement, ArgumentsContainer):

    _fields = ['name', 'arguments', 'body']

    def __init__(self, name, parent, ast=None):
        super().__init__(parent, ast)
        self._name = name
        self.process = None
        self.decorators = []
        self.arguments = []

    @property
    def name(self):
        return self._name

class ClassStmt(BlockStatement, NameScope):

    _fields = ['name', 'bases', 'body']

    def __init__(self, name, parent, bases=[], ast=None):
        super().__init__(parent, ast)
        self.name = name
        self.bases = bases
        self.keywords = []
        self.starargs = None
        self.kwargs = None
        self.decorator_list = []

    @property
    def skip(self):
        """Class scope should be skipped when resolving names from a child scope.
        """
        return True

class NoopStmt(Statement): pass

class AssignmentStmt(Statement):

    _fields = ['targets', 'value']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.targets = []
        self.value = None

class OpAssignmentStmt(AssignmentStmt):

    _fields = ['target', 'value']

    def __init__(self, op, parent, ast=None):
        super().__init__(parent, ast)
        self.op = op
        self.targets[0] = None

    @property
    def target(self):
        return self.targets[0]

    @target.setter
    def target(self, tgt):
        self.targets[0] = tgt

class IfStmt(Statement):

    _fields = ['condition', 'body', 'elsebody']

    def __init__(self, scope, ast=None):
        super().__init__(scope, ast)
        self.condition = None
        self.body = []
        self.elsebody = []

class WhileStmt(Statement):

    _fields = ['condition', 'body', 'elsebody']

    def __init__(self, scope, ast=None):
        super().__init__(scope, ast)
        self.condition = None
        self.body = []
        self.elsebody = []

class ForStmt(Statement):

    _fields = ['target', 'iter', 'body', 'elsebody']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.target = None
        self.iter = None
        self.body = []
        self.elsebody = []

class TryStmt(Statement):

    _fields = ['body', 'excepthandlers', 'elsebody']

    def __init__(self, scope, ast=None):
        super().__init__(scope, ast)
        self.body = []
        self.excepthandlers = []
        self.elsebody = []
        self.finalbody = []

class ExceptHandler(DistNode):

    _fields = ['type', 'name', 'body']

    def __init__(self, ast=None):
        super().__init__(ast)
        self.type = None
        self.name = None
        self.body = []

class TryFinallyStmt(Statement):

    _fields = ['body', 'finalbody']

    def __init__(self, scope, ast=None):
        super().__init__(scope, ast)
        self.body = []
        self.finalbody = []

class AwaitStmt(Statement):

    _fields = ['condition', 'timeout']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.branches = []
        self.orelse = []
        self.timeout = None

class LoopingAwaitStmt(Statement):

    _fields = ['condition', 'timeout', 'body']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.condition = None
        self.timeout = None
        self.body = []

class Branch(DistNode):

    _fields = ['condition', 'body']

    def __init__(self, conditon, parent, ast=None):
        super().__init__(parent, ast)
        self.condition = conditon
        self.body = []

class ReturnStmt(Statement):

    _fields = ['value']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.value = None

class DeleteStmt(Statement):

    _fields = ['targets']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.targets = []

class YieldStmt(Statement):

    _fields = ['value']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.value = None

class YieldFromStmt(Statement):

    _fields = ['value']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.value = None

class WithStmt(Statement, LockableNameScope):

    _fields = ['ctxexpr', 'optional_vars', 'body']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.items = []
        self.body = []

class RaiseStmt(Statement):

    _fields = ['expr', 'cause']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.expr = None
        self.cause = None

class SimpleStmt(Statement):

    _fields = ['expr']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.expr = None

class BreakStmt(Statement): pass
class PassStmt(Statement): pass
class ContinueStmt(Statement): pass

class PythonStmt(SimpleStmt): pass

class AssertStmt(PythonStmt):

    _fields = ['expr', 'msg']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.msg = None

class GlobalStmt(PythonStmt):

    _fields = ['names']

    def __init__(self, parent, ast=None, names=[]):
        super().__init__(parent, ast)
        self.names = list(names)

class NonlocalStmt(PythonStmt):

    _fields = ['names']

    def __init__(self, parent, ast=None, names=[]):
        super().__init__(parent, ast)
        self.names = list(names)

class SendStmt(Statement):

    _fields = ['message', 'target']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.message = None
        self.target = None

class OutputStmt(Statement):

    _fields = ['message', 'level']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.message = None
        self.level = None

class EventType: pass
class ReceivedEvent(EventType): pass
class SentEvent(EventType): pass

class Event(DistNode):

    _fields = ['type', 'pattern', 'sources', 'destinations',
               'timestamps', 'handlers']
    _index = 0

    def __init__(self, process, event_type, pattern=None, ast=None):
        super().__init__(ast)
        self.process = process
        self.type = event_type
        self.pattern = pattern
        self.sources = []
        self.destinations = []
        self.timestamps = []
        self.handlers = []
        self.record_history = False
        self.index = Event._index
        Event._index += 1

    @property
    def name(self):
        return "%s_%d" % (self.type.__name__, self.index)

    @property
    def boundvars(self):
        if self.pattern is not None:
            result = self.pattern.boundvars
        else:
            result = set()
        for vs in chain(self.sources, self.destinations, self.timestamps):
            result |= vs.boundvars
        return result

    @property
    def freevars(self):
        if self.pattern is not None:
            result = self.pattern.freevars
        else:
            result = set()
        for vs in chain(self.sources, self.destinations, self.timestamps):
            result |= vs.freevars
        return result

    def match(self, target):
        if (not self.pattern.match(target.pattern) or
                not len(self.sources) == len(target.sources) or
                not len(self.destinations) == len(target.destinations) or
                not len(self.timestamps) == len(target.timestamps)):
            return False
        for sp, tp in zip(chain(self.sources, self.destinations,
                                self.timestamps),
                          chain(target.sources, target.destinations,
                                target.timestamps)):
            if not sp.match(tp):
                return False
        return True


class EventHandler(Function):

    _fields = ['name', 'arguments', 'body']
    _index = 0

    def __init__(self, name, parent, events=[],
                 labels=None, notlabels=None, ast=None):
        super().__init__(name, parent, ast)
        self.events = events
        self.labels = labels
        self.notlabels = notlabels
        self.index = EventHandler._index
        EventHandler._index += 1

    @property
    def name(self):
        if super().name is not None:
            return super().name
        return "event_handler_%d" % self.index

class Process(BlockStatement, ArgumentsContainer):

    _fields = ['name', 'bases', 'initializers', 'methods', 'events', 'body']

    def __init__(self, name, parent, bases, ast=None):
        super().__init__(parent, ast)
        self.name = name
        self.bases = bases
        self.initializers = []
        self.methods = []
        self.entry_point = None
        self.events = []
        self.labels = []
        self.queries = []

    @property
    def methodnames(self):
        return {f.name for f in self.functions}

    def add_events(self, events):
        filtered = []
        for e in events:
            match = self.find_event(e)
            if match is None and e is not None:
                self.events.append(e)
                filtered.append(e)
            else:
                filtered.append(match)
        return filtered

    def add_event(self, event):
        match = self.find_event(event)
        if match is None and event is not None:
            self.events.append(event)
            return event
        else:
            return match

    def find_event(self, event):
        for e in self.events:
            if e.match(event):
                return e
        return None
