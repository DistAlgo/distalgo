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

from ast import AST
from itertools import chain

##################################################
# Auxiliary functions:

def flatten_attr(obj, attr_name):
    if isinstance(obj, list):
        return list(chain(*[flatten_attr(elem, attr_name) for elem in obj]))
    else:
        return getattr(obj, attr_name)

##################################################
# AST classes:

class DistNode(AST):
    """Base class for the DistAlgo AST.

    This is a subclass of ast.AST, so the same AST 'visitor' mechanism can be
    used to traverse DistPy ASTs. However care must be taken when using an AST
    'transformer' to change a DistPy AST tree, since a DistPy AST may not be a
    strict tree structure, due to NamedVar instances having non-unique parents.

    """

    _fields = []
    _index = 0
    _attributes = ['lineno', 'col_offset']

    def __init__(self, parent=None, ast=None):
        """Instantiate a node.

        Params:

        parent - The parent node

        ast - The original Python AST node used to generate this node.

        """
        self._ast = ast
        self._parent = parent
        self.lineno = 0
        self.col_offset = 0
        DistNode._index += 1
        if ast is not None:
            self.copy_location(ast)

    @classmethod
    def reset_index(cls):
        cls._index = 0

    def clone(self):
        nodecls = type(self)
        # All DistNode subtypes must have __init__ signature:
        # __init__(parent, ast, etc...)
        node = nodecls(self._parent, self._ast)
        return node

    def copy_location(self, ast):
        if hasattr(ast, "lineno"):
            self.lineno = ast.lineno
        if hasattr(ast, "col_offset"):
            self.col_offset = ast.col_offset

    def replace_child(self, oldnode, newnode):
        """Replace all occurances of 'oldnode' with 'newnode' in the subtree
        rooted at this node.

        This is mainly used to implement Python 'global' and 'nonlocal'
        statements, which has the ability to retroactively rebind local names
        to NamedVar defined in outer scopes.

        """
        for fname, fvalue in iter_fields(self):
            if isinstance(fvalue, list):
                for idx, node in enumerate(fvalue):
                    if node is oldnode:
                        fvalue[idx] = newnode
                    elif isinstance(node, DistNode):
                        node.replace_child(oldnode, newnode)
            elif fvalue is oldnode:
                setattr(self, fname , newnode)

    def immediate_container_of_type(self, nodetype):
        node = self
        while node is not None:
            if isinstance(node, nodetype):
                return node
            else:
                node = node.parent
        return None

    def first_parent_of_type(self, nodetype):
        node = self.parent
        while node is not None:
            if isinstance(node, nodetype):
                return node
            else:
                node = node.parent
        return None

    def first_parent_of_types(self, nodetypes):
        node = self.parent
        while node is not None:
            for typ in nodetypes:
                if isinstance(node, typ):
                    return node
            node = node.parent
        return None

    def last_parent_of_type(self, nodetype):
        last = node = self.parent
        if not isinstance(node, nodetype):
            return None
        while node is not None:
            node = node.parent
            if not isinstance(node, nodetype):
                break
            else:
                last = node
        return last

    def is_child_of(self, node):
        """True if 'node' is an ancestor of this node."""
        p = self.parent
        while p is not None:
            if p is node:
                return True
            else:
                p = p.parent
        return False

    def is_contained_in(self, node):
        return self is node or self.is_child_of(node)

    @property
    def scope(self):
        """The local scope containing this node."""
        if self._parent is not None:
            return self._parent.scope
        else:
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

    NameScope maps names to NamedVar objects. Each NameScope has a local
    mapping that contains names defined in the local scope. The 'parent_scope'
    property forms a reverse tree structure of NameScope hierachy up to the
    global scope, which can be recursively traversed to resolve non-local
    names. Names defined in the local scope shadow names in the outer scopes.

    """

    _fields = []

    def __init__(self, parent=None, ast=None):
        super().__init__(parent, ast)
        # Map names to corresponding NamedVar instances:
        self._names = dict()

    def clone(self):
        node = super().clone()
        node._names = dict(self._names)
        return node

    def find_name(self, name, local=False):
        """Looks up a name from this scope.

        Returns the NamedVar object corresponding to 'name', or None if 'name'
        is not found. If 'local' is True then search only the local bindings
        and do not recursively search parent scopes.

        """
        assert isinstance(name, str)

        entity = self._names.get(name)
        if (entity is not None) or local:
            return entity
        elif self.parent_scope is not None:
            return self.parent_scope.find_name(name)
        else:
            return None

    def add_name(self, name):
        """Adds a name to this scope if it doesn't yet exist.

        Return the NamedVar object for this name.

        """
        obj = self.find_name(name, local=True)
        if obj is not None:
            return obj
        else:
            obj = NamedVar(name=name)
            self._names[name] = obj
            return obj

    def link_name(self, namedvar):
        """Links a name object into this scope.

        If the name already exists in the current scope, the info from the
        existing name object is merged into the new object and all pointers to
        the old name is updated to the new name. This method is mainly used to
        implement Python rules for "global" and "nonlocal" declarations.

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

    def merge_scope(self, target):
        """Merges names defined in 'target' scope into this scope.
        """

        if self is target:
            # Do nothing in the trivial case:
            return
        for name in target._names:
            if name not in self._names:
                self._names[name] = target._names[name]
            else:
                self._names[name].merge(target._names[name])

    @property
    def skip(self):
        """True if this scope should be skipped in the scope hierachy.

        According to Python name resolution rules, Class scopes are not
        included when resolving names from child scopes. This property allows
        such rules to be implemented.

        """
        return False

    @property
    def scope(self):
        return self

    @property
    def parent_scope(self):
        """Returns the immediate parent scope, or None if this is the top-level
    (global) scope.

        """
        p = self.parent
        while p is not None:
            if isinstance(p, NameScope) and not p.skip:
                return p
            else:
                p = p.parent
        return None

    @property
    def local_names(self):
        return set(self._names.keys())

    @property
    def ordered_local_names(self):
        return list(sorted(self._names.keys()))

    @property
    def local_nameobjs(self):
        return set(self._names.values())

    @property
    def ordered_local_nameobjs(self):
        res = []
        for name in self.ordered_local_names:
            res.append(self._names[name])
        return res

class LockableNameScope(NameScope):
    """A special type of NameScope that only accepts new names when it's
       'unlocked'.

    When this scope is in the 'unlocked' state, it behaves like a normal
    NameScope; when this scope is in the 'locked' state, it behaves like a
    transparent scope where all name creation requests are passed on to the
    parent scope. This is mainly used to implement generator expressions.

    """

    def __init__(self, parent=None, ast=None):
        super().__init__(parent, ast)
        self.locked = False

    def clone(self):
        node = super().clone()
        node.locked = self.locked
        return node

    def add_name(self, name):
        if not self.locked:
            return super().add_name(name)
        else:
            assert self.parent_scope is not None
            return self.parent_scope.add_name(name)

    def lock(self):
        self.locked = True

    def unlock(self):
        self.locked = False

class Arguments(DistNode):
    """A node representing arguments.

    This class simply mirrors the 'arguments' node in the Python AST.

    """

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

    def clone(self):
        node = super().clone()
        node.args = [a.clone() for a in self.args]
        node.defaults = [d.clone() for d in self.args]
        if self.vararg is not None:
            node.vararg = self.vararg.clone()
        node.kwonlyargs = [a.clone() for a in self.kwonlyargs]
        node.kw_defaults = [kw.clone() for kw in self.kw_defaults]
        if self.kwarg is not None:
            node.kwarg = self.kwarg.clone()
        return node

    @property
    def scope(self):
        return self.parent

    def add_arg(self, name, annotation=None):
        assert isinstance(name, str)
        e = self.parent.add_name(name)
        if e not in self.args:
            e.add_assignment(self, annotation)
            self.args.append(e)

    def add_defaultarg(self, name, value, annotation=None):
        assert isinstance(name, str) and isinstance(value, DistNode)
        e = self.parent.add_name(name)
        # User annotated type takes precedence:
        if annotation is None:
            e.add_assignment(self, typectx=value)
        else:
            e.add_assignment(self, typectx=annotation)
        self.args.append(e)
        self.defaults.append(value)

    def add_vararg(self, name, annotation=None):
        assert self.vararg is None and isinstance(name, str)
        e = self.parent.add_name(name)
        # User annotated type takes precedence:
        e.add_assignment(self, typectx=(list if annotation is None else annotation))
        self.vararg = e

    def add_kwonlyarg(self, name, value, annotation=None):
        assert isinstance(name, str)
        e = self.parent.add_name(name)
        # User annotated type takes precedence:
        tctx = value if annotation is None else annotation
        if tctx is not None:
            e.add_assignment(self, typectx=tctx)
        self.kwonlyargs.append(e)
        self.kw_defaults.append(value)

    def add_kwarg(self, name, annotation=None):
        assert self.kwarg is None and isinstance(name, str)
        e = self.parent.add_name(name)
        # User annotated type takes precedence:
        e.add_assignment(self, typectx=(dict if annotation is None else annotation))
        self.kwarg = e

    @property
    def ordered_names(self):
        res = [name.name for name in chain(self.args, self.kwonlyargs)]
        if self.vararg is not None:
            res.append(self.vararg.name)
        if self.kwarg is not None:
            res.append(self.kwarg.name)
        return res

    @property
    def names(self):
        return set(self.ordered_names)


class ArgumentsContainer(NameScope):
    """A special type of NameScope that takes arguments.

    """

    _fields = ["args"]

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.args = Arguments(parent=self)

    def clone(self):
        node = super().clone()
        node.args = self.args.clone()
        return node

    @property
    def ordered_names(self):
        return self.args.ordered_names

    @property
    def names(self):
        return self.args.names

class NamedVar(DistNode):
    """Node representing a named variable.

    Unlike other node types, a NamedVar instance may have multiple parents.
    This is because the same names that refer to the same variable (those
    that occur in the same naming scope) in the program are represented by
    the same NamedVar instance.

    """

    _fields = []
    _attributes = ["name"] + DistNode._attributes

    def __init__(self, parent=None, ast=None, name=""):
        super().__init__(ast)
        self.name = name
        self._scope = None
        self.assignments = []
        self.updates = []
        self.reads = []
        self.aliases = []

    def clone(self):
        # NamedVar instances should not be cloned:
        return self

    def merge(self, target):
        """Merges all info from another NamedVar.

        """
        assert isinstance(target, NamedVar)
        self.assignments.extend(target.assignments)
        self.updates.extend(target.updates)
        self.reads.extend(target.reads)
        self.aliases.extend(target.aliases)

    def replace_node(self, oldnode, newnode=None):
        """Replaces all references to 'oldnode' with 'newnode'.

        If 'newnode' is None then delete all 'oldnode' references.
        """

        pairs = ['assignments', 'reads', 'aliases']
        triples = ['updates']
        if newnode is None:
            for attr in pairs:
                setattr(self, attr,
                        [(node, ctx)
                         for node, ctx in getattr(self, attr)
                         if node is not oldnode])
            for attr in triples:
                setattr(self, attr,
                        [(node, ctx, val)
                         for node, ctx, val in getattr(self, attr)
                         if node is not oldnode])
        else:
            for attr in pairs:
                setattr(self, attr,
                        [(node, ctx) if node is not oldnode else
                         (newnode, ctx)
                         for node, ctx in getattr(self, attr)])
            for attr in triples:
                setattr(self, attr,
                        [(node, ctx, val) if node is not oldnode else
                         (newnode, ctx, val)
                         for node, ctx, val in getattr(self, attr)])

    def set_scope(self, scope):
        self._scope = scope

    def add_assignment(self, node, typectx=None):
        """Add a node where this variable is being assigned to.

        An 'assignment' is a point in the program where a new value is
        assigned to this variable. This includes assignment statements, delete
        statements, and argument definitions.

        """
        assert node.parent is not None
        self.assignments.append((node, typectx))

    def add_update(self, node, attr=None, attrtype=None):
        """Add a node where this variable is being updated.

        An 'update' is a point in the program where the state of the object
        referred to by this variable is changed (in Python, primitive types
        such as int and float can not be updated). This includes assigning to
        or deleting attributes or slices, and calling methods which
        potentially change object state.

        """
        assert node.parent is not None
        self.updates.append((node, attr, attrtype))

    def add_read(self, node, typectx=None):
        """Add a node where the value of this variable is being read.
        """
        assert node.parent is not None
        self.reads.append((node, typectx))

    def purge_reads(self, scope):
        """Purges all read references in given scope from this NamedVar.

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

    def is_assigned_in(self, node):
        """True if this name is being assigned to inside 'node'."""

        for place, _ in self.assignments:
            if place is node or place.is_child_of(node):
                return True
        return False

    def is_a(self, typename):
        """True if we can deduce this name is of type 'typename'.

        'typename' should be a string. Result is guessed from the type
        context this name first appeared in, type context conflict (arises
        when this name is subsequently used in a different type context) is
        not yet handled.

        """

        ctx = self.get_typectx()
        typenode = self.scope.find_name(typename)
        if ctx is None or typenode is None:
            # Missing information
            return False

        if ((typename == "set" and
             (isinstance(ctx, SetCompExpr) or isinstance(ctx, SetExpr))) or
            (typename == "dict" and
             (isinstance(ctx, DictCompExpr) or isinstance(ctx, DictExpr))) or
            (typename == "tuple" and
             (isinstance(ctx, TupleCompExpr) or isinstance(ctx, TupleExpr))) or
            (typename == "list" and
             (isinstance(ctx, ListCompExpr) or isinstance(ctx, ListExpr))) or
            (isinstance(ctx, SimpleExpr) and ctx.value is typenode) or
            (isinstance(ctx, CallExpr) and
             isinstance(ctx.func, SimpleExpr) and
             ctx.func.value is typenode) or
            (isinstance(ctx, CallExpr) and
             isinstance(ctx.func, str) and
             isinstance(typenode, NamedVar) and
             ctx.func == typenode.name)):
            return True
        else:
            return False

    def get_typectx(self):
        """Returns the type context of this name, if known.

        This is a best-effort guess at the type of the variable: assignments
        take precedence over updates, and updates take precedence over
        reads.

        """

        for _, typectx in self.assignments:
            if typectx is not None:
                return typectx
        for _, _, typectx in self.updates:
            if typectx is not None:
                return typectx
        for _, typectx in self.reads:
            if typectx is not None:
                return typectx
        return None

    @property
    def scope(self):
        """Returns the scope that this name is defined in, or None if this name was
        never defined.

        The defining scope of a NamedVar is the scope in which this variable was
        first assigned to or updated.

        """

        if self._scope is not None:
            return self._scope
        if len(self.assignments) > 0:
            return self.assignments[0][0].parent.scope
        elif len(self.updates) > 0:
            return self.updates[0][0].parent.scope
        else:
            return None

    @property
    def ordered_boundvars(self):
        return []

    @property
    def ordered_freevars(self):
        return []

    @property
    def is_arg(self):
        """True if this variable is an argument of an ArgumentsContainer.

        """
        return (len(self.assignments) > 0 and
                isinstance(self.assignments[0], Arguments))

    def __str__(self):
        return "<NamedVar " + self.name + ">"

    def __repr__(self):
        return str(self)

# Expressions:

class Expression(DistNode):
    """Base class for expressions.
    """

    _fields = ['subexprs']

    def __init__(self, parent, ast=None, subexprs=None):
        super().__init__(parent, ast)
        if subexprs is None:
            self.subexprs = []
        else:
            self.subexprs = list(subexprs)

    def clone(self):
        node = super().clone()
        node.subexprs = list(self.subexprs)
        for i, e in enumerate(self.subexprs):
            if isinstance(e, DistNode):
                node.subexprs[i] = e.clone()
        return node

    @property
    def scope(self):
        if isinstance(self, NameScope):
            return self
        else:
            assert self.parent is not None
            return self.parent.scope

    @property
    def ordered_nameobjs(self):
        """A list of NamedVar objects contained within this expression, in textual
    order.

        This default property recursively calls 'ordered_nameobjs' on the
        sub-expressions in order and chains them together. Specialized
        expressions should override this property to generate the proper
        ordered name object list.

        """
        return list(chain(*[e.ordered_nameobjs for e in self.subexprs
                            if e is not None]))

    @property
    def ordered_names(self):
        """A list of names contained in this expression, in textual order.

        This is generated from 'ordered_nameobjs'.
        """
        return list(n.name for n in self.ordered_nameobjs)

    @property
    def names(self):
        """A set of names appearing in this expression.

        This is generated from 'ordered_names'.

        """
        return set(self.ordered_names)

    @property
    def nameobjs(self):
        """A set of NamedVar objects appearing in this expression.

        This is generated from 'ordered_nameobjs'.

        """
        return set(self.ordered_nameobjs)

    @property
    def ordered_boundvars(self):
        """Returns a list of bound variables, in left-to-right order.

        """
        return list(chain(*[e.ordered_boundvars for e in self.subexprs
                            if e is not None]))

    @property
    def boundvars(self):
        """A set containing all bound variables in the pattern."""
        return set(self.ordered_boundvars)

    @property
    def ordered_freevars(self):
        """Returns a list of free variables, in left-to-right order.

        """
        return list(chain(*[e.ordered_freevars for e in self.subexprs
                            if e is not None]))

    @property
    def freevars(self):
        """A set containing all free variables in the pattern."""
        return set(self.ordered_freevars)

    @property
    def statement(self):
        """The first statement parent of this expression, if any.

        """
        return self.first_parent_of_type(Statement)

    def __str__(self):
        s = [type(self).__name__, "("]
        for e in self.subexprs:
            s.append(str(e))
            s.append(", ")
        if len(self.subexprs) > 0:
            del s[-1]
        s.append(")")
        return "".join(s)


class PythonExpr(Expression):
    """This is placeholder for unsupported Python expressions."""
    _fields = []

class SimpleExpr(Expression):

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
    def ordered_nameobjs(self):
        if isinstance(self.value, NamedVar):
            return [self.value]
        elif isinstance(self.value, Expression):
            return self.value.ordered_nameobjs
        else:
            return []

    @property
    def ordered_boundvars(self):
        return []

    @property
    def ordered_freevars(self):
        return []

class AttributeExpr(SimpleExpr):

    _attributes = ['attr'] + SimpleExpr._attributes

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.attr = None

    def clone(self):
        node = super().clone()
        node.attr = self.attr
        return node

    def clone_from(self, node):
        super().clone_from(node)
        self.attr = node.attr

class SubscriptExpr(SimpleExpr):

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.subexprs.append(None)

    @property
    def ordered_nameobjs(self):
        res = super().ordered_nameobjs
        if self.index is not None:
            res += self.index.ordered_nameobjs
        return res

    @property
    def index(self):
        return self.subexprs[1]

    @index.setter
    def index(self, idx):
        self.subexprs[1] = idx

class SliceExpr(Expression):

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
class FormattedValueExpr(ConstantExpr):    # Since Python 3.6
    def __init__(self, parent, ast=None, value=None):
        super().__init__(parent, ast, value)
        self.conversion = -1
        self.format_spec = None

class SequenceExpr(Expression): pass
class TupleExpr(SequenceExpr): pass
class ListExpr(SequenceExpr): pass
class SetExpr(SequenceExpr): pass
class FormattedStrExpr(SequenceExpr): pass # Since Python 3.6

class DictExpr(Expression):

    _fields = ['keys', 'values']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.keys = self.subexprs
        self.values = []

    def clone(self):
        node = super().clone()
        node.values = [v.clone() for v in self.values]
        return node

    @property
    def ordered_nameobjs(self):
        return list(chain(*[v.ordered_nameobjs
                            for v in chain(self.keys, self.values)]))

class IfExpr(Expression):

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

    _fields = ['func', 'args', 'keywords', 'starargs', 'kwargs']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.subexprs = [None for i in range(5)]

    @property
    def func(self):
        return self.subexprs[0]

    @property
    def args(self):
        return self.subexprs[1]

    @property
    def keywords(self):
        return self.subexprs[2]

    @property
    def starargs(self):
        return self.subexprs[3]

    @property
    def kwargs(self):
        return self.subexprs[4]

    @func.setter
    def func(self, func):
        assert isinstance(func, Expression)
        self.subexprs[0] = func

    @args.setter
    def args(self, args):
        assert isinstance(args, list)
        self.subexprs[1] = args

    @keywords.setter
    def keywords(self, keywords):
        assert isinstance(keywords, list)
        self.subexprs[2] = keywords

    @starargs.setter
    def starargs(self, starargs):
        assert starargs is None or isinstance(starargs, Expression)
        self.subexprs[3] = starargs

    @kwargs.setter
    def kwargs(self, kwargs):
        assert kwargs is None or isinstance(kwargs, Expression)
        self.subexprs[4] = kwargs

    @property
    def ordered_nameobjs(self):
        res = []
        if self.func is not None:
            res.extend(self.func.ordered_nameobjs)
        res.extend(chain(*[a.ordered_nameobjs for a in self.args
                           if a is not None]))
        res.extend(chain(*[v.ordered_nameobjs for _, v in self.keywords
                           if v is not None]))
        if self.starargs is not None:
            res.extend(self.starargs.ordered_nameobjs)
        if self.kwargs is not None:
            res.extend(self.kwargs.ordered_nameobjs)
        return res

    @property
    def ordered_boundvars(self):
        res = []
        if self.func is not None:
            res.extend(self.func.ordered_boundvars)
        res.extend(chain(*[a.ordered_boundvars for a in self.args
                           if a is not None]))
        res.extend(chain(*[v.ordered_boundvars for _, v in self.keywords
                           if v is not None]))
        if self.starargs is not None:
            res.extend(self.starargs.ordered_boundvars)
        if self.kwargs is not None:
            res.extend(self.kwargs.ordered_boundvars)
        return res

    @property
    def ordered_freevars(self):
        res = []
        if self.func is not None:
            res.extend(self.func.ordered_freevars)
        res.extend(chain(*[a.ordered_freevars for a in self.args
                           if a is not None]))
        res.extend(chain(*[v.ordered_freevars for _, v in self.keywords
                           if v is not None]))
        if self.starargs is not None:
            res.extend(self.starargs.ordered_freevars)
        if self.kwargs is not None:
            res.extend(self.kwargs.ordered_freevars)
        return res

class ApiCallExpr(CallExpr):
    @property
    def func(self):
        return self.subexprs[0]

    @func.setter
    def func(self, func):
        assert isinstance(func, str)
        self.subexprs[0] = func

    @property
    def ordered_nameobjs(self):
        return list(chain(*[a.ordered_nameobjs for a in self.args
                            if a is not None]))

class BuiltinCallExpr(CallExpr):
    @property
    def func(self):
        return self.subexprs[0]


    @func.setter
    def func(self, func):
        assert isinstance(func, str)
        self.subexprs[0] = func

    @property
    def ordered_nameobjs(self):
        return list(chain(*[a.ordered_nameobjs for a in self.args
                            if a is not None]))

class SetupExpr(BuiltinCallExpr):
    @property
    def func(self):
        return "_setup"

class StartExpr(BuiltinCallExpr):
    @property
    def func(self):
        return "_start"

class ConfigExpr(BuiltinCallExpr):
    @property
    def func(self):
        return "_config"

class BooleanOperator(DistNode): pass
class AndOp(BooleanOperator): pass
class OrOp(BooleanOperator): pass
class NotOp(BooleanOperator): pass

class BooleanExpr(Expression): pass

class LogicalExpr(BooleanExpr):

    _fields = ['operator', 'subexprs']

    def __init__(self, parent, ast=None, op=None, subexprs=None):
        super().__init__(parent, ast, subexprs)
        self.operator = op

    def clone(self):
        node = super().clone()
        node.operator = self.operator
        return node

    @property
    def left(self):
        if len(self.subexprs) > 0:
            return self.subexprs[0]
        else:
            return None

    def __str__(self):
        s = [type(self).__name__, "("]
        s.extend(["op=", self.operator.__name__, ", "])
        for e in self.subexprs:
            s.append(str(e))
            s.append(", ")
        if len(self.subexprs) > 0:
            del s[-1]
        s.append(")")
        return "".join(s)

class KeyValue(Expression):
    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.subexprs = [None, None]

    def clone(self):
        node = super().clone()
        node.subexprs = [e.clone() for e in self.subexprs]
        return node

    @property
    def key(self):
        return self.subexprs[0]

    @property
    def value(self):
        return self.subexprs[1]

    @key.setter
    def key(self, key):
        self.subexprs[0] = key

    @value.setter
    def value(self, value):
        self.subexprs[1] = value

    def __str__(self):
        s = [str(self.key), " : ", str(self.value)]
        return "".join(s)


class QuantifierOperator(DistNode): pass
class ExistentialOp(QuantifierOperator): pass
class UniversalOp(QuantifierOperator): pass

class DomainSpec(Expression):
    """Represents a domain constraint.

    A DomainSpec is 'some X in D' or 'each X in D', where 'X' is a valid
    pattern spec, and D is an expression that should evaluate to an iterable
    (the domain of X).

    """

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.subexprs = [None, None]
        self.index = DomainSpec._index

    def clone(self):
        node = super().clone()
        node.index = self.index
        return node

    @property
    def ordered_boundvars(self):
        if self.pattern is not None and isinstance(self.pattern, PatternExpr):
            return self.pattern.ordered_boundvars
        else:
            return []

    @property
    def ordered_freevars(self):
        if self.pattern is not None and isinstance(self.pattern, PatternExpr):
            return self.pattern.ordered_freevars
        else:
            return []

    @property
    def ordered_nameobjs(self):
        res = []
        if self.pattern is not None:
            res.extend(self.pattern.ordered_nameobjs)
        if self.domain is not None:
            res.extend(self.domain.ordered_nameobjs)
        return res

    @property
    def pattern(self):
        return self.subexprs[0]

    @pattern.setter
    def pattern(self, expr):
        self.subexprs[0] = expr

    @property
    def domain(self):
        return self.subexprs[1]

    @domain.setter
    def domain(self, expr):
        assert isinstance(expr, Expression)
        self.subexprs[1] = expr

    def __str__(self):
        return str(self.pattern) + " in " + str(self.domain)

class QueryExpr(Expression):
    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)

    @property
    def ordered_local_freevars(self):
        return []

    @property
    def top_level_query(self):
        top = self
        p = self._parent
        while p is not None:
            # Try to traverse to the top-level QueryExpr:
            if isinstance(p, QueryExpr):
                top = p
            elif isinstance(p, NameScope):
                # A query scope can not cover expressions which are themselves
                # namescopes (e.g. Lambda expressions):
                break
            p = p._parent
        return top

class QuantifiedExpr(BooleanExpr, QueryExpr):

    _fields = ['domains', 'operator'] + BooleanExpr._fields

    def __init__(self, parent, ast=None, op=None):
        super().__init__(parent, ast)
        # List of DomainSpec:
        self.domains = []
        # Quantifier operator, ExistentialOp or UniversalOp:
        self.operator = op
        # Single element - Predicate expression:
        self.subexprs = [None]
        # Index for unique name generation:
        self.index = QuantifiedExpr._index

    def clone(self):
        node = super().clone()
        node.domains = [d.clone() for d in self.domains]
        node.operator = self.operator
        node.index = self.index
        return node

    @property
    def ordered_nameobjs(self):
        return list(chain(*[e.ordered_nameobjs
                            for e in chain(self.subexprs, self.domains)
                            if e is not None]))

    @property
    def ordered_boundvars(self):
        return list(chain(*[d.ordered_boundvars
                            for d in chain(self.domains, self.subexprs)
                            if d is not None]))

    @property
    def ordered_freevars(self):
        return list(chain(*[d.ordered_freevars
                            for d in chain(self.domains, self.subexprs)
                            if d is not None]))

    @property
    def ordered_local_freevars(self):
        return list(chain(*[d.ordered_freevars
                            for d in self.domains if d is not None]))

    @property
    def name(self):
        return self.operator.__name__ + ("Expr_%d" % self.index)

    @property
    def predicate(self):
        return self.subexprs[0]

    @predicate.setter
    def predicate(self, expr):
        assert isinstance(expr, Expression)
        self.subexprs[0] = expr

    def __str__(self):
        s = [type(self).__name__, "("]
        s.extend(["op=", self.operator.__name__, ", "])
        for e in self.domains:
            s.append(str(e))
            s.append(", ")
        if len(self.subexprs) > 0:
            del s[-1]
        s.extend([" | ", str(self.predicate), ")"])
        return "".join(s)

class ComprehensionExpr(QueryExpr, LockableNameScope):

    _fields = ['elem', 'conditions']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.elem = None
        # List of conditions, some of which may be DomainSpecs:
        self.conditions = self.subexprs

    def clone(self):
        node = super().clone()
        node.elem = self.elem.clone() if self.elem is not None else None
        return node

    @property
    def ordered_nameobjs(self):
        return list(chain(*[e.ordered_nameobjs
                            for e in chain([self.elem],
                                           self.conditions)
                            if e is not None]))

    @property
    def ordered_freevars(self):
        return list(chain(*[e.ordered_freevars
                            for e in self.conditions if e is not None]))

    @property
    def ordered_boundvars(self):
        return list(chain(*[e.ordered_boundvars
                            for e in self.conditions if e is not None]))

    @property
    def ordered_local_freevars(self):
        return list(chain(*[d.ordered_freevars
                            for d in self.conditions
                            if isinstance(d, DomainSpec)]))

    def __str__(self):
        s = [type(self).__name__, "(", str(self.elem), ": "]
        for d in self.conditions:
            s.append(str(d))
            s.append(", ")
        if len(self.conditions) > 0:
            del s[-1]
        s.append(")")
        return "".join(s)

class GeneratorExpr(ComprehensionExpr): pass
class SetCompExpr(ComprehensionExpr): pass
class ListCompExpr(ComprehensionExpr): pass
class TupleCompExpr(ComprehensionExpr): pass
class DictCompExpr(ComprehensionExpr): pass

class AggregateExpr(CallExpr, QueryExpr): pass
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

    _fields = ['left', 'comparator', 'right']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.comparator = None
        self.subexprs = [None, None]

    def clone(self):
        node = super().clone()
        node.comparator = self.comparator
        return node

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
class MatMultOp(Operator): pass
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
    def __init__(self, parent, ast=None, op=None):
        super().__init__(parent, ast)
        self.operator = op

    def clone(self):
        node = super().clone()
        node.operator = self.operator
        return node

class UnaryExpr(ArithmeticExpr):

    _fields = ['operator', 'right']

    def __init__(self, parent, ast=None, op=None):
        super().__init__(parent, ast, op)
        self.subexprs = [None]

    @property
    def right(self):
        return self.subexprs[0]

    @right.setter
    def right(self, val):
        self.subexprs[0] = val

class BinaryExpr(ArithmeticExpr):

    _fields = ['left', 'operator', 'right']

    def __init__(self, parent, ast=None, op=None):
        super().__init__(parent, ast, op)
        self.subexprs = [None, None]

    @property
    def left(self):
        return self.subexprs[0]

    @left.setter
    def left(self, val):
        self.subexprs[0] = val

    @property
    def right(self):
        return self.subexprs[1]

    @right.setter
    def right(self, val):
        self.subexprs[1] = val


class PatternElement(DistNode):
    """A tree-structure representing a sub-component of a pattern.
    """

    _fields = ['value']

    def __init__(self, parent=None, ast=None, value=None):
        super().__init__(parent, ast)
        self.value = value
        self.index = PatternElement._index

    def clone(self):
        node = super().clone()
        if isinstance(self.value, DistNode):
            node.value = self.value.clone()
            node.value._parent = node
        else:
            node.value = self.value
        return node

    @property
    def unique_name(self):
        return "_" + type(self).__name__ + str(self.index) + "_"

    @property
    def ordered_boundpatterns(self):
        return []

    @property
    def ordered_boundvars(self):
        """Returns a list of bound variables, in left-to-right order.

        """
        raise NotImplementedError

    @property
    def boundvars(self):
        """A set containing all bound variables in the pattern."""
        return set(self.ordered_boundvars)

    @property
    def ordered_freevars(self):
        """Returns a list of free variables, in left-to-right order.

        """
        raise NotImplementedError

    @property
    def freevars(self):
        """A set containing all free variables in the pattern."""
        return set(self.ordered_freevars)

    @property
    def parent_expression(self):
        return self.first_parent_of_type(Expression)

    @property
    def ordered_nameobjs(self):
        return self.ordered_boundvars

    def __str__(self):
        return type(self).__name__ + ("{%s}" % str(self.value))

    def __repr__(self):
        return str(self)

    def match(self, target):
        """Compare two Elements to see if they describe the same pattern.
        """

        if target is None:
            return False

        assert isinstance(target, PatternElement)

        if type(self) is not type(target):
            return False
        elif type(self) is FreePattern or type(self) is BoundPattern or \
             type(self) is ConstantPattern:
            return target.value == self.value
        elif type(self) is TuplePattern or type(self) is ListPattern:
            if len(self.value) != len(target.value):
                return False
            for v, t in zip(self.value, target.value):
                if not v.match(t):
                    return False
            return True

class ConstantPattern(PatternElement):
    def __init__(self, parent=None, ast=None, value=None):
        super().__init__(parent, ast, value)

    @property
    def ordered_boundvars(self):
        return []

    @property
    def ordered_freevars(self):
        return []

class FreePattern(PatternElement):
    def __init__(self, parent, ast=None, value=None):
        if value is not None:
            assert isinstance(value, NamedVar)
        super().__init__(parent, ast, value)

    @property
    def ordered_boundvars(self):
        return []

    @property
    def ordered_freevars(self):
        return [self.value] if self.value is not None else []

class BoundPattern(PatternElement):
    def __init__(self, parent, ast=None, value=None):
        super().__init__(parent, ast, value)

    @property
    def ordered_boundpatterns(self):
        return [self]

    @property
    def ordered_boundvars(self):
        if isinstance(self.value, NamedVar):
            return [self.value]
        elif self.value is not None:
            return self.value.ordered_nameobjs
        else:
            return []

    @property
    def ordered_freevars(self):
        return []

class TuplePattern(PatternElement):
    def __init__(self, parent, ast=None, value=None):
        if value is None:
            value = []
        for pat in value:
            assert isinstance(pat, PatternElement)
            pat._parent = self
        super().__init__(parent, ast, value)

    def clone(self):
        node = super().clone()
        node.value = [v.clone() for v in self.value]
        return node

    @property
    def subexprs(self):
        return self.value

    @property
    def ordered_boundpatterns(self):
        return list(chain(*[v.ordered_boundpatterns for v in self.value]))

    @property
    def ordered_boundvars(self):
        return list(chain(*[v.ordered_boundvars for v in self.value]))

    @property
    def ordered_freevars(self):
        return list(chain(*[v.ordered_freevars for v in self.value]))

    def __str__(self):
        s = [type(self).__name__, "{("]
        for e in self.value:
            s.append(str(e))
            s.append(", ")
        if len(self.value) > 0:
            del s[-1]
        s.append(")}")
        return "".join(s)

class ListPattern(PatternElement):
    def __init__(self, parent, ast=None, value=None):
        assert isinstance(value, list)
        for pat in value:
            assert isinstance(pat, PatternElement)
            pat._parent = self
        super().__init__(parent, ast, value)

    def clone(self):
        node = super().clone()
        node.value = [v.clone() for v in self.value]
        return node

    @property
    def subexprs(self):
        return self.value

    @property
    def ordered_boundpatterns(self):
        return list(chain(*[v.ordered_boundpatterns for v in self.value]))

    @property
    def ordered_boundvars(self):
        return list(chain(*[v.ordered_boundvars for v in self.value]))

    @property
    def ordered_freevars(self):
        return list(chain(*[v.ordered_freevars for v in self.value]))

    def __str__(self):
        s = [type(self).__name__, "{("]
        for e in self.value:
            s.append(str(e))
            s.append(", ")
        if len(self.value) > 0:
            del s[-1]
        s.append(")}")
        return "".join(s)

class PatternExpr(Expression):

    def __init__(self, parent, ast=None, pattern=None):
        super().__init__(parent, ast)
        self.subexprs = [pattern]
        self.index = PatternExpr._index

    def clone(self):
        node = super().clone()
        node.index = self.index
        return node

    @property
    def name(self):
        return "PatternExpr_" + str(self.index)

    @property
    def pattern(self):
        return self.subexprs[0]

    @pattern.setter
    def pattern(self, pattern):
        assert isinstance(pattern, PatternElement)
        self.subexprs[0] = pattern

    @property
    def ordered_boundpatterns(self):
        return self.pattern.ordered_boundpatterns

    def match(self, target):
        if not isinstance(target, PatternExpr):
            return False
        return self.pattern.match(target.pattern)

# XXX HACK!!!: work around query auditing
class LiteralPatternExpr(PatternExpr):
    @property
    def ordered_boundvars(self):
        return []

    @property
    def ordered_freevars(self):
        return []

class HistoryExpr(Expression):

    def __init__(self, parent, ast=None, context=None):
        super().__init__(parent, ast)
        self.subexprs = [None]
        self.context = context

    def clone(self):
        node = super().clone()
        node.context = self.context.clone() \
                       if self.context is not None else None
        return node

    @property
    def event(self):
        return self.subexprs[0]

    @event.setter
    def event(self, event):
        assert isinstance(event, Event)
        self.subexprs[0] = event

    @property
    def evtidx(self):
        if self.event is None: return None
        return self.event.index

    @property
    def ordered_names(self):
        return self.ordered_boundvars

    @property
    def ordered_boundvars(self):
        return []

    @property
    def ordered_freevars(self):
        return []

class ReceivedExpr(HistoryExpr): pass
class SentExpr(HistoryExpr): pass

class LambdaExpr(Expression, ArgumentsContainer):

    _fields = Expression._fields + ArgumentsContainer._fields

    def __init__(self, parent, ast=None):
        super().__init__(parent ,ast)
        self.subexprs.append(None)

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

    _fields = []
    _attributes = ['label'] + DistNode._attributes

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self._label = None
        self.index = Statement._index

    def clone(self):
        node = super().clone()
        node.label = self.label
        node.index = self.index
        return node

    @property
    def label(self):
        return self._label

    @label.setter
    def label(self, label):
        self._label = label

    @property
    def ordered_nameobjs(self):
        return []

    @property
    def nameobjs(self):
        """A set of NamedVar objects appearing in this expression.

        This is generated from 'ordered_nameobjs'.

        """
        return set(self.ordered_nameobjs)

    @property
    def statement(self):
        return self

    @property
    def unique_label(self):
        return "_st_label_%d" % self.index

    @property
    def scope(self):
        if isinstance(self, NameScope):
            return self
        else:
            assert self._parent is not None
            return self._parent.scope

class SimpleStmt(Statement):
    """A SimpleStmt is a statement that does not contain sub-statements.

    """

    _fields = ['expr']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.expr = None

    def clone(self):
        node = super().clone()
        node.expr = self.expr.clone() if self.expr is not None else None
        return node

    @property
    def ordered_nameobjs(self):
        return list(chain(*[flatten_attr(getattr(self, field_name),
                                         "ordered_nameobjs")
                            for field_name in self._fields
                            if getattr(self, field_name) is not None]))

class CompoundStmt(Statement):
    """Block statements are compound statements that contain one or more blocks of
    sub-statements.

    """
    _fields = ["body"]

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.body = []

    @property
    def ordered_nameobjs(self):
        return list(chain(*[l.ordered_nameobjs for l in self.body
                            if l is not None]))

class Program(CompoundStmt, NameScope):
    """The global NameScope.
    """

    _fields = ['processes', 'entry_point'] + \
              CompoundStmt._fields
    _attributes = ['configurations', 'directives'] + \
                  CompoundStmt._attributes

    def __init__(self, parent=None, ast=None):
        super().__init__(parent, ast)
        self._compiler_options = None
        self.configurations = []
        self.directives = []
        self.processes = []
        self.nodecls = None
        # The 'da' module is always available to all DistAlgo programs:
        self.add_name("da")

    @property
    def skip(self):
        return False

class InteractiveProgram(CompoundStmt, NameScope):
    """For interactive code.
    """

    _fields = ['processes', 'entry_point', 'body']

    def __init__(self, parent=None, ast=None):
        super().__init__(parent, ast)
        self.processes = []
        self.entry_point = None

    @property
    def skip(self):
        return False

class Function(CompoundStmt, ArgumentsContainer):

    _fields = ['decorators'] + \
              ArgumentsContainer._fields + CompoundStmt._fields
    _attributes = ['name'] + CompoundStmt._attributes

    def __init__(self, parent, ast=None, name=""):
        super().__init__(parent, ast)
        self._name = name
        self.process = None
        # List of decorator expressions:
        self.decorators = []

    @property
    def name(self):
        return self._name

    def __str__(self):
        res = ["<func ", self.name, ">"]
        return "".join(res)

    def __repr__(self):
        return str(self)

class ClassStmt(CompoundStmt, NameScope):

    _fields = ['bases', 'decorators', 'keywords', 'starargs', 'kwargs'] + \
              CompoundStmt._fields
    _attributes = ['name'] + CompoundStmt._attributes

    def __init__(self, parent, ast=None, name="", bases=[]):
        super().__init__(parent, ast)
        self.name = name
        self.bases = bases
        self.keywords = []
        self.starargs = None
        self.kwargs = None
        # List of decorator expressions:
        self.decorators = []

    @property
    def skip(self):
        """Class scope should be skipped when resolving names from a child scope.
        """
        return True

class NoopStmt(SimpleStmt): pass

class AssignmentStmt(SimpleStmt):

    _fields = ['targets', 'value']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.targets = []
        self.value = None

    @property
    def ordered_nameobjs(self):
        return list(chain(*[t.ordered_nameobjs
                            for t in chain(self.targets, [self.value])
                            if t is not None]))

class OpAssignmentStmt(AssignmentStmt):

    _fields = ['operator'] + AssignmentStmt._fields

    def __init__(self, parent, ast=None, op=None):
        super().__init__(parent, ast)
        self.operator = op
        self.targets = [None]

    @property
    def target(self):
        return self.targets[0]

    @target.setter
    def target(self, tgt):
        self.targets[0] = tgt

class IfStmt(CompoundStmt):

    _fields = ['condition', 'body', 'elsebody']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.condition = None
        self.body = []
        self.elsebody = []

    @property
    def ordered_nameobjs(self):
        return list(chain(*[l.ordered_nameobjs
                            for l in chain(self.body, self.elsebody)
                            if l is not None]))

class LoopStmt(CompoundStmt):
    """Abstract class for loops."""
    pass

class WhileStmt(LoopStmt):

    _fields = ['condition', 'body', 'elsebody']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.condition = None
        self.body = []
        self.elsebody = []

    @property
    def ordered_nameobjs(self):
        return list(chain(*[l.ordered_nameobjs
                            for l in chain(self.body, self.elsebody)
                            if l is not None]))

class ForStmt(LoopStmt):

    _fields = ['domain', 'body', 'elsebody']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.domain = None
        self.body = []
        self.elsebody = []

    @property
    def ordered_nameobjs(self):
        return list(chain(*[l.ordered_nameobjs
                            for l in chain(self.body, self.elsebody)
                            if l is not None]))

class TryStmt(CompoundStmt):

    _fields = ['body', 'excepthandlers', 'elsebody', 'finalbody']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.body = []
        self.excepthandlers = []
        self.elsebody = []
        self.finalbody = []

    @property
    def ordered_nameobjs(self):
        return list(chain(*[l.ordered_nameobjs
                            for l in chain(self.body, self.excepthandlers,
                                           self.elsebody, self.finalbody)
                            if l is not None]))

class ExceptHandler(DistNode):

    _fields = ['type', 'body']
    _attributes = ['name'] + DistNode._attributes

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.name = None
        self.type = None
        self.body = []

    @property
    def ordered_nameobjs(self):
        return list(chain(*[l.ordered_nameobjs for l in self.body
                            if l is not None]))

class AwaitStmt(CompoundStmt):

    _fields = ['branches', 'orelse', 'timeout']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.branches = []
        self.orelse = []
        self.timeout = None

    @property
    def label(self):
        if self._label is not None:
            return self._label
        else:
            return self.unique_label

    @label.setter
    def label(self, label):
        self._label = label

    @property
    def is_in_loop(self):
        loop_par = self.first_parent_of_type(LoopStmt)
        if loop_par is None:
            return False
        func_par = self.first_parent_of_types({ArgumentsContainer, ClassStmt})
        if func_par is not None and func_par.is_child_of(loop_par):
            return False
        return True

    @property
    def ordered_nameobjs(self):
        return list(chain(*[l.ordered_nameobjs
                            for l in chain(self.branches, self.orelse)
                            if l is not None]))

class LoopingAwaitStmt(AwaitStmt):

    _fields = AwaitStmt._fields + ['orfail']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.orfail = []

class Branch(DistNode):

    _fields = ['condition', 'body']

    def __init__(self, parent, ast=None, condition=None):
        super().__init__(parent, ast)
        self.condition = condition
        self.body = []

    @property
    def ordered_nameobjs(self):
        return list(chain(*[l.ordered_nameobjs
                            for l in chain([self.condition], self.body)
                            if l is not None]))

class ReturnStmt(SimpleStmt):

    _fields = ['value']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.value = None

class DeleteStmt(SimpleStmt):

    _fields = ['targets']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.targets = []

class YieldStmt(SimpleStmt):

    _fields = ['value']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.value = None

class YieldFromStmt(SimpleStmt):

    _fields = ['value']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.value = None

class WithStmt(CompoundStmt):

    _fields = ['items', 'body']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.items = []
        self.body = []

class RaiseStmt(SimpleStmt):

    _fields = ['expr', 'cause']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.expr = None
        self.cause = None

class PassStmt(SimpleStmt): pass

class LoopCtrlStmt(SimpleStmt):
    def __init__(self, parent, ast=None, loopstmt=None):
        super().__init__(parent, ast)
        self.loopstmt = loopstmt
class BreakStmt(LoopCtrlStmt): pass
class ContinueStmt(LoopCtrlStmt): pass

class ImportStmt(SimpleStmt):
    _fields = ['items']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.items = []

class ImportFromStmt(SimpleStmt):
    _fields = ['module', 'items', 'level']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.module = None
        self.items = []
        self.level = 0

class Alias(DistNode):
    _fields = ['name', 'asname']
    def __init__(self, parent, ast=None, name=None, asname=None):
        super().__init__(parent, ast)
        self.name = name
        self.asname = asname

class AssertStmt(SimpleStmt):

    _fields = ['expr', 'msg']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.msg = None

class GlobalStmt(SimpleStmt):

    _fields = ['names']

    def __init__(self, parent, ast=None, names=[]):
        super().__init__(parent, ast)
        self.names = list(names)

class NonlocalStmt(SimpleStmt):

    _fields = ['names']

    def __init__(self, parent, ast=None, names=[]):
        super().__init__(parent, ast)
        self.names = list(names)

class OutputStmt(SimpleStmt):

    _fields = ['message', 'level']

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)
        self.message = None
        self.level = None
        self.separator = None

class ResetStmt(SimpleStmt):

    def __init__(self, parent, ast=None):
        super().__init__(parent, ast)

class EventType: pass
class ReceivedEvent(EventType): pass
class SentEvent(EventType): pass

class Event(DistNode):

    _fields = ['pattern', 'sources', 'destinations', 'timestamps', 'handlers']
    _attributes = ['type'] + DistNode._attributes

    def __init__(self, parent=None, ast=None, event_type=None, pattern=None):
        super().__init__(parent, ast)
        self.process = parent
        self.type = event_type
        self.pattern = pattern
        self.sources = []
        self.destinations = []
        self.timestamps = []
        self.handlers = []
        self.record_history = False
        self.index = 0

    def clone(self):
        node = super().clone()
        node.type = self.type
        node.pattern = self.pattern.clone() \
                       if self.pattern is not None else None
        node.sources = [s.clone() for s in self.sources]
        node.destinations = [d.clone() for d in self.destinations]
        node.timestamps = [t.clone() for t in self.timestamps]
        node.record_history = self.record_history
        node.index = self.index
        return node

    @property
    def name(self):
        return "_%s%s_%d" % (self.process.name, (self.type).__name__, self.index)

    @property
    def ordered_boundvars(self):
        return list(chain(*[p.ordered_boundvars for p in
                            chain([self.pattern], self.sources,
                                  self.destinations, self.timestamps)
                            if p is not None]))

    @property
    def boundvars(self):
        return set(self.ordered_boundvars)

    @property
    def ordered_freevars(self):
        return list(chain(*[p.ordered_freevars for p in
                            chain([self.pattern], self.sources,
                                  self.destinations, self.timestamps)
                            if p is not None]))

    @property
    def freevars(self):
        return set(self.ordered_freevars)

    @property
    def ordered_nameobjs(self):
        return list(chain(*[p.ordered_nameobjs for p in
                            chain([self.pattern], self.sources,
                                  self.destinations, self.timestamps)
                            if p is not None]))

    @property
    def nameobjs(self):
        return set(ordered_nameobjs)

    def match(self, target):
        if target is None:
            return False
        if type(target) is not type(self):
            return False
        if self.pattern is None:
            if target.pattern is not None:
                return False
        elif not self.pattern.match(target.pattern):
            return False
        if (not len(self.sources) == len(target.sources) or
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

    def __str__(self):
        return self.name

class EventHandler(Function):

    _attributes = ['labels', 'notlabels'] + Function._attributes

    def __init__(self, parent, ast=None, name=None, events=[],
                 labels=None, notlabels=None):
        super().__init__(parent, ast, name=name)
        self.events = events
        self.labels = labels
        self.notlabels = notlabels
        self.index = EventHandler._index

    @property
    def name(self):
        if super().name is not None:
            return super().name
        return "_%s_handler_%d" % (self.first_parent_of_type(Process).name,
                                   self.index)

class Process(CompoundStmt, ArgumentsContainer):

    _fields = ['bases', 'decorators', 'initializers', 'methods',
               'events', 'entry_point'] + \
        CompoundStmt._fields + ArgumentsContainer._fields
    _attributes = ['name'] + CompoundStmt._attributes

    def __init__(self, parent=None, ast=None, name="", bases=[]):
        super().__init__(parent, ast)
        self.name = name
        # List of base classes (other than da.DistProcess):
        self.bases = bases
        # List of decorator expressions:
        self.decorators = []
        # List of configurations:
        self.configurations = []
        # List of member methods:
        self.methods = []
        # 'setup' method:
        self.setup = None
        # 'main' method:
        self.entry_point = None
        # List of event handlers:
        self.events = []

    @property
    def methodnames(self):
        return {f.name for f in self.methods}

    @property
    def event_handlers(self):
        return list(chain(*[evt.handlers for evt in self.events]))

    def add_events(self, events):
        filtered = []
        for e in events:
            match = self.find_event(e)
            if match is None and e is not None:
                e.index = len(self.events)
                self.events.append(e)
                filtered.append(e)
            else:
                filtered.append(match)
        return filtered

    def add_event(self, event):
        match = self.find_event(event)
        if match is None and event is not None:
            event.index = len(self.events)
            self.events.append(event)
            return event
        else:
            return match

    def find_event(self, event):
        if event is not None:
            for e in self.events:
                if e.match(event):
                    return e
        return None

    def __str__(self):
        res = ["<process ", self.name, ">"]
        return "".join(res)

    def __repr__(self):
        return str(self)
