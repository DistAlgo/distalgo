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

import ast
import sys
import importlib.util

from . import dast
from .parser import Parser
from .utils import ResolverException, CompilerMessagePrinter

# all DistAlgo modules parsed so far:
modules = dict()

class Resolver(CompilerMessagePrinter):
    """Finds definitions for names and attributes."""

    def __init__(self, filename, options, package, _parent=None):
        super().__init__(filename, _parent=_parent)
        self.package = package
        self.options = options

    def _resolve_relative_import(self, impstmt):
        """Calculates real module name from relative name."""
        assert isinstance(impstmt, dast.ImportFromStmt)
        try:
            real_name = '.' * impstmt.level + impstmt.module
            return importlib.util.resolve_name(real_name, self.package)
        except ValueError as e:
            raise ResolverException('module name resolution failed', impstmt) \
                from e

    def _daast_from_str(self, src, filename, package):
        # a modified version of `parser.daast_from_str` that does not print
        # spurious messages:
        try:
            dt = Parser(filename, self.options, _package=package, _parent=self)
            dt.visit(ast.parse(src, filename))
            if dt.errcnt == 0:
                return dt.program
            else:
                raise ResolverException(
                    "parsing errors while trying to resolve '{}'".format(package))
        except SyntaxError as e:
            sys.stderr.write("%s:%d:%d: SyntaxError: %s" % (e.filename, e.lineno,
                                                            e.offset, e.text))
            raise ResolverException(
                "SyntaxError occurred while trying to resolve '{}'"
                .format(package)) from e

    def _get_ast_for_module(self, name):
        """Given a module `name`, return an AST representation of the source."""
        if name is None:
            return None
        if name in modules:
            return modules[name]
        try:
            spec = importlib.util.find_spec(name)
            if spec is None:
                raise ResolverException(
                    "unable to find source file for module '{}'".format(name))
            src = spec.loader.get_source(name)
        except Exception as e:
            raise ResolverException("unable to load module '{}'".format(name)) \
                from e
        mod = self._daast_from_str(src, filename=spec.origin, package=name)
        assert isinstance(mod, dast.Program)
        modules[name] = mod
        return mod

    def find_process_definiton(self, expr):
        if isinstance(expr, dast.NameExpr):
            name = expr.value
            if not isinstance(name, dast.NamedVar):
                raise ResolverException('unsupported expression {}'.format(name))
            defstmt = name.last_assignment_before(expr)
            if defstmt is None:
                raise ResolverException(
                    "unable to find definition for '{}'".format(name.name), expr)
            elif isinstance(defstmt, dast.Process):
                return defstmt
            elif isinstance(defstmt, dast.ImportFromStmt):
                orig_name = name.name
                for alias in defstmt.items:
                    if alias.name == name.name or alias.asname == name.name:
                        orig_name = alias.name
                        break
                mod_name = self._resolve_relative_import(defstmt)
                mod = self._get_ast_for_module(mod_name)
                for procdef in mod.processes:
                    if procdef.name == orig_name:
                        return procdef
                raise ResolverException(
                    'unable to find definition for {} in module {}'
                    .format(orig_name, mod_name), expr)
            else:
                raise ResolverException('unsupported definition type {}'
                                        .format(type(defstmt)), defstmt)

        elif isinstance(expr, dast.AttributeExpr):
            defstmt = self._find_definitions_for_attr(expr)
            if not isinstance(defstmt, dast.Process):
                raise ResolverException(
                    "unable to resolve '{}' to a process".format(expr.text_repr))
            else:
                return defstmt
        else:
            raise ResolverException('unsupported expression type {}'.format(expr))

    def _find_definitions_for_attr(self, expr):
        if isinstance(expr, dast.NameExpr):
            name = expr.value
            assert isinstance(name, dast.NamedVar)
            defstmt = name.last_assignment_before(expr)

            if defstmt is None:
                raise ResolverException(
                    "unable to find definition for '{}'".format(name.name), expr)

            elif isinstance(defstmt, dast.NameScope):
                return defstmt

            elif isinstance(defstmt, dast.ImportFromStmt):
                orig_name = name.name
                for alias in defstmt.items:
                    if alias.name == name.name or alias.asname == name.name:
                        orig_name = alias.name
                        break
                mod_name = self._resolve_relative_import(defstmt)
                mod = self._get_ast_for_module(mod_name)
                nobj = mod.find_name(orig_name)
                if nobj:
                    for stmt, _ in reversed(nobj.assignments):
                        if isinstance(stmt, dast.NameScope):
                            return stmt
                raise ResolverException(
                    'unable to find definition for {} in module {}'
                    .format(orig_name, mod_name), expr)

            elif isinstance(defstmt, dast.ImportStmt):
                # first check if this name is an alias..
                orig_name = None
                for alias in defstmt.items:
                    if alias.asname == name.name:
                        orig_name = alias.name
                if orig_name:
                    # it's an alias, and since it's an ImportStmt, orig_name can
                    # not be a relative name, so just load the aliased module
                    return self._get_ast_for_module(orig_name)

                # ..otherwise, must find all imports with 'name' as first
                # component...
                imps = [defstmt]
                last = defstmt
                while True:
                    last = name.last_assignment_before(last)
                    if not last:
                        break
                    elif isinstance(last, dast.ImportStmt):
                        imps.append(last)
                    # we can ignore any other types of assignments since they
                    # are guaranteed to be irrelevant

                # ... and gather all modules names that has name.name as first
                # component:
                candidates = []
                for stmt in imps:
                    for alias in stmt.items:
                        if alias.name.split('.')[0] == name.name:
                            candidates.append(alias.name)
                return (1, candidates)

            else:
                raise ResolverException('unsupported definition type {}'
                                        .format(type(defstmt)), defstmt)

        elif isinstance(expr, dast.AttributeExpr):
            defs = self._find_definitions_for_attr(expr.value)
            if isinstance(defs, dast.NameScope):
                nobj = defs.find_name(expr.attr)
                if nobj:
                    for stmt, _ in reversed(nobj.assignments):
                        if isinstance(stmt, dast.NameScope):
                            return stmt
                raise ResolverException(
                    "unable to find definition for '{}'".format(expr.text_repr))
            elif isinstance(defs, tuple):
                prefix_len, defs = defs
                candidates = []
                for name in defs:
                    parts = name.split('.')
                    if len(parts) > prefix_len and parts[prefix_len] == expr.attr:
                        candidates.append(name)
                if len(candidates) > 1:
                    return (prefix_len + 1, candidates)
                elif len(candidates) == 1:
                    # only one candidate left, check to see if we have the full
                    # module name:
                    name =  candidates[0]
                    parts = name.split('.')
                    if len(parts) == prefix_len + 1:
                        # we have full module name, so load it:
                        return self._get_ast_for_module(name)
                    else:
                        # otherwise, pass it on:
                        return (prefix_len + 1, candidates)
                else:
                    raise ResolverException(
                        "unable to find definition for '{}'"
                        .format(expr.text_repr))
            else:
                raise ResolverException(
                    "unsupported definition type {} for {}"
                    .format(defs, expr.value))

