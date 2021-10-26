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

import os
import os.path
import ast
import sys
import time
import argparse

from .. import __version__
from ..importer import da_cache_from_source
from .utils import is_valid_debug_level, set_debug_level, to_source, to_file
from .parser import daast_from_file
from .parser import daast_from_str
from .pygen import PythonGenerator
from .incgen import gen_inc_module
from .pseudo import DastUnparser

# DistAlgo filename suffix
DA_SUFFIX = "da"

stdout = sys.stdout
stderr = sys.stderr

# Benchmark stats:
WallclockStart = 0
InputSize = 0
OutputSize = 0

def dastr_to_pyast(src, filename='<str>', args=None):
    """Translates DistAlgo source string into executable Python AST.

    'src' is the DistAlgo source string to parse. Optional argument 'filename'
    is the filename that appears in error messages. Optional argument 'args'
    is a Namespace object containing the command line parameters for the
    compiler. Returns the generated Python AST.

    """
    daast = daast_from_str(src, filename, args)
    if daast is not None:
        pyast = PythonGenerator(filename, args).visit(daast)
        if pyast is None:
            print("Error: unable to generate Python AST from DistAlgo AST"
                  " for file ", filename, file=stderr)
        assert isinstance(pyast, list) and len(pyast) == 1 and \
            isinstance(pyast[0], ast.Module)
        pyast = pyast[0]
        ast.fix_missing_locations(pyast)
        return pyast
    else:
        return None

def dafile_to_pyast(filename, args=None):
    """Translates DistAlgo source file into executable Python AST.

    'filename' is the filename of source file. Optional argument 'args' is a
    Namespace object containing the command line parameters for the compiler.
    Returns the generated Python AST.

    """
    if args is None:
        args = parse_compiler_args([])
    daast = daast_from_file(filename, args)
    if daast is not None:
        pyast = PythonGenerator(filename, args).visit(daast)
        if pyast is None:
            print("Error: unable to generate Python AST from DistAlgo AST"
                  " for file ", filename, file=stderr)
            return None
        assert isinstance(pyast, list) and len(pyast) == 1 and \
            isinstance(pyast[0], ast.Module)
        pyast = pyast[0]
        ast.fix_missing_locations(pyast)
        if args and hasattr(args, 'dump_ast') and args.dump_ast:
            print(ast.dump(pyast, include_attributes=True), file=stderr)
        return pyast
    else:
        return None

def _pyast_to_pycode(pyast, filename, _optimize=-1):
    try:
        return compile(pyast, filename, mode='exec',
                       dont_inherit=True, optimize=_optimize)
    except Exception as e:
        print("Unable to generate bytecode: {}".format(e), file=stderr)
        return None

def dafile_to_pycode(filename, args=None, _optimize=-1, dfile=None):
    """Generates compiled Python code object from DistAlgo source file.

    'filename' is the source file to compile. Optional argument 'args' is a
    Namespace object containing the command line parameters for the compiler.
    Returns the compiled Python code object, or None in case of errors.

    """
    pyast = dafile_to_pyast(filename, args)
    if pyast is not None:
        return _pyast_to_pycode(pyast,
                                dfile if dfile is not None else filename,
                                _optimize)
    else:
        return None

def dastr_to_pycode(src, filename='<string>', args=None, _optimize=-1):
    """Generates compiled Python code object from DistAlgo source string.

    'src' is the DistAlgo source string to compile. Optional argument 'filename'
    is the filename that appears in error messages. Optional argument 'args' is
    a Namespace object containing the command line parameters for the compiler.
    Returns the compiled Python code object, or None in case of errors.

    """
    pyast = dastr_to_pyast(src, filename, args)
    if pyast is not None:
        return _pyast_to_pycode(pyast, filename, _optimize)
    else:
        return None

def dafile_to_pystr(filename, args=None):
    """Generates executable Python code from DistAlgo source string.

    'filename' is the filename of DistAlgo source file. Optional argument 'args'
    is a Namespace object containing the command line parameters for the
    compiler. Returns the generated Python code as a string.

    """
    pyast = dafile_to_pyast(filename, args)
    if pyast is not None:
        return to_source(pyast)
    else:
        return None

def dastr_to_pystr(src, filename='<str>', args=None):
    """Generates executable Python code from DistAlgo source string.

    'src' is the DistAlgo source string to parse. Optional argument 'filename'
    is the filename that appears in error messages. Optional argument 'args'
    is a Namespace object containing the command line parameters for the
    compiler. Returns the generated Python code as a string.

    """
    pyast = dastr_to_pyast(src, filename, args)
    if pyast is not None:
        return to_source(pyast)
    else:
        return None

def _sanitize_filename(dfile, no_symlink=True):
    """Check and sanitize 'dfile' for use as a target file.

    """
    dirname, basename = os.path.split(dfile)
    dirname = os.path.abspath(dirname)
    dfile = os.path.join(dirname, basename)
    if no_symlink:
        if os.path.islink(dfile):
            msg = ('{} is a symlink and will be changed into a regular file if '
                   'the compiler writes a compiled file to it')
            raise FileExistsError(msg.format(dfile))
        elif os.path.exists(dfile) and not os.path.isfile(dfile):
            msg = ('{} is a non-regular file and will be changed into a regular '
                   'one if the compiler writes a compiled file to it')
            raise FileExistsError(msg.format(dfile))
    os.makedirs(dirname, exist_ok=True)
    return dfile

def dafile_to_pseudofile(filename, outname=None, args=None):
    """Compiles a DistAlgo source file to Python file.

    'filename' is the input DistAlgo source file. Optional parameter 'outname'
    specifies the file to write the result to. If 'outname' is None the
    filename is inferred by replacing the suffix of 'filename' with '.py'.

    """
    purename, _, suffix = filename.rpartition(".")
    if len(purename) == 0:
        purename = suffix
        suffix = ""
    if suffix == "py":
        stderr.write("Warning: skipping '.py' file %s\n" % filename)
        return
    elif suffix != DA_SUFFIX:
        stderr.write("Warning: unknown suffix '%s' in filename '%s'\n" %
                      (suffix, filename))
    if outname is None:
        outname = purename + ".dap"
    outname = _sanitize_filename(outname)
    daast = daast_from_file(filename, args)
    if daast:
        with open(outname, "w", encoding='utf-8') as outfd:
            DastUnparser(daast, outfd)
            stderr.write("Written pseudo code file %s.\n"% outname)

def dafile_to_pyfile(filename, outname=None, args=None):
    """Compiles a DistAlgo source file to Python file.

    If specified, 'args' should be an object (usually generated by argparse from
    the command line arguments) that contains the following properties:

    'args.filename' is the input DistAlgo source file. Optional property
    'args.outname' specifies the file to write the result to. If
    'args.outname' is None the filename is inferred by replacing the suffix of
    'args.filename' with '.py'.

    """
    purename, _, suffix = filename.rpartition(".")
    if len(purename) == 0:
        purename = suffix
        suffix = ""
    if suffix == "py":
        stderr.write("Warning: skipping '.py' file %s\n" % filename)
        return
    elif suffix != DA_SUFFIX:
        stderr.write("Warning: unknown suffix '%s' in filename '%s'\n" %
                      (suffix, filename))
    if outname is None:
        outname = purename + ".py"
    outname = _sanitize_filename(outname)
    pyast = dafile_to_pyast(filename, args)
    if pyast is not None:
        with open(outname, "w", encoding='utf-8') as outfd:
            global OutputSize
            OutputSize += to_file(pyast, outfd)
            stderr.write("Written compiled file %s.\n"% outname)
        return 0
    else:
        return 1

def dafile_to_pycfile(filename, outname=None, optimize=-1, args=None,
                      dfile=None):
    """Byte-compile one DistAlgo source file to Python bytecode.

    """
    import importlib._bootstrap_external

    if outname is None:
        if optimize >= 0:
            opt = '' if optimize == 0 else optimize
            outname = da_cache_from_source(filename, optimization=opt)
        else:
            outname = da_cache_from_source(filename)
    outname = _sanitize_filename(outname)
    code = dafile_to_pycode(filename, args, _optimize=optimize, dfile=dfile)
    if code is not None:
        source_stats = os.stat(filename)
        PythonVersion = sys.version_info
        if PythonVersion < (3, 7):
            bytecode = importlib._bootstrap_external._code_to_bytecode(
                code, source_stats.st_mtime, source_stats.st_size)
        else:
             bytecode = importlib._bootstrap_external._code_to_timestamp_pyc(
                code, source_stats.st_mtime, source_stats.st_size)
        mode = importlib._bootstrap_external._calc_mode(filename)
        importlib._bootstrap_external._write_atomic(outname, bytecode, mode)
        stderr.write("Written bytecode file {}.\n".format(outname))
        return 0
    else:
        return 1

def check_python_version():
    if sys.version_info < (3, 5):
        stderr.write("DistAlgo requires Python version 3.5 or newer.\n")
        return False
    else:
        return True

def dafile_to_incfiles(args):
    """Compiles a DistAlgo source file to Python file and generate an interface
    file for incrementalization.

    'args' is the object generated by argparse from the command line
    arguments, and should contain the following properties:

    'filename' is the input DistAlgo source file. Optional property 'outname'
    specifies the file to write the result to. If 'outname' is None the
    filename is inferred by replacing the suffix of 'filename' with '.py'.
    Optional property 'incname' is the file to write the incrementalization
    module to. If 'incname' is None it defaults to the base of 'filename'
    plus '_inc.py'.

    """

    filename = args.infile
    outname = args.outfile
    incname = args.incfile
    purename, _, suffix = filename.rpartition(".")
    if len(purename) == 0:
        purename = suffix
        suffix = ""
    if suffix == "py":
        stderr.write("Warning: skipping '.py' file %s\n" % filename)
        return 2
    elif suffix != DA_SUFFIX:
        stderr.write("Warning: unknown suffix '%s' in filename '%s'\n" %
                      (suffix, filename))
    daast = daast_from_file(filename, args)
    if outname is None:
        outname = purename + ".py"
    if incname is None:
        incname = purename + "_inc.py" 
    if daast is not None:
        global OutputSize
        inc, ast = gen_inc_module(daast, args, filename=incname)
        with open(outname, "w", encoding='utf-8') as outfd:
            OutputSize += to_file(ast, outfd)
            stderr.write("Written compiled file %s.\n"% outname)
        with open(incname, "w", encoding='utf-8') as outfd:
            OutputSize += to_file(inc, outfd)
            stderr.write("Written interface file %s.\n" % incname)
        return 0
    else:
        return 1

def _add_compiler_args(parser):
    """Install the set of options affecting compilation.

    """
    ap = parser
    ap.add_argument('--full-event-pattern',
                    help="If set, use the 'full' format "
                    "(TYPE, (CLK, DST, SRC), MSG) for event patterns;"
                    "otherwise, use 'short' format (MSG, SRC)",
                    action='store_true')
    ap.add_argument('--enable-object-pattern',
                    help="Enable the use of object-style tuple pattern syntax:"
                    " Object(ARGS...); which is equivalent to "
                    "('Object', ARGS...)",
                    action='store_true')
    ap.add_argument('--enable-membertest-pattern',
                    help="Overloads the Python 'in' operator to support using "
                    "tuple patterns, e.g.: '(_a, 1, _) in S', which is "
                    "equivalent to 'some((_a, 1, _) in S)'",
                    action='store_true')
    ap.add_argument('--enable-iterator-pattern',
                    help="Overloads the Python 'for ... in ...' keywords to "
                    "support using tuple patterns in the target, "
                    "e.g.: '[b for (_a, 1, b) in S]', which is equivalent to "
                    "'[b for (var1, var2, b) in S if var1 == a if var2 == b]'",
                    action='store_true')
    ap.add_argument('--use-top-semantic',
                    help="Use 'top' semantics for query variable and "
                    "parameter resolution. Under 'top' semantics, only "
                    "parameters to the top-level query are marked.",
                    action='store_true')
    ap.add_argument('--no-table1',
                    help="Disable table 1 quantification transformations. "
                    "Only used when '-i' is enabled.",
                    action='store_true')
    ap.add_argument('--no-table2',
                    help="Disable table 2 quantification transformations. "
                    "Only used when '-i' is enabled.",
                    action='store_true')
    ap.add_argument('--no-table3',
                    help="Disable table 3 quantification transformations. "
                    "Only used when '-i' is enabled.",
                    action='store_true')
    ap.add_argument('--no-table4',
                    help="Disable table 4 quantification transformations. "
                    "Only used when '-i' is enabled.",
                    action='store_true')
    ap.add_argument('--jb-style',
                    help="Generate Jon-friendly quantification transformations. "
                    "Only useful with '-i'.",
                    action='store_true')
    ap.add_argument('--no-all-tables',
                    help="Disable all quantification transformations. "
                    "Only useful with '-i'.",
                    action='store_true')
    ap.add_argument('--module-name', type=str, default='__main__',
                    help="Name of the module to be compiled.")

def parse_compiler_args(argv):
    ap = argparse.ArgumentParser(argument_default=argparse.SUPPRESS)
    _add_compiler_args(ap)
    args = ap.parse_args(argv)
    return args

def main(argv=None):
    """Main entry point when invoking compiler module from command line.

    """
    if not check_python_version():
        return 2

    if argv is None:
        argv = sys.argv[1:]
    ap = argparse.ArgumentParser(description="DistAlgo compiler.",
                                 argument_default=argparse.SUPPRESS)
    _add_compiler_args(ap)
    ap.add_argument('-o', help="Output file name.",
                    dest="outfile", default=None)
    ap.add_argument('-L', help="Logging output level.",
                    dest="debug", default=None)
    ap.add_argument('-i',
                    help="Generate interface code for plugging"
                    " into incrementalizer.",
                    action='store_true', dest="geninc", default=False)
    ap.add_argument("-m", "--inc-module-name",
                    help="name of the incrementalized interface module, "
                    "defaults to source module name + '_inc'. ",
                    dest="incfile", default=None)
    ap.add_argument('-O', '--optimize', type=int, default=-1)
    ap.add_argument('-D', '--dump-ast', default=False, action='store_true')
    ap.add_argument('-C', '--write-bytecode', default=False, action='store_true')
    ap.add_argument('-I', '--interactive',
                    help="Launch interactive shell.",
                    action='store_true', default=False)
    ap.add_argument('-B', '--benchmark',
                    help="Print the elapsed wallclock time of the compile session.",
                    action='store_true', default=False)
    ap.add_argument('-p', help="Generate DistAlgo pseudo code.",
                    action='store_true', dest="genpsd", default=False)
    ap.add_argument("-v", "--version", action="version", version=__version__)
    ap.add_argument('--psdfile', help="Name of DistAlgo pseudo code output file.",
                    dest="psdfile", default=None)
    ap.add_argument('infile', metavar='SOURCEFILE', type=str,
                    help="DistAlgo input source file.")
    args = ap.parse_args(argv)

    if args.benchmark:
        global WallclockStart
        WallclockStart = time.perf_counter()

    if args.interactive:
        import code
        code.interact()
        return

    if args.debug is not None:
        try:
            level = int(args.debug)
            if is_valid_debug_level(level):
                set_debug_level(level)
            else:
                raise ValueError()
        except ValueError:
            stderr.write("Invalid debugging level %s.\n" % str(args.debug))

    if args.genpsd:
        res = dafile_to_pseudofile(args.infile, args.psdfile, args)
    elif args.geninc:
        res = dafile_to_incfiles(args)
    elif args.write_bytecode:
        res = dafile_to_pycfile(args.infile, args.outfile, args.optimize,
                                args=args)
    else:
        res = dafile_to_pyfile(args.infile, args.outfile, args)

    if args.benchmark:
        import json
        walltime = time.perf_counter() - WallclockStart
        jsondata = {'Wallclock_time' : walltime,
                    "Input_size" : InputSize,
                    "Output_size" : OutputSize}
        print("###OUTPUT: " + json.dumps(jsondata))

    return res
