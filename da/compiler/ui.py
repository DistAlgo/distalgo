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

import ast
import sys
import time
import argparse

import da

from da.compiler.parser import Parser, daast_from_file
from da.compiler.pygen import PythonGenerator
from da.compiler.pseudo import DastUnparser
from da.compiler.incgen import gen_inc_module
from da.compiler.utils import is_valid_debug_level, set_debug_level, to_source, to_file

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
        return pyast
    else:
        return None

def dafile_to_pyast(filename, args=None):
    """Translates DistAlgo source file into executable Python AST.

    'filename' is the filename of source file. Optional argument 'args' is a
    Namespace object containing the command line parameters for the compiler.
    Returns the generated Python AST.

    """
    daast = daast_from_file(filename, args)
    if daast is not None:
        pyast = PythonGenerator(filename, args).visit(daast)
        if pyast is None:
            print("Error: unable to generate Python AST from DistAlgo AST"
                  " for file ", filename, file=stderr)
        return pyast
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

def dafile_to_pseudofile(filename, outname=None):
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

    daast = daast_from_file(filename)
    if daast:
        if outname is None:
            outname = purename + ".dap"
        with open(outname, "w") as outfd:
            DastUnparser(daast, outfd)
            stderr.write("Written pseudo code file %s.\n"% outname)

def dafile_to_pyfile(args):
    """Compiles a DistAlgo source file to Python file.

    'args' is the object generated by argparse from the command line
    arguments, and should contain the following properties:

    'args.filename' is the input DistAlgo source file. Optional property
    'args.outname' specifies the file to write the result to. If
    'args.outname' is None the filename is inferred by replacing the suffix of
    'args.filename' with '.py'.

    """
    filename = args.infile
    outname = args.outfile
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

    pyast = dafile_to_pyast(filename, args)
    if pyast is not None:
        if outname is None:
            outname = purename + ".py"
        with open(outname, "w") as outfd:
            global OutputSize
            OutputSize += to_file(pyast, outfd)
            stderr.write("Written compiled file %s.\n"% outname)
        return 0
    else:
        return 1

def check_python_version():
    if sys.version_info < (3, 4):
        stderr.write("DistAlgo requires Python version 3.4 or newer.\n")
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
        return
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
        with open(outname, "w") as outfd:
            OutputSize += to_file(ast, outfd)
            stderr.write("Written compiled file %s.\n"% outname)
        with open(incname, "w") as outfd:
            OutputSize += to_file(inc, outfd)
            stderr.write("Written interface file %s.\n" % incname)
        return 0
    else:
        return 1

def main(argv=None):
    """Main entry point when invoking compiler module from command line.
    """
    if not check_python_version():
        return 2

    ap = argparse.ArgumentParser(description="DistAlgo compiler.",
                                 argument_default=argparse.SUPPRESS)
    ap.add_argument('-o', help="Output file name.",
                    dest="outfile", default=None)
    ap.add_argument('-L', help="Logging output level.",
                    dest="debug", default=None)
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
    ap.add_argument('-i',
                    help="Generate interface code for plugging"
                    " into incrementalizer.",
                    action='store_true', dest="geninc", default=False)
    ap.add_argument("-m", "--inc-module-name",
                    help="name of the incrementalized interface module, "
                    "defaults to source module name + '_inc'. ",
                    dest="incfile", default=None)
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
    ap.add_argument('-I', '--interactive',
                    help="Launch interactive shell.",
                    action='store_true', default=False)
    ap.add_argument('-B', '--benchmark',
                    help="Print the elapsed wallclock time of the compile session.",
                    action='store_true', default=False)
    ap.add_argument('-p', help="Generate DistAlgo pseudo code.",
                    action='store_true', dest="genpsd", default=False)
    ap.add_argument("-v", "--version", action="version", version=da.__version__)
    ap.add_argument('--psdfile', help="Name of DistAlgo pseudo code output file.",
                    dest="psdfile", default=None)
    ap.add_argument('infile', metavar='SOURCEFILE', type=str,
                    help="DistAlgo input source file.")

    if argv is None:
        argv = sys.argv[1:]
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
        res =dafile_to_pseudofile(args.infile, args.psdfile)
    if args.geninc:
        res = dafile_to_incfiles(args)
    else:
        res = dafile_to_pyfile(args)

    if args.benchmark:
        import json
        walltime = time.perf_counter() - WallclockStart
        jsondata = {'Wallclock_time' : walltime,
                    "Input_size" : InputSize,
                    "Output_size" : OutputSize}
        print("###OUTPUT: " + json.dumps(jsondata))

    return res
