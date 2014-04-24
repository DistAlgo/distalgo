import ast
import argparse
from sys import stdout, stderr

from .parser import Parser
from .pygen import PythonGenerator
from .pypr import to_source
from .pseudo import to_pseudo

Debug = None

def dpyast_from_file(filename):
    """Generates a DistPy AST representation from the specified DistPy source
       file.

    """
    dt = Parser(filename)
    dt.set_debug(Debug)
    with open(filename, 'r') as infd:
        pytree = ast.parse(infd.read())
        dt.visit(pytree)
        stderr.write("%s compiled with %d errors and %d warnings.\n" %
                     (filename, dt.errcnt, dt.warncnt))
        if dt.errcnt == 0:
            return dt.program
        else:
            return None

def dpyfile_to_pyast(filename):
    """Translates the given DistPy source file into Python code. Returns an AST
       representation of the result.

    """
    dpyast = dpyast_from_file(filename)
    if dpyast is None:
        stderr.write("Error: unable to generate DistPy AST for file %s\n" % filename)
        return None

    pyast = PythonGenerator(filename).visit(dpyast)
    if pyast is None:
        stderr.write("Error: unable to generate Python AST for file %s\n" % filename)
        return None
    else:
        return pyast

def dpyfile_to_pseudofile(filename, outname=None):
    """Compiles a DistPy source file to Python file.

    'filename' is the input DistPy source file. Optional parameter 'outname'
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
    elif suffix != "dpy":
        stderr.write("Warning: unknown suffix '%s' in filename '%s'\n" %
                      (suffix, filename))

    dpyast = dpyast_from_file(filename)
    psdstr = to_pseudo(dpyast)
    if outname is None:
        outname = purename + ".da"
    with open(outname, "w") as outfd:
        outfd.write(psdstr)
        stderr.write("Written pseudo code file %s.\n"% outname)

def dpyfile_to_pyfile(filename, outname=None):
    """Compiles a DistPy source file to Python file.

    'filename' is the input DistPy source file. Optional parameter 'outname'
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
    elif suffix != "dpy":
        stderr.write("Warning: unknown suffix '%s' in filename '%s'\n" %
                      (suffix, filename))

    pyast = dpyfile_to_pyast(filename)
    if pyast is not None:
        pystr = to_source(pyast)
        if outname is None:
            outname = purename + ".py"
        with open(outname, "w") as outfd:
            outfd.write(pystr)
            stderr.write("Written compiled file %s.\n"% outname)

def main():
    """Main entry point when invoking compiler module from command line.
    """
    global Debug
    ap = argparse.ArgumentParser(description="DistPy compiler.")
    ap.add_argument('-o', help="Output file name.", dest="outfile")
    ap.add_argument('-d', help="Print debugging info.",
                    dest="debug", default=None)
    ap.add_argument('-p', help="Generate pseudo code instead of Python code.",
                    action='store_true', dest="genpsd")
    ap.add_argument('--psdfile', help="Name of output pseudo code file.",
                    dest="psdfile", default=None)
    ap.add_argument('infile', help="DistPy input source file.")
    args = ap.parse_args()
    Debug = args.debug
    if args.genpsd:
        dpyfile_to_pseudofile(args.infile, args.psdfile)
    else:
        dpyfile_to_pyfile(args.infile, args.outfile)
