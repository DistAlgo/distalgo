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

import os
import sys
import time
import time
import stat
import types
import pickle
import signal
import logging
import collections.abc
import importlib
import threading
import traceback
import multiprocessing
import os.path

from sys import stderr
from . import common, sim, endpoint
from .common import api, deprecated, get_runtime_option, ProcessId

DISTPY_SUFFIXES = [".da", ""]
PYTHON_SUFFIX = ".py"
NODECLS = "Node_"
DEFAULT_MASTER_PORT = 15000
ASYNC_TIMEOUT = 10

CONSOLE_LOG_FORMAT = \
    '[%(relativeCreated)d] %(name)s%(daPid)s:%(levelname)s: %(message)s'
FILE_LOG_FORMAT = \
    '[%(asctime)s] %(name)s%(daPid)s:%(levelname)s: %(message)s'

log = logging.getLogger(__name__)


def find_file_on_paths(filename, paths):
    """Looks for a given 'filename' under a list of directories, in order.

    If found, returns a pair (path, mode), where 'path' is the full path to
    the file, and 'mode' is the result of calling 'os.stat' on the file.
    Otherwise, returns (None, None).

    """
    for path in paths:
        fullpath = os.path.join(path, filename)
        try:
            filemode = os.stat(fullpath)
            return fullpath, filemode
        except OSError:
            pass
    return None, None

def strip_suffix(filename):
    """Returns a filename minus it's extension."""

    dotidx = filename.rfind(".")
    return filename[:dotidx] if dotidx != -1 else filename

@api
def init(**configs):
    """Initializes the DistAlgo runtime.

    """
    common.initialize_runtime_options(configs)

@api
def import_da(name, from_dir=None, compiler_args=[]):
    """Imports DistAlgo module 'module', returns the module object.

    This function mimics the Python builtin __import__() function for DistAlgo
    modules. 'name' is the name of the module to be imported, in
    "dotted module name" format. The module must be implemented in one regular
    DistAlgo source file with a '.da' filename suffix; package modules are not
    supported.

    This function returns the imported module object upon successful import;
    otherwise, 'ImportError' is raised.

    Optional argument 'compiler_args' is a list of command line arguments to
    pass to the compiler, if a compiler invocation is needed. Optional
    argument 'from_dir' should be a valid module search path that overrides
    'sys.path'.

    """
    import da.compiler as compiler
    paths = sys.path if from_dir is None else [from_dir]
    pathname = name.replace(".", os.sep)
    for suffix in DISTPY_SUFFIXES:
        fullpath, mode = find_file_on_paths(pathname + suffix, paths)
        if fullpath is not None:
            break
    if fullpath is None:
        raise ImportError("Module %s not found." % name)
    pyname = strip_suffix(fullpath) + PYTHON_SUFFIX
    try:
        pymode = os.stat(pyname)
    except OSError:
        pymode = None

    force_recompile = get_runtime_option('recompile', default=False)
    if (pymode is None or
            pymode[stat.ST_MTIME] < mode[stat.ST_MTIME] or force_recompile):
        oldargv = sys.argv
        try:
            argv = oldargv[0:0] + compiler_args + [fullpath]
            res = compiler.ui.main(argv)
        except Exception as err:
            raise RuntimeError("Compiler failure!", err)
        finally:
            sys.argv = oldargv

        if res != 0:
            raise ImportError("Unable to compile %s, errno: %d" %
                              (fullpath, res))

    moduleobj = importlib.import_module(name)
    common.setup_logging_for_module(name, CONSOLE_LOG_FORMAT, FILE_LOG_FORMAT)
    return moduleobj

def entrypoint():
    """Entry point when running DistAlgo as the main module.

    """
    startmeth = get_runtime_option('start_method')
    if startmeth != multiprocessing.get_start_method(allow_none=True):
        multiprocessing.set_start_method(startmeth)
    target = get_runtime_option('file')
    source_dir = os.path.dirname(target)
    basename = strip_suffix(os.path.basename(target))
    compiler_args = get_runtime_option('compiler_flags').split()
    if not os.access(target, os.R_OK):
        die("Can not access source file %s" % target)

    sys.path.insert(0, source_dir)
    try:
        module = import_da(basename,
                           from_dir=source_dir,
                           compiler_args=compiler_args)
    except ImportError as e:
        die("ImportError: " + str(e))

    if not (hasattr(module, 'Node_') and
            type(module.Node_) is type and
            issubclass(module.Node_, sim.DistProcess)):
        die("Main process not defined!")

    common.set_runtime_option('this_module_name', module.__name__)
    common.set_runtime_option('main_module_name', module.__name__)
    if get_runtime_option('inc_module_name') is None:
        common.set_runtime_option('inc_module_name', module.__name__ + "_inc")
    common.sysinit()

    # Start main program
    sys.argv = [target] + get_runtime_option('args')
    nodename = get_runtime_option('nodename')
    niters = get_runtime_option('iterations')
    strict = linear = False
    hostname = None
    port = None
    nodeimpl = None
    if len(nodename) > 0:
        linear = True
        hostname = get_runtime_option('hostname')
        port = get_runtime_option('port')
        if port is None:
            port = DEFAULT_MASTER_PORT
        else:
            strict = True
    try:
        trman = endpoint.TransportManager()
        trman.initialize(hostname=hostname, port=port,
                         strict=strict, linear=linear)
        nid = ProcessId._create(pcls=module.Node_,
                                transports=trman.transport_addresses,
                                name=nodename)
        common._set_node(nid)
        log.info("Node %s initialized at %s:%d.", nid,
                 get_runtime_option('hostname'), trman.transport_addresses[0])
        log.info("Starting program %s...", target)
        for i in range(0, niters):
            log.info("Running iteration %d ...", (i+1))

            nodeimpl = sim.OSThreadContainer(process_class=module.Node_,
                                             transport_manager=trman,
                                             process_id=nid,
                                             parent_id=nid,
                                             process_name=nodename)
            nodeimpl.start()

            log.info("Waiting for remaining child processes to terminate..."
                     "(Press \"Ctrl-C\" to force kill)")
            nodeimpl.join()
            nodeimpl = None
            log.info("Main process terminated.")
        return 0

    except endpoint.TransportException as e:
        log.error("Transport initialization failed due to: %r", e)
        stderr.write("System failed to start. \n")
        return 5
    except KeyboardInterrupt as e:
        log.warning("Received keyboard interrupt. ")
        if nodeimpl is not None:
            stderr.write("Terminating node.")
            nodeimpl.end()
            t = 0
            while nodeimpl.is_alive() and t < ASYNC_TIMEOUT:
                stderr.write(".")
                t += 1
                nodeimpl.join(timeout=1)
        if nodeimpl is not None and nodeimpl.is_alive():
            stderr.write("\nNode did not terminate gracefully, "
                         "some zombie child processes may be present.\n")
            return 2
        else:
            stderr.write("\nNode terminated. Goodbye!\n")
            return 1
    except Exception as e:
        err_info = sys.exc_info()
        log.error("Caught unexpected global exception: %r", e)
        traceback.print_tb(err_info[2])
        return 4

@api
def nameof(pid):
    assert isinstance(pid, ProcessId)
    return pid.name

def die(mesg = None):
    if mesg != None:
        stderr.write(mesg + "\n")
    sys.exit(1)
