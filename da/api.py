# Copyright (c) 2010-2014 Bo Lin
# Copyright (c) 2010-2014 Yanhong Annie Liu
# Copyright (c) 2010-2014 Stony Brook University
# Copyright (c) 2010-2014 The Research Foundation of SUNY
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
import importlib
import threading
import traceback
import multiprocessing
import os.path

from . import compiler, common, sim, endpoint as ep

api = common.api
deprecated = common.deprecated

DISTPY_SUFFIXES = [".da", ""]
PYTHON_SUFFIX = ".py"

log = logging.getLogger(__name__)
formatter = logging.Formatter(
    '[%(asctime)s]%(name)s:%(levelname)s: %(message)s')
log._formatter = formatter

PerformanceCounters = {}
CounterLock = threading.Lock()
RootLock = threading.Lock()
EndPointType = ep.UdpEndPoint
TotalUnits = None

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

    GlobalOptions = common.global_options()
    force_recompile = GlobalOptions.recompile \
                      if GlobalOptions is not None else False
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

    return importlib.import_module(name)

def use_channel(channel_properties):
    global EndPointType

    ept = ep.UdpEndPoint
    if isinstance(channel_properties, str):
        channel_properties = [channel_properties]
    for prop in channel_properties:
        if prop == "fifo":
            ept = ep.TcpEndPoint
        elif prop == "reliable":
            ept = ep.TcpEndPoint
        elif prop not in {"unfifo", "unreliable"}:
            log.error("Unknown channel property %s", str(prop))
            return

    if common.current_process() is not None:
        if EndPointType != ept:
            log.warn(
                "Can not change channel type after creating child processes.")
        return
    EndPointType = ept

@api
def config(**properties):
    for prop in properties:
        if prop == 'channel':
            use_channel(properties['channel'])
        elif prop == 'clock':
            setattr(common.global_options(), 'clock', properties[prop])
        elif prop == 'handling':
            setattr(common.global_options(), 'handling', properties[prop])
        else:
            log.warn("Unknown configuration type '%s'." % str(prop))

def entrypoint():
    GlobalOptions = common.global_options()
    if GlobalOptions.start_method != \
       multiprocessing.get_start_method(allow_none=True):
        multiprocessing.set_start_method(GlobalOptions.start_method)
    target = GlobalOptions.file
    source_dir = os.path.dirname(target)
    basename = strip_suffix(os.path.basename(target))
    if not os.access(target, os.R_OK):
        die("Can not access source file %s" % target)

    sys.path.insert(0, source_dir)
    try:
        module = import_da(basename,
                           from_dir=source_dir,
                           compiler_args=GlobalOptions.compiler_flags.split())
    except ImportError as e:
        die("ImportError: " + str(e))
    if not (hasattr(module, 'main') and
            isinstance(module.main, types.FunctionType)):
        die("'main' function not defined!")
    GlobalOptions.this_module_name = module.__name__
    GlobalOptions.main_module_name = module.__name__
    if GlobalOptions.inc_module_name is None:
        GlobalOptions.inc_module_name = module.__name__ + "_inc"
    common.sysinit()

    # Start the background statistics thread:
    RootLock.acquire()
    stat_th = threading.Thread(target=collect_statistics,
                               name="Stat Thread")
    stat_th.daemon = True
    stat_th.start()

    niters = GlobalOptions.iterations
    stats = {'sent' : 0, 'usrtime': 0, 'systime' : 0, 'time' : 0,
              'units' : 0, 'mem' : 0}
    # Start main program
    sys.argv = [target] + GlobalOptions.args
    try:
        for i in range(0, niters):
            log.info("Running iteration %d ..." % (i+1))

            walltime_start = time.perf_counter()
            module.main()

            print("Waiting for remaining child processes to terminate..."
                  "(Press \"Ctrl-C\" to force kill)")

            walltime = time.perf_counter() - walltime_start

            #log_performance_statistics(walltime)
            r = aggregate_statistics()
            for k, v in r.items():
                stats[k] += v

        for k in stats:
            stats[k] /= niters

        perffd = None
        # if GlobalOptions.perffile is not None:
        #     perffd = open(GlobalOptions.perffile, "w")
        if perffd is not None:
            print_simple_statistics(perffd)
            perffd.close()

        dumpfd = None
        # if GlobalOptions.dumpfile is not None:
        #     dumpfd = open(GlobalOptions.dumpfile, "wb")
        if dumpfd is not None:
            pickle.dump(stats, fd)
            dumpfd.close()

    except KeyboardInterrupt as e:
        log.info("Received keyboard interrupt.")
    except Exception as e:
        err_info = sys.exc_info()
        log.error("Caught unexpected global exception: %r", e)
        traceback.print_tb(err_info[2])

    log.info("Terminating...")

@api
def new(pcls, args=None, num=1, **props):
    if not issubclass(pcls, sim.DistProcess):
        log.error("Can not create non-DistProcess.")
        return set()

    if common.current_process() is None:
        if type(EndPointType) == type:
            common.set_current_process(EndPointType())
            common.current_process().shared = multiprocessing.Value("i", 0)
            RootLock.release()
        else:
            log.error("EndPoint not defined")
            return
    log.debug("Current process is %s" % str(common.current_process()))

    log.debug("Creating %d instances of %s.." % (num, str(pcls)))
    pipes = []
    iterator = []
    if isinstance(num, int):
        iterator = range(num)
    elif isinstance(num, set):
        iterator = num
    else:
        log.error("Unrecognised parameter: %s", str(num))
        return set()

    procs = set()
    daemon = props['daemon'] if 'daemon' in props else False
    for i in iterator:
        (childp, ownp) = multiprocessing.Pipe()
        p = pcls(common.current_process(), childp,
                 type(common.current_process()), props)
        p.daemon = daemon
        if isinstance(i, str):
            p.set_name(i)
        # Buffer the pipe
        pipes.append((i, childp, ownp, p))
        # We need to start proc right away to obtain EndPoint and pid for p:
        p.start()
        procs.add(p)

    log.debug("%d instances of %s created.", len(procs), pcls.__name__)
    result = dict()
    for i, childp, ownp, p in pipes:
        childp.close()
        cid = ownp.recv()
        cid._initpipe = ownp    # Tuck the pipe here
        cid._proc = p           # Set the process object
        result[i] = cid

    if (args != None):
        setup(result, args)

    if isinstance(num, int):
        return set(result.values())
    else:
        return result

@api
def setup(pids, args):
    if isinstance(pids, dict):
        ps = pids.values()
    elif not hasattr(pids, '__iter__'):
        ps = [pids]
    else:
        ps = pids

    for p in ps:
        p._initpipe.send(("setup", args))

@api
def start(procs, args=None):
    if isinstance(procs, dict):
        ps = procs.values()
    elif not hasattr(procs, '__iter__'):
        ps = [procs]
    else:
        ps = procs

    if args is not None:
        setup(ps, args)

    init_performance_counters(ps)
    log.debug("Starting %s..." % str(procs))
    for p in ps:
        p._initpipe.send("start")
        del p._initpipe

@api
def send(data, to):
    result = True
    if (hasattr(to, '__iter__')):
        for t in to:
            r = t.send(data, common.current_process(), 0)
            if not r: result = False
    else:
        result = to.send(data, common.current_process(), 0)
    return result

def collect_statistics():
    global PerformanceCounters
    global CounterLock

    completed = 0
    try:
        RootLock.acquire()
        for mesg in common.current_process().recvmesgs():
            src, tstamp, tup = mesg
            event_type, count = tup

            CounterLock.acquire()
            if PerformanceCounters.get(src) is not None:
                if PerformanceCounters[src].get(event_type) is None:
                    PerformanceCounters[src][event_type] = count
                else:
                    PerformanceCounters[src][event_type] += count
            else:
                log.debug("Unknown proc: " + str(src))
            CounterLock.release()

    except KeyboardInterrupt:
        pass

    except Exception as e:
        err_info = sys.exc_info()
        log.debug("Caught unexpected global exception: %r", e)
        traceback.print_tb(err_info[2])

def init_performance_counters(procs):
    global PerformanceCounters
    global CounterLock
    CounterLock.acquire()
    PerformanceCounters = {}
    CounterLock.release()
    for p in procs:
        CounterLock.acquire()
        PerformanceCounters[p] = dict()
        CounterLock.release()

def log_performance_statistics(walltime):
    global PerformanceCounters
    global CounterLock

    statstr = "***** Statistics *****\n"
    tot_sent = 0
    tot_usrtime = 0
    tot_systime = 0
    tot_time = 0
    tot_units = 0
    total = dict()

    CounterLock.acquire()
    for proc, data in PerformanceCounters.items():
        for key, val in data.items():
            if total.get(key) is not None:
                total[key] += val
            else:
                total[key] = val

    statstr += ("* Total procs: %d\n" % len(PerformanceCounters))
    CounterLock.release()
    statstr += ("* Wallclock time: %f\n" % walltime)

    if total.get('totalusrtime') is not None:
        statstr += ("** Total usertime: %f\n" % total['totalusrtime'])
        if TotalUnits is not None:
            statstr += ("*** Average usertime: %f\n" %
                        (total['totalusrtime']/TotalUnits))

    if total.get('totalsystime') is not None:
        statstr += ("** Total systemtime: %f\n" % total['totalsystime'])

    if total.get('mem') is not None:
        statstr += ("** Total memory: %d\n" % total['mem'])
        if TotalUnits is not None:
            statstr += ("*** Average memory: %f\n" % (total['mem'] / TotalUnits))

    log.info(statstr)

def print_simple_statistics(outfd):
    st = aggregate_statistics()
    statstr = str(st['usrtime']) + '\t' + str(st['time']) + '\t' + str(st['mem'])
    outfd.write(statstr)

def aggregate_statistics():
    global PerformanceCounters
    global CounterLock

    result = {'sent' : 0, 'usrtime': 0, 'systime' : 0, 'time' : 0,
              'units' : 0, 'mem' : 0}

    CounterLock.acquire()
    for key, val in PerformanceCounters.items():
        if val.get('sent') is not None:
            result['sent'] += val['sent']
        if val.get('totalusrtime') is not None:
            result['usrtime'] += val['totalusrtime']
        if val.get('totalsystime') is not None:
            result['systime'] += val['totalsystime']
        if val.get('totaltime') is not None:
            result['time'] += val['totaltime']
        if val.get('mem') is not None:
            result['mem'] += val['mem']
    CounterLock.release()

    if TotalUnits is not None:
        for key, val in result.items():
            result[key] /= TotalUnits

    return result

def set_proc_attribute(procs, attr, values):
    if isinstance(procs, dict):
        ps = procs.values()
    else:
        ps = procs

    for p in ps:
        p._initpipe.send((attr, values))

def die(mesg = None):
    if mesg != None:
        sys.stderr.write(mesg + "\n")
    sys.exit(1)
