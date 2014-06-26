import os
import sys
import time
import time
import stat
import types
import signal
import logging
import importlib
import threading
import traceback
import multiprocessing
import os.path

from logging import DEBUG
from logging import INFO
from logging import ERROR
from logging import CRITICAL
from logging import FATAL

import dpy.compiler.ui
from dpy.sim import UdpEndPoint, TcpEndPoint, DistProcess
from .common import api, deprecated, Null, api_registry

DISTPY_SUFFIXES = ["", ".dpy", ".da"]
PYTHON_SUFFIX = ".py"

log = logging.getLogger(__name__)
formatter = logging.Formatter(
    '[%(asctime)s]%(name)s:%(levelname)s: %(message)s')
log._formatter = formatter

CmdlineParams = Null()
PerformanceCounters = {}
CounterLock = threading.Lock()
RootProcess = None
RootLock = threading.Lock()
EndPointType = UdpEndPoint
PrintProcStats = False
TotalUnits = None
ProcessIds = []

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

@api
def dpyimport(filename, force_recompile=False, compiler_args=[], indir=None):
    dotidx = filename.rfind(".")
    paths = sys.path if indir is None else [indir]
    if dotidx == -1:
        # filename does not contain suffix, let's try each one
        purename = filename
        for suffix in DISTPY_SUFFIXES:
            fullpath, mode = find_file_on_paths(filename + suffix, paths)
            if fullpath is not None:
                break
    else:
        purename = filename[:dotidx]
        fullpath, mode = find_file_on_paths(filename, paths)
    if fullpath is None:
        raise ImportError("Module %s not found." % filename)
    dotidx = fullpath.rfind(".")
    pyname = (fullpath[:dotidx] + PYTHON_SUFFIX) \
             if dotidx != -1 else (fullpath + PYTHON_SUFFIX)
    try:
        pymode = os.stat(pyname)
    except OSError:
        pymode = None

    if (pymode is None or
            pymode[stat.ST_MTIME] < mode[stat.ST_MTIME] or
            force_recompile):
        oldargv = sys.argv
        try:
            argv = oldargv[0:0] + compiler_args + [fullpath]
            res = dpy.compiler.ui.main(argv)
        except Exception as err:
            raise RuntimeError("Compilation failure", err)
        finally:
            sys.argv = oldargv

        if res != 0:
            raise ImportError("Unable to compile %s, errno: %d" %
                              (fullpath, res))

    return importlib.import_module(purename)

def maximum(iterable):
    if (len(iterable) == 0): return -1
    else: return max(iterable)

@api
def use_channel(endpoint):
    global EndPointType

    ept = None
    if endpoint == "udp":
        ept = UdpEndPoint
    elif endpoint == "tcp":
        ept = TcpEndPoint
    else:
        log.error("Unknown channel type %s", endpoint)
        return

    if RootProcess is not None:
        if EndPointType != ept:
            log.warn(
                "Can not change channel type after creating child processes.")
        return
    EndPointType = ept

def get_channel_type():
    return EndPointType

def setup_root_logger():
    rootlog = logging.getLogger("")

    if not CmdlineParams.nolog:
        rootlog.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '[%(asctime)s]%(name)s:%(levelname)s: %(message)s')
        rootlog._formatter = formatter

        consolelvl = logging._nameToLevel[CmdlineParams.logconsolelevel.upper()]

        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        ch.setLevel(consolelvl)
        rootlog._consolelvl = consolelvl
        rootlog.addHandler(ch)

        if CmdlineParams.logfile:
            filelvl = logging._nameToLevel[CmdlineParams.logfilelevel.upper()]
            logfilename = CmdlineParams.logfilename \
                          if CmdlineParams.logfilename is not None else \
                             (os.path.basename(CmdlineParams.file) + ".log")
            fh = logging.FileHandler(logfilename)
            fh.setFormatter(formatter)
            fh.setLevel(filelvl)
            rootlog._filelvl = filelvl
            rootlog.addHandler(fh)

        if CmdlineParams.logdir is not None:
            os.makedirs(CmdlineParams.logdir, exist_ok=True)
            rootlog._logdir = CmdlineParams.logdir
        else:
            rootlog._logdir = None
    else:
        rootlog.addHandler(logging.NullHandler())

def entrypoint(options):
    global CmdlineParams
    CmdlineParams = options

    setup_root_logger()
    target = options.file
    source_dir = os.path.dirname(target)
    basename = os.path.basename(target)
    if not os.access(target, os.R_OK):
        die("Can not access source file %s" % target)

    sys.path.insert(0, source_dir)
    module = dpyimport(basename,
                       force_recompile=options.recompile,
                       compiler_args=options.compiler_flags.split(),
                       indir=source_dir)
    if not (hasattr(module, 'main') and
            isinstance(module.main, types.FunctionType)):
        die("'main' function not defined!")

    if options.useincmodule:
        try:
            name = options.incmodulename if options.incmodulename is not None \
                   else module.__name__ + "_inc"
            module.IncModule = importlib.import_module(name)
        except ImportError as e:
            die("Unable to import incrementalization module %s" % name)

    # Start the background statistics thread:
    RootLock.acquire()
    stat_th = threading.Thread(target=collect_statistics,
                               name="Stat Thread")
    stat_th.daemon = True
    stat_th.start()

    niters = options.iterations
    stats = {'sent' : 0, 'usrtime': 0, 'systime' : 0, 'time' : 0,
              'units' : 0, 'mem' : 0}
    # Start main program
    sys.argv = [target] + options.args
    try:
        for i in range(0, niters):
            log.info("Running iteration %d ..." % (i+1))

            walltime_start = time.perf_counter()
            module.main()

            print("Waiting for remaining child processes to terminate..."
                  "(Press \"Ctrl-C\" to force kill)")

            for p in ProcessIds:
                p.join()
            walltime = time.perf_counter() - walltime_start

            log_performance_statistics(walltime)
            r = aggregate_statistics()
            for k, v in r.items():
                stats[k] += v

        for k in stats:
            stats[k] /= niters

        perffd = None
        if options.perffile is not None:
            perffd = open(options.perffile, "w")
        if perffd is not None:
            print_simple_statistics(perffd)
            perffd.close()

        dumpfd = None
        if options.dumpfile is not None:
            dumpfd = open(options.dumpfile, "wb")
        if dumpfd is not None:
            store_statistics(stats, dumpfd)
            dumpfd.close()

    except KeyboardInterrupt as e:
        log.info("Received keyboard interrupt.")
    except Exception as e:
        err_info = sys.exc_info()
        log.error("Caught unexpected global exception: %r", e)
        traceback.print_tb(err_info[2])

    for p in ProcessIds:      # Make sure we kill all sub procs
        try:
            if p.is_live():
                p.terminate()
        except Exception:
            pass

    log.info("Terminating...")

@api
def createprocs(pcls, power, args=None):
    if not issubclass(pcls, DistProcess):
        log.error("Can not create non-DistProcess.")
        return set()

    global RootProcess
    if RootProcess is None:
        if type(EndPointType) == type:
            RootProcess = EndPointType()
            RootProcess.shared = multiprocessing.Value("i", 0)
            RootLock.release()
        else:
            log.error("EndPoint not defined")
            return
    log.debug("RootProcess is %s" % str(RootProcess))

    log.info("Creating instances of %s..", pcls.__name__)
    pipes = []
    iterator = []
    if isinstance(power, int):
        iterator = range(power)
    elif isinstance(power, set):
        iterator = power
    else:
        log.error("Unrecognised parameter %r", n)
        return set()

    procs = set()
    for i in iterator:
        (childp, ownp) = multiprocessing.Pipe()
        p = pcls(RootProcess, childp, EndPointType)
        if isinstance(i, str):
            p.set_name(i)
        # Buffer the pipe
        pipes.append((i, childp, ownp, p))
        # We need to start proc right away to obtain EndPoint and pid for p:
        p.start()
        ProcessIds.append(p)
        procs.add(p)

    log.info("%d instances of %s created.", len(procs), pcls.__name__)
    result = dict()
    for i, childp, ownp, p in pipes:
        childp.close()
        cid = ownp.recv()
        cid._initpipe = ownp    # Tuck the pipe here
        cid._proc = p           # Set the process object
        result[i] = cid

    if (args != None):
        setupprocs(result, args)

    if isinstance(power, int):
        return set(result.values())
    else:
        return result

@deprecated
def createnamedprocs(pcls, names, args=None):
    if not issubclass(pcls, DistProcess):
        log.error("Can not create non-DistProcess.")
        return set()

    global RootProcess
    if RootProcess == None:
        if type(EndPointType) == type:
            RootProcess = EndPointType()
            RootLock.release()
        else:
            sys.stderr.write("Error: EndPoint not defined.\n")
    log.debug("RootProcess is %s" % str(RootProcess))

    log.info("Creating procs %s.." % pcls.__name__)
    pipes = []
    for n in names:
        (childp, ownp) = multiprocessing.Pipe()
        p = pcls(RootProcess, childp, EndPointType, log)
        p.set_name(n)
        pipes.append((n, childp, ownp))      # Buffer the pipe
        p.start()               # We need to start proc right away to obtain
                                # EndPoint and pid for p
        ProcessIds.append(p)

    log.info("%d instances of %s created."%(len(names), pcls.__name__))
    result = dict()
    for name, childp, ownp in pipes:
        childp.close()
        cid = ownp.recv()
        cid._initpipe = ownp    # Tuck the pipe here
        result[name] = cid
    if (args != None):
        setupprocs(result.values(), args)

    return result

@api
def setupprocs(pids, args):
    if isinstance(pids, dict):
        pset = pids.values()
    else:
        pset = pids

    for p in pset:
        p._initpipe.send(("setup", args))

@api
def startprocs(procs):
    if isinstance(procs, dict):
        ps = procs.values()
    else:
        ps = procs

    init_performance_counters(ps)
    log.info("Starting procs...")
    for p in ps:
        p._initpipe.send("start")
        del p._initpipe

def collect_statistics():
    global PerformanceCounters
    global CounterLock

    completed = 0
    try:
        RootLock.acquire()
        for mesg in RootProcess.recvmesgs():
            src, tstamp, tup = mesg
            event_type, count = tup

            CounterLock.acquire()
            if PerformanceCounters.get(src) is not None:
                if PerformanceCounters[src].get(event_type) is None:
                    PerformanceCounters[src][event_type] = count
                else:
                    PerformanceCounters[src][event_type] += count

                # if event_type == 'totaltime':
                #     completed += 1
                #     if TotalUnits != None and completed == TotalUnits:
                #         raise KeyboardInterrupt()

            else:
                log.debug("Unknown proc: " + str(src))
            CounterLock.release()

    except KeyboardInterrupt:
        pass

    except Exception as e:
        err_info = sys.exc_info()
        log.debug("Caught unexpected global exception: %r", e)
        traceback.print_tb(err_info[2])

def config_print_individual_proc_stats(p):
    global PrintProcStats
    PrintProcStats = p

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

def store_statistics(stats, fd):
    import pickle

    pickle.dump(stats, fd)

def config_total_units(num):
    global TotalUnits
    TotalUnits = num

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


if __name__ == "__main__":
    @api
    def testapi(a : int, b : list) -> dict:
        print (a, b)
        return []

    testapi(1, [2])
    testapi(1, {})
    print(api_registry)
