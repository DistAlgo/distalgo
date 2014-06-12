import multiprocessing
import time
import sys
import types
import traceback
import os
import stat
import signal
import time
import logging
import threading
import warnings
import os.path
import imp

from logging import DEBUG
from logging import INFO
from logging import ERROR
from logging import CRITICAL
from logging import FATAL
from functools import wraps
from inspect import signature, Parameter

import dpy.compiler
from dpy.sim import UdpEndPoint, TcpEndPoint, DistProcess

log = logging.getLogger(__name__)
formatter = logging.Formatter(
    '[%(asctime)s]%(name)s:%(levelname)s: %(message)s')
log._formatter = formatter

api_registry = dict()

def api(func):
    """Declare 'func' as DistPy API.

    This wraps the function to perform basic type checking for type-annotated
    parameters and return value.
    """

    global api_registry
    funame = func.__name__
    if api_registry.get(funame) is not None:
        raise RuntimeError("Double definition of API function: %s" % funame)

    sig = signature(func)

    @wraps(func)
    def _func_impl(*args, **kwargs):
        try:
            binding = sig.bind(*args, **kwargs)
        except TypeError as e:
            log.error(str(e))
            return None
        for argname in binding.arguments:
            atype = sig.parameters[argname].annotation
            if (atype is not Parameter.empty and
                    not isinstance(binding.arguments[argname], atype)):
                log.error(
                    ("'%s' called with wrong type argument: "
                     "%s, expected %s, got %s.") %
                    (funame, argname, str(atype),
                     str(binding.arguments[argname].__class__)))
                return None
        result = func(*args, **kwargs)
        if (sig.return_annotation is not Parameter.empty and
                not isinstance(result, sig.return_annotation)):
            log.warn(
                ("Possible bug: API function '%s' return value type mismatch: "
                 "declared %s, returned %s.") %
                (funame, sig.return_annotation, result.__class__))
        return result

    api_registry[funame] = _func_impl
    return _func_impl

class Null(object):
    def __init__(self, *args, **kwargs): pass
    def __call__(self, *args, **kwargs): return self
    def __getattribute__(self, attr): return self
    def __setattr__(self, attr, value): pass
    def __delattr__(self, attr): pass

def deprecated(func):
    """This is a decorator which can be used to mark functions
    as deprecated. It will result in a warning being emmitted
    when the function is used."""
    def newFunc(*args, **kwargs):
        warnings.warn("Call to deprecated function %s." % func.__name__,
                      category=DeprecationWarning)
        return func(*args, **kwargs)
    newFunc.__name__ = func.__name__
    newFunc.__doc__ = func.__doc__
    newFunc.__dict__.update(func.__dict__)
    return newFunc

PerformanceCounters = {}
CounterLock = threading.Lock()
RootProcess = None
RootLock = threading.Lock()
EndPointType = UdpEndPoint
PrintProcStats = False
TotalUnits = None
ProcessIds = []
log = logging.getLogger("runtime")

def dist_source(dir, filename):
    doti = filename.rfind(".")
    purename = filename[0:doti]
    distsource = filename
    pysource = purename + ".py"

    distsrc = os.path.join(dir, distsource)
    pysrc = os.path.join(dir, pysource)

    if filename.endswith(".dpy") or filename.endswith(".da"):
        distmode, pymode, codeobj = None, None, None
        try:
            distmode = os.stat(distsrc)
        except OSError:
            die("DistAlgo source not found.")
        try:
            pymode = os.stat(pysrc)
        except OSError:
            pymode = None
        if pymode == None or pymode[stat.ST_MTIME] < distmode[stat.ST_MTIME]:
            dpy.compiler.dpyfile_to_pyfile(filename)
    file, pathname, desc = imp.find_module(purename, [dir])
    return (purename, imp.load_module(purename, file, pathname, desc))

def dist_import(filename):
    dir = os.path.dirname(filename)
    base = os.path.basename(filename)
    _, mod = dist_source(dir, base)
    return mod

def maximum(iterable):
    if (len(iterable) == 0): return -1
    else: return max(iterable)

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

def create_root_logger(options, logfile, logdir):
    global log

    if not options.nolog:
        log.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            '[%(asctime)s]%(name)s:%(levelname)s: %(message)s')
        log._formatter = formatter

        ch = logging.StreamHandler()
        ch.setFormatter(formatter)

        try:
            ch.setLevel(options.logconsolelevel)
            log._consolelvl = options.logconsolelevel
        except ValueError:
            sys.stderr.write("Unknown logging level %s. Defaulting to INFO.\n" %
                             options.loglevel)
            ch.setLevel(logging.INFO)
            log._consolelvl = logging.INFO

        fh = logging.FileHandler(logfile)
        fh.setFormatter(formatter)

        try:
            fh.setLevel(options.logfilelevel)
            log._filelvl = options.logfilelevel
        except ValueError:
            sys.stderr.write("Unknown logging level %s. Defaulting to INFO.\n" %
                             options.loglevel)
            fh.setLevel(logging.INFO)
            log._filelvl = logging.INFO

        if logdir is not None:
            os.makedirs(logdir, exist_ok=True)
            log._logdir = logdir
        else:
            log._logdir = None

            #log.addHandler(fh)
        log.addHandler(ch)

    else:
        log = Null()

def entrypoint(options, args, cmdl):
    target = args[0]
    source_dir = os.path.dirname(target)
    basename = os.path.basename(target)

    if not os.access(target, os.R_OK):
        die("Can not access source file %s" % target)

    name, module = dist_source(source_dir, basename)
    create_root_logger(options,
                       os.path.join(source_dir, name + ".log"),
                       os.path.join(source_dir, options.logdir)
                       if options.logdir is not None else None)


    sys.argv = cmdl

    # Start the background statistics thread:
    RootLock.acquire()
    stat_th = threading.Thread(target=collect_statistics,
                               name="Stat Thread")
    stat_th.daemon = True
    stat_th.start()

    niters = int(options.iterations)
    stats = {'sent' : 0, 'usrtime': 0, 'systime' : 0, 'time' : 0,
              'units' : 0, 'mem' : 0}

    if not (hasattr(module, 'main')
            and isinstance(module.main, types.FunctionType)):
        die("'main' function not defined!")

    # Start main program
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
        p = pcls(RootProcess, childp, EndPointType,
                 (log._formatter, log._consolelvl, log._filelvl, log._logdir ))
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

def setupprocs(pids, args):
    if isinstance(pids, dict):
        pset = pids.values()
    else:
        pset = pids

    for p in pset:
        p._initpipe.send(("setup", args))

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
