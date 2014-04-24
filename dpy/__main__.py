import sys

from .api import entrypoint

def parseArgs(argv):
    import optparse
    p = optparse.OptionParser()

    p.add_option("-s", action="store", dest='perffile')
    p.add_option("--dumpfile", action="store", dest='dumpfile')
    p.add_option("-U", action="store", dest='dumpfile')
    p.add_option("-i", action="store", dest='iterations')
    p.add_option("--nolog", action="store_true", dest="nolog")
    p.add_option("--logfile", action="store", dest="logfile")
    p.add_option("--logdir", action="store", dest="logdir")
    p.add_option("--logconsolelevel", action="store", dest="logconsolelevel")
    p.add_option("--logfilelevel", action="store", dest="logfilelevel")

    p.set_defaults(perffile=None,
                   dumpfile=None,
                   numprocs="1",
                   iterations="1",
                   other=None,
                   nolog=False,
                   logfile=None,
                   logdir=None,
                   logconsolelevel="INFO",
                   logfilelevel="DEBUG")

    return p.parse_args(argv)


def cut_cmdline():
    for i, a in enumerate(sys.argv):
        if a.endswith(".dpy") or a.endswith(".da"):
            return (sys.argv[1:i+1], sys.argv[i:])
    die("No DistAlgo source file specified.")

def libmain():
    """
    Main program entry point. Parses command line options, sets up global
    variables, and calls the 'main' function of the DistAlgo program.
    """
    libcmdl, distcmdl = cut_cmdline()

    cmdline_options, args = parseArgs(libcmdl)

    entrypoint(cmdline_options, args, distcmdl)

def die(mesg = None):
    if mesg != None:
        sys.stderr.write(mesg + "\n")
    sys.exit(1)

if __name__ == '__main__':
    libmain()
