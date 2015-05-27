import os
import re
import sys
import time
import json
import os.path
import subprocess
from collections import namedtuple

class TestProfile(namedtuple("TestProfile", 'target, args, repeat')):
    @property
    def run_profile(self):
        return [self.target] + list(self.args)

    @property
    def data_file(self):
        return self.target

    @property
    def data_key(self):
        return self.run_profile

class CompileProfile(TestProfile):
    @property
    def run_profile(self):
        return ['dac', '-B', self.target]

    @property
    def data_file(self):
        return 'dac'

class CompileIncProfile(TestProfile):
    @property
    def run_profile(self):
        return ['dac', '-B', '-i', self.target]

    @property
    def data_file(self):
        return 'dac_i'

class CProfile(TestProfile): pass

class PyProfile(TestProfile):
    @property
    def run_profile(self):
        return ['python3', self.target] + list(self.args)

class DAProfile(TestProfile):
    @property
    def run_profile(self):
        dafile, incfile = self.target
        prof = ['dar', '-i']
        if incfile is not None:
            prof += ['-m', incfile]
        prof.append(dafile)
        prof += self.args
        return prof

    @property
    def data_file(self):
        dafile, _ = self.target
        return dafile

class DALoopProfile(TestProfile):
    @property
    def run_profile(self):
        return ['dar', self.target] + list(self.args)

class ErlProfile(TestProfile):
    @property
    def run_profile(self):
        path_components = self.target.split(os.sep)
        mod = path_components[-1]
        indir = os.sep.join(path_components[:-1])
        return ['erl', '-noshell', '-pa', indir, '-run', mod, 'start'] + \
            list(self.args)

targets = [
    ErlProfile(
        target='lamutex/Erlang/lamutex',
        args=(range(25, 150+1, 25), '30'),
        repeat=10),
    ErlProfile(
        target='lamutex/Erlang/lamutex',
        args=('10', range(100, 1000+1, 100)),
        repeat=10),

    DAProfile(
        target=('lamutex/orig.da', None),
        args=(range(5, 20+1, 2), '5'),
        repeat=10),
    DAProfile(
        target=('lamutex/orig.da', "lamutex_orig_inc_inc"),
        args=(range(5, 20+1, 2), '5'),
        repeat=10),
    DAProfile(
        target=('lamutex/orig.da', None),
        args=(range(25, 150+1, 25), '5'),
        repeat=10),
    DAProfile(
        target=('lamutex/orig.da', "lamutex_orig_inc_inc"),
        args=(range(25, 150+1, 25), '5'),
        repeat=10),
    DAProfile(
        target=('lamutex/orig.da', "orig_inc_invts"),
        args=(range(25, 150+1, 25), '5'),
        repeat=10),

    DAProfile(
        target=('lamutex/orig.da', None),
        args=('10', range(100, 1000+1, 100)),
        repeat=5),
    DAProfile(
        target=('lamutex/orig.da', "lamutex_orig_inc_inc"),
        args=('10', range(100, 1000+1, 100)),
        repeat=5),
    DAProfile(
        target=('lamutex/orig.da', "orig_inc_invts"),
        args=('10', range(100, 1000+1, 100)),
        repeat=5),

    DAProfile(
        target=('lamutex/spec.da', None),
        args=(range(5, 20+1, 2), '5'),
        repeat=10),
    # --------------------------------------------------
    CProfile(
        target='lamutex/C/lamport',
        args=(range(25, 150+1, 25), '5'),
        repeat=10),
    CProfile(
        target='lamutex/C/lamport',
        args=(range(25, 150+1, 25), '30'),
        repeat=10),
    CProfile(
        target='lamutex/C/lamport',
        args=('10', range(100, 1000+1, 100)),
        repeat=10),
    PyProfile(
        target='lamutex/Python/lamutex.py',
        args=('10', range(100, 1000+1, 100)),
        repeat=5),
    PyProfile(
        target='lamutex/Python/lamutex.py',
        args=('5', range(10, 100+1, 10)),
        repeat=5),
    PyProfile(
        target='lamutex/Python/lamutex.py',
        args=(range(5, 20+1, 2), '5'),
        repeat=10),
    PyProfile(
        target='lamutex/Python/lamutex.py',
        args=(range(15, 150+1, 20), '5'),
        repeat=10),
    PyProfile(
        target='lamutex/Python/lamutex.py',
        args=(range(25, 150+1, 25), '5'),
        repeat=10),
    # ==================================================
    DALoopProfile(
        target='lamutex/orig2.da',
        args=(range(25, 150+1, 25), '5'),
        repeat=10),
    DALoopProfile(
        target='lamutex/orig2.da',
        args=('10', range(100, 1000+1, 100)),
        repeat=10),
    # --------------------------------------------------
    DAProfile(
        target=('lamutex/spec.da', "spec_inc_inc"),
        args=(range(5, 20+1, 2), '5'),
        repeat=10),
    DAProfile(
        target=('lamutex/spec.da', None),
        args=(range(15, 150+1, 20), '5'),
        repeat=10),
    DAProfile(
        target=('lamutex/spec.da', None),
        args=(range(25, 150+1, 25), '5'),
        repeat=10),
    DAProfile(
        target=('lamutex/spec.da', "spec_inc_inc"),
        args=(range(15, 150+1, 20), '5'),
        repeat=10),
    DAProfile(
        target=('lamutex/spec.da', "spec_inc_inc"),
        args=(range(25, 150+1, 25), '5'),
        repeat=10),
    DAProfile(
        target=('lamutex/spec.da', None),
        args=('5', range(10, 100+1, 10)),
        repeat=5),
    # DAProfile(
    #     target=('lamutex/spec.da', None),
    #     args=('5', range(100, 1000+1, 100)),
    #     repeat=5),
    DAProfile(
        target=('lamutex/spec.da', "spec_inc_inc"),
        args=('5', range(10, 100+1, 10)),
        repeat=5),
    DAProfile(
        target=('lamutex/spec.da', "spec_inc_inc"),
        args=('5', range(100, 1000+1, 100)),
        repeat=5),
    # ==================================================
    # DAProfile(
    #     target=('2pcommit/spec.da', None),
    #     args=(range(5, 20+1, 2), '0'),
    #     repeat=10),
    # DAProfile(
    #     target=('2pcommit/spec.da', "tpcommit_inc_inc"),
    #     args=(range(5, 20+1, 2), '0'),
    #     repeat=10),
    DAProfile(
        target=('2pcommit/spec.da', None),
        args=(range(25, 150+1, 25), '0'),
        repeat=10),
    DAProfile(
        target=('2pcommit/spec.da', "tpcommit_inc_inc"),
        args=(range(25, 150+1, 25), '0'),
        repeat=10),
    DALoopProfile(
        target='2pcommit/spec2.da',
        args=(range(25, 150+1, 25), '0'),
        repeat=10),
    # ==================================================
    DAProfile(
        target=('clpaxos/spec.da', None),
        args=(range(5, 20+1, 2), '5'),
        repeat=10),
    DAProfile(
        target=('clpaxos/spec.da', "clpaxos_inc_inc"),
        args=(range(5, 20+1, 2), '5'),
        repeat=10),
    # ==================================================
    DAProfile(
        target=('lapaxos/orig.da', None),
        args=('5', range(25, 150+1, 25)),
        repeat=10),
    DAProfile(
        target=('lapaxos/orig.da', "lapaxos_inc_inc"),
        args=('5', range(25, 150+1, 25)),
        repeat=10),
    DAProfile(
        target=('lapaxos/orig.da', "lapaxos_inc_dem"),
        args=('5', range(25, 150+1, 25)),
        repeat=10),
    DALoopProfile(
        target='lapaxos/orig2.da',
        args=('5', range(25, 150+1, 25)),
        repeat=10)
    # DAProfile(
    #     target=('clpaxos/spec.da', None),
    #     args=('10', range(25, 150+1, 25)),
    #     repeat=10),
    # DAProfile(
    #     target=('clpaxos/oopsla.da', None),
    #     args=('10', range(25, 150+1, 25)),
    #     repeat=10),
    # DAProfile(
    #     target=('clpaxos/oopsla.da', "oopsla_inc_inc"),
    #     args=('10', range(25, 150+1, 25)),
    #     repeat=10),
    # DAProfile(
    #     target=('clpaxos/spec.da', "clpaxos_inc_inc"),
    #     args=('10', range(25, 150+1, 25)),
    #     repeat=10),
    # DAProfile(
    #     target=('clpaxos/spec.da', None),
    #     args=('5', '10', range(5, 20+1, 2)),
    #     repeat=10),
    # DAProfile(
    #     target=('clpaxos/spec.da', "clpaxos_inc_inc"),
    #     args=('5', '10', range(5, 20+1, 2)),
    #     repeat=10),
]
compile_targets = [
    "../examples/vrpaxos/orig.da",
    "../examples/crleader/orig.da",
    "../examples/dscrash/orig.da",
    "../examples/dscrash/spec.da",
    "../examples/ratoken/spec.da",
    "../examples/raft/orig.da",
    "../examples/lamutex/orig.da",
    "../examples/lamutex/spec.da",
    "../examples/pingpong/ping.da",
    "../examples/hsleader/orig.da",
    "../examples/ramutex/orig.da",
    "../examples/sktoken/orig.da",
    "../examples/clpaxos/spec.da",
    "../examples/lapaxos/orig.da",
    "../examples/2pcommit/orig.da",
]
for ct in compile_targets:
    targets.append(CompileProfile(target=ct, args=None, repeat=10))
    targets.append(CompileIncProfile(target=ct, args=None, repeat=10))


class DistAlgoError(subprocess.CalledProcessError):
    
    def __str__(self):
        return ('Command {} returned non-zero exit status {}\n'
                'stderr output:\n{}'.format(
                self.cmd, self.returncode, self.output))

def parse_output(s):
    """Parse a string of standard output text for the "OUTPUT: <JSON>"
    line and return the parsed JSON object.
    """
    m = re.search(r'^###OUTPUT: (.*)', s, re.MULTILINE)
    if m is None:
        return None
    return json.loads(m.group(1))

# Adapted from IncOQ:
def launch(run_args):
    """Launch the specified run profile in a subprocess that
    captures/parses standard output and error. Return a JSON object
    obtained by parsing stdout for a line "OUTPUT: <JSON>", where
    <JSON> is JSON-encoded data.
    """
    child = subprocess.Popen(
        run_args, bufsize=-1,
        # To debug, comment out this line to make stdout/stderr
        # the same standard out and error streams as the parent.
        # Alternatively (if the process terminates), uncomment
        # the print statements below.
        # In the future, maybe use something like
        #   http://stackoverflow.com/questions/375427/non-blocking-read-on-a-subprocess-pipe-in-python
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        #cwd=dirname,
        #env=env,
        universal_newlines=True)
    
    stdout, stderr = child.communicate()
#    print(stderr)
#    print(stdout)
    if child.returncode != 0:
        raise subprocess.CalledProcessError(child.returncode, run_args, stderr)
    results = parse_output(stdout)
    return results

def run_all_tests(resultsdir):
    for config in targets:
        expanded = []
        resultsfile = os.path.join(resultsdir,
                                   config.data_file.replace('/', '_') + '.json')
        varidx = varange = None
        try:
            with open(resultsfile, "r") as rf:
                results = json.load(rf)
                if isinstance(results, list):
                    print("Found existing results.")
                else:
                    print("ERROR: results corrupted!", file=sys.stderr)
                    exit(1)
        except Exception as e:
            results = []

        # Expand arg ranges
        for idx, param in enumerate(config.run_profile):
            if isinstance(param, range):
                varidx = idx
                varange = param
                # Only support one range parameter
                break
        if varidx is not None:
            arglist = config.run_profile
            for n in varange:
                arglist[varidx] = str(n)
                expanded.append(list(arglist))
        else:
            expanded.append(config.run_profile)
        for item in expanded:
            existing = len([(config, res, ts)
                            for config, res, ts in results if config == item])
            if existing > 0:
                print("** Found %d results for %r" % (existing, item))
            for i in range(existing, config.repeat):
                print("> Running iteration %d for %r" % (i, item))
                results.append((item, launch(item), time.time()))
                # Save right away:
                with open(resultsfile, "w") as wf:
                    json.dump(results, wf)

def main():
    output_dir = sys.argv[1] if len(sys.argv) > 1 else "results"
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    if not os.path.isdir(output_dir):
        sys.stderr.write("Error: %s is not a directory!" % output_dir)
        exit(1)
    run_all_tests(output_dir)

if __name__ == "__main__":
    main()
