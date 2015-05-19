import sys
import os.path
import json
from collections import namedtuple
from itertools import chain
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

def avg(iterable):
    return sum(iterable) / len(iterable)

# ==================================================
class DataSet:
    def __init__(self, target, args=None):
        self.target = target
        self.args = args
    @property
    def data_file(self):
        return self.target.replace('/', '_')
    @property
    def run_profile(self):
        return [self.target] + list(self.args)
    def __iter__(self):
        arglist = self.run_profile
        vaidx = vararg = None
        for idx, arg in enumerate(arglist):
            if not isinstance(arg, str) and hasattr(arg, '__iter__'):
                vaidx = idx
                vararg = arg
                break
        assert vararg is not None
        for val in vararg:
            arglist[vaidx] = str(val)
            yield val, list(arglist)

class CompilerDataSet(DataSet):
    @property
    def run_profile(self):
        return ['dac', '-B', self.target]
    @property
    def data_file(self):
        return 'dac'
    def __iter__(self):
        arglist = self.run_profile
        vaidx = vararg = None
        for idx, arg in enumerate(arglist):
            if not isinstance(arg, str) and hasattr(arg, '__iter__'):
                vaidx = idx
                vararg = arg
                break
        assert vararg is not None
        for i, val in enumerate(vararg):
            arglist[vaidx] = str(val)
            yield i, list(arglist)
class CompilerIncDataSet(CompilerDataSet):
    @property
    def run_profile(self):
        return ['dac', '-B', '-i', self.target]
    @property
    def data_file(self):
        return 'dac_i'

class DADataSet(DataSet):
    def __init__(self, program, inc_module, args):
        super().__init__(target=program, args=args)
        self.inc_module = inc_module
    @property
    def run_profile(self):
        prof = ['dar', '-i']
        if self.inc_module is not None:
            prof += ['-m', self.inc_module]
        prof.append(self.target)
        prof += self.args
        return prof

class DALoopDataSet(DataSet):
    @property
    def run_profile(self):
        return ['dar', self.target] + list(self.args)

class CDataSet(DataSet): pass

class PyDataSet(DataSet):
    @property
    def run_profile(self):
        return ['python3', self.target] + list(self.args)

class ErlDataSet(DataSet):
    @property
    def run_profile(self):
        path_components = self.target.split(os.sep)
        mod = path_components[-1]
        indir = os.sep.join(path_components[:-1])
        return ['erl', '-noshell', '-pa', indir, '-run', mod, 'start'] + \
            list(self.args)

# ==================================================
class GraphLine(namedtuple("_GraphLine",
                           "dataset, key, aggregate, avg_over_procs, \
                           avg_factor, \
                           label, fit_degree,          \
                           linestyle, color, marker, markersize, \
                           markeredgecolor, markeredgewidth, markerfacecolor")):
    def __new__(cls, dataset, key, aggregate=avg, avg_over_procs=False,
                avg_factor=1,
                label='No Legend', fit_degree=1,
                linestyle='-', color=None, marker=None, markersize=9,
                markeredgecolor=None, markeredgewidth=0, markerfacecolor=None):
        return super().__new__(cls, dataset, key, aggregate, avg_over_procs,
                               avg_factor,
                               label, fit_degree,
                               linestyle, color, marker, markersize,
                               markeredgecolor, markeredgewidth, markerfacecolor)

    @property
    def __dict__(self):
        return super().__dict__

    non_visual_properties = {'dataset', 'key', 'aggregate',
                             'avg_over_procs', 'avg_factor', 'fit_degree'}
    @property
    def line_properties(self):
        res = super().__dict__
        for prop in GraphLine.non_visual_properties:
            res.pop(prop)
        return res

class GraphBar(namedtuple("_GraphBar",
                           "dataset, key, aggregate, \
                           avg_factor, bottom, \
                           label, color         \
                           width, offset")):
    def __new__(cls, dataset, key, aggregate=min,
                avg_factor=1, bottom=None,
                label='No Legend', color=None,
                width=0.5, offset=0):
        return super().__new__(cls, dataset, key, aggregate,
                               avg_factor, bottom,
                               label, color, width, offset)

    @property
    def __dict__(self):
        return super().__dict__

    non_visual_properties = {'dataset', 'key', 'aggregate', 'avg_factor',
                             'bottom', 'offset'}
    @property
    def bar_properties(self):
        res = super().__dict__
        for prop in GraphBar.non_visual_properties:
            res.pop(prop)
        return res

class GraphInfo(namedtuple("_GraphInfo",
                           "title, lines, bars, bars2, \
                           xlabel, xlim, xticks, xticklabels, \
                           xticklabel_rotation, \
                           ylabel, ylim,  yticks, \
                           ylabel2, ylim2, yticks2, \
                           legend_position, show_grid")):
    def __new__(cls, title, lines=(), bars=(), bars2=(),
                xlabel='', xlim=(None, None), xticks=None, xticklabels=None,
                xticklabel_rotation=45,
                ylabel='', ylim=(0, None), yticks=None,
                ylabel2='', ylim2=(0, None), yticks2=None,
                legend_position="upper left", show_grid=False):
        return super().__new__(cls, title, lines, bars, bars2,
                               xlabel, xlim, xticks, xticklabels,
                               xticklabel_rotation,
                               ylabel, ylim, yticks,
                               ylabel2, ylim2, yticks2,
                               legend_position, show_grid)

DataDir = "results/"
CompileTargets = [
    ("../examples/2pcommit/orig.da" , '2P Commit'),
    ("../examples/clpaxos/spec.da"  , 'Byz Paxos'),
    ("../examples/crleader/orig.da" , 'CR Leader'),
    ("../examples/dscrash/orig.da"  , 'DS Crash'),
    ("../examples/hsleader/orig.da" , 'HS Leader'),
    ("../examples/lamutex/orig.da"  , 'LA Mutex'),
    ("../examples/lapaxos/orig.da"  , 'LA Paxos'),
    ("../examples/raft/orig.da"     , 'Raft'),
    ("../examples/ramutex/orig.da"  , 'RA Mutex'),
    ("../examples/ratoken/spec.da"  , 'RA Token'),
    ("../examples/sktoken/orig.da"  , 'SK Token'),
    ("../examples/vrpaxos/orig.da"  , 'VR Paxos'),
]
Arg_lamutex_vary_rounds = ('5', range(100, 1000+1, 100))
Arg_lamutex_vary_rounds_low = ('5', range(10, 100+1, 10))
Arg_lamutex_vary_procs_low = (range(5, 20+1, 2), '5')
Arg_lamutex_vary_procs_high = (range(15, 150+1, 20), '5')
Arg_lamutex_vary_procs_oopsla = (range(25, 150+1, 25), '5')
Arg_lamutex_vary_procs_erlang = (range(25, 150+1, 25), '30')
Arg_clpaxos_vary_procs_oopsla = (10, range(25, 150+1, 25))
Arg_lamutex_vary_procs_all = ([15, 25, 35, 50, 55, 75, 95, 100, 115, 125,
                               135, 150], '5')
Arg_tpcommit_vary_procs_low = (range(5, 20+1, 2), '0')
DataSet_compile = CompilerDataSet(
    target=[fn for fn, _ in CompileTargets])
DataSet_compile_inc = CompilerIncDataSet(
    target=[fn for fn, _ in CompileTargets])
DataSet_lamutex_orig_vary_rounds = DADataSet(
    program="lamutex/orig.da",
    inc_module=None,
    args=Arg_lamutex_vary_rounds)
DataSet_lamutex_orig_inc_vary_rounds = DADataSet(
    program="lamutex/orig.da",
    inc_module="lamutex_orig_inc_inc",
    args=Arg_lamutex_vary_rounds)
DataSet_lamutex_orig_invts_vary_rounds = DADataSet(
    program="lamutex/orig.da",
    inc_module="orig_inc_invts",
    args=Arg_lamutex_vary_rounds)
DataSet_lamutex_orig_loop_vary_rounds = DALoopDataSet(
    target="lamutex/orig2.da",
    args=Arg_lamutex_vary_rounds)
DataSet_lamutex_orig_vary_procs = DADataSet(
    program="lamutex/orig.da",
    inc_module=None,
    args=Arg_lamutex_vary_procs_oopsla)
DataSet_lamutex_orig_inc_vary_procs = DADataSet(
    program="lamutex/orig.da",
    inc_module="lamutex_orig_inc_inc",
    args=Arg_lamutex_vary_procs_oopsla)
DataSet_lamutex_orig_invts_vary_procs = DADataSet(
    program="lamutex/orig.da",
    inc_module="orig_inc_invts",
    args=Arg_lamutex_vary_procs_oopsla)
DataSet_lamutex_orig_loop_vary_procs = DALoopDataSet(
    target="lamutex/orig2.da",
    args=Arg_lamutex_vary_procs_oopsla)
# ==================================================
DataSet_lamutex_C_vary_procs = CDataSet(
    target="lamutex/C/lamport",
    args=Arg_lamutex_vary_procs_erlang)
DataSet_lamutex_C_vary_rounds = CDataSet(
    target="lamutex/C/lamport",
    args=Arg_lamutex_vary_rounds)
# ==================================================
DataSet_lamutex_erlang_vary_procs = ErlDataSet(
    target="lamutex/Erlang/lamutex",
    args=Arg_lamutex_vary_procs_erlang)
DataSet_lamutex_erlang_vary_rounds = ErlDataSet(
    target="lamutex/Erlang/lamutex",
    args=Arg_lamutex_vary_rounds)
# ==================================================
DataSet_lamutex_python_vary_rounds_low = PyDataSet(
    target="lamutex/Python/lamutex.py",
    args=Arg_lamutex_vary_rounds_low)
DataSet_lamutex_python_vary_rounds = PyDataSet(
    target="lamutex/Python/lamutex.py",
    args=Arg_lamutex_vary_rounds)
DataSet_lamutex_python_vary_procs_low = PyDataSet(
    target="lamutex/Python/lamutex.py",
    args=Arg_lamutex_vary_procs_low)
DataSet_lamutex_python_vary_procs_high = PyDataSet(
    target="lamutex/Python/lamutex.py",
    args=Arg_lamutex_vary_procs_high)
DataSet_lamutex_python_vary_procs_oopsla = PyDataSet(
    target="lamutex/Python/lamutex.py",
    args=Arg_lamutex_vary_procs_oopsla)
# ==================================================
DataSet_lamutex_spec_inc_vary_rounds_low = DADataSet(
    program="lamutex/spec.da",
    inc_module="spec_inc_inc",
    args=Arg_lamutex_vary_rounds_low)
DataSet_lamutex_spec_inc_vary_rounds = DADataSet(
    program="lamutex/spec.da",
    inc_module="spec_inc_inc",
    args=Arg_lamutex_vary_rounds)
DataSet_lamutex_spec_vary_rounds_low = DADataSet(
    program="lamutex/spec.da",
    inc_module=None,
    args=Arg_lamutex_vary_rounds_low)
DataSet_lamutex_spec_vary_procs_low = DADataSet(
    program="lamutex/spec.da",
    inc_module=None,
    args=Arg_lamutex_vary_procs_low)
DataSet_lamutex_spec_inc_vary_procs_low = DADataSet(
    program="lamutex/spec.da",
    inc_module="spec_inc_inc",
    args=Arg_lamutex_vary_procs_low)
DataSet_lamutex_spec_vary_procs_high = DADataSet(
    program="lamutex/spec.da",
    inc_module=None,
    args=Arg_lamutex_vary_procs_high)
DataSet_lamutex_spec_inc_vary_procs_high = DADataSet(
    program="lamutex/spec.da",
    inc_module="spec_inc_inc",
    args=Arg_lamutex_vary_procs_high)
DataSet_lamutex_spec_inc_vary_procs_oopsla = DADataSet(
    program="lamutex/spec.da",
    inc_module="spec_inc_inc",
    args=Arg_lamutex_vary_procs_all)
DataSet_clpaxos_spec_vary_procs_low = DADataSet(
    program="clpaxos/spec.da",
    inc_module=None,
    args=Arg_lamutex_vary_procs_low)
DataSet_clpaxos_oopsla_vary_procs = DADataSet(
    program="clpaxos/oopsla.da",
    inc_module=None,
    args=('10', [25, 50, 75]))
DataSet_clpaxos_oopsla_inc_vary_procs = DADataSet(
    program="clpaxos/oopsla.da",
    inc_module="oopsla_inc_inc",
    args=('10', [25, 50, 75]))
DataSet_clpaxos_spec_inc_vary_procs_low = DADataSet(
    program="clpaxos/spec.da",
    inc_module="clpaxos_inc_inc",
    args=Arg_lamutex_vary_procs_low)
DataSet_tpcommit_spec_vary_procs_low = DADataSet(
    program="2pcommit/spec.da",
    inc_module=None,
    args=Arg_tpcommit_vary_procs_low)
DataSet_tpcommit_spec_inc_vary_procs_low = DADataSet(
    program="2pcommit/spec.da",
    inc_module="tpcommit_inc_inc",
    args=Arg_tpcommit_vary_procs_low)

Graph_lamutex_orig_running_time_vary_rounds = \
    GraphInfo(
        title="La Mutex CPU time fix 5 procs vary rounds",
        xlabel="Number of times entered CS",
        ylabel="CPU time (in seconds)",
        ylim=(0, 10),
        lines=(
            GraphLine(
                dataset=DataSet_lamutex_orig_vary_rounds,
                key='Total_process_time',
                linestyle='-',
                color='y',
                marker='s',
                fit_degree=2,
                label='DistAlgo original',
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_orig_loop_vary_rounds,
                key='Total_process_time',
                linestyle='-.',
                color='k',
                marker='*',
                fit_degree=2,
                label='DistAlgo loop',
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_orig_inc_vary_rounds,
                key='Total_process_time',
                linestyle='--',
                color='m',
                marker='p',
                label="DistAlgo IncOQ",
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_orig_invts_vary_rounds,
                key='Total_process_time',
                linestyle='-.',
                color='y',
                marker='x',
                markeredgewidth=1,
                label="DistAlgo InvTS",
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_python_vary_rounds,
                key='Total_process_time',
                linestyle='-',
                color='c',
                marker='^',
                label='Python',
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_erlang_vary_rounds,
                key='Total_process_time',
                linestyle='--',
                color='b',
                marker='D',
                label='Erlang',
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_C_vary_rounds,
                key='Total_process_time',
                linestyle='-',
                color='r',
                marker='*',
                label="C",
                avg_over_procs=True)
        ))
Graph_lamutex_orig_running_time_vary_procs = \
    GraphInfo(
        title="Lamutex_orig running time vary procs",
        xlabel="Number of processes",
        ylabel="CPU time (in seconds)",
        xlim=(25, 150),
        ylim=(0.0, 0.35),
        xticks=np.arange(25, 150+1, 25),
        lines=(
            GraphLine(
                dataset=DataSet_lamutex_orig_vary_procs,
                key='Total_process_time',
                linestyle='-',
                color='y',
                marker='x',
                markeredgewidth=1,
                fit_degree=2,
                avg_factor=5,
                label='DistAlgo original',
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_orig_loop_vary_procs,
                key='Total_process_time',
                linestyle='-.',
                color='k',
                marker='*',
                markeredgewidth=1,
                fit_degree=2,
                avg_factor=5,
                label='DistAlgo loop',
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_orig_inc_vary_procs,
                key='Total_process_time',
                linestyle='--',
                color='m',
                marker='p',
                avg_factor=5,
                label="DistAlgo IncOQ",
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_orig_invts_vary_procs,
                key='Total_process_time',
                linestyle='-',
                color='g',
                marker='^',
                avg_factor=5,
                label="DistAlgo InvTS",
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_python_vary_procs_oopsla,
                key='Total_process_time',
                linestyle='--',
                color='c',
                marker='D',
                avg_factor=5,
                label='Python',
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_erlang_vary_procs,
                key='Total_process_time',
                linestyle='--',
                color='b',
                marker='s',
                avg_factor=30,
                label='Erlang',
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_C_vary_procs,
                key='Total_process_time',
                linestyle='-.',
                color='r',
                marker='*',
                avg_factor=30,
                label="C",
                avg_over_procs=True)
        ))
Graph_lamutex_orig_memory_vary_procs = \
    GraphInfo(
        title="Lamutex_orig memory vary procs",
        xlabel="Number of processes",
        ylabel="Avg. Process Peak RSS (in kB)",
        xlim=(25, 150),
        # ylim=(0, 2200000),
        # yticks=np.arange(0, 2200001, 220000*2),
        xticks=np.arange(25, 150+1, 25),
        legend_position='outside',
        lines=(
            GraphLine(
                dataset=DataSet_lamutex_orig_vary_procs,
                key='Total_memory',
                linestyle='-',
                color='y',
                marker='s',
                label='DistAlgo\n(original)',
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_orig_loop_vary_procs,
                key='Total_memory',
                linestyle='-.',
                color='k',
                marker='o',
                label='DistAlgo\n(loop)',
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_python_vary_procs_oopsla,
                key='Total_memory',
                linestyle='-',
                color='c',
                marker='s',
                label='Python',
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_orig_inc_vary_procs,
                key='Total_memory',
                linestyle='--',
                color='m',
                marker='p',
                label="IncOQ",
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_orig_invts_vary_procs,
                key='Total_memory',
                linestyle='--',
                color='r',
                marker='x',
                markeredgewidth=1,
                label="InvTS",
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_erlang_vary_procs,
                key='Total_memory',
                linestyle='-',
                color='b',
                marker='s',
                label='Erlang',
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_C_vary_procs,
                key='Total_memory',
                linestyle='-',
                color='r',
                marker='*',
                label="C",
                avg_over_procs=True)
        ))
Graph_lamutex_orig_memory_vary_rounds = \
    GraphInfo(
        title="Lamutex memory fix 5 procs vary rounds",
        xlabel="Number of times entered cs",
        ylabel="Avg. Process Peak RSS (in kB)",
        legend_position='outside',
        lines=(
            GraphLine(
                dataset=DataSet_lamutex_orig_vary_rounds,
                key='Total_memory',
                linestyle='-',
                color='y',
                markeredgewidth=1,
                marker='x',
                label='DistAlgo\n(original)',
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_orig_loop_vary_rounds,
                key='Total_memory',
                linestyle='-.',
                color='k',
                markeredgewidth=1,
                marker='*',
                label='DistAlgo\n(loop)',
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_orig_inc_vary_rounds,
                key='Total_memory',
                linestyle='--',
                color='m',
                marker='p',
                label="IncOQ",
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_orig_invts_vary_rounds,
                key='Total_memory',
                linestyle='--',
                color='g',
                marker='s',
                label="InvTS",
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_python_vary_rounds,
                key='Total_memory',
                linestyle='-',
                color='c',
                marker='^',
                label='Python',
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_erlang_vary_rounds,
                key='Total_memory',
                linestyle='--',
                color='b',
                marker='D',
                label='Erlang',
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_C_vary_rounds,
                key='Total_memory',
                linestyle='-',
                color='r',
                marker='*',
                label="C",
                avg_over_procs=True)
        ))
Graph_clpaxos_spec_running_time_vary_processes_low = \
    GraphInfo(
        title="Clpaxos_spec running time vary processes (low)",
        xlabel="Number of processes",
        ylabel="Running time (in seconds)",
        lines=(
            GraphLine(
                dataset=DataSet_clpaxos_spec_vary_procs_low,
                key='Wallclock_time',
                linestyle='-',
                color='b',
                marker='o',
                label='original (wall-clock time)',
                fit_degree=2,
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_clpaxos_spec_inc_vary_procs_low,
                key='Wallclock_time',
                linestyle='--',
                color='g',
                marker='v',
                label="incremental (wall-clock time)",
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_clpaxos_spec_vary_procs_low,
                key='Total_process_time',
                linestyle='-',
                color='y',
                marker='s',
                fit_degree=2,
                label='original (total process time)',
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_clpaxos_spec_inc_vary_procs_low,
                key='Total_process_time',
                linestyle='--',
                color='m',
                marker='p',
                label="incremental (total process time)",
                avg_over_procs=False)))
Graph_clpaxos_oopsla_running_time_vary_processes = \
    GraphInfo(
        title="Clpaxos_oopsla running time vary processes",
        xlabel="Number of processes",
        ylabel="Running time (in seconds)",
        lines=(
            # GraphLine(
            #     dataset=DataSet_clpaxos_oopsla_vary_procs,
            #     key='Wallclock_time',
            #     linestyle='-',
            #     color='b',
            #     marker='o',
            #     label='original (wall-clock time)',
            #     fit_degree=2,
            #     avg_over_procs=False),
            # GraphLine(
            #     dataset=DataSet_clpaxos_oopsla_inc_vary_procs,
            #     key='Wallclock_time',
            #     linestyle='--',
            #     color='g',
            #     marker='v',
            #     label="incremental (wall-clock time)",
            #     avg_over_procs=False),
            GraphLine(
                dataset=DataSet_clpaxos_oopsla_vary_procs,
                key='Total_process_time',
                linestyle='-',
                color='y',
                marker='s',
                fit_degree=2,
                label='original (total process time)',
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_clpaxos_oopsla_inc_vary_procs,
                key='Total_process_time',
                linestyle='--',
                color='m',
                marker='p',
                label="incremental (total process time)",
                avg_over_procs=True)))
Graph_tpcommit_running_time_vary_processes_low = \
    GraphInfo(
        title="2pcommit running time vary processes (low)",
        xlabel="Number of processes",
        ylabel="Running time (in seconds)",
        lines=(
            GraphLine(
                dataset=DataSet_tpcommit_spec_vary_procs_low,
                key='Wallclock_time',
                aggregate=min,
                linestyle='-',
                color='b',
                marker='o',
                label='original (wall-clock time)',
                fit_degree=2,
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_tpcommit_spec_inc_vary_procs_low,
                key='Wallclock_time',
                aggregate=min,
                linestyle='--',
                color='g',
                marker='v',
                label="incremental (wall-clock time)",
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_tpcommit_spec_vary_procs_low,
                key='Total_process_time',
                linestyle='-',
                color='y',
                marker='s',
                fit_degree=2,
                label='original (total process time)',
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_tpcommit_spec_inc_vary_procs_low,
                key='Total_process_time',
                linestyle='--',
                color='m',
                marker='p',
                label="incremental (total process time)",
                avg_over_procs=False)))
Graph_tpcommit_memory_vary_processes_low = \
    GraphInfo(
        title="2pcommit memory vary processes (low)",
        xlabel="Number of processes",
        ylabel="Total memory (in bytes)",
        lines=(
            GraphLine(
                dataset=DataSet_tpcommit_spec_vary_procs_low,
                key='Total_memory',
                linestyle='-',
                color='b',
                marker='o',
                label='original (total memory)',
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_tpcommit_spec_inc_vary_procs_low,
                key='Total_memory',
                linestyle='--',
                color='g',
                marker='v',
                label="incremental (total memory)",
                avg_over_procs=False)))
Graph_lamutex_spec_running_time_vary_processes_low = \
    GraphInfo(
        title="Lamutex_spec running time vary processes (low)",
        xlabel="Number of processes",
        ylabel="Running time (in seconds)",
        lines=(
            GraphLine(
                dataset=DataSet_lamutex_spec_vary_procs_low,
                key='Wallclock_time',
                linestyle='-',
                color='b',
                marker='o',
                label='original (wall-clock time)',
                fit_degree=2,
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_spec_inc_vary_procs_low,
                key='Wallclock_time',
                linestyle='--',
                color='g',
                marker='v',
                fit_degree=2,
                label="incremental (wall-clock time)",
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_spec_vary_procs_low,
                key='Total_process_time',
                linestyle='-',
                color='y',
                marker='s',
                fit_degree=2,
                label='original (total process time)',
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_spec_inc_vary_procs_low,
                key='Total_process_time',
                linestyle='--',
                color='m',
                marker='p',
                fit_degree=2,
                label="incremental (total process time)",
                avg_over_procs=False)))
Graph_lamutex_spec_running_time_vary_processes_high = \
    GraphInfo(
        title="Lamutex_spec running time vary processes (high)",
        xlabel="Number of processes",
        ylabel="Running time (in seconds)",
        lines=(
            GraphLine(
                dataset=DataSet_lamutex_spec_vary_procs_high,
                key='Wallclock_time',
                linestyle='-',
                color='b',
                marker='o',
                label='original (wall-clock time)',
                fit_degree=2,
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_spec_inc_vary_procs_high,
                key='Wallclock_time',
                linestyle='--',
                color='g',
                marker='v',
                fit_degree=1,
                label="incremental (wall-clock time)",
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_spec_vary_procs_high,
                key='Total_process_time',
                linestyle='-',
                color='y',
                marker='s',
                fit_degree=4,
                label='original (total process time)',
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_spec_inc_vary_procs_high,
                key='Total_process_time',
                linestyle='--',
                color='m',
                marker='p',
                fit_degree=1,
                label="incremental (total process time)",
                avg_over_procs=False)))
Graph_lamutex_spec_memory_vary_processes_low = \
    GraphInfo(
        title="Lamutex_spec memory vary processes (low)",
        xlabel="Number of processes",
        ylabel="Total memory (in bytes)",
        lines=(
            GraphLine(
                dataset=DataSet_lamutex_spec_vary_procs_low,
                key='Total_memory',
                linestyle='-',
                color='b',
                marker='o',
                label='original (total memory)',
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_spec_inc_vary_procs_low,
                key='Total_memory',
                linestyle='--',
                color='g',
                marker='v',
                label="incremental (total memory)",
                avg_over_procs=False)))
Graph_lamutex_spec_memory_vary_processes_high = \
    GraphInfo(
        title="Lamutex_spec memory vary processes (high)",
        xlabel="Number of processes",
        ylabel="Total memory (in bytes)",
        lines=(
            GraphLine(
                dataset=DataSet_lamutex_spec_vary_procs_high,
                key='Total_memory',
                linestyle='-',
                color='b',
                marker='o',
                label='original (total memory)',
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_spec_inc_vary_procs_high,
                key='Total_memory',
                linestyle='--',
                color='g',
                marker='v',
                label="incremental (total memory)",
                avg_over_procs=False)))
Graph_clpaxos_spec_memory_vary_processes_low = \
    GraphInfo(
        title="Clpaxos_spec memory vary processes (low)",
        xlabel="Number of processes",
        ylabel="Total memory (in bytes)",
        lines=(
            GraphLine(
                dataset=DataSet_clpaxos_spec_vary_procs_low,
                key='Total_memory',
                linestyle='-',
                color='b',
                marker='o',
                label='original (total memory)',
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_clpaxos_spec_inc_vary_procs_low,
                key='Total_memory',
                linestyle='--',
                color='g',
                marker='v',
                label="incremental (total memory)",
                avg_over_procs=False)))
Graph_clpaxos_oopsla_memory_vary_processes = \
    GraphInfo(
        title="Clpaxos_oopsla memory vary processes (low)",
        xlabel="Number of processes",
        ylabel="Total memory (in bytes)",
        lines=(
            GraphLine(
                dataset=DataSet_clpaxos_oopsla_vary_procs,
                key='Total_memory',
                linestyle='-',
                color='b',
                marker='o',
                label='original (total memory)',
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_clpaxos_oopsla_inc_vary_procs,
                key='Total_memory',
                linestyle='--',
                color='g',
                marker='v',
                label="incremental (total memory)",
                avg_over_procs=True)))
Graph_lamutex_spec_vs_python_running_time_vary_processes_low = \
    GraphInfo(
        title="Lamutex spec vs Python running time vary processes (low)",
        xlabel="Number of processes",
        ylabel="Running time (in seconds)",
        lines=(
            GraphLine(
                dataset=DataSet_lamutex_python_vary_procs_low,
                key='Wallclock_time',
                linestyle='-',
                color='b',
                marker='o',
                label='Python (wall-clock time)',
                fit_degree=2,
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_spec_inc_vary_procs_low,
                key='Wallclock_time',
                linestyle='--',
                color='g',
                marker='v',
                fit_degree=4,
                label="incremental (wall-clock time)",
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_python_vary_procs_low,
                key='Total_process_time',
                linestyle='-',
                color='y',
                marker='s',
                fit_degree=1,
                label='Python (average process time)',
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_spec_inc_vary_procs_low,
                key='Total_process_time',
                linestyle='--',
                color='m',
                marker='p',
                fit_degree=1,
                label="incremental (average process time)",
                avg_over_procs=True)))
Graph_lamutex_spec_vs_python_running_time_vary_rounds = \
    GraphInfo(
        title="Lamutex spec vs Python running time vary rounds",
        xlabel="Number of rounds",
        ylabel="Running time (in seconds)",
        lines=(
            GraphLine(
                dataset=DataSet_lamutex_python_vary_rounds,
                key='Wallclock_time',
                linestyle='-',
                color='b',
                marker='o',
                label='Python (wall-clock time)',
                fit_degree=1,
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_spec_inc_vary_rounds,
                key='Wallclock_time',
                linestyle='--',
                color='g',
                marker='v',
                fit_degree=2,
                label="incremental (wall-clock time)",
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_python_vary_rounds,
                key='Total_process_time',
                linestyle='-',
                color='y',
                marker='s',
                fit_degree=1,
                label='Python (average process time)',
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_spec_inc_vary_rounds,
                key='Total_process_time',
                linestyle='--',
                color='m',
                marker='p',
                fit_degree=2,
                label="incremental (average process time)",
                avg_over_procs=True)))
Graph_lamutex_spec_vs_python_memory_vary_processes_oopsla = \
    GraphInfo(
        title="Lamutex spec vs Python memory vary processes",
        xlabel="Number of processes",
        ylabel="Total memory (in bytes)",
        lines=(
            GraphLine(
                dataset=DataSet_lamutex_python_vary_procs_oopsla,
                key='Total_memory',
                linestyle='-',
                color='b',
                marker='o',
                label='Python (total memory)',
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_spec_inc_vary_procs_oopsla,
                key='Total_memory',
                linestyle='--',
                color='g',
                marker='v',
                label="incremental (total memory)",
                avg_over_procs=False)))
Graph_lamutex_spec_vs_python_running_time_vary_processes_oopsla = \
    GraphInfo(
        title="Lamutex spec vs Python running time vary processes (oopsla)",
        xlabel="Number of processes",
        ylabel="Running time (in seconds)",
        lines=(
            # GraphLine(
            #     dataset=DataSet_lamutex_python_vary_procs_oopsla,
            #     key='Wallclock_time',
            #     linestyle='-',
            #     color='b',
            #     marker='o',
            #     label='Python (wall-clock time)',
            #     fit_degree=2,
            #     avg_over_procs=False),
            # GraphLine(
            #     dataset=DataSet_lamutex_spec_inc_vary_procs_oopsla,
            #     key='Wallclock_time',
            #     linestyle='--',
            #     color='g',
            #     marker='v',
            #     fit_degree=2,
            #     label="incremental (wall-clock time)",
            #     avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_python_vary_procs_oopsla,
                key='Total_process_time',
                linestyle='-',
                color='y',
                marker='x',
                markeredgewidth=1,
                fit_degree=1,
                label='Python (average process time)',
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_spec_inc_vary_procs_oopsla,
                key='Total_process_time',
                linestyle='--',
                color='m',
                marker='*',
                markeredgewidth=1,
                markerfacecolor='none',
                markeredgecolor='m',
                fit_degree=1,
                label="incremental (average process time)",
                avg_over_procs=True)))
BarWidth=0.35
Graph_compile = \
    GraphInfo(
        title="Compilation times",
        xlabel='',
        xticks=np.arange(len(CompileTargets))+BarWidth,
        xticklabels=[label for _, label in CompileTargets],
        show_grid=True,
        ylabel='Compilation time (in seconds)',
        ylim=(0, 0.06),
        yticks=np.arange(0, 0.061, 0.06/10),
        bars=(
            GraphBar(
                dataset=DataSet_compile,
                key='Wallclock_time',
                aggregate=min,
                width=BarWidth,
                color='y',
                label='Compilation time'),),
        ylabel2='I/O (in bytes)',
        ylim2=(0, 30000+1),
        yticks2=np.arange(0, 30000+1, 30000/10),
        bars2=(
            GraphBar(
                dataset=DataSet_compile,
                key='Input_size',
                aggregate=min,
                offset=BarWidth,
                width=BarWidth,
                color='g',
                label='Input size'),
            GraphBar(
                dataset=DataSet_compile,
                key='Output_size',
                aggregate=min,
                offset=BarWidth,
                width=BarWidth,
                color='r',
                bottom=GraphBar(
                    dataset=DataSet_compile,
                    key='Input_size',
                    aggregate=min),
                label='Output size')
        ))
Graph_compile_inc = \
    GraphInfo(
        title="Compilation times (with inc)",
        show_grid=True,
        xlabel='',
        xticks=np.arange(len(CompileTargets))+BarWidth,
        xticklabels=[label for _, label in CompileTargets],
        ylabel='Compilation time (in seconds)',
        ylim=(0, 0.12),
        yticks=np.arange(0, 0.121, 0.12/10),
        bars=(
            GraphBar(
                dataset=DataSet_compile_inc,
                key='Wallclock_time',
                aggregate=min,
                width=BarWidth,
                color='y',
                label='Compilation time'),),
        ylabel2='I/O Volume (in bytes)',
        ylim2=(0, 50000+1),
        yticks2=np.arange(0, 50000+1, 5000),
        bars2=(
            GraphBar(
                dataset=DataSet_compile_inc,
                key='Input_size',
                aggregate=min,
                offset=BarWidth,
                width=BarWidth,
                color='g',
                label='Input size'),
            GraphBar(
                dataset=DataSet_compile_inc,
                key='Output_size',
                aggregate=min,
                offset=BarWidth,
                width=BarWidth,
                color='r',
                bottom=GraphBar(
                    dataset=DataSet_compile_inc,
                    key='Input_size',
                    aggregate=min),
                label='Output size')
        ))

def load_data(fromfile):
    realfile = os.path.join(DataDir, fromfile + ".json")
    with open(realfile, "r") as infd:
        results = json.load(infd)
        assert results is not None and isinstance(results, list)
        return results

def load_bardata(graphbar):
    datafile = graphbar.dataset.data_file
    results = load_data(datafile)
    xset = []
    yset = []
    for xaxis, datapoint in graphbar.dataset:
        datas = [data[graphbar.key]
                 for config, data, ts in results if config == datapoint]
        if len(datas) == 0:
            print("No data for ", graphbar.key, datapoint)
        xset.append(xaxis + graphbar.offset)
        yset.append(graphbar.aggregate(datas))
    return xset, yset

def load_graphbar(graphbar, ax=plt):
    """Plot a bar on the graph, return its handle."""
    xset, yset = load_bardata(graphbar)
    if graphbar.bottom is not None:
        _, ybot = load_bardata(graphbar.bottom)
        return ax.bar(xset, yset, bottom=ybot, **graphbar.bar_properties)
    else:
        return ax.bar(xset, yset, **graphbar.bar_properties)

def load_graphline(graphline, ax=plt):
    """Plot a line on the graph, return its handle."""
    datafile = graphline.dataset.data_file
    results = load_data(datafile)
    xset = []
    yset = []
    for xaxis, datapoint in graphline.dataset:
        datas = [(data[graphline.key] if not graphline.avg_over_procs
                  else data[graphline.key] / data['Total_processes'])
                 / graphline.avg_factor
                 for config, data, ts in results if config == datapoint]
        if len(datas) == 0:
            print("No data for ", graphline.key, datapoint)
        xset.append(xaxis)
        yset.append(graphline.aggregate(datas))
    if graphline.fit_degree is not None:
        pol = np.polyfit(xset, yset, graphline.fit_degree)
        sample_xs = np.linspace(xset[0], xset[-1], len(xset) * 10)
        fitline_args = graphline.line_properties
        datapoint_args = graphline.line_properties
        fitline_args.pop('marker')
        datapoint_args.pop('linestyle')
        datapoint_args.pop('label')
        ax.plot(sample_xs, np.polyval(pol, sample_xs), **fitline_args)
        ax.plot(xset, yset, label='_nolegend_', linestyle="", **datapoint_args)
        return Line2D([0, 1], [0, 1], **graphline.line_properties)
    else:
        return ax.plot(xset, yset, **graphline.line_properties)

def plot_graph(graph):
    plt.clf()
    plt.title(graph.title)
    plt.xlabel(graph.xlabel)
    plt.ylabel(graph.ylabel)
    ax = plt.subplot(111)
    ax2 = None
    handles = []
    handles += [load_graphline(line) for line in graph.lines]
    handles += [load_graphbar(bar, ax) for bar in graph.bars]
    if len(graph.bars2) > 0:
        ax2 = ax.twinx()
        ax2.set_ylabel(graph.ylabel2)
        ax2.set_ylim(*graph.ylim2)
        if graph.yticks2 is not None:
            ax2.set_yticks(graph.yticks2)
        handles += [load_graphbar(bar, ax2) for bar in graph.bars2]
    if graph.xticklabels is not None:
        box = ax.get_position()
        newpos = [box.x0, box.y0 + box.height * 0.05,
                  box.width, box.height * 0.95]
        ax.set_position(newpos)
        if ax2 is not None:
            ax2.set_position(newpos)
        ax.set_xticklabels(graph.xticklabels,
                           fontsize=9,
                           rotation=graph.xticklabel_rotation,
                           ha='right')
    if graph.legend_position == "outside":
        box = ax.get_position()
        newpos = [box.x0, box.y0, box.width * 0.8, box.height]
        ax.set_position(newpos)
        if ax2 is not None:
            ax2.set_position(newpos)
        legend_params = {'frameon': True,
                         'shadow': True,
                         'fancybox': True,
                         'loc' : 'center left',
                         'bbox_to_anchor' : (1.0, 0.5)}
    else:
        legend_params = {'frameon' : False,
                         'loc' : graph.legend_position}
    legend_params['numpoints'] = 1
    ax.legend(handles=handles, **legend_params)
    ax.set_ylim(*graph.ylim)
    ax.set_xlim(*graph.xlim)
    if graph.yticks is not None:
        ax.set_yticks(graph.yticks)
    if graph.xticks is not None:
        ax.set_xticks(graph.xticks)
    if graph.show_grid:
        plt.grid()

def show_graph(graph):
    plot_graph(graph)
    plt.show()

def main():
    output_dir = sys.argv[1] if len(sys.argv) > 1 else "graphs"
    if not os.path.exists(output_dir):
        os.mkdir(output_dir)
    if not os.path.isdir(output_dir):
        sys.stderr.write("Error: %s is not a directory!" % output_dir)
        exit(1)
    for graph in [value for name, value in globals().items()
                  if isinstance(value, GraphInfo) and
                  name.startswith("Graph")]:
        print("Plotting %s..." % graph.title)
        plot_graph(graph)
        plt.savefig(os.path.join(output_dir,
                                 graph.title.replace(' ', '_') + ".png"))

if __name__ == "__main__":
    main()
