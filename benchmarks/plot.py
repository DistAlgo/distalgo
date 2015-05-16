import sys
import os.path
import json
from collections import namedtuple
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

def avg(iterable):
    return sum(iterable) / len(iterable)

# ==================================================
class DataSet:
    def __init__(self, target, args):
        self.target = target
        self.args = args
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
                           label, curve_degree,          \
                           linestyle, color, marker, markersize, \
                           markeredgecolor, markeredgewidth, markerfacecolor")):
    def __new__(cls, dataset, key, aggregate=avg, avg_over_procs=False,
                avg_factor=1,
                label='No Legend', curve_degree=1,
                linestyle='-', color=None, marker=None, markersize=9,
                markeredgecolor=None, markeredgewidth=0, markerfacecolor=None):
        return super().__new__(cls, dataset, key, aggregate, avg_over_procs,
                               avg_factor,
                               label, curve_degree,
                               linestyle, color, marker, markersize,
                               markeredgecolor, markeredgewidth, markerfacecolor)

    @property
    def __dict__(self):
        return super().__dict__

    non_visual_properties = {'dataset', 'key', 'aggregate',
                             'avg_over_procs', 'avg_factor', 'curve_degree'}
    @property
    def line_properties(self):
        res = super().__dict__
        for prop in GraphLine.non_visual_properties:
            res.pop(prop)
        return res

class GraphInfo(namedtuple("_GraphInfo",
                           "title, xlabel, ylabel, lines, xlim, xticks")):
    def __new__(cls, title, xlabel, ylabel, lines,
                xlim=(None, None), xticks=(None, None, None)):
        return super().__new__(cls, title, xlabel, ylabel, lines,
                               xlim, xticks)

DataDir = "results/"
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
DataSet_lamutex_orig_vary_rounds = DADataSet(
    program="lamutex/orig.da",
    inc_module=None,
    args=Arg_lamutex_vary_rounds)
DataSet_lamutex_orig_inc_vary_rounds = DADataSet(
    program="lamutex/orig.da",
    inc_module="lamutex_orig_inc_inc",
    args=Arg_lamutex_vary_rounds)
DataSet_lamutex_orig_vary_procs = DADataSet(
    program="lamutex/orig.da",
    inc_module=None,
    args=Arg_lamutex_vary_procs_oopsla)
DataSet_lamutex_orig_inc_vary_procs = DADataSet(
    program="lamutex/orig.da",
    inc_module="lamutex_orig_inc_inc",
    args=Arg_lamutex_vary_procs_oopsla)
# ==================================================
DataSet_lamutex_C_vary_procs = CDataSet(
    target="lamutex/C/lamport",
    args=Arg_lamutex_vary_procs_erlang)
# ==================================================
DataSet_lamutex_erlang_vary_procs = ErlDataSet(
    target="lamutex/Erlang/lamutex",
    args=Arg_lamutex_vary_procs_erlang)
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
        title="Lamutex_orig running time vary rounds",
        xlabel="Number of rounds",
        ylabel="Running time (in seconds)",
        lines=(
            GraphLine(
                dataset=DataSet_lamutex_orig_vary_rounds,
                key='Wallclock_time',
                aggregate=min,
                linestyle='-',
                color='b',
                marker='o',
                label='original (wall-clock time)',
                curve_degree=2,
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_orig_inc_vary_rounds,
                key='Wallclock_time',
                aggregate=min,
                linestyle='--',
                color='g',
                marker='v',
                label="incremental (wall-clock time)",
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_orig_vary_rounds,
                key='Total_process_time',
                linestyle='-',
                color='y',
                marker='s',
                curve_degree=2,
                label='original (total process time)',
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_orig_inc_vary_rounds,
                key='Total_process_time',
                linestyle='--',
                color='m',
                marker='p',
                label="incremental (total process time)",
                avg_over_procs=False)))
Graph_lamutex_orig_running_time_vary_procs = \
    GraphInfo(
        title="Lamutex_orig running time vary procs",
        xlabel="Number of processes",
        ylabel="Running time (in seconds)",
        xlim=(25, 150),
        xticks=(25, 150+1, 25),
        lines=(
            # GraphLine(
            #     dataset=DataSet_lamutex_orig_vary_procs,
            #     key='Total_process_time',
            #     linestyle='-',
            #     color='y',
            #     marker='s',
            #     curve_degree=2,
            #     avg_factor=5,
            #     label='original (average process time)',
            #     avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_erlang_vary_procs,
                key='Total_process_time',
                linestyle='-',
                color='k',
                marker='s',
                avg_factor=30,
                label='Erlang (average process time)',
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_python_vary_procs_oopsla,
                key='Total_process_time',
                linestyle='--',
                color='c',
                marker='s',
                avg_factor=5,
                label='Python (average process time)',
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_orig_inc_vary_procs,
                key='Total_process_time',
                linestyle='--',
                color='m',
                marker='p',
                avg_factor=5,
                label="incremental (average process time)",
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_C_vary_procs,
                key='Total_process_time',
                linestyle='-',
                color='r',
                marker='*',
                avg_factor=30,
                label="C (average process time)",
                avg_over_procs=True)))
Graph_lamutex_orig_memory_vary_procs = \
    GraphInfo(
        title="Lamutex_orig memory vary procs",
        xlabel="Number of processes",
        ylabel="Peak RSS (in kB)",
        lines=(
            GraphLine(
                dataset=DataSet_lamutex_orig_vary_procs,
                key='Total_memory',
                linestyle='-',
                color='y',
                marker='s',
                label='original (Peak Memory)',
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_erlang_vary_procs,
                key='Total_memory',
                linestyle='-',
                color='k',
                marker='s',
                label='Erlang (Peak Memory)',
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_python_vary_procs_oopsla,
                key='Total_memory',
                linestyle='--',
                color='c',
                marker='s',
                label='Python (Peak Memory)',
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_orig_inc_vary_procs,
                key='Total_memory',
                linestyle='--',
                color='m',
                marker='p',
                label="incremental (Peak Memory)",
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_C_vary_procs,
                key='Total_memory',
                linestyle='-',
                color='r',
                marker='*',
                label="C (Peak Memory)",
                avg_over_procs=False)))
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
                curve_degree=2,
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
                curve_degree=2,
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
            #     curve_degree=2,
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
                curve_degree=2,
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
                curve_degree=2,
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
                curve_degree=2,
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
                curve_degree=2,
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_spec_inc_vary_procs_low,
                key='Wallclock_time',
                linestyle='--',
                color='g',
                marker='v',
                curve_degree=2,
                label="incremental (wall-clock time)",
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_spec_vary_procs_low,
                key='Total_process_time',
                linestyle='-',
                color='y',
                marker='s',
                curve_degree=2,
                label='original (total process time)',
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_spec_inc_vary_procs_low,
                key='Total_process_time',
                linestyle='--',
                color='m',
                marker='p',
                curve_degree=2,
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
                curve_degree=2,
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_spec_inc_vary_procs_high,
                key='Wallclock_time',
                linestyle='--',
                color='g',
                marker='v',
                curve_degree=1,
                label="incremental (wall-clock time)",
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_spec_vary_procs_high,
                key='Total_process_time',
                linestyle='-',
                color='y',
                marker='s',
                curve_degree=4,
                label='original (total process time)',
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_spec_inc_vary_procs_high,
                key='Total_process_time',
                linestyle='--',
                color='m',
                marker='p',
                curve_degree=1,
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
                curve_degree=2,
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_spec_inc_vary_procs_low,
                key='Wallclock_time',
                linestyle='--',
                color='g',
                marker='v',
                curve_degree=4,
                label="incremental (wall-clock time)",
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_python_vary_procs_low,
                key='Total_process_time',
                linestyle='-',
                color='y',
                marker='s',
                curve_degree=1,
                label='Python (average process time)',
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_spec_inc_vary_procs_low,
                key='Total_process_time',
                linestyle='--',
                color='m',
                marker='p',
                curve_degree=1,
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
                curve_degree=1,
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_spec_inc_vary_rounds,
                key='Wallclock_time',
                linestyle='--',
                color='g',
                marker='v',
                curve_degree=2,
                label="incremental (wall-clock time)",
                avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_python_vary_rounds,
                key='Total_process_time',
                linestyle='-',
                color='y',
                marker='s',
                curve_degree=1,
                label='Python (average process time)',
                avg_over_procs=True),
            GraphLine(
                dataset=DataSet_lamutex_spec_inc_vary_rounds,
                key='Total_process_time',
                linestyle='--',
                color='m',
                marker='p',
                curve_degree=2,
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
            #     curve_degree=2,
            #     avg_over_procs=False),
            # GraphLine(
            #     dataset=DataSet_lamutex_spec_inc_vary_procs_oopsla,
            #     key='Wallclock_time',
            #     linestyle='--',
            #     color='g',
            #     marker='v',
            #     curve_degree=2,
            #     label="incremental (wall-clock time)",
            #     avg_over_procs=False),
            GraphLine(
                dataset=DataSet_lamutex_python_vary_procs_oopsla,
                key='Total_process_time',
                linestyle='-',
                color='y',
                marker='x',
                markeredgewidth=1,
                curve_degree=1,
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
                curve_degree=1,
                label="incremental (average process time)",
                avg_over_procs=True)))

def load_data(fromfile):
    realfile = os.path.join(DataDir, fromfile + ".json")
    with open(realfile, "r") as infd:
        results = json.load(infd)
        assert results is not None and isinstance(results, list)
        return results

def load_graphline(graphline):
    """Plot a line on the graph, return its handle."""
    datafile = graphline.dataset.target.replace('/', '_')
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
    if graphline.curve_degree is not None:
        pol = np.polyfit(xset, yset, graphline.curve_degree)
        sample_xs = np.linspace(xset[0], xset[-1], len(xset) * 10)
        fitline_args = graphline.line_properties
        datapoint_args = graphline.line_properties
        fitline_args.pop('marker')
        datapoint_args.pop('linestyle')
        datapoint_args.pop('label')
        plt.plot(sample_xs, np.polyval(pol, sample_xs), **fitline_args)
        plt.plot(xset, yset, label='_nolegend_', linestyle="", **datapoint_args)
        return Line2D([0, 1], [0, 1], **graphline.line_properties)
    else:
        return plt.plot(xset, yset, **graphline.line_properties)

def plot_graph(graph):
    plt.clf()
    plt.title(graph.title)
    plt.xlabel(graph.xlabel)
    plt.ylabel(graph.ylabel)
    plt.legend(handles=[load_graphline(line) for line in graph.lines],
               numpoints=1,
               frameon=False,
               loc='upper left')
    axes = plt.gca()
    axes.set_ylim(bottom=0)
    axes.set_xlim(*graph.xlim)
    axes.xaxis.set_ticks(np.arange(*graph.xticks))

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
                  if isinstance(value, GraphInfo) and name.startswith("Graph")]:
        plot_graph(graph)
        plt.savefig(os.path.join(output_dir, graph.title + ".png"))

if __name__ == "__main__":
    main()
