#!/usr/bin/env python3

import sys
import runpy
import os.path as path

binpath = sys.path[0]
rootpath = path.dirname(path.abspath(binpath))
sys.path.insert(0, rootpath)
sys.path.insert(0, ".")

del binpath
del rootpath
del path

sys._real_argv = sys.argv[0]
runpy.run_module("da",
                 run_name="__main__", alter_sys=True)
