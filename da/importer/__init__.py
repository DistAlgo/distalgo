# Copyright (c) 2010-2017 Bo Lin
# Copyright (c) 2010-2017 Yanhong Annie Liu
# Copyright (c) 2010-2017 Stony Brook University
# Copyright (c) 2010-2017 The Research Foundation of SUNY
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

import sys

PythonVersion = sys.version_info
if PythonVersion < (3, 7):
    from . import py36 as real_lib
elif PythonVersion.major == 3 and PythonVersion.minor == 7:
    from . import py37 as real_lib
else:
    raise RuntimeError("Python version {} is not supported."
                       .format(PythonVersion))

da_cache_from_source = real_lib.da_cache_from_source

__all__ = ["da_cache_from_source"]

# Inject the DistAlgo module loader:
real_lib._install()
