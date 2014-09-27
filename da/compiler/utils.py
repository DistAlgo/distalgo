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

import sys

DB_ERROR = 0
DB_WARN = 1
DB_INFO = 2
DB_DEBUG =3
Debug = DB_INFO

def set_debug_level(level):
    global Debug
    if is_valid_debug_level(level):
        Debug = level

def is_valid_debug_level(level):
    return type(level) is int and DB_ERROR <= level and level <= DB_DEBUG

# Common utility functions

def printe(mesg, lineno=0, col_offset=0, filename="", outfd=sys.stderr):
    if Debug >= DB_ERROR:
        fs = "%s:%d:%d: error: %s"
        print(fs % (filename, lineno, col_offset, mesg), file=outfd)

def printw(mesg, lineno=0, col_offset=0, filename="", outfd=sys.stderr):
    if Debug >= DB_WARN:
        fs = "%s:%d:%d: warning: %s"
        print(fs % (filename, lineno, col_offset, mesg), file=outfd)

def printd(mesg, lineno=0, col_offset=0, filename="", outfd=sys.stderr):
    if Debug >= DB_DEBUG:
        fs = "%s:%d:%d: DEBUG: %s"
        print(fs % (filename, lineno, col_offset, mesg), file=outfd)

def printi(mesg, lineno=0, col_offset=0, filename="", outfd=sys.stdout):
    if Debug >= DB_INFO:
        fs = "%s:%d:%d: %s"
        print(fs % (filename, lineno, col_offset, mesg), file=outfd)
