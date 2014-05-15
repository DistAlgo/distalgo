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
