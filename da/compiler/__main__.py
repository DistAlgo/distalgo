# Main module entry point
import sys
from . import ui

if hasattr(sys, '_real_argv'):
    sys.argv[0] = sys._real_argv

if __name__ == '__main__':
    ui.main()
