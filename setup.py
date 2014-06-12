from distutils.core import setup

import dpy

setup(name = "DistAlgo",
      version = dpy.__version__,
      author= "bolin",
      author_email = "bolin@cs.stonybrook.edu",
      packages = ['dpy', 'dpy.compiler'])
