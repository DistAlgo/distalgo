from distutils.core import setup

import da

setup(name = "DistAlgo",
      version = da.__version__,
      author= "bolin",
      author_email = "bolin@cs.stonybrook.edu",
      packages = ['da', 'da.compiler'])
