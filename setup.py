from distutils.core import setup

import da

setup(name = "DistAlgo",
      version = da.__version__,
      author= "bolin",
      author_email = "bolin@cs.stonybrook.edu",
      classifiers = [
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.4',
          'Topic :: Software Development :: Compilers',
      ],
      packages = ['da', 'da.compiler', 'da.tools'])
