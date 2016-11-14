import setuptools
from distutils.core import setup

import da

setup(name = "pyDistAlgo",
      version = da.__version__,
      url = "https://github.com/DistAlgo/distalgo",
      description = "A high-level language for distributed algorithms.",
      author = "bolin",
      author_email = "bolin@cs.stonybrook.edu",
      classifiers = [
          'Development Status :: 4 - Beta',
          'Intended Audience :: Developers',
          'Programming Language :: Python :: 3',
          'Programming Language :: Python :: 3.4',
          'Programming Language :: Python :: 3.5',
          'Topic :: Software Development :: Compilers',
      ],
      packages = setuptools.find_packages(),
      include_package_data = True
)
