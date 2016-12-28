import os
import distutils
import setuptools
import subprocess
import setuptools.command.build_py
import setuptools.command.sdist
from distutils.core import setup
from distutils.cmd import Command

import da

class CompileDACommand(Command):
  """A custom command to run `dac` on all .da files under da/."""

  description = 'build all DistAlgo modules under da/.'
  user_options = [
      # The format is (long option, short option, description).
      ('compiler-flags', 'F', 'options for the compiler'),
  ]

  def initialize_options(self):
    """Set default values for options."""
    # Each user option must be listed here with their default value.
    self.compiler_flags = []

  def finalize_options(self):
    """Post-process options."""
    pass

  def run(self):
    """Run command."""
    import da.compiler
    exroot = os.path.join(os.getcwd(), 'da/')
    for root, dirs, files in os.walk(exroot):
        for filename in files:
            if filename.endswith('.da'):
                target = os.path.join(root, filename)
                self.announce('Compiling {}...'.format(target),
                              level=distutils.log.INFO)
                da.compiler.ui.main(self.compiler_flags + [str(target)])

class CompileDocCommand(Command):
    """A custom command to run `pdflatex` on 'doc/language.tex'."""

    description = "generate 'doc/language.pdf' from 'doc/language.tex'."
    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        rootdir = os.getcwd()
        os.chdir('./doc')
        command = ['pdflatex', 'language.tex']
        self.announce('Running command: {}'.format(command))
        subprocess.check_call(command)
        os.chdir(rootdir)

class DABuildCommand(setuptools.command.build_py.build_py):
    """Auto build all examples before packaging."""

    def run(self):
        self.run_command('compile_modules')
        super().run()

class DASdistCommand(setuptools.command.sdist.sdist):
    """Generate doc/language.pdf before packaging."""

    def run(self):
        self.run_command('compile_modules')
        self.run_command('genpdf')
        super().run()

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
          'Programming Language :: Python :: 3.6',
          'Topic :: Software Development :: Compilers',
      ],
      packages = setuptools.find_packages(),
      include_package_data = True,
      cmdclass = {
          'genpdf'          : CompileDocCommand,
          'compile_modules' : CompileDACommand,
          'build_py'        : DABuildCommand,
          'sdist'           : DASdistCommand,
      }
)
