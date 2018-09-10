import os
import os.path
import sys
import subprocess
import setuptools
import distutils.command.build
import setuptools.command.build_py
import setuptools.command.install_lib
import setuptools.command.sdist
from setuptools import setup
from distutils import log
from distutils.cmd import Command
from distutils.dep_util import newer

import da

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

class DABuildCommand(distutils.command.build.build):
    """build everything needed to install."""

    sub_commands = [('build_doc', None)] + \
                   distutils.command.build.build.sub_commands

# auxiliary function adapted from `distutils.util.byte_compile`:
def _byte_compile(files, optimize=-1, force=False, prefix=None,
                  base_dir=None, dry_run=False):
    from da.compiler import dafile_to_pycfile
    from da.importer import da_cache_from_source

    # XXX: do we need "indirect" mode??
    for file in files:
        if file[-3:] != ".da":
            continue
        if optimize >= 0:
            opt = '' if optimize == 0 else optimize
            cfile = da_cache_from_source(file, optimization=opt)
        else:
            cfile = da_cache_from_source(file)
        dfile = file
        if prefix:
            if file[:len(prefix)] != prefix:
                raise ValueError("invalid prefix: filename {} doesn't start with {}".format(file, prefix))
            dfile = dfile[len(prefix):]
        if base_dir:
            dfile = os.path.join(base_dir, dfile)
        if force or newer(file, cfile):
            log.info("byte-compiling {} to {}".format(file, cfile))
            if not dry_run:
                dafile_to_pycfile(file, outname=cfile, optimize=optimize,
                                  dfile=dfile)
        else:
            log.debug("skipping byte-compilation of {} to {}."
                      .format(file, cfile))

class DABuildPyCommand(setuptools.command.build_py.build_py):
    """Auto build all examples before packaging."""

    def byte_compile(self, files):
        super().byte_compile(files)
        if sys.dont_write_bytecode:
            self.warn('byte-compiling is disabled, skipping.')
            return

        prefix = self.build_lib
        if prefix[-1] != os.sep:
            prefix = prefix + os.sep
        if self.compile:
            _byte_compile(files, optimize=0, force=self.force,
                          prefix=prefix, dry_run=self.dry_run)
        if self.optimize:
            _byte_compile(files, optimize=self.optimize, force=self.force,
                          prefix=prefix, dry_run=self.dry_run)

class DAInstallCommand(setuptools.command.install_lib.install_lib):
    """Install all modules."""

    def byte_compile(self, files):
        super().byte_compile(files)
        if sys.dont_write_bytecode:
            self.warn('byte-compiling is disabled, skipping.')
            return

        install_root = self.get_finalized_command('install').root
        if self.compile:
            _byte_compile(files, optimize=0,
                          force=self.force, prefix=install_root,
                          dry_run=self.dry_run)
        if self.optimize > 0:
            _byte_compile(files, optimize=self.optimize,
                          force=self.force, prefix=install_root,
                          verbose=self.verbose, dry_run=self.dry_run)

class DASdistCommand(setuptools.command.sdist.sdist):
    """Generate doc/language.pdf before packaging."""

    sub_commands = [('build_doc', None)] + \
                   setuptools.command.sdist.sdist.sub_commands

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
          'Programming Language :: Python :: 3.5',
          'Programming Language :: Python :: 3.6',
          'Programming Language :: Python :: 3.7',
          'Topic :: Software Development :: Compilers',
      ],

      packages = [
          'da',
          'da.compiler',
          'da.examples',
          'da.importer',
          'da.lib',
          'da.tools',
          'da.transport',
      ],
      include_package_data = True,
      package_data = {
        'da.examples' : ['**/*.da']
      },

      cmdclass = {
          'build_doc'   : CompileDocCommand,
          'build'       : DABuildCommand,
          'build_py'    : DABuildPyCommand,
          'install_lib' : DAInstallCommand,
          'sdist'       : DASdistCommand,
      }
)
