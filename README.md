# DistAlgo

  DistAlgo is a very high-level language for programming distributed
  algorithms. This project implements a DistAlgo compiler with Python as the
  target language. In the following text, the name 'DistAlgo' refers to the
  compiler and not the language.


# Requirements

## Python

   DistAlgo requires Python version 3.4 or higher, which can be obtained from
   http://www.python.org. This document assumes that you use the default name
   `python` for the Python executable.

   NOTE: If your system has both Python 2.x and Python 3.x installed, your
   Python executable is likely Python 2. In that case, you should replace
   `python` with `python3` (or `pythonX.Y` where 'X.Y' is the exact Python
   version you want to use) in all following command line examples. To find
   out which version of Python is installed on your system, type:

      python --version

## Operating system

   DistAlgo has been tested on GNU/Linux and Microsoft Windows. The command
   line instructions given in this document use GNU Bash syntax. If you are
   using a different shell (e.g., Windows 'cmd.exe' shell), please adjust the
   commands accordingly.


# Installation

  In all following commands, please replace `<DAROOT>` with the path of the
  DistAlgo root directory (the directory containing this file).

  Installation of DistAlgo is optional for running DistAlgo. You can install
  DistAlgo by using the Python 'distutils' module, or by adding the DistAlgo
  root directory to your `PYTHONPATH` environment variable.

## Using 'distutils'

   To see full usage description, type:

      cd <DAROOT>; python setup.py --help

The following command installs DistAlgo as system-wide package:

      cd <DAROOT>; python setup.py install

The following command installs DistAlgo for the current user:

      cd <DAROOT>; python setup.py install --user

   If you have installed DistAlgo for both system and user, the user
   installation takes precedence.

## Adding DistAlgo root to `PYTHONPATH`

   Simply add the DistAlgo root directory to your `PYTHONPATH` environment
   variable:

      export PYTHONPATH=<DAROOT>:${PYTHONPATH}

   This takes precedence over 'distutils' installations.

  After installation, the `da` module will be available for use.

## Running DistAlgo without installation

   Directory `<DAROOT>/bin` contains two scripts, `dac` and `dar`, that run
   the DistAlgo compiler and runtime, respectively. Running these scripts
   properly sets `sys.path` so no installation is needed. To avoid typing
   `<DAROOT>/bin` in running the scripts, add it to your `PATH` environment
   variable:

      export PATH=<DAROOT>/bin:${PATH}

   **NOTE**: The scripts assume your Python executable is installed to
   `/usr/bin/python3`; if that is not the case, you must modify the first line
   in the scripts to point to your Python executable.

   **For Windows only**: The Windows program loader does not recognize the
   Shebang (#!) sequence, so scripts `dac` and `dar` will not work under the
   'cmd.exe' shell. To work around this problem, batch scripts `dac.bat` and
   `dar.bat` are wrappers to launch `dac` and `dar` under Windows. For all
   following examples, substitute `dac` and `dar` with `dac.bat` and
   `dar.bat`, respectively. The batch scripts assume that your Python
   executable is `python`; if that is not the case, you must modify the batch
   files with the full path to your Python executable.


# Running DistAlgo

  The DistAlgo system consists of a compiler and a runtime. Under normal
  circumstances, you do not need to invoke the compiler directly, because the
  runtime will call the compiler if it detects that the generated Python code
  for your DistAlgo source file is missing or outdated.

  For both the compiler and runtime, use command line argument `-h` to see a
  full description of available options.

## Invoking the compiler

   If you have installed DistAlgo, run module `da.compiler`, passing a
   DistAlgo source file `<SOURCE>` as argument:

      python -m da.compiler <SOURCE>
 
   Otherwise, call the `dac` script (adding prefix `<DAROOT>/bin/` if you did
   not add `<DAROOT>/bin` to your `PATH` variable):

      dac <SOURCE>

## Invoking the runtime

   To run DistAlgo programs, run the `da` module, passing a DistAlgo source
   file as argument:

      python -m da <SOURCE>

   or call the `dar` script:

      dar <SOURCE>

### Passing command line arguments to the DistAlgo program

   Command line arguments before the `<SOURCE>` argument are passed to the
   DistAlgo runtime. Arguments after the `<SOURCE>` argument are passed to the
   DistAlgo source program in the global `sys.argv` list.

   For example, the following command passes argument `-i` to the DistAlgo
   runtime, and passes arguments `a` and `1` to source program `mutex.da`:

       dar -i mutex.da a 1

### Quitting

   If you wish to quit your program before it terminates, press `Ctrl-C`.
   Depending on the timing of this interrupt, you may see some exceptions
   being thrown. This is expected behavior; simply press `Ctrl-C` again to
   fully terminate the program.

   **For Cygwin with native Python only**: If you are running native Windows
   Python under a Cygwin terminal, `Ctrl-C` will *not* propagate to the child
   processes: only the parent process will be killed, and the children will
   continue to run in the background. You need to manually terminate the child
   processes from the Windows task manager.

## Examples

   This section assumes you have installed DistAlgo; otherwise, please replace
   `python -m da` with `dar` or with `<DAROOT>/bin/dar`.

   The following command runs the Lamport mutual exclusion example:

       python -m da <DAROOT>/examples/lamutex/orig.da

   After running the above command, you should find a file
   `<DAROOT>/examples/lamutex/orig.py` containing the generated Python code
   for `orig.da`.

   The following command runs the same program, but passes `20` to `orig.da`,
   causing the program to create 20 processes:

       python -m da <DAROOT>/examples/lamutex/orig.da 20

   The following command runs the same program, but passes `-f` to the
   runtime, causing a log file to be created for this run:

       python -m da -f <DAROOT>/examples/lamutex/orig.da

   After running the above command, you should find a file `orig.da.log` under
   the current directory.

   The following command runs the same program, but passes `-L debug` to the
   runtime, causing debugging output to be printed to the console:

       python -m da -L debug <DAROOT>/examples/lamutex/orig.da
