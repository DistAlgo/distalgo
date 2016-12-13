# DistAlgo

  DistAlgo is a very high-level language for programming distributed
  algorithms. This project implements a DistAlgo compiler with Python as the
  target language. In the following text, the name 'DistAlgo' refers to the
  compiler and not the language.


# Requirements

## Python

   DistAlgo requires Python version 3.4 or higher, which can be obtained
   from http://www.python.org. This document assumes that your installation
   uses the default name `python` for the Python executable.

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
  DistAlgo by using the Python `distutils` module, by adding the DistAlgo
  root directory to your `PYTHONPATH` environment variable, or by using the
  Python package manager, `pip`.

## Using `distutils`

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

   This takes precedence over `distutils` installations.

  After installation, the `da` module will be available for use.
  
## Using `pip` to install DistAlgo

   `pip` is a command line utility for installing Python packages from the
   Python Package Index(PyPI). `pip` is the recommended method of installing
   DistAlgo. Using `pip`, you do not need to manually download the DistAlgo
   distribution package or setup environment variables, as `pip` will manage
   all of that for you. The name of the DistAlgo package on PyPI is
   'pyDistAlgo'.
   
   To install DistAlgo as a system-wide package:
   
     pip install pyDistAlgo
     
   This command will likely require administrator privileges.
   
   To install DistAlgo for the current user only:
   
     pip install --user pyDistAlgo

   Just as with `distutils`, if you have installed DistAlgo for both system
   and the current user, the user installation will take precedence.

   To upgrade an existing DistAlgo installation:
   
     pip install --upgrade [--user] pyDistAlgo

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
   "Shebang" (#!) sequence, so scripts `dac` and `dar` will not work under
   the 'cmd.exe' shell. To work around this limitation, the '<PROJROOT>/bin'
   directory also contains `dac.bat` and `dar.bat`, which are simple batch
   script wrappers for `dac` and `dar`. To use these batch scripts under
   Windows, substitute `dac` and `dar` with `dac.bat` and `dar.bat`,
   respectively, for all following examples. The batch scripts assume your
   Python executable is `python`, if that is not the case then you must
   modify the batch files with the full path to your Python executable.

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
      
   The `--version`(or `-v`) command line option will print out the version
   number of the installed DistAlgo system. The `--help`(or `-h`) command
   line option will print out a list of all command line options along with
   a brief description of each option.

### Running a DistAlgo module as a script

   Instead of passing a path to a DistAlgo source file on the command line,
   you can use the '-m' option to run a DistAlgo module as though it were a
   script:
   
     python -m da -m <MODULE>
     
   The DistAlgo command line option '-m' mimics Python's own '-m' option.
   `<MODULE>` must be a DistAlgo module in dotted form and without the '.da'
   suffix. The source file for the module is located for using the same
   rules that govern Python's own module loading process.

### Passing command line arguments to the DistAlgo program

   Command line arguments before the `<SOURCE>` argument are passed to the
   DistAlgo runtime; arguments after the `<SOURCE>` argument are passed to
   the DistAlgo source program in the global `sys.argv` list. Alternatively,
   if you are using the '-m' option to run a DistAlgo module, command line
   arguments before the `-m <MODULE>` argument are passed to the DistAlgo
   runtime; arguments after the `-m <MODULE>` argument are passed to the
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

       python -m da -m da.examples.lamutex.orig

   The following command runs the same program, but passes `20` to `orig.da`,
   causing the program to create 20 processes:

       python -m da -m da.examples.lamutex.orig 20

   The following command runs the same program, but passes `-f` to the
   runtime, causing a log file to be created for this run:

       python -m da -f -m da.examples.lamutex.orig

   After running the above command, you should find a file `orig.da.log` under
   the current directory.

   The following command runs the same program, but passes `-L debug` to the
   runtime, causing debugging output to be printed to the console:

       python -m da -L debug -m da.examples.lamutex.orig

### Running multiple nodes

  When you start a DistAlgo program, a special DistAlgo process known as the
  "node process" is created. The node process is responsible for running the
  `main` method of the program.
  
  By default, the node process is unnamed, and as such will not be able to
  talk to other node processes, and any DistAlgo process running on an
  unnamed node will not be able to communicate with DistAlgo processes
  running on other nodes. In order to have multiple node processes that
  communicate with each other, you must give each one a unique name. A node
  name can be any string that does not include the characters '@', '#', and
  ':'. A node process can be named by using the `-n <NAME>` command line
  argument. For example, the command
  
      python -m da -n Node1 -m da.examples.lamutex.orig
      
  will start a node named 'Node1', which then runs the mutual exclusion
  example. Notice that the formatted process ids in the output of this
  command now include a "#Node1" suffix, to indicate that the processes are
  running on the 'Node1' node.
  
  Node names can be used for the `at` argument when calling the `new`
  function, which instructs the system to create the new processes on the
  named node(s) instead of locally. The following program, 'pingpong.da',
  creates a `Pong` process on the node named 'PongNode', then creates a
  `Ping` process on the local node, and finally starts both:

      class Ping(process):
          def setup(pong): pass
          def run():
              send(('Ping',), to=pong)
              await(received(('Pong',)))
              output("Ponged.")
      
      class Pong(process):
          def setup(): pass
          def run():
              await(some(received(('Ping',), from_=ping)))
              output("Pinged.")
              send(('Pong',), to=ping)
      
      def main():
          pong = new(Pong, args=(), at='PongNode')
          ping = new(Ping, args=(pong,))
          start(pong)
          start(ping)
      
  To run this example program, first start a node named anything other than
  'PongNode', for example 'PingNode', and tell it to run 'pingpong.da':
  
     python -m da -n PingNode pingpong.da

  'PingNode' will run the `main` method and attempt to create a `Pong`
  process on a node named 'PongNode'. But since it does not yet know which
  node is 'PongNode', it will block at the first `new` statement waiting to
  resolve the name 'PongNode'. 
  
  In order for the program execution to continue, you must start another
  node and name it 'PongNode':
  
     python -m da -n PongNode -D pingpong.da
     
  The same 'pingpong.da' file has to be specified on the command line,
  because the 'PongNode' needs to have access to the `Pong` class in order
  to create `Pong` processes. The command line parameter '-D'(or
  equivalently, '--idle') tells the system to create an "idle" node. Idle
  nodes do not execute their `main` method, and their only responsibility is
  to create DistAlgo processes on behalf of other nodes. If you omit '-D',
  then 'PongNode' will also run the `main` method, creating an additional
  `Ping` and `Pong` process each, which may or may not be your desired
  outcome.
  
  At this point, 'PingNode' will be able to resolve the name 'PongNode', and
  the execution of `main` can continue as usual. After 'PingNode'
  terminates, you should be able to observe the line
  
     pingpong.Ping<Ping:eb002#PingNode>:OUTPUT: Ponged.
     
  on the terminal running the 'PingNode', and the line
  
     pingpong.Pong<Pong:54802#PongNode>:OUTPUT: Pinged.
     
  on the terminal running the 'PongNode' (the 5 hex-digit process id values
  may differ).

#### Running nodes on multiple hosts

By default, DistAlgo nodes and processes are only able to communicate with
nodes and processes running on the same host. This is because they only
listen for incoming messages on the interface named 'localhost'. The
hostname 'localhost' usually resolves to the loopback interface '127.0.0.1',
which can only receive messages that were sent from the local host.

In order to run a distributed DistAlgo system with nodes on multiple hosts,
you need to perform two additional operations:

  1. instruct the DistAlgo processes to listen on an interface that is able
     to receive network messages from remote hosts, by using the
     '--hostname'(or equivalently, '-H') command line parameter;

  2. instruct DistAlgo nodes to connect to an existing node running on a
     remote host, by using the '--peer'(or equivalently, '-R') command line
     parameter.

You can specify either a hostname that resolves to a local interace, or the
IP address of a local interface as argument to '-H'. To obtain a list of
local interfaces and their corresponding IP addresses, use `ifconfig` on
Linux systems, or `ipconfig` on Windows. The special IP address '0.0.0.0'
can be used to indicate all available interfaces on the local host.

As an example, consider the scenario of running the 'pingpong' program on
two hosts, 'jupiter' and 'io'. First, start the 'PingNode' on host
'jupiter', instructing the system to listen on all interfaces:

    jupiter> python -m da -H 0.0.0.0 -n PingNode pingpong.da

You can now start the 'PongNode' on host 'io', telling it to connect to the
node on 'jupiter':

    io> python -m da -H 0.0.0.0 -R jupiter -n PongNode -D pingpong.da

The DistAlgo nodes network are conceptually fully connected, so for a new
node to join a network of existing nodes, it is sufficient to specify the
address of any node in the existing network. Continuing the previous
example, suppose you want to start another node, 'PongNode2', on the host
'thebe' and have it join the network of 'PingNode' and 'PongNode' already
running on 'jupiter' and 'io', then the commands

    thebe> python -m da -H 0.0.0.0 -R jupiter -n PongNode2 -D pingpong.da

and

    thebe> python -m da -H 0.0.0.0 -R io -n PongNode2 -D pingpong.da

will have the same effect.

#### Cookies

  In a DistAlgo system involving multiple nodes, a pre-shared secret key,
  known as a "cookie", can be used to authenticate processes and prevent
  unauthorized processes from sending messages to DistAlgo processes.
  Cookies can be set when starting a node process, and any DistAlgo
  processes started on that node will automatically inherit its cookie
  value. You can set the cookie for a node by using the '--cookie' command
  line option:
  
     python -m da --cookie SECRET -n PongNode pingpong.da
     
  In this case, any process that does not have a matching cookie will not be
  able to send messages to 'PongNode' or any DistAlgo process running on
  'PongNode'.
  
  Alternatively, you can store the cookie value in a file named '.da.cookie'
  under you home directory:
  
     echo -n "SECRET" > ${HOME}/.da.cookie
     chmod 600 ${HOME}/.da.cookie
     
  This way, all DistAlgo nodes will automatically use the contents of
  '${HOME}/.da.cookie' as their cookie, unless you explicitly specify one on
  the command line using '--cookie'.

  As a special case, when an unnamed node is started, it sets its cookie to
  a random value, thus preventing this node and any DistAlgo processes
  created by this node from accidentally communicating with other nodes and
  processes.
  
  **SECURITY WARNING**: Any remote or local process that knows your cookie
    and can send UDP packets to the UDP port or make TCP connections to the
    TCP port used by any DistAlgo process, will be able to trick the
    DistAlgo system into *executing arbitrary code* on your system. *Never*
    share your cookie with untrusted parties!

# Further References

  For a full description of the DistAlgo language, see
  `<DAROOT>/doc/language.pdf`. For a quick reference of all DistAlgo
  built-in functions, run the following command:
  
      python -m da -B
