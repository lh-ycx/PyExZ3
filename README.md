PyExZ3
======

###A Nicer Symbolic Execution for Python (Z3)

This code is a port/rewrite of the NICE project's (http://code.google.com/p/nice-of/) 
symbolic execution engine for Python to use the Z3 theorem prover (http://z3.codeplex.com). We have removed 
the NICE-specific dependences, platform-specific code, and
makes various improvements so the code base can be used by students or anyone wanting to
experiment with dynamic symbolic execution. 

A novel aspect of the rewrite is to rely solely
on Python's operator overloading to accomplish all 
the  interception needed for symbolic execution; no AST rewriting or bytecode instrumentation 
is required, as was done in the NICE project. This significantly improves the robustness and 
portability of the engine, as well as reducing the code size.

###Setup instructions:

- Make sure that you use Python 32-bit (64-bit) if-and-only-if you use the Z3 32-bit (64-bit) binaries. Testing so far has been on Python 3.2.3 and 32-bit.
- Install Python 3.2.3 (https://www.python.org/download/releases/3.2.3/)
- Install the latest "unstable" release of Z3 to directory Z3HOME from http://z3.codeplex.com/releases (click on the "Planned" link on the right to get the latest binaries for all platforms)
- Add Z3HOME\bin to PATH and PYTHONPATH
- MacOS: setup.sh for Homebrew default locations for Python and Z3; see end for MacOS specifi instructions
- Optional:
-- install GraphViz utilities (http://graphviz.org/)

### Check that everything works:
- "python run_tests.py test" should pass all tests
- "python sym_exec.py test\FILE.py" to run a single test from test directory

### Usage of sym_exec.py

- **Basic usage** is very simple - give a Python file `FILE.py` as input. By default, `sym_exec` expects `FILE.py` to contain a function named `FILE` where symbolic execution will start:

  - `sym_exec FILE.py`

- **Starting function**: You can override the default starting function with `--start MAIN`, where `MAIN` is the name of a function in `FILE`: 

  - sym_exec `--start MAIN` FILE.py
  
- **Arguments to starting function**: by default, sym_exec will associate a SymbolicInteger 
(with initial value 0) for each parameter to the starting function. 
You can decorate the start function to specify concrete values for parameters 
(@concrete) so that they never will be treated symbolically; you can also specify 
which values should be treated symbolically (@symbolic) - the type of associated 
initial value for the argument will be used to determine the proper symbolic type (if one exists)

```
from symbolic.args import *

@concrete(a=1,b=2)
@symbolic(c=3)
startingfun(a,b,c,d):
...
```

- **Oracle test functions*

- **Import behavior**


### MacOS specific

1. Grab yourself a Brew at http://brew.sh/
2. Get the newest python or python3 version: `brew install python`
3. Have the system prefer brew python over system python: `echo export PATH='/usr/local/bin:$PATH' >> ~/.bash_profile`  - 
4. Get z3: `brew install homebrew/science/z3`
5. Clone this repository: `git clone https://github.com/thomasjball/PyExZ3.git` 
6. Set the PATH: `. PyExZ3/setup.sh`  (do not run the setup script in a subshell `./ PyExZ3/`)
