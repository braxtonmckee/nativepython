#!/usr/bin/env python3

#   Copyright 2017-2019 Nativepython Authors
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

"""
This is the primary unit-test entrypoint for nativepython.

"""
import sys
import shutil
import logging
import unittest
import os
import re
import time
import nose
import nose.config
import nose.loader
import nose.plugins.manager
import nose.plugins.xunit
import argparse
import traceback
import subprocess
import pickle
import hashlib


class DirectoryScope(object):
    def __init__(self, directory):
        self.directory = directory

    def __enter__(self):
        self.originalWorkingDir = os.getcwd()
        os.chdir(self.directory)

    def __exit__(self, exc_type, exc_value, traceback):
        os.chdir(self.originalWorkingDir)


def sortedBy(elts, sortFun):
    return [x[1] for x in sorted([(sortFun(y), y) for y in elts])]


def loadTestModules(testFiles, rootDir):
    modules = set()
    for f in testFiles:
        try:
            with DirectoryScope(rootDir):
                moduleName = fileNameToModuleName(f, rootDir)
                logger.debug('importing module %s', moduleName)
                __import__(moduleName)
                modules.add(sys.modules[moduleName])
        except ImportError:
            logger.error("Failed to load test module: %s", moduleName)
            traceback.print_exc()
            raise

    return modules


def fileNameToModuleName(fileName, rootDir):
    tr = (
        fileName
        .replace('.py', '')
        .replace(rootDir, '', 1)  # only replace the first occurrence
        .replace(os.sep, '.')
    )
    if tr.startswith('.'):
        return tr[1:]
    return tr


class OrderedFilterAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if 'ordered_actions' not in namespace:
            setattr(namespace, 'ordered_actions', [])
        previous = namespace.ordered_actions
        previous.append((self.dest, values))
        setattr(namespace, 'ordered_actions', previous)


class PythonTestArgumentParser(argparse.ArgumentParser):
    def __init__(self):
        super(PythonTestArgumentParser, self).__init__(add_help=True)
        self.add_argument(
            '-v',
            dest='testHarnessVerbose',
            action='store_true',
            default=False,
            required=False,
            help="run test harness verbosely"
        )
        self.add_argument(
            '--dump_native',
            dest='dump_native',
            action='store_true',
            default=False,
            required=False,
            help="show all the llvm IR we're dumping as we go"
        )
        self.add_argument(
            '-s',
            dest='skip_build',
            action='store_true',
            default=False,
            required=False,
            help="dont rebuild"
        )
        self.add_argument(
            '--list',
            dest='list',
            action='store_true',
            default=False,
            required=False,
            help="don't run tests, just list them"
        )
        self.add_argument(
            '--filter',
            nargs=1,
            help='restrict tests to a subset matching FILTER',
            action=OrderedFilterAction,
            default=None
        )
        self.add_argument(
            '--add',
            nargs=1,
            help='add back tests matching ADD',
            action=OrderedFilterAction,
            default=None
        )
        self.add_argument(
            '--exclude',
            nargs=1,
            help="exclude python unittests matching 'regex'. "
                 + "These go in a second pass after -filter",
            action=OrderedFilterAction,
            default=None
        )
        self.add_argument(
            '--error_output_dir',
            help="error output dir; will create if doesn't exist.",
            type=str,
            default=None
        )

    def parse_args(self, toParse):
        argholder = super(PythonTestArgumentParser, self).parse_args(toParse)

        args = None
        if 'ordered_actions' in argholder:
            args = []
            for arg, l in argholder.ordered_actions:
                args.append((arg, l[0]))

        return argholder, args


def regexMatchesSubstring(pattern, toMatch):
    for _ in re.finditer(pattern, toMatch):
        return True
    return False


def applyFilterActions(filterActions, tests):
    filtered = [] if filterActions[0][0] == 'add' else list(tests)

    for action, pattern in filterActions:
        if action == "add":
            filtered += [x for x in tests
                         if regexMatchesSubstring(pattern, x.id())]
        elif action == "filter":
            filtered = [x for x in filtered
                        if regexMatchesSubstring(pattern, x.id())]
        elif action == "exclude":
            filtered = [x for x in filtered
                        if not regexMatchesSubstring(pattern, x.id())]
        else:
            assert False

    return filtered


def printTests(testCases):
    for test in testCases:
        print(test.id())


def runPyTestSuite(config, testFiles, testCasesToRun, testArgs):
    testProgram = nose.core.TestProgram(
        config=config,
        defaultTest=testFiles,
        suite=testCasesToRun,
        argv=testArgs,
        exit=False
    )

    return not testProgram.success


def loadTestsFromModules(config, modules):
    loader = nose.loader.TestLoader(config=config)
    allSuites = []
    for module in modules:
        cases = loader.loadTestsFromModule(module)
        allSuites.append(cases)

    return allSuites


def extractTestCases(suites):
    testCases = flattenToTestCases(suites)
    # make sure the tests are sorted in a sensible way.
    sortedTestCases = sortedBy(testCases, lambda x: x.id())

    return [x for x in sortedTestCases if not testCaseHasAttribute(x, 'disabled')]


def flattenToTestCases(suite):
    if isinstance(suite, list) or isinstance(suite, unittest.TestSuite):
        return sum([flattenToTestCases(x) for x in suite], [])
    return [suite]


def testCaseHasAttribute(testCase, attributeName):
    """Determine whether a unittest.TestCase has a given attribute."""
    if hasattr(getattr(testCase, testCase._testMethodName), attributeName):
        return True
    if hasattr(testCase.__class__, attributeName):
        return True
    return False


def loadTestCases(config, testFiles, rootDir):
    modules = sortedBy(loadTestModules(testFiles, rootDir), lambda module: module.__name__)
    allSuites = loadTestsFromModules(config, modules)
    return extractTestCases(allSuites)


def findTestFiles(rootDir, testRegex):
    logger.debug('finding files from root %s', rootDir)
    testPattern = re.compile(testRegex)
    testFiles = []
    for directory, subdirectories, files in os.walk(rootDir):
        testFiles += [os.path.join(directory, f) for f in files if testPattern.match(f) is not None]

    return testFiles


def logAsInfo(*args):
    if len(args) == 1:
        print(time.asctime(), " | ", args)
    else:
        print(time.asctime(), " | ", args[0] % args[1:])


def setLoggingLevel(level):
    logger.setLevel(level)
    for handler in logger.handlers:
        handler.setLevel(level)


class OutputCapturePlugin(nose.plugins.base.Plugin):
    """
    Output capture plugin. Enabled by default. Disable with ``-s`` or
    ``--nocapture``. This plugin captures stdout during test execution,
    appending any output captured to the error or failure output,
    should the test fail or raise an error.
    """
    enabled = True
    name = 'OutputCaptureNosePlugin'
    score = 16010

    def __init__(self):
        self.stdout = []
        self.stdoutFD = None
        self.stderrFD = None
        self.fname = None
        self.hadError = False
        self.outfile = None
        self.testStartTime = None
        self.nocaptureall = False

    def options(self, parser, env):
        """Register commandline options
        """
        parser.add_option(
            "--nocaptureall", action="store_true",
            default=False, dest="nocaptureall"
        )

    def configure(self, options, conf):
        """Configure plugin. Plugin is enabled by default.
        """
        self.conf = conf
        if options.nocaptureall:
            self.nocaptureall = True

    def afterTest(self, test):
        """Clear capture buffer.
        """
        if self.nocaptureall:
            if not self.hadError:
                logAsInfo("\tpassed in %s", time.time() - self.testStartTime)
            else:
                logAsInfo("\tfailed in %s seconds. See logs in %s", time.time() - self.testStartTime, self.fname)

        if self.stdoutFD is None:
            return

        setLoggingLevel(logging.ERROR)

        sys.stdout.flush()
        sys.stderr.flush()

        os.dup2(self.stdoutFD, 1)
        os.close(self.stdoutFD)

        os.dup2(self.stderrFD, 2)
        os.close(self.stderrFD)

        self.stdoutFD = None
        self.stderrFD = None

        self.outfile.flush()
        self.outfile.close()
        self.outfile = None

        if not self.hadError:
            try:
                os.remove(self.fname)
            except OSError:
                pass
            logAsInfo("\tpassed in %s", time.time() - self.testStartTime)
        else:
            # the test failed. Report the failure
            logAsInfo("\tfailed in %s seconds. See logs in %s", time.time() - self.testStartTime, self.fname)

    def begin(self):
        pass

    def beforeTest(self, test):
        """Flush capture buffer.
        """
        logAsInfo("Running test %s", test)

        self.testStartTime = time.time()

        if self.nocaptureall:
            self.hadError = False
            return

        sys.stdout.flush()
        sys.stderr.flush()

        self.stdoutFD = os.dup(1)
        self.stderrFD = os.dup(2)

        self.fname = "nose.%s.%s.log" % (test.id(), os.getpid())

        if os.getenv("TEST_ERROR_OUTPUT_DIRECTORY", None) is not None:
            self.fname = os.path.join(
                os.getenv("TEST_ERROR_OUTPUT_DIRECTORY"), self.fname)

        self.outfile = open(self.fname, "w")

        os.dup2(self.outfile.fileno(), 1)
        os.dup2(self.outfile.fileno(), 2)

        self.hadError = False

        setLoggingLevel(logging.INFO)

    def formatError(self, test, err):
        """Add captured output to error report.
        """
        self.hadError = True

        if self.nocaptureall:
            return err

        ec, ev, tb = err

        ev = self.addCaptureToErr(ev, tb)

        # print statements here show up in the logfile
        print("Test ", test, ' failed: ')
        print(ev)

        self.failureReason = ev

        return (ec, ev, tb)

    def formatFailure(self, test, err):
        """Add captured output to failure report.
        """
        self.hadError = True
        return self.formatError(test, err)

    def addCaptureToErr(self, ev, tb):
        return ''.join([str(ev) + "\n"] + traceback.format_tb(tb) + ['\n>> output captured in %s <<' % self.fname])

    def end(self):
        pass

    def finalize(self, result):
        """Restore stdout.
        """


def runPythonUnitTests(args, filterActions, modules):
    testArgs = ["dummy"]

    if args.dump_native:
        import nativepython.runtime as runtime
        runtime.Runtime.singleton().verboselyDisplayNativeCode()

    if args.testHarnessVerbose or args.list:
        testArgs.append('--nocapture')

    testArgs.append('--verbosity=0')

    testCasesToRun = []

    plugins = nose.plugins.manager.PluginManager([OutputCapturePlugin()])

    config = nose.config.Config(plugins=plugins)
    config.configure(testArgs)

    testCases = []
    for module in modules:
        dir = os.path.dirname(module.__file__)
        testCases += loadTestCases(
            config,
            findTestFiles(dir, '.*_test.py$'),
            os.path.dirname(dir)
        )

    if filterActions:
        testCases = applyFilterActions(filterActions, testCases)

    testCasesToRun += testCases

    if args.list:
        for test in testCasesToRun:
            print(test.id())

        os._exit(0)

    return runPyTestSuite(config, None, testCasesToRun, testArgs)


def executeTests(args, filter_actions):
    if not args.list:
        print("Running python unit tests.")
        print("nose version: ", nose.__version__)
        print(time.ctime(time.time()))

    import nativepython
    import typed_python
    import object_database

    if runPythonUnitTests(args, filter_actions, [nativepython, typed_python, object_database]):
        anyFailed = True
    else:
        anyFailed = False

    print("\n\n\n")

    if anyFailed:
        return 1
    return 0


def hashSource(rootPath):
    contents = []
    for root, dirs, files in os.walk(rootPath):
        for f in files:
            path = os.path.join(root, f)

            if "/.git/" not in path and "/build/" not in path and (
                    path.endswith(".cc") or
                    path.endswith(".hpp") or
                    path.endswith(".cpp") or
                    path.endswith(".c") or
                    path.endswith(".h") or
                    "setup.py" in path
            ):
                with open(path, "rb") as f:
                    contents.append((path, f.read()))

    contents = sorted(contents)
    sha = hashlib.sha256()
    sha.update(pickle.dumps(contents))
    return sha.hexdigest()


def buildModule(args):
    t0 = time.time()
    print("Building nativepython using 'make lib'...", end='')

    result = subprocess.run(
        ['make', 'lib', '-j2'],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )

    if result.returncode != 0:
        print("Build failed: ")
        print(str(result.stdout, 'utf-8'))
        return 1

    print(". Finished in %.2f seconds" % (time.time() - t0))
    print()

def main(args):
    global logger
    # parse args, return zero and exit if help string was printed
    parser = PythonTestArgumentParser()
    args, filter_actions = parser.parse_args(args[1:])

    try:
        if not args.skip_build:
            result = buildModule(args)
            if result:
                return result

        if args.error_output_dir is not None:
            os.makedirs(args.error_output_dir, exist_ok=True)
            # export an env var; this is what the tests config look for in the
            # OutputCapturePlugin class above
            os.environ["TEST_ERROR_OUTPUT_DIRECTORY"] = args.error_output_dir

        from object_database.util import configureLogging
        configureLogging("test", logging.INFO)
        logger = logging.getLogger(__name__)

        # set the python path so that we load the right version of the library.
        os.environ['PYTHONPATH'] = os.environ.get('PYTHONPATH', '') + ":" + os.path.abspath("./build/install")

        return executeTests(args, filter_actions)
    except Exception:
        import traceback
        logging.getLogger(__name__).error("executeTests() threw an exception: \n%s", traceback.format_exc())
        return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
