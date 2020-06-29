import unittest
from pathlib import Path
import json
from datetime import datetime

from testSetup import *

__version__ = '0.20200523'
printVersion(__version__,
             wd.__name__,
             wd.__version__
             )


class outputSetup(unittest.TestCase):

    @classmethod
    def setUpClass(self):
        self.stateObj = datetime.utcnow().strftime("%m%d%Y%H%M%S")
        # ---------------------------------------------------------------------
        # Load testDict
        # ---------------------------------------------------------------------
        with open(Path
                  (__file__
                   ).parent / "mockObj.json") as dependencyFile:
            self.testDict = json.load(dependencyFile)


suites = []
tests = [outputSetup]
for test_class in tests:
    suite = unittest.TestLoader().loadTestsFromTestCase(test_class)
    suites.append(suite)
big_suite = unittest.TestSuite(suites)
runner = unittest.TextTestRunner(verbosity=2)
runner.run(big_suite)
