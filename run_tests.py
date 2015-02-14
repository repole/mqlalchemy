import unittest
from mqlalchemy.tests import MQLAlchemyTests
suite = unittest.TestLoader().loadTestsFromTestCase(MQLAlchemyTests)
unittest.TextTestRunner(verbosity=2).run(suite)