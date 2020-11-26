from test_dbs import *
import unittest

class TestDBs(unittest.TestCase):
    def test_databases(self):
        self.assertAlmostEqual(mysql_hive_test(),True)