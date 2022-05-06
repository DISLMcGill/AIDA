import unittest

from aidac.common.DataIterator import generator
from aidac.data_source.PostgreDataSource import PostgreDataSource
import pandas as pd
import numpy as np


class DataSourceTest(unittest.TestCase):
    def setUp(self) -> None:
        from aidac.tests.data_source.server.ds_config import PG_CONFIG
        config = PG_CONFIG
        self.ds = PostgreDataSource(config['host'], config['user'], config['passwd'], config['port'], config['dbname'])
        self.ds.connect()

    def test_ls_tables(self):
        tables = self.ds.ls_tables()
        self.assertEqual(tables, ['student', 'test', 'users', 'review',
                                  'releaselanguages', 'moviegenres', 'movies', 'station'])  # add assertion here

    def test_transfer(self):
        df = pd.DataFrame({'col1': np.random.rand(1000), 'col2': np.random.rand(1000)})
        cols = {'col1': np.float64, 'col2': np.float64}
        self.ds.create_table('temp1', cols)
        self.ds.import_table('temp1', cols, generator(df))

if __name__ == '__main__':
    unittest.main()
