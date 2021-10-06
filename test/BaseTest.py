import unittest

import pandas as pd
import pickle

config = __import__('TPCHconfig')


class BaseTest(unittest.TestCase):
    def setUp(self):
        self.db = config.getDBC(config.jobName)
        self.expected_file = None
        self.range = 500

    def load_object(self, name, file=None):
        file = file or self.expected_file
        with open(file, 'rb') as f:
            data = pickle.load(f)
            return data[name]

    def validate_result(self, result: pd.DataFrame, expected: pd.DataFrame):
        # for col in result:
        #     if col.dtypes == 'int64':
        #         col.astype('int32')
        self.assertEqual(result.shape, expected[1])
        # print(f'{result.head(10)} \n*****\n {result.head(10).dtypes}')
        print('-------------------------------')
        # print(f'{expected[0].head(10)}\n*****\n {expected[0].head(10).dtypes}')
        print(result.head(10).equals(expected[0].head(10)))
        print('\n')
        self.assertTrue(result.head(self.range).equals(expected[0]))