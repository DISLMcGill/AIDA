import unittest

import numpy as np

import aidac


class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        path = 'resources/dummy_table.csv'
        self.local_table = aidac.read_csv(path, header=0)

    def test_column(self):
        cols = self.local_table.columns
        self.assertIn('id', cols)
        self.assertIn('name', cols)
        self.assertIn('salary', cols)

        self.assertEqual(cols.get('id').dtype, np.int64)
        self.assertEqual(cols.get('name').dtype, object)
        self.assertEqual(cols.get('salary').dtype, np.float64)



if __name__ == '__main__':
    unittest.main()
