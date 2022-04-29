import unittest

import psycopg2 as p2

from aidac.data_source.ResultSet import ResultSet


class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.data = [('val1', 1, ), ('val2', 2, )]
        col1 = p2.extensions.Column('col1', 1043) # varchar oid code
        col2 = p2.extensions.Column('col2', 23) # int oid code
        self.cols = [col1, col2]

    def test_flatten_result(self):
        rs = ResultSet(self.cols, self.data)
        self.assertEqual(rs.get_result_ls(), ['val1', 'val2'])  # add assertion here


if __name__ == '__main__':
    unittest.main()
