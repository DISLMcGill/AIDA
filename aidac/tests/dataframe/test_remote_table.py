import unittest

import aidac
from aidac import SQLProjectionTransform


class MyTestCase(unittest.TestCase):
    def setUp(self) -> None:
        from aidac.tests.ds_config import PG_CONFIG
        config = PG_CONFIG
        aidac.add_data_source('postgres', config['host'], config['user'], config['passwd'], config['dbname'], 'p1', config['port'])
        self.station = aidac.read_remote_data('p1', 'station')

    def test_remote_project(self):
        proj = self.station['id']
        self.assertTrue(isinstance(proj._transform_, SQLProjectionTransform))
        sql = proj.transform.genSQL
        self.assertEqual(sql, 'SELECT id AS id FROM (SELECT * FROM station) station')



if __name__ == '__main__':
    unittest.main()
