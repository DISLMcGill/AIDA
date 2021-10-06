import unittest

from BaseTest import BaseTest
from aidacommon.dborm import F, EXTRACT, CASE, Q, CMP, C


class TestSQLProject(BaseTest):

    def setUp(self):
        super().setUp()
        self.expected_file = 'expected/project'

    def test_rename(self):
        """q07"""
        n = self.db.nation
        n.loadData()
        ns = n.project(({'n_name': 'supp_nation'}, {'n_nationkey': 'ns_nationkey'}))
        expected = self.load_object('test_rename')
        self.validate_result(ns, expected)

    def test_f1(self):
        """q15"""
        l = self.db.lineitem
        l.loadData()
        l = l.project(({'l_suppkey': 'supplier_no'}, {F('l_extendedprice') * (1 - F('l_discount')): 'rev'}))
        expected = self.load_object('test_f1')
        self.validate_result(l, expected)

    def test_extract(self):
        """q07"""
        l = self.db.lineitem
        l.loadData()
        l = l.project(('l_suppkey', 'l_orderkey', {EXTRACT('l_shipdate', EXTRACT.OP.YEAR): 'l_year'}))
        expected = self.load_object('test_extract')
        self.validate_result(l, expected)

    def test_case(self):
        o = self.db.orders
        o.loadData()
        o = o.project(('o_orderkey', 'o_orderdate', 'o_totalprice'
                       , {CASE(((Q('o_orderpriority', ('1-URGENT', '2-HIGH'), CMP.IN), 1),), C(0)): 'ho'}))
        expected = self.load_object('test_case')
        self.validate_result(o, expected)


if __name__ == '__main__':
    unittest.main()