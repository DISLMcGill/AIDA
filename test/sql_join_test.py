import unittest

from BaseTest import BaseTest
from aidacommon.dborm import F, COL, C, Q, DATE, CMP, JOIN


class TestSQLJoin(BaseTest):

    def test_join1(self):
        """q02"""
        n = self.db.nation
        r = self.region.filter(Q('r_name', C('EUROPE')))
        j = n.join(r, ('n_regionkey',), ('r_regionkey',), COL.ALL, COL.ALL)
        expected = self.load_object('test_join1')
        self.validate_result(j.head(20), expected)

    def test_join2(self):
        """q03"""
        c = self.db.customer.filter(Q('c_mktsegment', C('BUILDING')));
        o = self.db.orders.filter(Q('o_orderdate', DATE('1995-03-15'), CMP.LT));
        l = self.db.lineitem.filter(Q('l_shipdate', DATE('1995-03-15'), CMP.GT)).project(
            ('l_orderkey', {F('l_extendedprice') * (1 - F('l_discount')): 'rev'}));
        c.loadData()
        o.loadData()
        l.loadData()
        t = c.join(o, ('c_custkey',), ('o_custkey',), COL.ALL, COL.ALL);
        t = t.join(l, ('o_orderkey',), ('l_orderkey',), COL.ALL, COL.ALL);
        expected = self.load_object('test_join2')
        self.validate_result(t.head(20), expected)

    def test_join3(self):
        """q13"""
        c = self.db.customer;
        o = self.db.orders.filter(Q('o_comment', C('%special%requests%'), CMP.NOTLIKE));
        c.loadData()
        o.loadData()
        t = c.join(o, ('c_custkey',), ('o_custkey',), COL.ALL, COL.ALL, JOIN.LEFT);
        expected = self.load_object('test_join3')
        self.validate_result(t.head(20), expected)


if __name__ == '__main__':
    TestSQLJoin.expected_file = 'test_join_expected'
    unittest.main()