import unittest

from BaseTest import BaseTest
from aidacommon.dborm import Q, C, DATE, CMP

config = __import__('TPCHconfig')
expected = __import__('sql_filter_output')


class TestSQLProject(BaseTest):

    def test_Q_class1(self):
        """"qself.range"""
        n = self.db.nation
        n.loadData()
        n = n.filter(Q('n_name', C('CANADA')))
        expected = self.load_object('test_Q_class1')
        self.validate_result(n, expected)

    def test_Q_class2(self):
        """"q03"""
        c = self.db.customer
        c.loadData()
        c = c.filter(Q('c_mktsegment', C('BUILDING')))
        expected = self.load_object('test_Q_class2')
        self.validate_result(c, expected)

    def test_cmp_date(self):
        """"q01"""
        l = self.db.lineitem
        l.loadData()
        l = l.filter(Q('l_shipdate', DATE('1998-09-02'), CMP.LTE))
        expected = self.load_object('test_cmp_date')
        self.validate_result(l, expected)

    def test_multi_q(self):
        """"q05"""
        o = self.db.orders
        o.loadData()
        o = o.filter(Q('o_orderdate', DATE('1994-01-01'), CMP.GTE), Q('o_orderdate', DATE('1995-01-01'), CMP.LT))
        expected = self.load_object('test_multi_q')
        self.validate_result(o, expected)

    def test_inlist(self):
        """"q16"""
        p = self.db.part
        p.loadData()
        p = p.filter(Q('p_brand', C('Brand#45'), CMP.NE), Q('p_size', (49, 14, 23, 45, 19, 3, 36, 9), CMP.IN))
        expected = self.load_object('test_inlist')
        self.validate_result(p, expected)

    def test_like(self):
        """"q02"""
        p = self.db.part
        p.loadData()
        p = p.filter(Q('p_size', C(15), Q('p_type', C('%BRASS'), CMP.LIKE)))
        expected = self.load_object('test_like')
        self.validate_result(p, expected)

    def test_or(self):
        """"q19"""
        p = self.db.part
        p.loadData()
        p = p.filter((Q('p_brand', C('Brand#12')) & Q('p_container', ('SM CASE', 'SM BOX', 'SM PACK', 'SM PKG'),
                                                      CMP.IN) & Q('p_size', C(1), CMP.GTE) & Q('p_size', C(5),
                                                                                               CMP.LTE))
                     |(
                                 Q('p_brand', C('Brand#23')) & Q('p_container',
                                                                 ('MED BAG', 'MED BOX', 'MED PKG', 'MED PACK'),
                                                                 CMP.IN) & Q('p_size', C(1), CMP.GTE) & Q('p_size',
                                                                                                          C(10),
                                                                                                          CMP.LTE)))
        expected = self.load_object('test_or')
        self.validate_result(p, expected)


if __name__ == '__main__':
    TestSQLProject.expected_file = 'expected/filter1'
    unittest.main()