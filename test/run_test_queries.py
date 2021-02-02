import os
import pickle
import unittest
import pandas as pd
import numpy as np
from time import time

config = __import__('TPCHconfig-AIDA')
tpchqueries = __import__('TPCHqueries-AIDA')

db = config.getDBC(config.jobName);

os.system('mkdir -p {}'.format(config.outputDir))

cols =  {'region': ('r_regionkey', 'r_name', 'r_comment'),
         'nation': ('n_nationkey', 'n_name', 'n_regionkey', 'n_comment'),
         'part': ('p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size', 'p_container', 'p_retailprice', 'p_comment'),
         'supplier': ('s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment'),
         'partsupp': ('ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment'),
         'customer': ('c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment'),
         'orders': ('o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment'),
         'lineitem': ('l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment'),
        }

EPSILON = 0.00001

class Database:

    def __init__(self, db):
        self.db = db

    def __getattr__(self, name):
        t = getattr(self.db, name)
        try:
            t = t.project(cols[name]) * 1
            self.__setattr__(name, t)
        except KeyError:
            self.__setattr__(name, t)
        return t


def load_monet_result(path):
    with open('{}/{}'.format(config.outputDir, path), 'rb') as f:
        data = pickle.load(f)
    return data


class PandasTest(unittest.TestCase):
    def setUp(self):
        print('----------[ Preloading ]----------')
        self.db = Database(db)
        for t in ('lineitem', 'part', 'partsupp', 'customer', 'orders', 'nation', 'region', 'supplier'):
            t = getattr(db, t)
            t.loadData()

        self.queries = ['0' + str(int(e)) if int(e) < 10 else str(int(e)) for e in range(1, 21)]
        self.queries.append('22')

    def validateQueryOutput(self, expected, real):
        # pandas dataframe
        if isinstance(expected, pd.DataFrame):
            #self.assertTrue(expected.equals(real), 'expected = {} \n real = {}'.format(expected.dtypes, real.dtypes))
            self.assertEqual(expected.shape, real.shape, "The shape of expected and real are different")
            # convert column data type
            for ecol, rcol in zip(expected.columns, real.columns):
                if expected[ecol].dtypes == float:
                    e = expected[ecol].astype(np.float32)
                    r = real[rcol].astype(np.float32)

                    # there might be numerical error, check rows one by one to allow small errors
                    for i in range(len(e)):
                        self.assertTrue(e.iloc[i] - r.iloc[i] < EPSILON, f"Column {ecol} at row {i} are not equal")
                    continue

                if expected[ecol].dtypes in [int, np.int32, np.int16, np.int64]:
                    print('inside int')
                    e = expected[ecol].astype('int32')
                    r = real[rcol].astype('int32')
                else:
                    e = expected[ecol]
                    r = real[rcol]
                print(f'col: {ecol}, expected_type: {e.dtypes}, real_type: {r.dtypes}')
                self.assertTrue(e.equals(r), f"Column {ecol} are not equal")
        # ordered dict
        else:
            self.assertEqual(len(expected), len(real))
            for e, r in zip(expected, real):
                self.assertEqual(len(e), len(r), "The shape of expected and real are different")
                self.assertEqual(e, r)
        # self.assertEqual(expected, real, f'{expected["revenue"].dtype}, {real["revenue"].dtype}')
        print(expected)
        print('----------------------------')
        print(real)

    def runQuery(self, q):
        print('-------------test q{}-------------'.format(q))
        r = getattr(tpchqueries, 'q' + q)(db)
        if (hasattr(r, '_genSQL_')):
            r.loadData()

        real = r.rows if hasattr(r, '_genSQL_') else r
        expected = load_monet_result('monet_{}'.format(q))
        self.validateQueryOutput(expected, real)

    def test_q01(self):
        self.runQuery('01')

    def test_q02(self):
        self.runQuery('02')

    def test_q03(self):
        self.runQuery('03')

    def test_q04(self):
        self.runQuery('04')

    def test_q05(self):
        self.runQuery('05')

    def test_q06(self):
        self.runQuery('06')

    def test_q07(self):
        self.runQuery('07')

    def test_q08(self):
        self.runQuery('08')

    def test_q09(self):
        self.runQuery('09')

    def test_q10(self):
        self.runQuery('10')

    def test_q11(self):
        self.runQuery('11')

    def test_q12(self):
        self.runQuery('12')

    def test_q13(self):
        self.runQuery('13')

    def test_q14(self):
        self.runQuery('14')

    def test_q15(self):
        self.runQuery('15')

    def test_q16(self):
        self.runQuery('16')

    def test_q17(self):
        self.runQuery('17')

    def test_q18(self):
        self.runQuery('18')

    def test_q19(self):
        self.runQuery('19')

    def test_q20(self):
        self.runQuery('20')

    def test_q22(self):
        self.runQuery('22')


if __name__ == '__main__':
    unittest.main()
