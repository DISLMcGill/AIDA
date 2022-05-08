import concurrent.futures
import collections
import numpy as np
import copyreg;
import time;

from functools import reduce
from aidacommon.dborm import TabularData, COL, JOIN
import aidacommon.rop;
from aidacommon.rdborm import TabularDataRemoteStub


class DistTabularData(TabularData):
    def join(self, otherTable, src1joincols, src2joincols, cols1=COL.NONE, cols2=COL.NONE, join=JOIN.INNER):
        if 3 % 2 == 0:  # Use block join
            if self.combined_data is None:
                s = self.cdata

            def external_join(c, table2):
                def load_data(df):
                    return df

                external = c._L(load_data, self.combined_data)
                return external.join(table2, src1joincols, src2joincols, cols1, cols2, join).cdata

            futures = []
            for i in range(self.tabular_datas):
                futures.append(self.executor.submit(external_join, self.connections[i], otherTable.tabular_datas[i]))
            results = []
            for f in concurrent.futures.as_completed(futures):
                results.append(f.result())
            return collections.OrderedDict((k, reduce(lambda a, b: [*a[k], *b[k]], results)) for k in results[0])
        else:  # Use hash join
            start = time.perf_counter()
            def partition_table(table, joincols, cols):
                tables = []
                for i in self.executor.map(lambda j: table[j].hash_partition(j, joincols, cols, self.connections), range(len(table))):
                    tables.append(i)
                return tables

            redist_tables = partition_table(self.tabular_datas, src1joincols, cols1)
            redist_other_tables = partition_table(otherTable.tabular_datas, src2joincols, cols2)
            chkpt_1 = time.perf_counter()
            def stack_table(tables):
                t = []
                for i in self.executor.map(lambda j: tables[0][j].vstack(tables[j][k] for k in range(1, len(self.tabular_datas)))):
                    t.append(i)
                return t

            r_tables = stack_table(redist_tables)
            r_other_tables = stack_table(redist_other_tables)
            chkpt_2 = time.perf_counter()
            def partition_join(table1, table2):
                return table1.join(table2, src1joincols, src2joincols, cols1, cols2, join).cdata

            futures = []
            for i in range(len(r_tables)):
                futures.append(self.executor.submit(partition_join, r_tables[i], r_other_tables[i]))
            results = []
            for f in concurrent.futures.as_completed(futures):
                results.append(f.result())
            chkpt_3 = time.perf_counter()
            print("hash partition time: {}".format(chkpt_1-start))
            print("vstack time: {}".format(chkpt_2-chkpt_1))
            print("join time: {}".format(chkpt_3-chkpt_2))
            return collections.OrderedDict((k, reduce(lambda a, b: [*a[k], *b[k]], results)) for k in results[0])

    def aggregate(self, projcols, groupcols=None):
        pass

    def project(self, projcols):
        pass

    def order(self, orderlist):
        pass

    def distinct(self):
        pass

    def loadData(self, matrix=False):
        pass

    def __add__(self, other):
        pass

    def __radd__(self, other):
        pass

    def __mul__(self, other):
        pass

    def __rmul__(self, other):
        pass

    def __sub__(self, other):
        pass

    def __rsub__(self, other):
        pass

    def __truediv__(self, other):
        pass

    def __rtruediv__(self, other):
        pass

    def __pow__(self, power, modulo=None):
        pass

    def __matmul__(self, other):
        pass

    def __rmatmul__(self, other):
        pass

    @property
    def T(self):
        pass

    def __getitem__(self, item):
        pass

    @property
    def shape(self):
        pass

    def vstack(self, othersrclist):
        pass

    def hstack(self, othersrclist, colprefixlist=None):
        pass

    def describe(self):
        pass

    def sum(self, collist=None):
        futures = []
        for t in self.tabular_datas:
            futures.append(self.executor.submit(t.sum, collist))
        results = []

        for f in concurrent.futures.as_completed(futures):
            results.append(f.result())

        if isinstance(results[0], int):
            return reduce(lambda a, b: a + b, results)
        else:
            def reducer(accumulator, element):
                for key, value in element.items():
                    accumulator[key] = accumulator.get(key, 0) + value
                return accumulator

            return reduce(reducer, results)

    def avg(self, collist=None):
        sum = self.sum(collist)
        count = self.count(collist)

        if isinstance(sum, int):
            return sum / count
        else:
            result = collections.OrderedDict();
            for key, value in sum:
                result[key] = value / count[key]
            return result

    def count(self, collist=None):
        futures = []
        for t in self.tabular_datas:
            futures.append(self.executor.submit(t.count, collist))
        results = []

        for f in concurrent.futures.as_completed(futures):
            results.append(f.result())

        if isinstance(results[0], int):
            return reduce(lambda a, b: a + b, results)
        else:
            def reducer(accumulator, element):
                for key, value in element.items():
                    accumulator[key] = accumulator.get(key, 0) + value
                return accumulator

            return reduce(reducer, results)

    def countd(self, collist=None):
        pass

    def countn(self, collist=None):
        pass

    def max(self, collist=None):
        pass

    def min(self, collist=None):
        pass

    def head(self, n=5):
        return self.tabular_datas[0].head(n)

    def tail(self, n=5):
        return self.tabular_datas[-1].tail(n)

    def _U(self, func, *args, **kwargs):
        pass

    def _genSQL_(self, *args, **kwargs):
        pass

    @property
    def cdata(self):
        if self.combined_data is None:
            futures = []
            for t in self.tabular_datas:
                futures.append(self.executor.submit(t.cdata))
            results = []
            for f in concurrent.futures.as_completed(futures):
                results.append(f.result())
            self.combined_data = collections.OrderedDict(
                (k, reduce(lambda a, b: [*a[k], *b[k]], results)) for k in results[0])
        return self.combined_data

    def __init__(self, executor, connections, tabular_datas):
        self.tabular_datas = tabular_datas
        self.connections = connections
        self.executor = executor
        self.combined_data = None

    def filter(self, *selcols):
        def get_filter(table, s):
            return table.filter(s).cdata

        futures = []
        for t in self.tabular_datas:
            futures.append(self.executor.submit(get_filter, t, selcols))
        results = []
        for f in concurrent.futures.as_completed(futures):
            results.append(f.result())
        return collections.OrderedDict((k, reduce(lambda a, b: [*a[k], *b[k]], results)) for k in results[0])

    def hash_partition(self, index, keys, cols, connections):
        pass

copyreg.pickle(DistTabularData, TabularDataRemoteStub.serializeObj);
