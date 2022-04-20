import concurrent.futures
import collections
import pandas as pd
import copyreg;

from functools import reduce
from aidacommon.dborm import TabularData, COL, JOIN
import aidacommon.rop;
from aidacommon.rdborm import TabularDataRemoteStub


class DistTabularData(TabularData):
    def get_cdata(self, table):
        return table.cdata

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
            return collections.OrderedDict((k, reduce(lambda a, b: [*a[k],*b[k]], results)) for k in results[0])
        else:  # Use hash join
            redist_tables = []
            redist_other_tables = []
            for i in range(len(self.tabular_datas)):
                redist_tables.append(self.tabular_datas[i].hash_partition(i, src1joincols, cols1, self.connections))
                redist_other_tables.append(otherTable.tabular_datas[i].hash_partition(i, src2joincols,
                                                                                      cols2, self.connections))
            r_tables = []
            r_other_tables = []
            for i in range(len(self.tabular_datas)):
                r_tables.append(redist_tables[1][i].vstack(*[redist_tables[j][i] for j in
                                                             range(1, len(self.tabular_datas))]))
                r_other_tables.append(redist_other_tables[1][i].vstack(*[redist_other_tables[j][i] for j in
                                                                         range(1, len(self.tabular_datas))]))

            def partition_join(table1, table2):
                return table1.join(table2, src1joincols, src2joincols, cols1, cols2, join).cdata

            futures = []
            for i in range(len(r_tables)):
                futures.append(self.executor.submit(partition_join, r_tables[i], r_other_tables[i]))
            results = []
            for f in concurrent.futures.as_completed(futures):
                results.append(f.result())
            return collections.OrderedDict((k, reduce(lambda a, b: [*a[k],*b[k]], results)) for k in results[0])

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
            self.combined_data = collections.OrderedDict((k, reduce(lambda a, b: [*a[k],*b[k]], results)) for k in results[0])
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
        return collections.OrderedDict((k, reduce(lambda a, b: [*a[k],*b[k]], results)) for k in results[0])


copyreg.pickle(DistTabularData, TabularDataRemoteStub.serializeObj);
