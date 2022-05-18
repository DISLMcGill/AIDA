import concurrent.futures
import collections
import numpy as np
import copyreg;
import time;

from functools import reduce
from aidacommon.rdborm import DistTabularDataRemoteStub
from aidas.dborm import *


class DistTabularData(TabularData):
    def join(self, otherTable, src1joincols, src2joincols, cols1=COL.NONE, cols2=COL.NONE, join=JOIN.INNER, hash_join=False):
        if not hash_join:  # Use block join
            results = []
            def external_join(db, index, table1, table2, joincols1, joincols2, cols1, cols2, join):
                from concurrent.futures import ThreadPoolExecutor
                from aidas.dborm import DataFrame
                other_indices = list(range(len(table1)))
                other_indices.remove(index)
                with ThreadPoolExecutor() as executor:
                    result = []
                    for i in executor.map(lambda j: DataFrame._loadExtData_(lambda t: t, db, table1[j].cdata, db),
                                          other_indices):
                        result.append(i)
                    table = table1[index].vstack(result)
                    return table.join(table2[index], joincols1, joincols2, cols1, cols2, join)
            def dist_join(index):
                return self.connections[index]._X(external_join, index, self.tabular_datas, otherTable.tabular_datas,
                                                  src1joincols, src2joincols, cols1, cols2, join)
            for i in self.executor.map(dist_join, range(len(self.tabular_datas))):
                results.append(i)
            return DistTabularData(self.executor, self.connections, self.tabular_datas, self.dbc)
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
                for i in self.executor.map(lambda j: tables[0][j].vstack(*[tables[j][k] for k in range(1, len(self.tabular_datas))]), range(len(self.tabular_datas))):
                    t.append(i)
                return t

            r_tables = stack_table(redist_tables)
            r_other_tables = stack_table(redist_other_tables)
            chkpt_2 = time.perf_counter()
            def partition_join(i):
                return r_tables[i].join(r_other_tables[i], src1joincols, src2joincols, cols1, cols2, join)

            results = []
            for i in self.executor.map(partition_join, range(len(r_tables))):
                results.append(i)
            chkpt_3 = time.perf_counter()
            print("hash partition time: {}".format(chkpt_1-start))
            print("vstack time: {}".format(chkpt_2-chkpt_1))
            print("join time: {}".format(chkpt_3-chkpt_2))
            return DistTabularData(self.executor, self.connections, results, self.dbc)

    def aggregate(self, projcols, groupcols=None):
        df = DataFrame._loadExtData_(lambda: self.cdata, self.dbc)
        return DataFrame(df, SQLAggregateTransform(df, projcols, groupcols))

    def project(self, projcols):
        results = []
        for i in self.executor.map(lambda t: t.project(projcols), self.tabular_datas):
            results.append(i)

        return DistTabularData(self.executor, self.connections, results, self.dbc)

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
        results = []
        for i in self.executor.map(lambda t: t.__getitem__(item), self.tabular_datas):
            results.append(i)

        return DistTabularData(self.executor, self.connections, results, self.dbc)

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
            result = collections.OrderedDict()
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
        futures = []
        for t in self.tabular_datas:
            futures.append(self.executor.submit(lambda: t.cdata))
        results = []
        for f in concurrent.futures.as_completed(futures):
            results.append(f.result())
        result = collections.OrderedDict(
            (k, reduce(lambda a, b: np.asarray([*a[k], *b[k]]), results)) for k in results[0])
        return result

    def __init__(self, dbc, executor, connections, tabular_datas):
        self.dbc = dbc
        self.tabular_datas = tabular_datas
        self.connections = connections
        self.executor = executor

    def filter(self, *selcols):
        results = []
        for i in self.executor.map(lambda t: t.filter(*selcols), self.tabular_datas):
            results.append(i)

        return DistTabularData(self.executor, self.connections, results, self.dbc)

    def hash_partition(self, index, keys, cols, connections): pass;

copyreg.pickle(DistTabularData, DistTabularDataRemoteStub.serializeObj)
