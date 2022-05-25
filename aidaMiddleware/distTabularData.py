import concurrent.futures
from concurrent.futures import as_completed
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
            results = {}
            def external_join(db, table1, table2, joincols1, joincols2, cols1, cols2, join):
                from concurrent.futures import ThreadPoolExecutor
                from aidas.dborm import DataFrame
                table = table1[db]
                other_table = None
                if db in table2:
                    other_table = table2[db]
                    del table2[db]
                if len(table2) > 0:
                    with ThreadPoolExecutor() as executor:
                        result = []
                        for i in executor.map(lambda con: DataFrame._loadExtData_(lambda: table2[con].cdata, con),
                                              table2.keys()):
                            result.append(i)
                    if other_table is None:
                        if len(result) > 1:
                            other_table = result[0].vstack(result[1:])
                        else:
                            other_table = result[0]
                    else:
                        other_table = other_table.vstack(result)
                return table.join(other_table, joincols1, joincols2, cols1, cols2, join)
            def dist_join(con):
                return con._X(external_join, self.tabular_datas, otherTable.tabular_datas,
                                                  src1joincols, src2joincols, cols1, cols2, join)
            futures = {self.executor.submit(dist_join, con): con for con in
                   self.tabular_datas.keys()}
            for future in as_completed(futures):
                results[futures[future]] = future.result()
            return DistTabularData(self.executor, results, self.dbc)
        else:  # Use hash join
            cons = list(set().union(self.tabular_datas.keys(), otherTable.tabular_datas.keys()))
            def hash_partition(db, table, keys, connections):
                from concurrent.futures import ThreadPoolExecutor, as_completed
                import logging
                import time
                index = cons.index(db)
                def load_data(df):
                    return df

                start = time.perf_counter()
                indices = [[] for i in range(len(connections))]
                if isinstance(keys, str):
                    for i in range(len(table[keys])):
                        h = hash(table[keys][i])
                        indices[h % len(connections)].append(i)
                else:
                    tu = list(zip(*[table[k] for k in keys]))
                    for i in range(len(tu)):
                        h = hash(tu[i])
                        indices[h % len(connections)].append(i)
                tables = []
                chkpt_1 = time.perf_counter()

                t = [self[i[0]] for i in indices]
                for i in range(len(connections)):
                    t[i] = t[i].vstack([self[indices[i][j]] for j in range(1, len(indices[i]))])
                chkpt_2 = time.perf_counter()
                ind = list(range(len(connections)))
                ind.remove(index)

                with ThreadPoolExecutor() as executor:
                    for i in executor.map(lambda j: connections[j]._L(load_data, t[j].cdata), ind):
                        tables.append(i)

                tables.insert(index, t[index])
                chkpt_3 = time.perf_counter()
                logging.warning("hash time: {}".format(chkpt_1 - start))
                logging.warning("stack tables: {}".format(chkpt_2 - chkpt_1))
                logging.warning("send tables: {}".format(chkpt_3 - chkpt_2))
                return tables
            start = time.perf_counter()
            def partition_table(table, joincols):
                tables = []
                for i in self.executor.map(lambda con: con._X(hash_partition, table[con], joincols, cons),table.keys()):
                    tables.append(i)
                return tables

            redist_tables = partition_table(self.tabular_datas, src1joincols)
            redist_other_tables = partition_table(otherTable.tabular_datas, src2joincols)
            chkpt_1 = time.perf_counter()
            def stack_table(tables):
                t = []
                for i in self.executor.map(lambda j: tables[0][j].vstack(*[tables[j][k] for k in range(1, len(cons))]), range(len(cons))):
                    t.append(i)
                return t

            r_tables = stack_table(redist_tables)
            r_other_tables = stack_table(redist_other_tables)
            chkpt_2 = time.perf_counter()
            def partition_join(i):
                return r_tables[i].join(r_other_tables[i], src1joincols, src2joincols, cols1, cols2, join)

            results = {}
            futures = {self.executor.submit(partition_join, i): i for i in range(len(r_tables))}
            for future in as_completed(futures):
                results[cons[futures[future]]] = future.result()
            chkpt_3 = time.perf_counter()
            logging.warning("hash partition time: {}".format(chkpt_1-start))
            logging.warning("vstack time: {}".format(chkpt_2-chkpt_1))
            logging.warning("join time: {}".format(chkpt_3-chkpt_2))
            return DistTabularData(self.executor, results, self.dbc)

    def aggregate(self, projcols, groupcols=None):
        df = DataFrame._loadExtData_(lambda: self.cdata, self.dbc)
        result = {self.dbc: DataFrame(df, SQLAggregateTransform(df, projcols, groupcols))}
        return DistTabularData(self.executor, result, self.dbc)

    def project(self, projcols):
        results = {}
        futures = {self.executor.submit(self.tabular_datas[con].project, projcols): con for con in
                   self.tabular_datas.keys()}
        for future in as_completed(futures):
            results[futures[future]] = future.result()

        return DistTabularData(self.executor, results, self.dbc)

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
        results = {}
        futures = {self.executor.submit(self.tabular_datas[con].__getitem__, item): con for con in self.tabular_datas.keys()}
        for future in as_completed(futures):
            results[futures[future]] = future.result()

        return DistTabularData(self.executor, results, self.dbc)

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
        for t in self.tabular_datas.values():
            futures.append(self.executor.submit(t.sum, collist))
        results = []

        for f in as_completed(futures):
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
        for t in self.tabular_datas.values():
            futures.append(self.executor.submit(t.count, collist))
        results = []

        for f in as_completed(futures):
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
        for t in self.tabular_datas.values():
            futures.append(self.executor.submit(lambda: t.cdata))
        results = []
        for f in as_completed(futures):
            results.append(f.result())
        result = collections.OrderedDict(
            (k, reduce(lambda a, b: np.asarray([*a[k], *b[k]]), results)) for k in results[0])
        return result

    def __init__(self, executor, tabular_datas, dbc):
        self.dbc = dbc
        self.tabular_datas = tabular_datas
        self.executor = executor

    def filter(self, *selcols):
        results = {}
        futures = {self.executor.submit(lambda: self.tabular_datas[con].filter(*selcols)): con for con in
                   self.tabular_datas.keys()}
        for future in as_completed(futures):
            results[futures[future]] = future.result()

        return DistTabularData(self.executor, results, self.dbc)

copyreg.pickle(DistTabularData, DistTabularDataRemoteStub.serializeObj)
