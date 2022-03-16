import concurrent.futures
import collections
import pandas as pd

from functools import reduce
from aidacommon.dborm import TabularData


class DistTabularData(TabularData):
    def join(self, otherTable, src1joincols, src2joincols, cols1=COL.NONE, cols2=COL.NONE, join=JOIN.INNER):
        if self.combined_data is None:
            futures = []
            for t in self.tabular_datas:
                futures.append(self.executor.submit(t.cdata))
            results = []
            for f in concurrent.futures.as_completed(futures):
                results.append(f.result())
            self.combined_data = collections.OrderedDict(reduce(lambda a, b: {**a, **b}, results))
        def load_dataframe(dict):
            return pd.DataFrame(dict, columns=dict.keys())
        futures = []
        for c in self.connections:
            futures.append(self.executor.submit(c._L, load_dataframe, self.combined_data))
        external_data = []
        concurrent.futures.wait(futures)
        for f in futures:
            external_data.append(f.result())
        futures = []

        def external_join(table1, table2):
            return table1.join(table2, src1joincols, src2joincols, cols1, col2, join).cdata
        for i in range(len(external_data)):
            futures.append(self.executor.submit(external_join, external_data[i], self.tabular_datas[i]))
        results = []
        for f in concurrent.futures.as_completed(futures):
            results.append(f.result())
        return collections.OrderedDict(reduce(lambda a,b : {**a, **b}, results))


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
            return reduce(lambda a,b: a + b, results)
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
            self.combined_data = collections.OrderedDict(reduce(lambda a, b: {**a, **b}, results))
        return self.combined_data

    def __init__(self, executor, connections):
        self.tabular_datas = []
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
        return collections.OrderedDict(reduce(lambda a, b: {**a, **b}, results))
