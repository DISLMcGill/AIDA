from __future__ import annotations
from abc import abstractmethod

from aidac.common.column import Column
from aidac.data_source.DataSource import DataSource
from aidac.exec.Executable import Executable
import pandas as pd
import uuid

import aidac.dataframe.Scheduler as Scheduler
sc = Scheduler.Scheduler()


class DataFrame:
    def __init__(self, table_name=None):
        self.__tid__ = uuid.uuid4()
        self.tbl_name = table_name
        self._transform_ = None

    @property
    def id(self):
        return self.__tid__

    @property
    def table_name(self):
        return self.tbl_name

    @property
    def shape(self) -> tuple[int, int]:
        pass

    @property
    def columns(self):
        cols = {}
        # create columns using pandas index column name and types
        for cname, ctype in zip(self._data_.dtypes.index, self._data_.dtypes):
            cols[cname] = Column(cname, ctype)
        return cols

    def __repr__(self) -> str:
        """
        @return: string representation of current dataframe
        """
        pass

    @abstractmethod
    def filter(self, exp: str): pass

    @abstractmethod
    def join(self, other: DataFrame, left_on: list | str, right_on: list | str, join_type: str):
        sc.schedule(self)
        """
        May involve mixed data source
        """

    @abstractmethod
    def aggregate(self, projcols, groupcols=None): pass

    @abstractmethod
    def project(self, cols: list | str): pass

    @abstractmethod
    def order(self, orderlist): pass

    @abstractmethod
    def distinct(self): pass

    @abstractmethod
    def preview_lineage(self): pass

    """
    All binary algebraic operations may involve data from different data source
    """
    @abstractmethod
    def __add__(self, other): pass

    @abstractmethod
    def __radd__(self, other): pass

    @abstractmethod
    def __mul__(self, other): pass

    @abstractmethod
    def __rmul__(self, other): pass

    @abstractmethod
    def __sub__(self, other): pass

    @abstractmethod
    def __rsub__(self, other): pass

    @abstractmethod
    def __truediv__(self, other): pass

    @abstractmethod
    def __rtruediv__(self, other): pass

    @abstractmethod
    def __pow__(self, power, modulo=None): pass

    @abstractmethod
    def __matmul__(self, other): pass

    @abstractmethod
    def __rmatmul__(self, other): pass

    @property
    @abstractmethod
    def T(self): pass

    @abstractmethod
    def __getitem__(self, item): pass

    #WARNING !! Permanently disabled  !
    #Weakref proxy invokes this function for some reason, which is forcing the dataframe objects to materialize.
    #@abstractmethod
    #def __len__(self): pass;

    @property
    @abstractmethod
    def shape(self): pass

    @abstractmethod
    def vstack(self, othersrclist): pass

    @abstractmethod
    def hstack(self, othersrclist, colprefixlist=None): pass

    @abstractmethod
    def describe(self): pass

    @abstractmethod
    def sum(self, collist=None): pass

    @abstractmethod
    def avg(self, collist=None): pass

    @abstractmethod
    def count(self, collist=None): pass

    @abstractmethod
    def countd(self, collist=None): pass

    @abstractmethod
    def countn(self, collist=None): pass

    @abstractmethod
    def max(self, collist=None): pass

    @abstractmethod
    def min(self, collist=None): pass

    @abstractmethod
    def head(self,n=5): pass

    @abstractmethod
    def tail(self,n=5): pass

    @property
    @abstractmethod
    def cdata(self): pass


class RemoteTable(DataFrame):
    def __init__(self, source: DataSource, tablename: str, parent: DataFrame = None, table_name: str=None):
        super().__init__(table_name)
        self.source = source
        self.tbl_name = tablename

        # try retrieve the meta info of the table from data source
        # if table does not exist, an error will occur
        self._link_table_meta()

        self.parent = parent
        self._data_ = None

    def _link_table_meta(self):
        """

        @return:
        """
        pass

    def to_string(self):
        if self.__data__ is None:
            self._materialize()

    def _materialize(self):
        pipes = self._schedule()
        self.__data__ = pipes.process()

