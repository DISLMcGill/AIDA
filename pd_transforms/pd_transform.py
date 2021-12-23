"""
base class of pandas transform
"""
import logging
import weakref

import pandas as pd

from aidacommon.dborm import COL, AggregateSQLFunction
from aidas.dborm import SQLSelectTransform, SQLProjectionTransform, SQLJoinTransform
from pd_transforms.pd_helper import *


class PDTransform:
    def __init__(self, source, columns):
        """
        This object should be created after the SQLTransform
        @param source: source TabularDataObjetc
        @param columns: columns is obtained from the columns property of the corresponding SQLTransform object
        """
        self._source_ = weakref.proxy(source) if source else None
        self.columns = columns

    def __init__(self, source1, source2, columns):
        self._source1_ = weakref.proxy(source1) if source1 else None
        self._source2_ = weakref.proxy(source2) if source2 else None
        self.columns = columns

    def materialize(self):
        pass

    def source_shape(self):
        pass


class PDSelectTransform(PDTransform, SQLSelectTransform):
    def __init__(self, source, *selcols):
        super().__init__(source)
        self.__selcols__  = selcols;

    def materialize(self):
        conditions = None
        data = self._source_.__pdData__
        #logging.info(f'[{time.ctime()}] execute transform pandas, data type = {type(data)}')

        #convert ordered dict to pandas df
        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame.from_dict(data)

        for sc in self.__selcols__:
            # logging.info(f'[{time.ctime()}] condition : {sc}, Expression: {sc.columnExpr}, collist: {sc.srcColList}')
            logging.info('operator: {}, col1: {}, col2: {}'.format(sc._operator_, sc._col1_, sc._col2_))
            if conditions is None:
                conditions = select2pandas(data, sc)
            else:
                conditions = conditions & select2pandas(data, sc)

        data = data[conditions]
        # logging.info(f'[{time.ctime()}] executed pandas select, condition = {conditions}, data = {data.head(3)}')

        return data


class PDProjectionTransform(PDTransform, SQLProjectionTransform):
    def __init__(self, source, projcols):
        super().__init__(source);
        self.__projcols__  = projcols if isinstance(projcols, tuple) else (projcols, )

    def materialize(self):
        data = self._source_.__pdData__

        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame.from_dict(data)

        # Convert __proj_cols__ to param list used by pandas
        # assign_params is used to handle F class and create a new column,
        # rename_params is used for rename column names later,
        # proj_cols is the final columns selected
        assign_params, rename_params = {}, {}
        proj_cols = set()

        for c in self.__projcols__:
            if (isinstance(c, dict)):  # check if the projected column is given an alias name
                sc1 = list(c.keys())[0];  # get the source column name / function
                pc1 = c.get(sc1) # and the alias name for projection.
            else:
                sc1 = pc1 = c  # otherwise projected column name / function is the same as the source column.

            if isinstance(sc1, str): # case {'col'} or {'col': 'new col'}
                proj_cols.add(sc1) # select original column first
                if sc1 != pc1:
                    rename_params[sc1] = pc1 # then rename it
            else:
                proj_cols.add(pc1) # case F class
                assign_params[pc1] = f2pandas(data, sc1)

        # get all columns required, and do computation on columns if needed
        data = data.assign(**assign_params)[proj_cols] if assign_params else data[proj_cols]
        #rename columns if required
        if rename_params:
            data.rename(**{'columns': rename_params, 'inplace': True})
        # logging.info(f"Projection result = {data}, type = {data.dtypes}")
        return data


class PDJoinTransform(PDTransform, SQLJoinTransform):
    def __init__(self, source1, source2, src1joincols, src2joincols, cols1=COL.NONE, cols2=COL.NONE, join=JOIN.INNER):
        super().__init__(source1, source2)

        if (not join == JOIN.CROSS_JOIN) and len(src1joincols) != len(src2joincols):
            raise AttributeError('src1joincols and src2joincols should have same number columns');

        self._src1joincols_ = src1joincols
        self._src2joincols_ = src2joincols
        self._src1projcols_ = cols1
        self._src2projcols_ = cols2
        self._jointype_ = join

    def materialize(self):
        data1 = self._source1_.__pdData__
        data2 = self._source2_.__pdData__

        rename_params = self.get_rename_cols(self._src1projcols_)
        rename_params.update(self.get_rename_cols(self._src2projcols_))

        proj_cols = [c.columnName if not isinstance(c, str) else c for c in self.columns]
        if self._jointype_ == JOIN.CROSS_JOIN:
            #logging.info(
            #    f'column info: {self.columns} \n proj_cols = {proj_cols} , rename = {rename_params} \n, '
            #    f'data1 columns= {data1.columns}, \n data2 = {data2.columns}')
            data1['_key'] = 0
            data2['_key'] = 0
            data = data1.merge(data2, on='_key')
        else:
            data = pd.merge(data1, data2, left_on=self._src1joincols_, right_on=self._src2joincols_,
                            how=PJOIN_MAP[self._jointype_])
        if rename_params:
            data.rename(**{'columns': rename_params, 'inplace': True})
        return data[proj_cols]


class PDAggregationTransform(PDTransform, SQLAggregationTransform):
    def __init__(self, source, projcols, groupcols=None):
        super().__init__(source);
        self.__projcols__  = projcols  if(isinstance(projcols, tuple))  else (projcols, );
        self.__groupcols__ = groupcols if(isinstance(groupcols, tuple)) else ( (groupcols, ) if(groupcols) else None );

    def materialize(self):
        data = self._source_.__pdData__ if self._source_.__pdData__ is not None else self._source_.execute_pandas()
        #convert ordered dict to pandas df
        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame.from_dict(data)

        agg_params, rename_params = {}, {}
        # proj_cols = [c.columnName if not isinstance(c, str) else c for c in self.columns]
        proj_cols = []
        group_cols = []
        if self.__groupcols__:
            for g in self.__groupcols__:
                rename_params[(g, '')] = g
                group_cols.append(g)

       # get groupbyDataframe
        if group_cols:
            data = data.groupby(group_cols)
        else:
            # group on entire dataset
            data = data.groupby(lambda _: True)
        count_col = None

        for col in self.__projcols__:
            custom_name = True

            if isinstance(col, dict):
                sc1 = list(col.keys())[0];  # get the source column name / function
                pc1 = col.get(sc1);  # and the alias name for projection.
            else:
                sc1 = pc1 = col
                custom_name = False
            proj_cols.append(pc1)
            if isinstance(sc1, AggregateSQLFunction):
                col_name = sc1.__srcColName__
                dname, adname, func = PAGG_MAP[sc1.__funcName__]
                # calculate COUNT(*) by the size of group dataframe directly
                if col_name == '*':
                    count_col = data.size()
                    rename_params[('_count', '')] = pc1 if custom_name else '_count'
                    # if there is no other aggregation functions
                    rename_params['_count'] = pc1 if custom_name else '_count'
                # other aggregation operation/ has a specific column
                else:
                    if col_name in agg_params:
                        agg_params[col_name].append(func)
                    else:
                        agg_params[col_name] = [func]
                    rename_params[(col_name, dname)] = pc1 if custom_name else adname + col_name
            else:
                rename_params[(sc1, '')] = pc1
                rename_params[sc1] = pc1

        # aggregate
        if agg_params:
            data = data.agg(agg_params)
            # add the count column if there is operation COUNT(*)
        else:
            # dummy operation, to get pd.dateframe object
            data = data.count()
        if count_col is not None:
            data = data.assign(_count=count_col)
        # handle 2D index caused by aggregation
        # move group by columns from index to data column
        data = data.reset_index()
        data.columns = data.columns.to_flat_index()
        if rename_params:
            # data.rename(columns=rename_params, inplace=True)
            data.rename(**{'columns': rename_params, 'inplace': True})
        # logging.info(f'Agg pandas, pandas columns = {data.columns}, proj_col = {proj_cols}, groups = {group_cols}, rename = {rename_params} \n {data.head(10)}')
        return data[proj_cols]


class PDOrderTransform(PDTransform):
    def __init__(self, source, orderlist):
        super().__init__(source);
        self._colorderlist_ = orderlist;

    def materialize(self, doOrder=True):
        data = self._source_.__pdData__ if self._source_.__pdData__ is not None else self._source_.execute_pandas()
        # logging.info(f'[{time.ctime()}] execute order pandas, data type = {type(data)}')
        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame.from_dict(data)

        if (not doOrder):
            return data
        else:
            colorderlist = [self._colorderlist_] if(isinstance(self._colorderlist_, str)) else self._colorderlist_;
            # logging.info(f'order vars: {colorderlist}')
            sortcol = []
            sortascend = []
            for ocol in colorderlist:
                if(ocol.endswith('#asc')):
                    sortcol.append(ocol[:-4]);
                    sortascend.append(True)
                elif(ocol.endswith('#desc')):
                    sortcol.append(ocol[:-5] )
                    sortascend.append(False)
                else:
                    sortcol.append(ocol);
                    sortascend.append(True)
            # logging.info(f'sort col = {sortcol}, sortascend = {sortascend}')
            data = data.sort_values(by=sortcol, ascending=sortascend)
            return data


class PDDistinctTransform(PDTransform):
    def __init__(self, source):
        super().__init__(source);

    def execute_pandas(self):
        data = self._source_.__pdData__ if self._source_.__pdData__ is not None else self._source_.execute_pandas()
        # logging.info(f'[{time.ctime()}] execute order pandas, data type = {type(data)}')
        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame.from_dict(data)

        return data.drop_duplicates()