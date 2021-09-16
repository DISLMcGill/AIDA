import cProfile;
import time;
import timeit;
import weakref;
import collections;
import re;
import copy;
import uuid;

import logging;

import numpy as np;
import pandas as pd;

from aidacommon.aidaConfig import AConfig, UDFTYPE;
from aidacommon.dborm import *;
from aidacommon.dbAdapter import DBC;
from aidas.pamp import *
from aidacommon.utils import VirtualOrderedColumnsDict;
from aidas.BlockManagerUnconsolidated import df_from_arrays

#Simple wrapper class to encapsulate a query string.
class SQLQuery:
    def __init__(self, query):
        self.__query   = None;
        if(not isinstance(query, str)):
            raise TypeError("Argument query should be of type str, not {}".format(type(query)));
        self.__query = query;

    @property
    def sqlText(self): return self.__query;

    def __str__(self):
        return "<{} : {}>".format(type(self), self.__query);

#Base class for all data transformations.
class Transform:
    def applyTransformation(self, data):
        pass;

#-------------- Unused --------------------------
class ColumnTransform(Transform):
    def __init__(self, colTransformFunc):
        self.colTransformFunc = colTransformFunc;

    def applyTransformation(self, data):
        return self.colTransformFunc(data);

class TableTransform(Transform):
    def __init__(self, tableTransformFunc):
        self.tableTransformFunc = tableTransformFunc;

    def applyTransformation(self, data):
        return self.tableTransformFunc(data);
#------------------------------------------------

#Base class for all SQL Transformations.
class SQLTransform(Transform):
    def __init__(self, source):
        self._source_    = weakref.proxy(source) if(source) else None;
        self.__columns__ = None;
    #The columns that will be produced once this transform is applied on its source.
    @property
    def columns(self):
        if(not self.__columns__):
            self.__columns__ = copy.deepcopy(self._source_.columns);
        return self.__columns__;

    #The SQL equivalent of applying this transformation in the database.
    @property
    def genSQL(self):pass;

    #assume data is already loaded from the db into the memory
    def execute_pandas(self): pass

    def gen_lineage(self): pass

    def get_data(self):
        data = self._source_.__pdData__ if self._source_.__pdData__ else self._source_.execute_pandas()
        return data

class SQLSelectTransform(SQLTransform):

    def __init__(self, source, *selcols):
        super().__init__(source);
        self.__selcols__  = selcols;

    def gen_lineage(self):
        nodes = []
        for sc in self.__selcols__:
            if isinstance(sc._col2_, DataFrame):
                nodes.append(sc._col2_.gen_lineage())
        return nodes

    def execute_pandas(self):
        conditions = None
        data = self._source_.__pdData__ if self._source_.__pdData__ is not None else self._source_.execute_pandas()
        #logging.info(f'[{time.ctime()}] execute transform pandas, data type = {type(data)}')

        #convert ordered dict to pandas df
        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame.from_dict(data)

        for sc in self.__selcols__:
            # logging.info(f'[{time.ctime()}] condition : {sc}, Expression: {sc.columnExpr}, collist: {sc.srcColList}')
            # logging.info(f'[{time.ctime()}] operator: {sc._operator_}, col1: {sc._col1_}, col2: {sc._col2_}')
            if conditions is None:
                conditions = select2pandas(data, sc)
            else:
                conditions = conditions & select2pandas(data, sc)

        data = data[conditions]
        # logging.info(f'[{time.ctime()}] executed pandas select, condition = {conditions}, data = {data.head(3)}')

        return data

    @property
    def genSQL(self):
        selCondition = '';
        for sc in self.__selcols__: #iterate through the filter conditions.
            srccollist = sc.srcColList; #get the source columns list involved in each one.
            for s in srccollist:
                c=self._source_.columns.get(s) #check if the columns are present in the source
                if(not c): #column not found in source.
                    raise AttributeError(c);
            #Append it to the select SQL condition.
            selCondition = ( (selCondition + ' AND ') if(selCondition) else ''  ) + sc.columnExpr;

        cols = None;
        for c in self.columns: #Form list of columns for select
            cols = (cols + ' ,' + c) if(cols) else c;
        sqlText =   ( 'SELECT ' + cols + ' FROM ' +
                          # This is the SQL for the source table
                         '(' + (self._source_.genSQL.sqlText) + ') ' + self._source_.tableName +
                      ' WHERE ' + selCondition
                    );
        return SQLQuery(sqlText);


#Transformation to capture SQL joins.
class SQLJoinTransform(SQLTransform):
    def __init__(self, source1, source2, src1joincols, src2joincols, cols1=COL.NONE, cols2=COL.NONE, join=JOIN.INNER):
        super().__init__(None);

        if(not(join == JOIN.CROSS_JOIN) and len(src1joincols) != len(src2joincols)):
            raise AttributeError('src1joincols and src2joincols should have same number columns');

        self._source1_      = weakref.proxy(source1);   self._source2_      = weakref.proxy(source2);
        self._src1joincols_ = src1joincols;             self._src2joincols_ = src2joincols;
        self._src1projcols_ = cols1;                    self._src2projcols_ = cols2;

        self._jointype_ = join;

    @property
    def columns(self):
        if(not self.__columns__):
            src1cols = self._source1_.columns;
            src2cols = self._source2_.columns;

            def __extractsrccols__(sourcecols, projcols, tableName=None):
                if(projcols is None or projcols == COL.NONE):
                    return [];
                if(projcols == COL.ALL): #All columns are requested from this table.
                    projcols=sourcecols;
                projcolumns = list();
                for c in projcols:
                    if(hasattr(c,'get')): #if projection is specified as dictionary, it is a source column name, projected column name combination.
                       srccol=list(c.keys())[0]; projcol=c.get(srccol);
                    else:
                        srccol=projcol=c; #otherwise we default projected column name to be the same as source column name.
                    scol = sourcecols.get(srccol);
                    if(not scol):
                        raise AttributeError(srccol);
                    pc = copy.deepcopy(scol); #Make a copy of the source column specs.
                    pc.columnName = projcol;   #Reset the mutable fields for projected column.
                    if(tableName):
                        pc.tableName = tableName;
                    pc.sourceColumnName = [scol.columnName];
                    pc.colTransform = None;
                    projcolumns.append((projcol,pc));
                    #projcolumns[projcol] = pc;
                return projcolumns;

            #columns generated by the join transform can contain columns from both the tables.
            self.__columns__ = collections.OrderedDict(__extractsrccols__(src1cols, self._src1projcols_, self._source1_.tableName) +  __extractsrccols__(src2cols, self._src2projcols_, self._source2_.tableName));
            #self.__columns__ = { **__extractsrccols__(src1cols, self._src1projcols_), **__extractsrccols__(src2cols, self._src2projcols_) };
        return self.__columns__;

    def get_rename_cols(self, project_list):
        rename_params = {}
        if project_list is not None and project_list != COL.NONE and project_list != COL.ALL:
            for c in project_list:
                if (hasattr(c, 'get')):  # if projection is specified as dictionary, it is a source column name, projected
                    # column name combination.
                    srccol = c.keys()[0]
                    projcol = c.get(srccol)
                    rename_params[srccol] = projcol
        return rename_params

    def execute_pandas(self):
        data1 = self._source1_.__pdData__ if self._source1_.__pdData__ is not None else self._source1_.execute_pandas()
        data2 = self._source2_.__pdData__ if self._source2_.__pdData__ is not None else self._source2_.execute_pandas()
        # logging.info(f'[{time.ctime()}] execute join pandas, data1 = {data1.head()}, \n data2 = {data2.head()} \n -----------------------------------\n '
        #              f'query = {self.genSQL} \n -------------------------- -----------\n')
        # logging.info(f'[{time.ctime()}] execute join pandas, data2 type = {type(data1)}')
        #convert ordered dict to pandas df
        if not isinstance(data1, pd.DataFrame):
            data1 = pd.DataFrame.from_dict(data1)
        if not isinstance(data2, pd.DataFrame):
            data2 = pd.DataFrame.from_dict(data2)

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

    @property
    def genSQL(self):

        projcoltxt=None;
        for c in self.columns: #Prepare the list of columns going into the select statement.
            col = self.columns[c];
            projcoltxt = ((projcoltxt+', ') if(projcoltxt) else '')  +  (col.tableName + '.' + col.sourceColumnName[0] + ' AS ' + col.columnName);

        jointxt=None; #The join condition SQL.
        if(self._jointype_ == JOIN.CROSS_JOIN):
            jointxt = '';
        elif(isinstance(self._src1joincols_, str)):
            jointxt = ' ON ' + self._source1_.tableName + '.' + self._src1joincols_ + ' = ' + self._source2_.tableName + '.' + self._src2joincols_;
        else:
            for j in range(len(self._src1joincols_)):
                jointxt = ((jointxt + ' AND ') if(jointxt) else ' ON ') + self._source1_.tableName + '.' + self._src1joincols_[j] + ' = ' + self._source2_.tableName + '.' + self._src2joincols_[j];

        sqlText = ( 'SELECT ' + projcoltxt + ' FROM '
                    + '(' + self._source1_.genSQL.sqlText + ') ' + self._source1_.tableName #SQL for source table 1
                    + ' ' + self._jointype_.value  + ' '
                    + '(' + self._source2_.genSQL.sqlText + ') ' + self._source2_.tableName  #SQL for source table 2
                        + jointxt
                    );

        return SQLQuery(sqlText);

#
##Base aggregation function.
#class AggregateSQLFunction(metaclass=ABCMeta):
#    def __init__(self, srcColName, distinct=False, funcName=None):
#        self.__srcColName__  = srcColName;
#        self.__distinct__    = distinct;
#        self.__funcName__    = funcName;
#
#    @property
#    def funcName(self):
#        return self.__funcName__;
#
#    @property
#    def sourceColumn(self):
#        return self.__srcColName__;
#
#    @property
#    def genSQL(self):
#        return self.__funcName__ + '(' +  ('DISTINCT ' if(self.__distinct__) else '') + self.__srcColName__ + ')';
#
##Specific types of aggregation functions.
#class COUNT(AggregateSQLFunction):
#    def __init__(self, srcColName, distinct=False):
#        super().__init__(srcColName, distinct=distinct, funcName='COUNT');
#
#    @property
#    def genSQL(self):
#        return self.__funcName__ + '(*)' if(self.__srcColName__ == '*') else super().genSQL;
#
#class MAX(AggregateSQLFunction):
#    def __init__(self, srcColName, distinct=False):
#        super().__init__(srcColName, distinct=distinct, funcName='MAX');
#
#class MIN(AggregateSQLFunction):
#    def __init__(self, srcColName, distinct=False):
#        super().__init__(srcColName, distinct=distinct, funcName='MIN');
#
#class AVG(AggregateSQLFunction):
#    def __init__(self, srcColName, distinct=False):
#        super().__init__(srcColName, distinct=distinct, funcName='AVG');
#
#class SUM(AggregateSQLFunction):
#    def __init__(self, srcColName, distinct=False):
#        super().__init__(srcColName, distinct=distinct, funcName='SUM');

class SQLAggregateTransform(SQLTransform):

    def __init__(self, source, projcols, groupcols=None):
        super().__init__(source);
        self.__projcols__  = projcols  if(isinstance(projcols, tuple))  else (projcols, );
        self.__groupcols__ = groupcols if(isinstance(groupcols, tuple)) else ( (groupcols, ) if(groupcols) else None );

    @property
    def columns(self):
        if(not self.__columns__):

            def __getProjColInfo__(c):#check if the projected column is given an alias name
                if(isinstance(c, dict)):
                    sc1 = list(c.keys())[0];    #get the source column name / function
                    pc1 = c.get(sc1);           #and the alias name for projection.
                else:
                    sc1 = pc1 = c;              #otherwise projected column name / function is the same as the source column.
                srccol = sc1.sourceColumn if(isinstance(sc1, AggregateSQLFunction)) else sc1; #Get the name of the source column
                #projection column alias name if specifed use it, otherwise dervice one from the function/source column names.
                projcol = (pc1.funcName.lower() + '_' + (pc1.sourceColumn if(pc1.sourceColumn != '*') else '') ) if(isinstance(pc1, AggregateSQLFunction)) else pc1;
                #The transformation function on the source column.
                coltransform = sc1 if(isinstance(sc1, AggregateSQLFunction)) else None;
                return (srccol, projcol, coltransform);

            cols = self._source_.columns;
            columns = collections.OrderedDict();
            #columns = {};
            for j in range(len(self.__projcols__)):
                col = self.__projcols__[j];
                (srccoln, projcoln, coltransform) = __getProjColInfo__(col);

                #Create a copy of column metadata for this transformation.
                if(coltransform and coltransform.sourceColumn == '*'):
                    tmptblcolumn = list(cols.values())[0];
                    column = DBTable.Column( (tmptblcolumn.schemaName, tmptblcolumn.dbTableName, projcoln, DBTable.Column.TYPE.INT.value, DBTable.Column.TYPE.INT.size, 0, False) );
                else:
                    column = cols.get(srccoln);
                if(not column):
                    raise AttributeError(srccoln);

                column = copy.deepcopy(column);
                column.sourceColumnName = [column.columnName];
                column.columnName = projcoln;
                column.colTransform = coltransform;
                columns[projcoln] = column;

            self.__columns__ = columns;

        return self.__columns__;

    def execute_pandas(self):
        # todo: no given name and *
        data = self._source_.__pdData__ if self._source_.__pdData__ is not None else self._source_.execute_pandas()
        #convert ordered dict to pandas df
        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame.from_dict(data)

        agg_params, rename_params = {}, {}
        proj_cols = [c.columnName if not isinstance(c, str) else c for c in self.columns]
        group_cols = []
        if self.__groupcols__:
            for g in self.__groupcols__:
                rename_params[(g, '')] = g
                group_cols.append(g)

        # logging.info(
        #     f'self column info: {self.columns} \n group_cols = {group_cols} \n, data columns= {data.columns}, \n data = {data.head()}')
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
        # logging.info(
        #     f'Agg pandas, pandas columns = {data.columns}, proj_col = {proj_cols}, groups = {group_cols}, '
        #     f'rename = {rename_params} \n {data.head(10)}')

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

    @property
    def genSQL(self):

        projcoltxt=None;
        for c in self.columns: #Prepare the list of columns going into the select statement.
            col = self.columns[c];
            projcoltxt = ((projcoltxt+', ') if(projcoltxt) else '')  +  ( (col.colTransform.genSQL if(col.colTransform) else col.sourceColumnName[0]) + ' AS ' + col.columnName);

        groupcoltxt=None;
        if(self.__groupcols__):
            for g in self.__groupcols__:
                groupcoltxt = ((groupcoltxt+', ') if(groupcoltxt) else '') + g;

        sqlText =   (  'SELECT ' + projcoltxt + ' FROM '
                        +   '(' + self._source_.genSQL.sqlText + ') ' + self._source_.tableName  # Source table transform SQL.
                        + ((' GROUP BY ' + groupcoltxt) if(groupcoltxt)  else '')
                    );

        return SQLQuery(sqlText);


class SQLProjectionTransform(SQLTransform):
    def __init__(self, source, projcols):
        super().__init__(source);
        self.__projcols__  = projcols  if(isinstance(projcols, tuple))  else (projcols, );

    @property
    def columns(self):
        if(not self.__columns__):

            colcount=0;
            def __getProjColInfo__(c):
                nonlocal  colcount;
                colcount += 1;
                if(isinstance(c, dict)):#check if the projected column is given an alias name
                    sc1 = list(c.keys())[0];    #get the source column name / function
                    pc1 = c.get(sc1);           #and the alias name for projection.
                else:
                    sc1 = pc1 = c;              #otherwise projected column name / function is the same as the source column.
                #get a list of source columns needed for this expression.
                srccollist = [sc1] if(isinstance(sc1, str)) else ( sc1.srcColList if(hasattr(sc1, 'srcColList')) else []  )
                #projected column alias, use the one given, else take it from the expression if it has one, or else generate one.
                projcol = pc1 if(isinstance(pc1, str)) else (sc1.columnExprAlias  if(hasattr(sc1, 'columnExprAlias')) else 'col_'.format(colcount))
                coltransform = sc1 if(isinstance(sc1, F)) else None;
                return (srccollist, projcol, coltransform);

            cols = self._source_.columns;
            #columns = {};
            columns = collections.OrderedDict();
            for j in range(len(self.__projcols__)):
                col = self.__projcols__[j];
                (srccolnlist, projcoln, coltransform) = __getProjColInfo__(col);

                sdbcols = []; sdbtables = []; srccols = [];
                for cn in srccolnlist:
                    scol = cols.get(cn);
                    if(not scol):
                        raise AttributeError("Cannot locate {} among {}".format(cn, srccolnlist.keys()));
                    else:
                        sdbcols +=  (scol.dbColumnName if(isinstance( scol.dbColumnName, list)) else [scol.dbColumnName]);
                        srccols +=  (scol.columnName if(isinstance( scol.columnName, list)) else [scol.columnName]);
                        sdbtables +=  (scol.dbTableName if(isinstance( scol.dbTableName, list)) else [scol.dbTableName]);
                column = DBTable.Column.makeEmptyColumn() if(len(sdbcols) == 0) else copy.deepcopy(scol);
                column.columnName = projcoln;
                column.dbColumnName = sdbcols;
                column.sourceColumnName = srccols;
                column.dbTableName = sdbtables;
                column.colTransform = coltransform;
                columns[projcoln] = column;

            self.__columns__ = columns;

        return self.__columns__;

    def execute_pandas(self):
        self.columns
        data = self._source_.__pdData__ if self._source_.__pdData__ is not None else self._source_.execute_pandas()
        # logging.info(f'[{time.ctime()}] execute projection pandas, data = {data.columns}, type = {data.dtypes}\n--------------------------\n '
        #              f'sql = {self.genSQL} \n--------------------------\n')

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
                pc1 = c.get(sc1);  # and the alias name for projection.
            else:
                sc1 = pc1 = c;  # otherwise projected column name / function is the same as the source column.

            #logging.info(f'{time.ctime()} Project columns: c = {c}, c type = {type(c)}, \n')
            # if hasattr(sc1, "__dict__"):
            #     items = [f'{key}: {val}' for key, val in sc1.__dict__.items()]
                #logging.info(', '.join(items))

            if isinstance(sc1, str): # case {'col'} or {'col': 'new col'}
                proj_cols.add(sc1) # select original column first
                if sc1 != pc1:
                    rename_params[sc1] = pc1 # then rename it
            else:
                proj_cols.add(pc1) # case F class
                assign_params[pc1] = f2pandas(data, sc1)

        # get all columns required, and do computation on columns if needed
        data = data.assign(**assign_params)[proj_cols] if assign_params else data[proj_cols]
        # logging.info(f'{time.ctime()} proj_cols: {proj_cols} \n rename_param: {rename_params} \n assign_param: {assign_params}')
        #rename columns if required
        if rename_params:
            data.rename(**{'columns': rename_params, 'inplace': True})
        # logging.info(f"Projection result = {data}, type = {data.dtypes}")
        return data

    @property
    def genSQL(self):

        projcoltxt=None;
        for c in self.columns: #Prepare the list of columns going into the select statement.
            col = self.columns[c];
            projcoltxt = ((projcoltxt+', ') if(projcoltxt) else '')  +  ( (col.colTransform.columnExpr if(col.colTransform) else col.sourceColumnName[0]) + ' AS ' + col.columnName);

        sqlText =   (  'SELECT ' + projcoltxt + ' FROM '
                       +   '(' + self._source_.genSQL.sqlText + ') ' + self._source_.tableName  # Source table transform SQL.
                    );

        return SQLQuery(sqlText);

class SQLOrderTransform(SQLTransform):
    def __init__(self, source, orderlist):
        super().__init__(source);
        self._colorderlist_ = orderlist;

    def execute_pandas(self, doOrder=True):
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

    def _genSQL_(self, doOrder=False):
        if(not doOrder):
            return self._source_.genSQL;
        else:
            ordersql = None;
            srccols = self._source_.columns;
            colorderlist = [self._colorderlist_] if(isinstance(self._colorderlist_, str)) else self._colorderlist_;
            for ocol in colorderlist:
                if(ocol.endswith('#asc')):
                    ocolstr = ocol[:-4];
                elif(ocol.endswith('#desc')):
                    ocolstr = ocol[:-5] + ' DESC';
                else:
                    ocolstr = ocol;
                ordersql = 'ORDER BY ' + ocolstr if(not ordersql) else ordersql + ',' + ocolstr;

            return SQLQuery(self._source_.genSQL.sqlText + ' ' + ordersql);

    genSQL = property(_genSQL_);

    @property
    def columns(self):
        if(not  self.__columns__):
            self.__columns__ = self._source_.columns;
        return self.__columns__;

class SQLDistinctTransform(SQLTransform):
    def __init__(self, source):
        super().__init__(source);

    @property
    def genSQL(self):
        projcoltxt=None;
        for c in self.columns: #Prepare the list of columns going into the select statement.
            col = self.columns[c];
            projcoltxt = ((projcoltxt+', ') if(projcoltxt) else '') + col.columnName;

        sqlText =   (  'SELECT DISTINCT ' + projcoltxt + ' FROM '
                       +   '(' + self._source_.genSQL.sqlText + ') ' + self._source_.tableName  # Source table transform SQL.
                    );

        return SQLQuery(sqlText);

    @property
    def columns(self):
        if(not  self.__columns__):
            self.__columns__ = self._source_.columns;
        return self.__columns__;


class SliceTransform(Transform):

    def __init__(self, source, sliceinfo):
        super().__init__();
        self._source_ = weakref.proxy(source);
        self.__sliceinfo__ = sliceinfo;

        srcdata = self._source_.rows;
        data = collections.OrderedDict();
        srcdataKeysList = list(srcdata.keys());
        srccolumns = self._source_.columns;
        columns = collections.OrderedDict();

        #If slicing is only across rows, all columns to be included.
        #Specific row is selected
        if(isinstance(sliceinfo, int)):
            rowslice = slice(sliceinfo, sliceinfo+1, 1); cols = srcdataKeysList;
        #If slicing is only across rows, all columns to be included.
        #A slice of rows are selected.
        elif (isinstance(sliceinfo, slice)):
            rowslice = sliceinfo; cols = srcdataKeysList;
        else:
            #Otherwise find the row slice, which is the first element in the tuple.
            rowinfo = sliceinfo[0];
            if(isinstance(rowinfo, int)):
                rowslice = slice(rowinfo, rowinfo+1, 1);
            elif (isinstance(rowinfo, slice)):
                rowslice = rowinfo;
            elif (isinstance(rowinfo, list)):
                rowslice = rowinfo;
            elif (isinstance(rowinfo, np.ndarray)):
                rowslice = rowinfo;
            #column information is the second element in the tuple.
            colinfo = sliceinfo[1];
            #column information is passed on as an integer.
            if(isinstance(colinfo, int)):
                cols = [srcdataKeysList[colinfo]];
            #column information is passed on as a name.
            elif(isinstance(colinfo, str)):
                cols = [str];
            elif(isinstance(colinfo, slice)):
                cols = srcdataKeysList[colinfo];
            #Otherwise it is already a list of columns
            else:
                #convert any integer positions in the column list to column names.
                cols = [ srcdataKeysList[c] if(isinstance(c, int)) else  c for c in colinfo ];

        #Go over each column, copy the required rows, and the source column metadata.
        for col in cols:
            coldata = srcdata[col][rowslice];
            if(coldata.flags['C_CONTIGUOUS'] == False):
                coldata = np.copy(coldata);
            data[col] = coldata;
            cinfo = copy.deepcopy(srccolumns[col]);
            cinfo.colTransform = None;
            columns[col] = cinfo;

        self.__data__ = data;
        self.__columns__ = columns;
        #logging.debug("SliceTransform columns {}".format(self.__columns__.keys()));

        ##TODO also copy rownames if it is there in the source.

    @property
    def columns(self):
        return self.__columns__;

    @property
    def rows(self):
        return self.__data__;


class StackTransform(Transform, metaclass=ABCMeta):
    def __init__(self, sourcelist):
        self._source_ = weakref.proxy(sourcelist[0]);
        self._sourcelist_ = sourcelist;
        self.__data__ = self.__columns__ = None;

    @abstractmethod
    def __processTransform__(self):
        pass;

    @property
    def hasMatrix(self):
        return False;

    @property
    def rows(self):
        if(not self.__data__ ):
            self.__processTransform__();
        return self.__data__;

    @property
    def columns(self):
        if(not self.__columns__ ):
            self.__processTransform__();
        return self.__columns__;


class HStackTransform(StackTransform):
    def __init__(self, sourcelist, colprefixlist=None):
        super().__init__(sourcelist);
        self._sourcelist_ = sourcelist;
        self._colprefixlist_ = colprefixlist;

    def __processTransform__(self):
        #TODO: check if all the columns have the same length.
        resultrows=collections.OrderedDict();
        columns=collections.OrderedDict();
        for s in range(0, len(self._sourcelist_)):
            rows=self._sourcelist_[s].rows;
            colprefix = self._colprefixlist_[s] if(self._colprefixlist_ is not None) else None;
            srccols = list(rows.keys());
            srccolumns = self._sourcelist_[s].columns;
            for c in range(0, len(rows)):
                colname = srccols[c]
                coldata = rows[colname];
                if(colprefix):
                    column = copy.deepcopy(srccolumns[colname]);
                    colname = colprefix+colname;
                    column.columnName = colname;
                else:
                    column = srccolumns[colname];
                resultrows[colname] = coldata;
                columns[colname] = column;
        self.__data__ = resultrows;
        self.__columns__ = columns;


class VStackTransform(StackTransform):
    def __init__(self, sourcelist):
        super().__init__(sourcelist);

    def __processTransform__(self):
        #srcdatalist = []
        numcols=None; src1=None;
        cols = collections.defaultdict(list);
        for src in self._sourcelist_:
            try:
                rows=src.rows;
                if(numcols is None):
                    numcols = len(rows);
                    src1 = src;
                elif(numcols != len(rows)):
                    logging.error("Error: number of columns do not match across the source list. Was expecting {} columns throughout.".format(numcols));
                    raise TypeError("Error: number of columns do not match across the source list. Was expecting {} columns throughout.".format(numcols));
                for c, col in zip(range(0, numcols), rows):
                    cols[c].append(rows[col]);
            except AttributeError:
                if(numcols != len(src)):
                    logging.error("Error: number of columns do not match across the source list. Was expecting {} columns throughout.".format(numcols));
                    raise TypeError("Error: number of columns do not match across the source list. Was expecting {} columns throughout.".format(numcols));
                for c, col in zip(range(0, numcols), src):
                    cols[c].append(src[col]);

        resultrows=collections.OrderedDict();
        colnames = list(src1.rows.keys());
        for c,col in zip(range(0, numcols), colnames):
            resultrows[col] = np.hstack(cols[c]);

        self.__data__ = resultrows;
        self.__columns__ = src1.columns;



class UserTransform(Transform):

    def __init__(self, source, func, *args, **kwargs):
        self._source_ = weakref.proxy(source);
        self.__userfunc__ = func;
        self.__args__ = args;
        self.__kwargs__ = kwargs;
        self.__data__ = self.__matrix__ = self.__columns__ = None;

    def __processTransform__(self):
        func = self.__userfunc__;
        args = self.__args__;
        kwargs = self.__kwargs__;
        src = self._source_;

        #execute the user function
        data = func(src, *args, **kwargs);
        #user functions are allowed to return data as a Dictionary or a (numpy matrix, [c1, c2, ...]) tuple.
        if(isinstance(data, collections.OrderedDict)):
            self.__data__ = data;
        elif(isinstance(data, dict)):
            self.__data__ = collections.OrderedDict(data)
        elif(isinstance(data, tuple) and len(data)==2):
            _umat = data[0];
            _ucols = data[1];
            if(_umat.shape[1] != len(_ucols)):
                logging.error("Error: user transform {} returned a matrix with {} columns but column list of length {}.".format(func.__name__, _umat.shape[1], len(_ucols)));
                raise TypeError("Error: user transform {} returned a matrix with {} columns but column list of length {}.".format(func.__name__, _umat.shape[1], len(_ucols)));
            #Create virtual columns for each of the matrix columns.
            _data = collections.OrderedDict();
            for i in range(0, len(_ucols)):
                _data[_ucols[i]] = _umat[:,i] ;
            self.__data__ = _data;
            self.__matrix__ = _umat;
        else:
            logging.error("Error: user transform {} should return data as a Dictionary or a (numpy matrix, [c1, c2, ...]) tuple.".format(func.__name__));
            raise TypeError("Error: user transform {} should return data as a Dictionary or a (numpy matrix, [c1, c2, ...]) tuple.".format(func.__name__));

        newCols = collections.OrderedDict();
        #for c in src.rows:
        for c in self.__data__:
            #TODO: make column metadata accurate.
            newCols[c] = DBTable.Column((src.dbc.dbName, src.tableName, c, None, None, 0, False));
        self.__columns__ = newCols;


    @property
    def rows(self):
        if(not self.__data__ ):
            self.__processTransform__();
        return self.__data__;

    @property
    def matrix(self):
        if(not self.__matrix__ ):
            if(not self.__data__):
                self.__processTransform__();
            #Sometimes processing the transform can result in a matrix, so check again.
            if(not self.__matrix__):
                rows = self.rows;
                self.__matrix__ = np.stack(tuple(rows[col] for col in rows));
        return self.__matrix__;

    @property
    def hasMatrix(self):
        return self.__matrix__ is not None;

    @property
    def columns(self):
        if(not self.__columns__ ):
            self.__processTransform__();
        return self.__columns__;


class ExternalDataTransform(Transform):

    def __init__(self, func, dbc, *args, **kwargs):
        self.__loadfunc__ = func;
        self._dbc_ = weakref.proxy(dbc);
        self.__args__ = args;
        self.__kwargs__ = kwargs;
        self.__data__ = self.__matrix__ = self.__columns__ = None;

    def __processTransform__(self):
        func = self.__loadfunc__;
        args = self.__args__;
        kwargs = self.__kwargs__;

        #execute the external data load function
        data = func(*args, **kwargs);
        #external data load functions are allowed to return data as a Dictionary or a (numpy matrix, [c1, c2, ...]) tuple.
        if(isinstance(data, collections.OrderedDict)):
            self.__data__ = data;
        elif(isinstance(data, dict)):
            self.__data__ = collections.OrderedDict(data)
        elif(isinstance(data, tuple) and len(data)==2):
            _umat = data[0];
            _ucols = data[1];
            if(_umat.shape[1] != len(_ucols)):
                logging.error("Error: external data transform {} returned a matrix with {} columns but column list of length {}.".format(func.__name__, _umat.shape[1], len(_ucols)));
                raise TypeError("Error: external data transform {} returned a matrix with {} columns but column list of length {}.".format(func.__name__, _umat.shape[1], len(_ucols)));
            #Create virtual columns for each of the matrix columns.
            _data = collections.OrderedDict();
            for i in range(0, len(_ucols)):
                _data[_ucols[i]] = _umat[:,i] ;
            self.__data__ = _data;
            self.__matrix__ = _umat;
        else:
            logging.error("Error: external data transform {} should return data as a Dictionary or a (numpy matrix, [c1, c2, ...]) tuple.".format(func.__name__));
            raise TypeError("Error: external data transform {} should return data as a Dictionary or a (numpy matrix, [c1, c2, ...]) tuple.".format(func.__name__));

        newCols = collections.OrderedDict();
        #for c in src.rows:
        for c in self.__data__:
            if(not isinstance(self.__data__[c], np.ndarray)):
                self.__data__[c] = np.asarray(self.__data__[c]);
            #TODO: make column metadata accurate.
            newCols[c] = DBTable.Column((self._dbc_.dbName, None, c, None, None, 0, False));
        self.__columns__ = newCols;


    @property
    def rows(self):
        if(not self.__data__ ):
            self.__processTransform__();
        return self.__data__;

    @property
    def matrix(self):
        if(not self.__matrix__ ):
            if(not self.__data__):
                self.__processTransform__();
            #Sometimes processing the transform can result in a matrix, so check again.
            if(not self.__matrix__):
                rows = self.rows;
                self.__matrix__ = np.stack(tuple(rows[col] for col in rows));
        return self.__matrix__;

    @property
    def hasMatrix(self):
        return self.__matrix__ is not None;

    @property
    def columns(self):
        if(not self.__columns__ ):
            self.__processTransform__();
        return self.__columns__;



class VirtualDataTransform(Transform):
    def __init__(self, transformFunc, dbc, colmeta, *args, **kwargs):
        self._dbc_ = weakref.proxy(dbc);
        self.__datafunc__ = transformFunc;
        self.__colmeta__ = colmeta;
        self.__args__ = args;
        self.__kwargs__ = kwargs;
        self.__data__ = self.__matrix__ = self.__columns__ = None;

    def __processTransform__(self):
        func = self.__datafunc__;
        args = self.__args__;
        kwargs = self.__kwargs__;

        #execute the user function
        #logging.debug('DEBUG: VirtualDataTransform {} __processTransform__: need to produce data from function'.format(id(self)));
        data = func(*args, **kwargs);
        #logging.debug('DEBUG: VirtualDataTransform {} __processTransform__: produced data from function'.format(id(self)));
        #user functions are allowed to return data as a Dictionary or a (numpy matrix, [c1, c2, ...]) tuple.
        if(isinstance(data, collections.OrderedDict)):
            self.__data__ = data;
        elif(isinstance(data, dict)):
            self.__data__ = collections.OrderedDict(data)
        elif(isinstance(data, tuple) and len(data)==2):
            _umat = data[0];
            _ucols = data[1];
            if(_umat.shape[1] != len(_ucols)):
                logging.error("Error: virtual data transform {} returned a matrix with {} columns but column list of length {}.".format(func.__name__, _umat.shape[1], len(_ucols)));
                raise TypeError("Error: virtual data transform {} returned a matrix with {} columns but column list of length {}.".format(func.__name__, _umat.shape[1], len(_ucols)));
            #Create virtual columns for each of the matrix columns.
            _data = collections.OrderedDict();
            for i in range(0, len(_ucols)):
                _data[_ucols[i]] = _umat[:,i] ;
            self.__data__ = _data;
            self.__matrix__ = _umat;
        else:
            logging.error("Error: virtual data transform {} should return data as a Dictionary or a (numpy matrix, [c1, c2, ...]) tuple.".format(func.__name__));
            raise TypeError("Error: virtual data transform {} should return data as a Dictionary or a (numpy matrix, [c1, c2, ...]) tuple.".format(func.__name__));

        newCols = collections.OrderedDict();
        for c in self.__data__:
            #TODO: make column metadata accurate.
            newCols[c] = DBTable.Column((self._dbc_.dbName, None, c, None, None, 0, False));
        self.__columns__ = newCols;


    @property
    def rows(self):
        if(not self.__data__ ):
            self.__processTransform__();
        return self.__data__;

    @property
    def matrix(self):
        if(not self.__matrix__ ):
            if(not self.__data__):
                self.__processTransform__();
            #Sometimes processing the transform can result in a matrix, so check again.
            if(not self.__matrix__):
                rows = self.rows;
                self.__matrix__ = np.stack(tuple(rows[col] for col in rows));
        return self.__matrix__;

    @property
    def hasMatrix(self):
        return self.__matrix__ is not None;

    @property
    def columns(self):
        if(not self.__columns__ ):
            self.__processTransform__();
        return self.__columns__;



#Base class for all Algebraic Transformations.
class AlgebraicTransform(Transform):
    def __init__(self, source):
        self._source_    = weakref.proxy(source) if (source) else None;
        self.__columns__ = None;
    #The columns that will be produced once this transform is applied on its source.
    @property
    def columns(self):
        if(not self.__columns__):
            self.__columns__ = copy.deepcopy(self._source_.columns);
        return self.__columns__;

    @property
    def rows(self):
        return None;

class AlgebraicScalarTransform(AlgebraicTransform):
    def __init__(self, source, scalar, op, side=OP.LHS):
        super().__init__(source)
        self.scalar = scalar;
        self.op = op;
        self.side = side;

    @property
    def genExpr(self):
        #if(self.op in [OP.ADD, OP.MULTIPLY]):
        #    return '({{}} {} {})'.format(self.op.value, self.scalar);
        #if(self.op in [OP.SUBTRACT, OP.DIVIDE]):
        return '({{}} {} {})'.format(self.op.value, self.scalar) if self.side==OP.LHS else  '({} {} {{}})'.format(self.scalar, self.op.value) ;
        #raise TypeError;

    def applyTransform(self, srcrows, srctransformlist):
        transforms = (srctransformlist if srctransformlist else []) + [self];
        expr='({{}})';
        for t in transforms:
            expr=t.genExpr.format(expr);
        expr = expr.format();

        data = collections.OrderedDict();
        for c in srcrows:
            coldata = eval(expr.format('srcrows[\'{}\']'.format(c)));
            data[c] = coldata;

        return data;

    @property
    def columns(self):
        if(not self.__columns__):
            self.__columns__ = copy.deepcopy(self._source_.columns);
            for c in self.__columns__:
                self.__columns__[c].colTransform = None;
        return self.__columns__;

class AlgebraicVectorTransform(AlgebraicTransform):
    def __init__(self, source1, source2, op, side=OP.LHS):
        #st=time.time();
        #logging.debug("AlgebraicVectorTransform operation {} init enter time {:0.20f}".format(op.value, time.time()));
        super().__init__(None);
        self._source1_ = weakref.proxy(source1);
        self._source2_ = weakref.proxy(source2) if(source2 is not None) else None;
        self.op = op;
        self.side = side;

        self.__data__ = self.__matrix__ = self.__columns__ = self.__rowNames__ = None;

        #logging.debug("AlgebraicVectorTransform copying columns {:0.20f}".format(time.time()));
        #columns = copy.deepcopy(self._source1_.columns);
        #columns = self._source1_.columns;
        #logging.debug("AlgebraicVectorTransform columns copied {:0.20f}".format(time.time()));

        if(self.op == OP.TRANSPOSE):
            #logging.debug("AlgebraicVectorTransform Transpose begins {:0.20f}".format(time.time()));
            columns = self.__columns__  = self._source1_.rowNames;
            #logging.debug("AlgebraicVectorTransform Transpose {} columns {:0.20f}".format(len(columns), time.time()));
            #if(len(columns) == 1):
            if(len(self._source1_.columns) == 1):
                self.__matrix__   = self._source1_.matrix.reshape(self._source1_.numRows, 1, order='C');
            else:
                self.__matrix__   = self._source1_.matrix.T;

            self.__rowNames__ = self._source1_.columns;
            #self.__rowNames__ = coluomns;
            #logging.debug("AlgebraicVectorTransform Transpose columns dictionary type is {} columns {:0.20f}".format(type(columns), time.time()));
            if(not isinstance(self.__columns__, VirtualOrderedColumnsDict)):
                data = collections.OrderedDict();
                colList = list(self.__columns__.keys());
                #logging.debug("AlgebraicVectorTransform Transpose first column {} {:0.20f}".format(colList[0], time.time()));
                for i in range(0, len(colList)): #If we have data in matrix representation, may be we can point the columns in rows to corresponding columns of matrix
                    data[colList[i]] = self.__matrix__[i]; # WARNING !!! all columns will have the same data type now !!
                self.__data__ = data;
            else:
                #logging.debug("AlgebraicVectorTransform Transpose lazy metadata {:0.20f}".format(time.time()));
                data = VirtualOrderedColumnsDict(len(self.__columns__), ColumnDataGenerator(self.__matrix__), numformatter=self.__columns__.numformatter);
                self.__data__ = data;
            #logging.debug("AlgebraicVectorTransform Transpose end {:0.20f}".format(time.time()));

        elif(isinstance(self._source2_, TabularData) and not self._source2_.isMatrixCached and self.op != OP.MATRIXMULTIPLY):
            columns = self._source1_.columns;
            src1data = self._source1_.rows;
            src1dataKeysList = list(src1data.keys());
            src2data = self._source2_.rows;
            src2dataKeysList = list(src2data.keys());
            data = collections.OrderedDict();

            #logging.debug("AlgebraicVectorTransform performing column level operation");

            for i in range(0, len(src1dataKeysList)):
                col1 = src1data[src1dataKeysList[i]];
                col2 = src2data[src2dataKeysList[i]];
                #res = np.multiply(col1, col2) if(self.op == OP.MULTIPLY) else eval('col1 {} col2'.format(self.op.value));
                res = eval('col1 {} col2'.format(self.op.value));
                data[src1dataKeysList[i]] = res;
                #logging.debug("AlgebraicVectorTransform {} = {}".format(src1dataKeysList[i], res));
        else:
            #WARNING !! once data is converted into matrix format, it will loose the "null" values.
            mat1 = self._source1_.matrix;
            #If the second operand is not a TabularData type, we need to transpose it to our storage format.
            mat2 = self._source2_.matrix if(isinstance(self._source2_, TabularData)) else source2.T;

            #logging.debug("AlgebraicVectorTransform performing matrix level operation");
            #logging.debug("AlgebraicVectorTransform {} {} {}".format(mat1, self.op.value, mat2));
            #logging.debug("AlgebraicVectorTransform {}, {} start time {}".format(self.op.value, self.side.value, time.time()));

            if(self.op == OP.MATRIXMULTIPLY):
                #if(not mat1.flags['C_CONTIGUOUS']):
                #    logging.warning('WARN: Matrix multiplication mat1 is not C_CONTIGUOUS');
                #if(not mat2.flags['C_CONTIGUOUS']):
                #    logging.warning('WARN: Matrix multiplication mat2 is not C_CONTIGUOUS');
                #Our matrices stored in transposed form, so we multiply them in the opposite order.
                res = mat2 @ mat1;
                #logging.debug("AlgebraicVectorTransform {} @ {} = {}".format(mat2, mat1, res));
            else:
                res = eval('mat1 {} mat2'.format(self.op.value)) if (self.side == OP.LHS) else eval('mat2 {} mat1'.format(self.op.value)) ;

            #logging.debug("AlgebraicVectorTransform {}, {} end time {}".format(self.op.value, self.side.value, time.time()));

            #If its a single column output, reshape it to look like a 2d array with just one column.
            res = res.reshape(1, len(res), order='C') if(len(res.shape)==1) else res;
            self.__matrix__ = res;
            #logging.debug("AlgebraicVectorTransform res = {}".format(res));

            #In case of real matrix multiplication, we need to copy the columns belonging to the RHS data frame.
            if(self.op == OP.MATRIXMULTIPLY):
                if(isinstance(self._source2_, TabularData)):
                    #columns = copy.deepcopy(self._source2_.columns);
                    columns = self._source2_.columns;
                else:
                    columns = collections.OrderedDict([('r_{:010d}'.format(i),DBTable.Column((None, None, None, None, None, None, None), tableName=self._source1_.tableName, columnName='r_{:010d}'.format(i))) for i in np.arange(0, len(res))]);
            else:
                columns = self._source1_.columns;

            colkeys = list(columns.keys());
            data = collections.OrderedDict();
            #numcols = len(colkeys);
            #if(numcols==1): #Output is just one column, already in columnar format.
            #    data[colkeys[0]] = res;
            #else:
            for c in range(0, len(colkeys)):    #pack results from matrix into columnar format.
                data[colkeys[c]] = res[c];

        #TODO may be for transpose we should do the same for rowNames
        if(self.op != OP.TRANSPOSE):
            self.__data__ = data;
            for c in columns:
                columns[c].colTransform = None;
            self.__columns__ = columns;

        #Bring over the rowNames from sources if they have already been computed.
        #TODO: We don't need to deepcopy them ?
        if(self.op != OP.MATRIXMULTIPLY):
            if(self._source1_.hasRowNames):
                #self.__rowNames__ = copy.deepcopy(self._source1_.rowNames);
                self.__rowNames__ = self._source1_.rowNames;
            elif(isinstance(self._source2_, TabularData) and self._source2_.hasRowNames):
                #self.__rowNames__ = copy.deepcopy(self._source2_.rowNames);
                self.__rowNames__ = self._source2_.rowNames;

        #et=time.time();
        #logging.debug("AlgebraicVectorTransform init exit time {:0.20f}".format(time.time()));

    @property
    def columns(self):
        return self.__columns__;

    @property
    def rowNames(self):
        return self.__rowNames__;

    @property
    def rows(self):
        return self.__data__;

    @property
    def matrix(self):
        return self.__matrix__;


class ColumnNameGenerator:
    def __init__(self, tableName):
        self.__tableName__ = tableName;

    def get(self, colno):
        return DBTable.Column((None, None, None, None, None, None, None), tableName=self.__tableName__, columnName='r_{:010d}'.format(colno));

class ColumnDataGenerator:
    def __init__(self, matrixData):
        self.__matrixData__ = matrixData;

    def get(self, colno):
        return self.__matrixData__[colno];

class DBTable(TabularData):

    class Column:
        class TYPE(Enum):
            INT='int'; DOUBLE='double'; CHAR='char'; VARCHAR='varchar'; DATE='date';
            @property
            def size(self):
                return self.sizes[self.value];
        TYPE.sizes = {TYPE.INT.value:32, TYPE.DOUBLE.value:53, TYPE.CHAR.value:None, TYPE.VARCHAR.value:None, TYPE.DATE.value:32};

        def __str__(self):
            return '(dbTableName = {}, dbColumnName = {}, tableName = {}, columnName = {}, sourceColumnName={})'.format(self.dbTableName, self.dbColumnName, self.tableName, self.columnName, self.sourceColumnName);


        def __init__(self, metadata, tableName=None, columnName=None):
            #Fields from the database.
            (self.schemaName, self.dbTableName, self.dbColumnName, self.type, self.size, self.pos, self.nullable) = metadata;
            #mutable fields.
            self.tableName = tableName if (tableName) else self.dbTableName;
            self.columnName = columnName if(columnName) else self.dbColumnName;
            self.colTransform = None;
            self.sourceColumnName = [ self.columnName ];

        @classmethod
        def makeEmptyColumn(cls):
            return DBTable.Column((None, None, None, None, None, 0, True));

    #TODO replace this with the above enum.
    dataTypeFormatStrings = {'int':' {} ', 'double':' {} '
        , 'varchar':' \'{}\' ', 'char':' \'{}\' ', 'date':' \'{}\' '}

    def __init__(self, DBC, metadata):
        self.__dbc__ = DBC;
        self.__metadata__ = metadata;

        #logging.debug("Table constructor received metadata {}".format(metadata));
        #logging.debug("schemaname = {}".format(metadata['schemaname']));
        #logging.debug("tablename = {}".format(metadata['tablename']));

        self.__schemaName__  = metadata['schemaname'][0];
        self.__tableName__   = metadata['tablename'][0];


        self.__columns__ = collections.OrderedDict();
        for numcolumns in range(0, len(metadata[list(metadata.keys())[0]])):
            cmeta = [];
            colname =None;
            for c in  [ 'schemaname', 'tablename', 'columnname', 'columntype', 'columnsize', 'columnpos', 'columnnullable']:
                cmeta.append(metadata[c][numcolumns]);
                if(c == 'columnname'):
                    colname = metadata[c][numcolumns];
            #logging.debug("{}.{} column {} metadata {}".format(self.__schemaName__, self.__tableName__, colname, cmeta));
            self.__columns__[colname] = DBTable.Column(cmeta);


        self.__data__ = None;
        self.__pdData__ = None;
        self.__matrix__ = None;
        self.__rowNames__ = None;
        self.__numRows__ = None;
        self.__shape__ = None;

    @property
    def schemaName(self): return self.__schemaName__;
    @property
    def tableName(self): return self.__tableName__;
    @property
    def columns(self): return self.__columns__;

    @property
    def numRows(self):
        if(not self.__numRows__):
            rows = self.rows;
            if(not rows or len(rows)==0):
                self.__numRows__ = 0;
            self.__numRows__ = len(rows[rows.keys().__iter__().__next__()]);
        return  self.__numRows__;

    #WARNING !! Permanently disabled  !
    #Weakref proxy invokes this function for some reason, which is forcing the TabularData objects to materialize.
    #def __len__(self):
    #    return self.numRows;

    @property
    def shape(self):
        if(not self.__shape__):
            numrows = self.numRows;
            numcols = len(self.columns);
            self.__shape__ = (numrows, numcols);
        return self.__shape__;


    @property
    def hasRowNames(self):
        return self.__rowNames__ is not None;

    @property
    def rowNames(self):
        if(not self.__rowNames__):
            #rn = collections.OrderedDict([('r_{:010d}'.format(i),DBTable.Column((None, None, None, None, None, None, None), tableName=self.tableName, columnName='r_{:010d}'.format(i))) for i in np.arange(0,self.numRows)]);
            #self.__rowNames__ = rn;
            self.__rowNames__ = VirtualOrderedColumnsDict(self.numRows, ColumnNameGenerator(self.tableName), colprefix='r_');
        return self.__rowNames__;

    @property
    def dbc(self): return self.__dbc__;

    @property
    def isDBQry(self): return True;

    def execute_pandas(self):
        if self.__pdData__ is None:
            t0 = time.time()
            data = df_from_arrays(self.__data__.values(), self.__data__.keys(), range(self.numRows))
            #logging.info(f"[{time.ctime()}] pandas type = {data.dtypes}")
            t1 = time.time()
            #logging.info(f'{self.tableName} convert time = {t1 - t0}')
            self.__pdData__ = data
        return self.__pdData__

    #@property
    def _genSQL_(self,rowNumbers=False, includeRowNum=False):
        cols = None;
        for c in self.__columns__: #Form list of columns for select
            cols = (cols + ' ,' + self.__tableName__ + '.' + c) if(cols) else c;

        if(rowNumbers or includeRowNum):
            orgCols = cols;
            cols = cols + ', ROW_NUMBER() OVER() as __rownum__'

        sqlText = 'SELECT ' + self.__tableName__ + '.' + cols + ' FROM ' \
                  + (self.__schemaName__+'.' if(self.__schemaName__) else '') + self.__tableName__;

        if(rowNumbers):
            sqlText = 'SELECT ' + orgCols + ' FROM (' +sqlText +')'  + self.__tableName__;

        return SQLQuery(sqlText);


    genSQL = property(_genSQL_);

    def gen_lineage(self):
        return LineageNode(self)

    @property
    def rows(self):
        if(self.__data__ is None):
            (data, rows) = self.__dbc__._executeQry(self.genSQL.sqlText + ';');
            #Convert the results to an ordered dictionary format.
            if(not isinstance(data, collections.OrderedDict)):
                data_ = collections.OrderedDict();
                for c in self.columns:
                    data_[c] = data[c];
                data.clear();
                data = data_;
            self.__data__ = data;
        return self.__data__;
    

    @property
    def cdata(self):
        return self.rows;

    def loadData(self, matrix=False):
        """Forces materialization of this Table"""
        self.rows;
        if(matrix):
            self.matrix;

    def filter(self, *selcols):
        return DataFrame(self, SQLSelectTransform(self, *selcols));

    def join(self, otherTable, src1joincols, src2joincols, cols1=COL.NONE, cols2=COL.NONE, join=JOIN.INNER):
        ot = DataFrame(otherTable, None) if(isinstance(otherTable, DBTable)) else otherTable
        return DataFrame( (self, ot)
                         ,SQLJoinTransform(self, ot, src1joincols, src2joincols, cols1=cols1, cols2=cols2, join=join));

    def aggregate(self, projcols, groupcols=None):
        return DataFrame(self, SQLAggregateTransform(self, projcols, groupcols));

    #Short form
    agg = aggregate;

    def project(self, projcols):
        return DataFrame(self, SQLProjectionTransform(self, projcols));

    def order(self, orderlist):
        return DataFrame(self, SQLOrderTransform(self, orderlist));

    def distinct(self):
        return DataFrame(self, SQLDistinctTransform(self));

    @property
    def matrix(self):
        if(self.__matrix__ is None):
            rows = self.rows;

            if(len(rows) == 1): #This object has only one column, no need to try build a matrix.
                matrix_ = rows[list(rows.keys())[0]];
            else:
                #stack columns to for a matrix (columns are stacked horizontally not vertically !!)
                matrix_ = np.stack( tuple(rows[col] for col in rows) );
            self.__matrix__ = matrix_;

            #Disabling this because once we make a matrix, we loose masked array null flags.
            #rowkeyslist = list(rows.keys());
            #for i in range(0, len(rowkeyslist)): #If we have data in matrix representation, may be we can point the columns in rows to corresponding columns of matrix
            #    rows[rowkeyslist[i]] = matrix_[i]; # WARNING !!! all columns will have the same data type now !!
            #self.__data__ = rows;

        return self.__matrix__;

    @property
    def isMatrixCached(self):
        return self.__matrix__ is not None;

    @property
    def isCached(self):
        return self.__data__ is not None;

    @property
    def rowsNtransform(self):
        return (self.rows, None, self.__rowNames__);

    def __add__(self, other):
        if(type(other) in AIDADtypes.numeric):
            return DataFrame(self, AlgebraicScalarTransform(self, other, OP.ADD));
        return DataFrame( (self, other), AlgebraicVectorTransform(self, other, OP.ADD) );

    def __radd__(self, other):
        if(type(other) in AIDADtypes.numeric):
            return DataFrame(self, AlgebraicScalarTransform(self, other, OP.ADD));
        return DataFrame( (self, other), AlgebraicVectorTransform(self, other, OP.ADD) );

    def __mul__(self, other):
        if(type(other) in AIDADtypes.numeric):
            return DataFrame(self, AlgebraicScalarTransform(self, other, OP.MULTIPLY));
        return DataFrame( (self, other), AlgebraicVectorTransform(self, other, OP.MULTIPLY) );

    def __rmul__(self, other):
        if(type(other) in AIDADtypes.numeric):
            return DataFrame(self, AlgebraicScalarTransform(self, other, OP.MULTIPLY));
        return DataFrame( (self, other), AlgebraicVectorTransform(self, other, OP.MULTIPLY) );

    def __sub__(self, other):
        if(type(other) in AIDADtypes.numeric):
            return DataFrame(self, AlgebraicScalarTransform(self, other, OP.SUBTRACT));
        return DataFrame( (self, other), AlgebraicVectorTransform(self, other, OP.SUBTRACT) );

    def __rsub__(self, other):
        if(type(other) in AIDADtypes.numeric):
            return DataFrame(self, AlgebraicScalarTransform(self, other, OP.SUBTRACT, side=OP.RHS));
        return DataFrame( (self, other), AlgebraicVectorTransform(self, other, OP.SUBTRACT, side=OP.RHS) );

    def __truediv__(self, other):
        if(type(other) in AIDADtypes.numeric):
            return DataFrame(self, AlgebraicScalarTransform(self, other, OP.DIVIDE));
        return DataFrame( (self, other), AlgebraicVectorTransform(self, other, OP.DIVIDE) );

    def __rtruediv__(self, other):
        if(type(other) in AIDADtypes.numeric):
            return DataFrame(self, AlgebraicScalarTransform(self, other, OP.DIVIDE, side=OP.RHS));
        return DataFrame( (self, other), AlgebraicVectorTransform(self, other, OP.DIVIDE, side=OP.RHS) );

    def __pow__(self, power, modulo=None):
        if(type(power) in AIDADtypes.numeric):
            return DataFrame(self, AlgebraicScalarTransform(self, power, OP.EXP));
        else:
            raise TypeError("Cannot use type {} as a power".format(type(power)));

    def __matmul__(self, other):
        return DataFrame( (self, other), AlgebraicVectorTransform(self, other, OP.MATRIXMULTIPLY) );

    def __rmatmul__(self, other):
        return DataFrame( (self, other), AlgebraicVectorTransform(self, other, OP.MATRIXMULTIPLY, side=OP.RHS) );

    @property
    def T(self):
        return DataFrame( self, AlgebraicVectorTransform(self, None, OP.TRANSPOSE) );

    #For slicing [] operations.
    def __getitem__(self, item):
        return DataFrame(self, SliceTransform(self, item));

    #For stacking columns one on top of the other
    def vstack(self, otherdatalist):
        if(isinstance(otherdatalist, tuple) or isinstance(otherdatalist, list)):
            return DataFrame(self, VStackTransform([self, *otherdatalist]));
        else:
            return DataFrame(self, VStackTransform([self, otherdatalist]));
        #return DataFrame(self, VStackTransform([self, *otherdatalist]));

    #For stacking columns side by side.
    def hstack(self, otherdatalist, colprefixlist=None):
        return DataFrame(self, HStackTransform([self, *otherdatalist], colprefixlist));

    #User transformations.
    def _U(self, func, *args, **kwargs):
        return DataFrame(self, UserTransform(self, func, *args, **kwargs));

    def describe(self):
        return self.dbc._describe(self);

    def sum(self, collist=None):
        return self.dbc._agg(DBC.AGGTYPE.SUM, self, collist);

    def avg(self, collist=None):
        return self.dbc._agg(DBC.AGGTYPE.AVG, self, collist);

    def count(self, collist=None):
        return self.dbc._agg(DBC.AGGTYPE.COUNT, self, collist);

    def countd(self, collist=None):
        return self.dbc._agg(DBC.AGGTYPE.COUNTD, self, collist);

    def countn(self, collist=None):
        return self.dbc._agg(DBC.AGGTYPE.COUNTN, self, collist);

    def max(self, collist=None):
        return self.dbc._agg(DBC.AGGTYPE.MAX, self, collist);

    def min(self, collist=None):
        return self.dbc._agg(DBC.AGGTYPE.MIN, self, collist);


    def head(self, n=5):
        if(self.__data__ is None):
            (data, rows) = self.__dbc__._executeQry(self.genSQL.sqlText + (' LIMIT {};').format(n));
            #Convert the results to an ordered dictionary format.
            if(not isinstance(data, collections.OrderedDict)):
                data_ = collections.OrderedDict();
                for c in self.columns:
                    data_[c] = data[c];
                data.clear();
                data = data_;
        else:
            data = collections.OrderedDict();
            for c in self.columns:
                data[c] = self.__data__[c][0:n];

        return pd.DataFrame(data=data);

    def tail(self, n=5):
        if(self.__data__ is None):
            (data, rows) = self.__dbc__._executeQry(self._genSQL_(rowNumbers=True).sqlText + (' WHERE __rownum__  > (SELECT COUNT(*)-{} FROM {} );').format(n, ((self.__schemaName__+'.' if(self.__schemaName__) else '') + self.__tableName__)  ));
            #Convert the results to an ordered dictionary format.
            if(not isinstance(data, collections.OrderedDict)):
                data_ = collections.OrderedDict();
                for c in self.columns:
                    data_[c] = data[c];
                data.clear();
                data = data_;
            #return self.head(n);
        else:
            data = collections.OrderedDict();
            for c in self.columns:
                data[c] = self.__data__[c][-1*n:];

        return pd.DataFrame(data=data);

    def __del__(self):
        #logging.debug("Removing dborm {}".format(self.__tableName__));
        if(self.__data__ is not None):
            #self.__data__.clear();
            del self.__data__;
        if(self.__pdData__ is not None):
            del self.__pdData__;
        if(self.__matrix__ is not None):
            del self.__matrix__;
        if(self.__rowNames__ is not None):
            del self.__rowNames__;


class DataFrame(TabularData):
    def __init__(self, source, transform, name=None, dbc=None):
        self.__source__ = source;
        self.__transform__ = transform;

        self.__data__ = None;
        self.__pdData__ = None;
        self.__columns__ = None;
        self.__rowNames__ = None;
        self.__numRows__ = None;
        self.__shape__ = None;
        self.__matrix__ = None;
        self.__tableUDFExists__ = False;

        self.__tableName__ = name if(name) else ('_tmp_' + re.sub(r'x','', str(uuid.uuid4())[:8])  ) ;
        self.__dbc__ = dbc;

        #TODO: This needs to be moved to "rows" as well to efficiently dispose of the lineage.
        if(isinstance(transform, SliceTransform)):
            self.__data__ = transform.rows;
#        elif(isinstance(transform, AlgebraicVectorTransform)):
#            self.__data__ = transform.rows;
#            self.__columns__ = transform.columns;
#            self.__matrix__ = transform.matrix;
#            self.__rowNames__ = transform.rowNames;
#
    @property
    def tableName(self):
        return self.__tableName__ if(self.__tableName__) else self.__source__.tableName;

    @property
    def isDBQry(self):
        """This data frame can be represented as a SQL query if this DF's transformations are of SQL type."""
        if(self.tableUDFExists or (not self.__transform__ and isinstance(self.__source__, DBTable)) or isinstance(self.__transform__, SQLTransform)):
            return True;
        #if(hasattr(self.__source__, 'isDBQry')):
        #    return self.__source__.isDBQry and (True if(not self.__transform__) else isinstance(self.__transform__, SQLTransform));
        #else:
        #    return self.__source__[0].isDBQry and self.__source__[1].isDBQry and  (True if(not self.__transform__) else isinstance(self.__transform__, SQLTransform));

    def filter(self, *selcols):
        return DataFrame(self, SQLSelectTransform(self, *selcols));

    def join(self, otherTable, src1joincols, src2joincols, cols1=COL.NONE, cols2=COL.NONE, join=JOIN.INNER):
        return DataFrame( (self, otherTable),  SQLJoinTransform(self, otherTable, src1joincols, src2joincols, cols1=cols1, cols2=cols2, join=join));

    def aggregate(self, projcols, groupcols=None):
        return DataFrame(self, SQLAggregateTransform(self, projcols, groupcols));

    #Short form
    agg = aggregate;

    def project(self, projcols):
        return DataFrame(self, SQLProjectionTransform(self, projcols));

    def order(self, orderlist):
        return DataFrame(self, SQLOrderTransform(self, orderlist));

    def distinct(self):
        return DataFrame(self, SQLDistinctTransform(self));

    @property
    def numRows(self):
        if(not self.__numRows__):
            rows = self.rows;
            if(not rows or len(rows)==0):
                self.__numRows__ = 0;
            self.__numRows__ = len(rows[rows.keys().__iter__().__next__()]);
        return  self.__numRows__;

    #WARNING !! Permanently disabled  !
    #Weakref proxy invokes this function for some reason, which is forcing the TabularData objects to materialize.
    #def __len__(self):
    #    return self.numRows;

    @property
    def shape(self):
        if(not self.__shape__):
            numrows = self.numRows;
            numcols = len(self.columns);
            self.__shape__ = (numrows, numcols);
        return self.__shape__;

    @property
    def columns(self):
        if(not self.__columns__):
            #TODO fix this deep copy.
            self.__columns__ = copy.deepcopy(self.__transform__.columns if(self.__transform__ and hasattr(self.__transform__, 'columns')) else self.__source__.columns);
            for c in self.__columns__:
                col = self.__columns__[c];
                col.tableName = self.tableName;
                col.colTransform = None;

        return self.__columns__;

    @property
    def hasRowNames(self):
        return self.__rowNames__ is not None;

    @property
    def rowNames(self):
        if(not self.__rowNames__):
            #rn = collections.OrderedDict([('r_{:010d}'.format(i),DBTable.Column((None, None, None, None, None, None, None), tableName=self.tableName, columnName='r_{:010d}'.format(i))) for i in np.arange(0,self.numRows)]);
            #self.__rowNames__ = rn;
            self.__rowNames__ = VirtualOrderedColumnsDict(self.numRows, ColumnNameGenerator(self.tableName), colprefix='r_');
        return self.__rowNames__;


    @property
    def dbc(self):
        if(not self.__dbc__):
            if(hasattr(self.__source__, 'dbc')):
                dbc_ = self.__source__.dbc;
            elif(hasattr(self.__transform__, 'dbc')):
                dbc_ = self.__transform__.dbc;
            else:
                for src in self.__source__:
                    if(hasattr(src, 'dbc')):
                        dbc_ = src.dbc;
            self.__dbc__ = dbc_;
        return self.__dbc__;

    @property
    def tableUDFExists(self):
        return self.__tableUDFExists__;

    def _genSQL_(self, doOrder=False):
        #If this data frame is not based on a direct sql transform, convert it into a table UDF.
        if(not self.isDBQry):
            #self.loadData(); #Not required to explicitly load data as the DBC adapter will do this.
            self.dbc._toTable(self);
            self.__tableUDFExists__ = True;
        #There is a table UDF for this data frame.
        if(self.tableUDFExists):
            cols = None;
            #logging.debug("columns {}".format(self.__columns__));
            #for c in self.__columns__: #Form list of columns for select
            for c in self.columns: #Form list of columns for select
                cols = (cols + ' ,' + c) if(cols) else c;
            sqlText = 'SELECT ' +  cols + ' FROM '  + self.__tableName__ + ( '()' if(AConfig.UDFTYPE == UDFTYPE.TABLEUDF) else '' );
            return SQLQuery(sqlText);

        if(self.__transform__):
            return self.__transform__._genSQL_(doOrder=doOrder) if(isinstance(self.__transform__, SQLOrderTransform)) else self.__transform__.genSQL;
        else:
            return self.__source__.genSQL;

    genSQL = property(_genSQL_);

    def gen_lineage(self):
        """create current linage node"""
        cur = LineageNode(self)
        """if this table has already been materialized, then it is considered as a leaf (end of the lineage)"""
        if self.__data__ is not None:
            return cur

        """If there is transform involved"""
        if self.__transform__:
            if isinstance(self.__transform__, SQLSelectTransform):
                src_nodes = self.__transform__.gen_lineage()
                for node in src_nodes:
                    edge = LineageEdge(self, node, 'filter')
                    cur.add_edge(edge)
        if self.__source__:
            if isinstance(self.__source__, tuple):
                src_node1 = self.__source__[0].gen_lineage()
                src_node2 = self.__source__[1].gen_lineage()
                cur.add_edge(LineageEdge(self, src_node1, 'join'))
                cur.add_edge(LineageEdge(self, src_node2, 'join'))
            else:
                src_node = self.__source__.gen_lineage()
                cur.add_edge(LineageEdge(self, src_node))

        return cur


    def upstream_data_exist(self):
        # logging.info(f'[{time.ctime()}] Inside upstream_data_exists. Checking for data for {type(self)} {self}, has data = {self.__data__}, '
        #               f'source {self.__source__}, has type {type(self.__source__)}')
        if self.__data__ is not None or self.__pdData__ is not None:
            return True
        elif isinstance(self.__source__, tuple):
            f1 = self.checkType(self.__source__[0])
            f2 = self.checkType(self.__source__[1])
            return f1 and f2
        else:
            return self.checkType(self.__source__)

    """
    Check if the object is DataFrame or TabularData
    """
    def checkType(self, source):
        if isinstance(source, DBTable):
            if source.__data__ is not None:
                return True
            elif AConfig.FORCEPANDAS:
                source.loadData()
                return True
            return False
        if isinstance(source, DataFrame):
            return source.upstream_data_exist()

    def execute_pandas(self):
        if self.upstream_data_exist():
            if self.__transform__ is not None:
                #logging.info(f'[{time.ctime()}] executePandasSql: is transform')
                # stmt = 'self.__transform__.execute_pandas()'
                # profile = cProfile.runctx(stmt, globals(), locals(), 'cProfile')
                # logging.info('[{}]\n {}'.format(time.ctime(), profile))
                return self.__transform__.execute_pandas()
            if self.__data__ is not None and (self.__pdData__ is None):
                self.__pdData__ = df_from_arrays(self.__data__.values(), self.__data__.keys(), range(self.numRows))

            return self.__pdData__ if self.__pdData__ is not None else self.__source__.execute_pandas()
        return None

    @property
    def rows(self):
        #logging.debug("DataFrame: id {}, {} : rows called.".format(id(self), self.tableName));
        if(self.__data__ is None):
            #logging.debug("DataFrame: {} : rows called, need to produce data.".format(self.tableName));
            if(self.isDBQry):
                fv = FeatureVector(self)
                np.set_printoptions(suppress=True)
                logging.info("Feature vector = {}".format(fv.vector))
                
                data = None
                if not AConfig.FORCEDB:
                    logging.info("Using intergrated pandas to execute the query")
                    data = self.execute_pandas()

                if data is None:
                    logging.info("Using database engine to execute the query ")
                    (data, rows) = self.dbc._executeQry(self._genSQL_(doOrder=True).sqlText + ';');
                #Convert the results to an ordered dictionary format.
                if isinstance(data, pd.DataFrame):
                    #logging.info('[{}]final result dtypes = {}'.format(time.ctime(), data.dtypes))
                    self.__pdData__ = data
                    #logging.info(f'Converting pd to dict, columns = {self.columns}')
                    data_ = collections.OrderedDict()
                    for c in self.columns:
                        data_[c] = data[c].to_numpy()
                    data = data_;
                    self.__data__ = data;
                if(not isinstance(data, collections.OrderedDict)):
                    data_ = collections.OrderedDict();
                    for c in self.columns:
                        data_[c] = data[c];
                    data.clear();
                    data = data_;
                    self.__data__ = data;
            elif(isinstance(self.__transform__, AlgebraicVectorTransform)):
                self.__data__ = self.__transform__.rows;
                self.__columns__ = self.__transform__.columns;
                self.__matrix__ = self.__transform__.matrix;
                self.__rowNames__ = self.__transform__.rowNames;
                #logging.debug("rows : this DataFrame is of instance AlgebraicVectorTransform and produced {} columns {}".format(len(self.__columns__), time.time()));
            elif(isinstance(self.__transform__, UserTransform) or isinstance(self.__transform__, ExternalDataTransform) or isinstance(self.__transform__, VirtualDataTransform) or isinstance(self.__transform__, StackTransform)):
                #logging.debug("DataFrame: {} : rows called retrieving source rows.".format(self.tableName));
                self.__data__ = self.__transform__.rows;
                #logging.debug("DataFrame: {} : rows transform rows retrieved.".format(self.tableName));
                self.__columns__ = self.__transform__.columns;
                #logging.debug("DataFrame: {} : rows transform columns retrieved.".format(self.tableName));
                if(self.__transform__.hasMatrix):
                    self.__matrix__ = self.__transform__.matrix;
                    #logging.debug("DataFrame: {} : rows transform matrix retrieved.".format(self.tableName));
            else:
                #logging.debug("DataFrame: {} : rows called retrieving source rowsNtransform.".format(self.tableName));
                (srcrows, srctransformlist, rowNames) = self.__source__.rowsNtransform;
                self.__data__ = self.__transform__.applyTransform(srcrows, srctransformlist);
                self.__rowNames__ = rowNames;
            #Once we have computed the data, we can dispose of the lineage, but make sure we have all the other things we need.
            self.columns;
            self.dbc;
            #self.rowNames;
            #Now dispose off the lineage.
            #if(isinstance(self.__source__, TabularData)):
                #logging.debug('Reference count to {} is {} before disposing lineage from {}'.format(self.__source__.tableName, sys.getrefcount(self.__source__), self.tableName));
            #else:
                #logging.debug('Reference count to {}&{} is {}&{} before disposing lineage from {}'.format(self.__source__[0].tableName, self.__source__[1].tableName, sys.getrefcount(self.__source__[0]), sys.getrefcount(self.__source__[1]), self.tableName));
            self.__source__ = self.__transform__ = None;

        #logging.debug("DataFrame: id {}, {} : returning rows.".format(id(self), self.tableName));
        return self.__data__;

    @property
    def cdata(self):
        return self.rows;

    def loadData(self, matrix=False):
        """Forces materialization of this Data Frame"""
        self.rows;
        if(matrix):
            self.matrix;

    @property
    def matrix(self):
        if(self.__matrix__ is None):
            rows = self.rows;

        #Sometimes materializing rows also materializes the matrix if the original transformation was a matrix transformation.
        if(self.__matrix__ is None):
            rows = self.rows;
            if(len(rows) == 1): #This object has only one column, no need to try build a matrix.
                #matrix_ = rows[list(rows.keys())[0]];
                matrix_  = rows[rows.keys().__iter__().__next__()];
            else:
                #stack columns next to each other and create a matrix. columns will be stacked horizontally !!
                matrix_ = np.stack( tuple(rows[r] for r in rows) );

            self.__matrix__ = matrix_;

            #Disabling this because once we make a matrix, we loose masked array null flags. therefore we keep both versions for now.
            #rowkeyslist = list(rows.keys());
            #for i in range(0, len(rowkeyslist)): #If we have data in matrix representation, may be we can point the columns in rows to corresponding columns of matrix
            #    rows[rowkeyslist[i]] = matrix_[i]; # WARNING !!! all columns will have the same data type now !!
            #self.__data__ = rows;

        return self.__matrix__;

    @property
    def isMatrixCached(self):
        return self.__matrix__ is not None;

    @property
    def isCached(self):
        return self.__data__ is not None;

    @property
    def rowsNtransform(self):
        #Either the data has already been computed, or will be now computed via SQL or from an AlgebraicVectorTransform
        if(self.isDBQry or self.isCached or isinstance(self.__transform__, AlgebraicVectorTransform) or isinstance(self.__transform__, UserTransform) or isinstance(self.__transform__, ExternalDataTransform) or isinstance(self.__transform__, VirtualDataTransform)):
            rows = self.rows;
            rowNames = self.rowNames if(self.hasRowNames) else None;
            return (rows, None, rowNames);
        #AlgebraicScalarTransform get the data from the lineage and all scalar transformations that needs to be applied on that data
        elif(isinstance(self.__transform__, AlgebraicScalarTransform)):
            (rows, srctransformlist, rowNames) = self.__source__.rowsNtransform;
            rowNames = self.rowNames if(self.hasRowNames) else rowNames;
            return (rows, (srctransformlist if srctransformlist else []) + [self.__transform__], rowNames);

    def __add__(self, other):
        if(type(other) in AIDADtypes.numeric):
            return DataFrame(self, AlgebraicScalarTransform(self, other, OP.ADD));
        return DataFrame( (self, other), AlgebraicVectorTransform(self, other, OP.ADD) );

    def __radd__(self, other):
        if(type(other) in AIDADtypes.numeric):
            return DataFrame(self, AlgebraicScalarTransform(self, other, OP.ADD));
        return DataFrame( (self, other), AlgebraicVectorTransform(self, other, OP.ADD) );

    def __mul__(self, other):
        if(type(other) in AIDADtypes.numeric):
            return DataFrame(self, AlgebraicScalarTransform(self, other, OP.MULTIPLY));
        return DataFrame( (self, other), AlgebraicVectorTransform(self, other, OP.MULTIPLY) );

    def __rmul__(self, other):
        if(type(other) in AIDADtypes.numeric):
            return DataFrame(self, AlgebraicScalarTransform(self, other, OP.MULTIPLY));
        return DataFrame( (self, other), AlgebraicVectorTransform(self, other, OP.MULTIPLY) );

    def __sub__(self, other):
        if(type(other) in AIDADtypes.numeric):
            return DataFrame(self, AlgebraicScalarTransform(self, other, OP.SUBTRACT));
        return DataFrame( (self, other), AlgebraicVectorTransform(self, other, OP.SUBTRACT) );

    def __rsub__(self, other):
        if(type(other) in AIDADtypes.numeric):
            return DataFrame(self, AlgebraicScalarTransform(self, other, OP.SUBTRACT, side=OP.RHS));
        return DataFrame( (self, other), AlgebraicVectorTransform(self, other, OP.SUBTRACT, side=OP.RHS) );

    def __truediv__(self, other):
        if(type(other) in AIDADtypes.numeric):
            return DataFrame(self, AlgebraicScalarTransform(self, other, OP.DIVIDE));
        return DataFrame( (self, other), AlgebraicVectorTransform(self, other, OP.DIVIDE) );

    def __rtruediv__(self, other):
        if(type(other) in AIDADtypes.numeric):
            return DataFrame(self, AlgebraicScalarTransform(self, other, OP.DIVIDE, side=OP.RHS));
        return DataFrame( (self, other), AlgebraicVectorTransform(self, other, OP.DIVIDE, side=OP.RHS) );

    def __pow__(self, power, modulo=None):
        if(type(power) in AIDADtypes.numeric):
            return DataFrame(self, AlgebraicScalarTransform(self, power, OP.EXP));
        else:
            raise TypeError("Cannot use type {} as a power".format(type(power)));

    def __matmul__(self, other):
        return DataFrame( (self, other), AlgebraicVectorTransform(self, other, OP.MATRIXMULTIPLY) );

    def __rmatmul__(self, other):
        return DataFrame( (self, other), AlgebraicVectorTransform(self, other, OP.MATRIXMULTIPLY, side=OP.RHS) );

    @property
    def T(self):
        return DataFrame( self, AlgebraicVectorTransform(self, None, OP.TRANSPOSE) );

    #For slicing [] operations.
    def __getitem__(self, item):
        return DataFrame(self, SliceTransform(self, item));

    #For stacking columns one on top of the other
    def vstack(self, otherdatalist):
        if(isinstance(otherdatalist, tuple) or isinstance(otherdatalist, list)):
            return DataFrame(self, VStackTransform([self, *otherdatalist]));
        else:
            return DataFrame(self, VStackTransform([self, otherdatalist]));

    #For stacking columns side by side.
    def hstack(self, otherdatalist, colprefixlist=None):
        if(isinstance(otherdatalist, tuple) or isinstance(otherdatalist, list)):
            return DataFrame(self, HStackTransform([self, *otherdatalist], colprefixlist));
        else:
            return DataFrame(self, HStackTransform([self, otherdatalist], colprefixlist));

    #User transformations.
    def _U(self, func, *args, **kwargs):
        return DataFrame(self, UserTransform(self, func, *args, **kwargs));

    #Load external data -> called by database workspace (DB Adapter)
    @classmethod
    def _loadExtData_(cls, func, dbc, *args, **kwargs):
        return DataFrame(None, ExternalDataTransform(func, dbc, *args, **kwargs), dbc=dbc);

    @classmethod
    def ones(cls, shape, cols=None, dbc=None):
        #return cls._virtualData_( lambda:np.ones(shape), cols, dbc);
        if(isinstance(shape, tuple)):
            return cls._virtualData_(lambda :np.ones(shape[0]) if(len(shape)==1 or shape[1]==1) else np.ones((shape[1], shape[0])), cols=cols, dbc=dbc);
        else:
            return cls._virtualData_(lambda :np.ones(shape), cols=cols, dbc=dbc);

    @classmethod
    def rand(cls, shape, cols=None, dbc=None):
        if(isinstance(shape, tuple)):
            return cls._virtualData_(lambda :np.random.rand(shape[0]) if(len(shape)==1 or shape[1]==1) else np.random.rand(shape[1], shape[0]), cols=cols, dbc=dbc);
        else:
            return cls._virtualData_( lambda:np.random.rand(shape), cols=cols, dbc=dbc);

    @classmethod
    def randn(cls, shape, cols=None, dbc=None):
        if(isinstance(shape, tuple)):
            return cls._virtualData_(lambda :np.random.randn(shape[0]) if(len(shape)==1 or shape[1]==1) else np.random.randn(shape[1], shape[0]), cols=cols, dbc=dbc);
        else:
            return cls._virtualData_( lambda:np.random.randn(shape), cols=cols, dbc=dbc);

    @classmethod
    def _virtualData_(cls, func, cols=None, colmeta=None, dbc=None):
        def onesmatrix(datafunc, tblcols=None):
            #logging.debug('DEBUG: onesmatrix: called, column names given {}.'.format(tblcols));
            data = datafunc();
            #logging.debug('DEBUG: onesmatrix: datafunc executed, obtained data of type {}.'.format(type(data)));
            #data = np.ones(shape);
            numdatacols = 1 if len(data.shape)  == 1 else data.shape[0];
            if(cols is not None and len(tblcols) != numdatacols):
                logging.error("Error: ones was passed {} column names for a matrix of {} columns".format(len(cols), data.shape[1]));
                raise TypeError("Error: ones was passed {} column names for a matrix of {} columns".format(len(cols), data.shape[1]));
            od = collections.OrderedDict();
            if(tblcols is not None):
                if(numdatacols==1):
                    od[tblcols[0]] = data;
                else:
                    for i in range(0, len(cols)):
                        od[tblcols[i]] = data[i];
            else:
                if(numdatacols==1):
                    od['_c{}_'.format(0)] = data;
                else:
                    for i in range(0, numdatacols):
                        od['_c{}_'.format(i)] = data[i];
            #logging.debug('DEBUG: onesmatrix: returning data.');
            return od;
        #return DataFrame(None, VirtualDataTransform(onesmatrix, dbc if dbc is not None else self.dbc, cols) );
        return DataFrame(None, VirtualDataTransform(onesmatrix, dbc, colmeta, func, tblcols=cols), dbc=dbc );

    def __toUDF__(self):
        if(not self.__tableUDFExists__):
            self.dbc._toTable(self);
            self.__tableUDFExists__ = True;

    def _toUDF_(self):
        return self.__toUDF__();

    def describe(self):
        self.__toUDF__();
        return self.dbc._describe(self);

    def sum(self, collist=None):
        self.__toUDF__();
        return self.dbc._agg(DBC.AGGTYPE.SUM, self, collist);

    def avg(self, collist=None):
        self.__toUDF__();
        return self.dbc._agg(DBC.AGGTYPE.AVG, self, collist);

    def count(self, collist=None):
        self.__toUDF__();
        return self.dbc._agg(DBC.AGGTYPE.COUNT, self, collist);

    def countd(self, collist=None):
        self.__toUDF__();
        return self.dbc._agg(DBC.AGGTYPE.COUNTD, self, collist);

    def countn(self, collist=None):
        self.__toUDF__();
        return self.dbc._agg(DBC.AGGTYPE.COUNTN, self, collist);

    def max(self, collist=None):
        self.__toUDF__();
        return self.dbc._agg(DBC.AGGTYPE.MAX, self, collist);

    def min(self, collist=None):
        self.__toUDF__();
        return self.dbc._agg(DBC.AGGTYPE.MIN, self, collist);

    def head(self, n=5):
        if(self.__data__ is None):
            #logging.debug("DEBUG: head() loading data ...")
            self.loadData();
            #logging.debug("DEBUG: head() loaded data ...")
        data = collections.OrderedDict();
        #logging.debug("DEBUG: head() processing {} columns ...".format(len(self.columns)));
        for c in self.columns:
            #logging.debug("DEBUG: head() processing column {} ...".format(c))
            data[c] = self.__data__[c][0:n];
        #logging.debug("DEBUG: head() returning data ...")
        return pd.DataFrame(data=data);

    def tail(self, n=5):
        if(self.__data__ is None):
            self.loadData();
        data = collections.OrderedDict();
        for c in self.columns:
            data[c] = self.__data__[c][-1*n:];
        return pd.DataFrame(data=data);

    def __del__(self):
        #logging.debug("Removing dborm {}".format(self.__tableName__));
        if(self.tableUDFExists):
            self.dbc._dropTblUDF(self);
        if(self.__data__ is not None):
            #self.__data__.clear();
            del self.__data__;
        if(self.__pdData__ is not None):
            del self.__pdData__;
        if(self.__matrix__ is not None):
            del self.__matrix__;
        if(self.__rowNames__ is not None):
            del self.__rowNames__;


class LineageNode:
    def __init__(self, df: TabularData):
        self.node = df
        self.edges = []

    def add_edge(self, edge):
        self.edges.append(edge)

    def __str__(self):
        s = str(self.node)
        for edge in self.edges:
            s += str(edge)
        return s


class LineageEdge:
    def __init__(self, start, end=None, linkage=None):
        self.start = start
        self.end = end
        self.linage = linkage

    def __str__(self):
        s = "--{}-->{}\n".format(self.linage, self.end)
        return s


class FeatureVector:
    COUNT_EXP = "SELECT COUNT(*) FROM {};"
    DB_NAME = "sf01"

    @classmethod
    def _collect_from_lineage(cls, root):
        tables = set()
        traversed = [root]
        while traversed:
            cur = traversed.pop()
            tables.add(cur.node)
            for edge in cur.edges:
                traversed.append(edge.end)
        return tables


    @classmethod
    def extract_feature(cls, tb, dp):
        if tb.__data__ is not None:
            if tb.__pdData__ is None:
                tb.__pdData__ = df_from_arrays(tb.__data__.values(), tb.__data__.keys(), range(tb.numRows))
            if str(tb) not in dp:
                dp.add(str(tb))
                types = list(filter(lambda x: x == 'object', tb.__pdData__.dtypes))
                #logging.info("Numpy type = {}, col={}, row={}".format(types, tb.columns, tb.numRows))
                return np.array([0, 0, len(tb.columns), tb.numRows, 0, len(types)], dtype=float)
        else:
            """
            If no data is inside the memory, send query to db to retrieve the column and row numbers
            """
            if isinstance(tb, DBTable):
                if str(tb) not in dp:
                    """retrieved value is a tuple with form ({'L1', array[]}, 1)"""
                    rowNum = list(tb.dbc._executeQry(cls.COUNT_EXP.format(tb.__tableName__))[0].values())[0][0]
                    colNum = list(tb.dbc._executeQry(tb.dbc.__COL_EXP__.format(tb.__tableName__))[0].values())[0][0]
                    strNum = list(tb.dbc._executeQry(tb.dbc.__STRING_TYPE_EXP__.format(tb.__tableName__))[0].values())[0][0]
                    # logging.info(
                    #     "Retrieve info via db query, table={}, row={}, col={}".format(tb.tableName, rowNum, colNum))
                    dp.add(str(tb))
                    return np.array([rowNum, colNum, 0, 0, strNum, 0])
            else:
                """
                add vectors together for all upstream data
                """
                lineage = tb.gen_lineage()
                tables = cls._collect_from_lineage(lineage)
                #logging.info("PRINT LINEAGE: \n {} \n tables#={}".format(lineage, len(tables)))
                vector = np.array([0, 0, 0, 0, 0, 0])
                for table in tables:
                    if isinstance(table, DBTable) or table.__data__ is not None:
                        vector = vector + cls.extract_feature(table, dp)
                        logging.info("Vector = {}, dp={}".format(vector, dp))
                return vector

        return np.zeros(6, dtype=int)

    def __init__(self, df):
        self.df = df
        self.__vector__ = None

    @property
    def vector(self):
        if self.__vector__ is None:
            self.__vector__ = self.extract_feature(self.df, set())
        return self.__vector__
