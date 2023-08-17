import logging
import sys;
import os;
import threading;

import collections;
import datetime;
## -- QLOG -- ##
##from timeit import default_timer as timer;
import weakref

import numpy as np;
import pandas as pd;

import pymonetdb.sql;

from aidacommon.aidaConfig import AConfig, UDFTYPE;

from aidacommon.dbAdapter import *;
from aidas.rdborm import *;
from aidas.dborm import DBTable, DataFrame;

import queue

DBC._dataFrameClass_ = DataFrame;

class DBCMonetDB(DBC):
    """Database adapter class for MonetDB"""

    #We will use this to map numpy data types to MonetDB compatible types.
    typeConverter = { int: 'integer', np.int8:'TINYINT', np.int16:'SMALLINT', np.int32:'INTEGER', np.int64:'BIGINT'
                    , np.float32:'FLOAT', np.float64:'FLOAT', object:'STRING', np.object_:'STRING', bytearray:'BLOB'
                    , 'date':'DATE', 'time':'TIME', 'timestamp':'TIMESTAMP' };

    datetimeFormats = {'%Y-%m-%d':'date', '%H:%M:%S':'time', '%Y-%m-%d %H:%M:%S':'timestamp'};

    #Query to get the names of all tables in the database.
    __TABLE_LIST_QRY__ = \
        "SELECT t.name as tableName " \
        "FROM sys.tables t " \
        "  INNER JOIN sys.schemas s " \
        "    ON t.schema_id = s.id " \
        "WHERE s.name = '{}'"\
        ";"

    #Query to fetch a table schema in MonetDB (does not support other database objects such as views)
    __TABLE_METADATA_QRY__ = \
        "SELECT s.name as schemaName, t.name as tableName, c.name as columnName, c.type as columnType, " \
        "  c.type_digits as columnSize, c.number as columnPos, c.\"null\" as columnNullable " \
        "FROM sys.tables t " \
        "  INNER JOIN sys.schemas s " \
        "    ON t.schema_id = s.id " \
        "  INNER JOIN sys.columns c " \
        "    ON t.id = c.table_id " \
        "WHERE s.name = '{}' AND t.name = '{}' " \
        "ORDER BY schemaName, tableName, columnPos " \
        ";"


    #Query to fetch a table schema in MonetDB (does not support other database objects such as views)
    __COLUMN_METADATA_QRY__ = \
        "SELECT c.name as columnName, c.type as columnType, " \
        "  c.type_digits as columnSize, c.number as columnPos, c.\"null\" as columnNullable " \
        "FROM sys.tables t " \
        "  INNER JOIN sys.schemas s " \
        "    ON t.schema_id = s.id " \
        "  INNER JOIN sys.columns c " \
        "    ON t.id = c.table_id " \
        "WHERE s.name = '{}' AND t.name = '{}' " \
        "ORDER BY columnPos " \
        ";"

    __NUMERIC_COL_DESCRIBE__ = \
        "  RIGHT ('                    ' || CAST(CAST(SYS.COUNT({}) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS count_{}" \
        ", RIGHT ('                    ' || CAST(CAST(SYS.COUNT(DISTINCT {}) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS countd_{}" \
        ", RIGHT ('                    ' || CAST(CAST(SYS.SUM(CASE WHEN {} IS NULL THEN 1 ELSE 0 END) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS countn_{}" \
        ", RIGHT ('                    ' || CAST(CAST(SYS.MAX({}) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS max_{}" \
        ", RIGHT ('                    ' || CAST(CAST(SYS.MIN({}) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS min_{}" \
        ", RIGHT ('                    ' || CAST(CAST(SYS.AVG({}) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS avg_{}" \
        ", RIGHT ('                    ' || CAST(CAST(SYS.MEDIAN({}) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS median_{}" \
        ", RIGHT ('                    ' || CAST(CAST(SYS.QUANTILE({}, 0.25) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS q25_{}" \
        ", RIGHT ('                    ' || CAST(CAST(SYS.QUANTILE({}, 0.50) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS q50_{}" \
        ", RIGHT ('                    ' || CAST(CAST(SYS.QUANTILE({}, 0.75) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS q75_{}" \
        ", RIGHT ('                    ' || CAST(CAST(SYS.STDDEV_POP({}) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS std_{}";

    __CHAR_COL_DESCRIBE__ = \
        "  RIGHT ('                    ' || CAST(CAST(SYS.COUNT({}) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS count_{}" \
        ", RIGHT ('                    ' || CAST(CAST(SYS.COUNT(DISTINCT {}) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS countd_{}" \
        ", RIGHT ('                    ' || CAST(CAST(SYS.SUM(CASE WHEN {} IS NULL THEN 1 ELSE 0 END) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS countn_{}" \
        ", RIGHT ('                    ' || CAST(SYS.MAX({}) AS VARCHAR(20)), 20) AS max_{}" \
        ", RIGHT ('                    ' || CAST(SYS.MIN({}) AS VARCHAR(20)), 20) AS min_{}" \
        ", '                    ' AS avg_{}" \
        ", '                    ' AS median_{}" \
        ", '                    ' AS q25_{}" \
        ", '                    ' AS q50_{}" \
        ", '                    ' AS q75_{}" \
        ", '                    ' AS std_{}";

    __COL_EXP__ = "SELECT COUNT(*) FROM sys.columns c WHERE c.table_id IN (SELECT id FROM sys.tables t WHERE t.name=\'{" \
              "}\');"
    __STRING_TYPE_EXP__ = "SELECT COUNT(*) FROM sys.columns c WHERE c.type=\'varchar\' " \
                      "AND c.table_id IN (SELECT id FROM sys.tables t WHERE t.name=\'{}\');"

    #TODO: Throw an error and abort object creation in case of failures.
    def __new__(cls, dbname, username, password, jobName, dbcRepoMgr, serverIPAddr):
        """See if the connection works, authentication fails etc. In which case we do not need to continue with the object creation"""
        #logging.debug("__new__ called for {}".format(jobName));
        con = pymonetdb.Connection(dbname,hostname='localhost',username=username,password=password,autocommit=True);
        #logging.debug("connection test for {} result {}".format(jobName, con));
        con.close();
        return super().__new__(cls);

    def __init__(self, dbname, username, password, jobName, dbcRepoMgr, serverIPAddr):
        """Actual object creation"""
        #logging.debug("__init__ called for {}".format(jobName));
        self.__qryLock__ = threading.Lock();
        self._username = username; self._password = password;
        self._extDBCcon = None;
        self._con_thread = None;
        self._requestQueue = queue.Queue();
        self._responseQueue = queue.Queue();
        #To setup things at the repository
        super().__init__(dbcRepoMgr, jobName, dbname, serverIPAddr);
        #Setup the actual database connection to be used.
        self.__setDBC__();


    def __setDBC__(self):
        con = pymonetdb.Connection(self.dbName,hostname='localhost',username=self._username,password=self._password,autocommit=True);
        ##cursor = con.cursor();
        #This function call should set the internal database connection to MonetDB in THIS DBC object, using the jobName passed to it.
        #The database function basically calls back the _setConnection method.
        ##rows = cursor.execute('select status from aidas_setdbccon(\'{}\');'.format(self._jobName));
        ##data = cursor.fetchall();
        #logging.debug("rows = {} data = {} after setting dbc con.".format(rows, data));
        ##cursor.close();

        self._con_thread = threading.Thread(target=con.execute, args=('select status from aidas_setdbccon(\'{}\');'.format(self._jobName),));
        self._con_thread.start()
        self.__extDBCcon = con;

    def _tables(self):
        """List tables in the database"""
        (tables, count) = self._executeQry(DBCMonetDB.__TABLE_LIST_QRY__.format(self.dbName));
        return pd.DataFrame(tables);

        #TODO: override __getattr__ to call this internally when refered to as dbc.tableName ? - DONE in the DBC class.
    def _getDBTable(self, relName, dbName=None):
        #logging.debug(DBCMonetDB.__TABLE_METADATA_QRY__.format( dbName if(dbName) else self.dbName, relName));
        (metaData_, rows) = self._executeQry(DBCMonetDB.__TABLE_METADATA_QRY__.format( dbName if(dbName is not None) else self.dbName, relName));
        if(rows ==0):
            logging.error("ERROR: cannot find table {} in {}".format(relName, dbName if(dbName is not None) else self.dbName ));
            raise KeyError("ERROR: cannot find table {} in {}".format(relName, dbName if(dbName is not None) else self.dbName ));
        #logging.debug("execute query returned for table metadata {}".format(metaData_));
        #metaData = _collections.OrderedDict();
        #for column in [ 'schemaname', 'tablename', 'columnname', 'columntype', 'columnsize', 'columnpos', 'columnnullable']:
        #    logging.debug("For column {} data is {}".format(column, metaData_[column].data if hasattr(metaData_[column], 'data') else metaData_[column]));
        #    metaData[column] = metaData_[column].data if hasattr(metaData_[column], 'data') else metaData_[column];

        #return DBTable(self, metaData_);
        d = DBTable(self, metaData_);
        return d;
        #return DBTable(self, (metaData['schemaname'], metaData['tablename'], metaData['columnname'], metaData['columntype'], metaData['columnsize'], metaData['columnpos'], metaData['columnnullable'] ));

    def _setConnection(self, con):
        """Called by the database function to set the internal database connection for executing queries"""
        #logging.debug("__setConnection_ called for {} with {}".format(self._jobName, con));
        self.__connection= con;

    def _getRequestQueue(self):
        # return weakref.proxy(self._requestQueue)
        return self._requestQueue

    def _getResponseQueue(self):
        # return weakref.proxy(self._responseQueue)
        return self._responseQueue

    def _executeQry(self, sql, resultFormat='column', sqlType=DBC.SQLTYPE.SELECT):
        self._requestQueue.put(sql);
        time.sleep(0);
        result = self._responseQueue.get();
        if isinstance(result, Exception):
            logging.error(f"MonetDB encountered error: {result}")
            raise result
        if (sqlType == DBC.SQLTYPE.SELECT):
            if (resultFormat == 'column'):
                # get some columns
                c_tmp = result[list(result.keys())[0]]
                # Find the length of the array (or masked array) that is the number of rows
                rows = len(c_tmp.data if hasattr(c_tmp, 'data') else c_tmp);
                # rows = None;

                # for col in result:
                # logging.debug("_executeQry col {} size {}  references {}".format(col, sys.getsizeof(result[col]), sys.getrefcount(result[col])));
                # for c in result:
                #    logging.debug("Result column {} type {}".format(c, result[c].dtype));

                return (result, rows);
            elif (resultFormat == 'row'):
                pass;

    def _execution(self, sql, resultFormat='column', sqlType=DBC.SQLTYPE.SELECT):
        """Execute a query and return results"""
        #TODO: either support row format results or throw an exception for not supported.
        #logging.debug("__executeQry called for {} with {}".format(self._jobName, sql));

        with self.__qryLock__:
            try:
                ## -- QLOG -- ##
                ##st = timer();
                result = self.__connection.execute(sql);
                ## -- QLOG -- 2##
                ##et = timer();
                ##logging.info("_executeQry: {} {}".format(et-st, sql.replace("\n", "")))
                #logging.debug("__executeQry result {}".format(result));
                if(sqlType==DBC.SQLTYPE.SELECT):
                    if(resultFormat == 'column'):
                        #get some columns
                        c_tmp = result[list(result.keys())[0]]
                        #Find the length of the array (or masked array) that is the number of rows
                        rows = len(c_tmp.data if hasattr(c_tmp, 'data')  else c_tmp);
                        #rows = None;

                        #for col in result:
                            #logging.debug("_executeQry col {} size {}  references {}".format(col, sys.getsizeof(result[col]), sys.getrefcount(result[col])));
                        #for c in result:
                        #    logging.debug("Result column {} type {}".format(c, result[c].dtype));

                        return (result, rows);
                    elif(resultFormat == 'row'):
                        pass;

            except Exception as e:
                the_type, the_value, the_traceback = sys.exc_info();
                logging.error("An exception occured while trying to execute query {}".format((the_type, the_value, the_traceback)));
                logging.exception("An exception occured while executing query {}".format(sql));
                #TODO: This is a patch fix as for some reason we are not able to execut queries using the connection once an exception is thrown.
                self.__setDBC__();
                #re-raise the exception.
                if (sqlType != DBC.SQLTYPE.DROP):
                    raise;

            #TODO: format....

    def _toTable(self, tblrData, tableName=None):
        if(tableName is None):
            if(hasattr(tblrData, 'tableName')):
                tableName = tblrData.tableName;
        if(tableName is None):
            logging.warning("Error cannot deduce a tableName for the Tabular Data passed");
            raise AttributeError("Error cannot deduce a tableName for the Tabular Data passed")

        #TODO: instead of infering the data type from the data, use columns to figure it out.
        #TODO: WARNING !! that might not work, as it means a sql calling table udf may need the udf to execute another sql to load its data.
        data = tblrData.rows;

        #Should we create a regular UDF or use a virtual table ?
        if(AConfig.UDFTYPE == UDFTYPE.TABLEUDF):
            cudf = 'CREATE FUNCTION {}() RETURNS TABLE({}) LANGUAGE PYTHON \n{{\n import copy; from aidacommon.dbAdapter import DBC; return copy.deepcopy(DBC._getDBTable(\'{}\').rows); \n}}\n;';
            #cudf = 'CREATE FUNCTION {}() RETURNS TABLE({}) LANGUAGE PYTHON \n{{\n from aidacommon.dbAdapter import DBC; return DBC._getDBTable(\'{}\').rows; \n}}\n;';

            #The column list returned by this table udf.
            collist=None;
            for colname in data:
                collist = (collist + ',') if(collist is not None) else '';
                dataType = data[colname].dtype.type;
                if(dataType is np.object_):
                    try:
                        cd = data[colname][0];
                        if(isinstance(cd, bytearray)):
                            dataType = bytearray;
                        elif(isinstance(cd, str)):
                            for f in self.datetimeFormats:
                                try:
                                    datetime.datetime.strptime(cd, f);
                                    dataType = self.datetimeFormats[f];
                                    break;
                                except ValueError:
                                    pass;
                    except IndexError:
                        pass;
                #logging.debug("UDF column {} type {}".format(colname, dataType));
                collist += colname + ' ' +DBCMonetDB.typeConverter[dataType] ;

            cudf = cudf.format(tableName, collist, tableName)
            #logging.debug("Creating Table UDF {}".format(cudf));
            self._executeQry(cudf,sqlType=DBC.SQLTYPE.CREATE);
        else :
            #for c in data:
                #logging.debug("Table {} Column {} Type {}".format(tableName, c, data[c].dtype ));
                #logging.debug("Table {} Column {} Type {}".format(tableName, c, type(data[c])));
            options = {}; #Figure out if we have any date, time, timetamp fields and use it for lazy evaluation and data type conversion.
            for c in data:
                try:
                    cd = data[c][0];
                    if(isinstance(cd, bytearray)):
                        options[c] = 'blob';
                    if(not isinstance(cd, str)):
                        continue;
                    for f in self.datetimeFormats:
                        try:
                           datetime.datetime.strptime(cd, f);
                           options[c] = self.datetimeFormats[f];
                           #logging.debug("VirtualTable: {} will be converted to {}".format(c, options[c]));
                           break;
                        except ValueError:
                           pass;
                except IndexError:
                    pass;
            #logging.debug("__connection.registerTable : data={} tableName={} dbname={} cols={} options={}".format(data, tableName, self.dbName, list(data.keys()), options));
            ## -- QLOG -- ##
            ##st = timer();
            self.__connection.registerTable(data, tableName, self.dbName, cols=list(data.keys()), options=options);
            ## -- QLOG -- 2##
            ##et = timer();
            ##logging.info("_registerTable: {} CREATE FUNCTION {}".format(et-st, tableName));
        #Keep track of our UDFs/Virtual tables;
        #logging.debug("Created Table UDF/VT {}.{}".format(self.dbName, tableName));
        self._tableRepo_[tableName] = tblrData;

    def _saveTblrData(self, tblrData, tableName, dbName=None, drop=False):
        if(isinstance(tblrData, DBTable)):
            logging.error("TabularData object is already a table, {}".format(tblrData.tableName));
            raise TypeError("TabularData object is already a table, {}".format(tblrData.tableName));

        try:
            #Does the table already exist ?
            self._getDBTable(tableName, dbName);
            #Should it be dropped ?
            if(not drop):
                logging.error('ERROR: {} already exists in {}'.format(tableName, dbName if (dbName is not None) else self.dbName));
                raise ValueError('ERROR: {} already exists in {}'.format(tableName, dbName if (dbName is not None) else self.dbName));
            else:
                self._dropTable(tableName);
                #delattr(self, tableName);
            #    #TODO: remove possible references in dw workspace, etc., ?
        except KeyError:
            pass

        if(AConfig.UDFTYPE == UDFTYPE.TABLEUDF):
            tblrData._toUDF_();
            ctbl = 'CREATE TABLE {} AS SELECT * FROM {}();'.format(tableName, tblrData.tableName);
            self._executeQry(ctbl, sqlType=DBC.SQLTYPE.CREATE);
            #self._toTable(tblrData, tableName);
        else:
            self._toTable(tblrData, tableName);
            try:
                #logging.debug('DEBUG: _saveTblrData: persisting table {} in {}'.format(tableName, dbName if (dbName) else self.dbName));
                self.__connection.persistTable(tableName, dbName if (dbName is not None) else self.dbName);
                #pass;
                #logging.debug('DEBUG: _saveTblrData: persisted table {}'.format(tableName));
            except:
                logging.exception("ERROR: _saveTblrData : while persisting");
                raise;

        setattr(self, tableName, self._getDBTable(tableName, dbName));


    def _dropTable(self, tableName, dbName=None):
        dtbl = 'DROP TABLE {}.{};'.format( dbName if(dbName is not None) else self.dbName, tableName );
        self._executeQry(dtbl, DBC.SQLTYPE.DROP);

        try:
            del self._tableRepo_[tableName];
        except KeyError:
            pass;


    def _dropTblUDF(self, tblrData, tableName=None):
        if(tableName is None):
            if(hasattr(tblrData, 'tableName')):
                tableName = tblrData.tableName;
        if(tableName is None):
            logging.warning("Error cannot deduce a tableName for the Tabular Data passed");
            raise AttributeError("Error cannot deduce a tableName for the Tabular Data passed");

        #logging.debug("Dropping Table UDF/VT {}".format(tableName));
        dropObjectType = 'FUNCTION' if(AConfig.UDFTYPE == UDFTYPE.TABLEUDF) else 'TABLE';
        dobj = 'DROP {} {};'.format(dropObjectType, tableName);
        #logging.debug("Dropping Table UDF/Virtual table {}".format(dobj));
        self._executeQry(dobj,sqlType=DBC.SQLTYPE.DROP);

    def _describe(self, tblrData):
        if(isinstance(tblrData, DBTable)):
            # logging.info("describing DBTable ");
            #logging.info(DBCMonetDB.__COLUMN_METADATA_QRY__.format(tblrData.schemaName, tblrData.tableName));

            colMeta, numcols = self._executeQry(DBCMonetDB.__COLUMN_METADATA_QRY__.format(tblrData.schemaName, tblrData.tableName));
            sqlsel = None;
            for c in range(0, numcols):
                cname = colMeta["columnname"][c];
                ctype = colMeta["columntype"][c];
                if(ctype in ('char', 'varchar', 'timestamp')):
                    colsel = DBCMonetDB.__CHAR_COL_DESCRIBE__.format(cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname);
                else:
                    colsel = DBCMonetDB.__NUMERIC_COL_DESCRIBE__.format(cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname);
                sqlsel = ((sqlsel + '\n,') if(sqlsel is not None) else '\n' ) + colsel;

            sqlsel = "SELECT {} \n FROM {};".format(sqlsel, tblrData.tableName);
            #logging.debug("Performing describe : {}".format(sqlsel));
            data,rows = self._executeQry(sqlsel);
            #descData = {};
            descData = collections.OrderedDict();
            for c in range(0, numcols):
                cname = colMeta["columnname"][c];
                cmetrics = [];
                for sinfo in ['count_', 'countd_', 'countn_', 'max_', 'min_', 'avg_', 'median_', 'q25_', 'q50_', 'q75_', 'std_']:
                    cmetrics.append(data[sinfo+cname]);
                descData[cname] = cmetrics;
            desc = ['count', 'unique', 'nulls', 'max', 'min', 'avg', 'median', '25%', '50%', '75%', 'stddev'];
            return pd.DataFrame(data=descData, index=desc);

        else:
            tdata = tblrData.rows;
            sqlsel = None;
            for cname in tdata:
                #logging.info("{} maps to {} {}".format(cname, tdata[cname].dtype.type, DBCMonetDB.typeConverter[tdata[cname].dtype.type]));
                if(DBCMonetDB.typeConverter[tdata[cname].dtype.type] in ['STRING']):
                    colsel = DBCMonetDB.__CHAR_COL_DESCRIBE__.format(cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname);
                else:
                    colsel = DBCMonetDB.__NUMERIC_COL_DESCRIBE__.format(cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname);
                sqlsel = ((sqlsel + '\n,') if(sqlsel is not None) else '\n' ) + colsel;

            sqlsel = "SELECT {} \n FROM {}{};".format(sqlsel, tblrData.tableName, '()' if(AConfig.UDFTYPE == UDFTYPE.TABLEUDF) else '');
            #logging.debug("Performing describe : {}".format(sqlsel));
            data,rows = self._executeQry(sqlsel);
            #descData = {};
            descData = collections.OrderedDict();
            for cname in tdata:
                cmetrics = [];
                for sinfo in ['count_', 'countd_', 'countn_', 'max_', 'min_', 'avg_', 'median_', 'q25_', 'q50_', 'q75_', 'std_']:
                    cmetrics.append(data[sinfo+cname]);
                descData[cname] = cmetrics;
            desc = ['count', 'unique', 'nulls', 'max', 'min', 'avg', 'median', '25%', '50%', '75%', 'stddev'];
            return pd.DataFrame(data=descData, index=desc);


    def _agg(self, agfn, tblrData, collist=None, valueOnly=True):
        collist = None if(collist is None) else [collist] if(isinstance(collist, str)) else collist;
        result = collections.OrderedDict();

        if(isinstance(tblrData, DBTable)):
            colMeta, numcols = self._executeQry(DBCMonetDB.__COLUMN_METADATA_QRY__.format(tblrData.schemaName, tblrData.tableName));
            sqlsel = None;
            for c in range(0, numcols):
                cname = colMeta["columnname"][c];
                ctype = colMeta["columntype"][c];
                if(collist is None or cname in collist):
                    if(ctype in ('char', 'varchar') and agfn in (DBC.AGGTYPE.SUM, DBC.AGGTYPE.AVG)):
                        colsel = "'' AS agg_{}".format(cname);
                    else:
                        colsel = agfn.value.format(cname) + (' AS agg_{}').format(cname);
                    sqlsel = ((sqlsel + '\n,') if(sqlsel is not None) else '\n' ) + colsel;

            sqlsel = "SELECT {} \n FROM {};".format(sqlsel, tblrData.tableName);
            data,rows = self._executeQry(sqlsel);
            #logging.debug("Performing agg : {}".format(sqlsel));
            for c in range(0, numcols):
                cname = colMeta["columnname"][c];
                if(collist is None or cname in collist):
                    result[cname] = data['agg_{}'.format(cname)][0];
        else:
            tdata = tblrData.rows;
            sqlsel = None;
            for cname in tdata:
                #logging.info("{} maps to {} {}".format(cname, tdata[cname].dtype.type, DBCMonetDB.typeConverter[tdata[cname].dtype.type]));
                if(DBCMonetDB.typeConverter[tdata[cname].dtype.type] in ['STRING'] and agfn in (DBC.AGGTYPE.SUM, DBC.AGGTYPE.AVG)):
                    colsel = "'' AS agg_{}".format(cname);
                else:
                    colsel = agfn.value.format(cname) + (' AS agg_{}').format(cname);
                sqlsel = ((sqlsel + '\n,') if(sqlsel is not None) else '\n' ) + colsel;

            sqlsel = "SELECT {} \n FROM {}{};".format(sqlsel, tblrData.tableName, '()' if(AConfig.UDFTYPE == UDFTYPE.TABLEUDF) else '');
            #logging.info("Performing agg : {}".format(sqlsel));
            data,rows = self._executeQry(sqlsel);
            for cname in tdata:
                if(collist is None or cname in collist):
                    result[cname] = data['agg_{}'.format(cname)][0];

        #logging.debug("DEBUG: returning agg results {}".format(result));

        #return result[list(result.keys())[0]] if(len(result)==1 and collist is not None) else result;
        return result[list(result.keys())[0]] if(len(result)==1 and valueOnly) else result;

    def _close(self):
        self.__del__()

    def __del__(self):
        logging.debug("__del__ called for {}".format(self._jobName));
        super()._close()
        del self._requestQueue
        del self._responseQueue

        #Where we using regular Table UDFs or Virtuable Tables ?
        dropObjectType = 'FUNCTION' if(AConfig.UDFTYPE == UDFTYPE.TABLEUDF) else 'TABLE';

        for obj in self._tableRepo_.keys():
            try:
                objval = self._tableRepo_[obj];
                if(not isinstance(objval, DBTable)):
                    #logging.debug("Dropping {} {}".format(dropObjectType, obj));
                    self._executeQry('DROP {} {};'.format(dropObjectType, obj), sqlType=DBC.SQLTYPE.DROP);
                    #logging.debug("dropped {} {}".format(dropObjectType, obj));
            except:
                pass;
        if(self.__extDBCcon is not None):
            self.__extDBCcon.close();
            self.__extDBCcon = None;

class DBCMonetDBStub(DBCRemoteStub):
    @aidacommon.rop.RObjStub.RemoteMethod()
    def _close(self):
        pass

copyreg.pickle(DBCMonetDB, DBCMonetDBStub.serializeObj);
copyreg.pickle(DBCMonetDBStub, DBCMonetDBStub.serializeObj);
