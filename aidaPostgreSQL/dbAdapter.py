import sys;
import threading;

import collections;
from datetime import datetime;

import numpy as np;
import pandas as pd;

import psycopg2;

from aidacommon.aidaConfig import AConfig, UDFTYPE;

from aidacommon.dbAdapter import *;
from aidas.rdborm import *;
from aidas.dborm import DBTable, DataFrame;


DBC._dataFrameClass_ = DataFrame;

class DBCPostgreSQL(DBC):
    """Database adapter class for PostgreSQL"""

    # Map numpy data types to Postgresql compatible types.
    typeConverter = {np.int16:'smallint', np.int32:'integer', np.int64:'bigint'
                    , np.float32:'double precision', np.float64:'double precision', np.object:'text', np.object_:'text'};


    #Query to get the names of all tables in the database.
    __TABLE_LIST_QRY__ = \
        "SELECT table_name as tableName " \
        "FROM information_schema.tables " \
        "WHERE table_schema = '{}'     " \
        "AND table_type = 'BASE TABLE' " \
        ";"

    # Not enough information to find  columnSize

    #Query to fetch a table schema in MonetDB (does not support other database objects such as views)
    __TABLE_METADATA_QRY__ = \
        "SELECT table_schema as schemaName, table_name as tableName, column_name as columnName, data_type as columnType, " \
        "0 as columnSize, ordinal_position as columnPos, is_nullable as columnNullable " \
        "FROM information_schema.columns      " \
        "WHERE table_schema = '{}' AND table_name = '{}' " \
        "ORDER BY schemaName, tableName, columnPos " \
        ";"

    #Query to fetch a table schema in MonetDB (does not support other database objects such as views)
    __COLUMN_METADATA_QRY__ = \
        "SELECT column_name as columnName, data_type as columnType, " \
        "0 as columnSize, ordinal_position as columnPos, is_nullable  as columnNullable " \
        "FROM information_schema.columns " \
        "WHERE table_schema = '{}' AND table_name = '{}' " \
        "ORDER BY columnPos " \
        ";"

    """unfinished"""
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

    """unfinished"""
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

    #TODO: Throw an error and abort object creation in case of failures.
    def __new__(cls, dbname, username, password, jobName, dbcRepoMgr):
        """See if the connection works, authentication fails etc. In which case we do not need to continue with the object creation"""
        #logging.debug("__new__ called for {}".format(jobName));
        con = psycopg2.connect(dbname=dbname,user=username,password=password,host='localhost');
        con.autocommit = True
        #logging.debug("connection test for {} result {}".format(jobName, con));
        con.close();
        return super().__new__(cls);

    def __init__(self, dbname, username, password, jobName, dbcRepoMgr):
        """Actual object creation"""
        #logging.debug("__init__ called for {}".format(jobName));
        self.__qryLock__ = threading.Lock();
        self._username = username; self._password = password;
        #To setup things at the repository
        super().__init__(dbcRepoMgr, jobName, dbname);

        self._executeEmpty = threading.Semaphore(1);
        self._executeFull = threading.Semaphore(0);
        self._executeLock = threading.RLock();
        #Setup the actual database connection to be used.
        self.__setDBC__();


    def __setDBC__(self):
        con = psycopg2.connect(dbname=self.dbName,user=self._username,password=self._password,host='localhost');
        con.autocommit = True
        cursor = con.cursor();
        #This function call should set the internal database connection to MonetDB in THIS DBC object, using the jobName passed to it.
        #The database function basically calls back the _setConnection method.
        #rows = cursor.execute('select status from aidas_setdbccon(\'{}\');'.format(self._jobName));
        #data = cursor.fetchall();
        #logging.debug("rows = {} data = {} after setting dbc con.".format(rows, data));
        cursor.close();
        self.__extDBCcon = con;


    def _tables(self):
        self._sql = DBCPostgreSQL.__TABLE_LIST_QRY__.format(self.dbName);
        (tables,rows) = self._getOutput();
        return pd.DataFrame(tables);

        #TODO: override __getattr__ to call this internally when refered to as dbc.tableName ? - DONE in the DBC class.
    def _getDBTable(self, relName, dbName=None):
        pass;

    def _setConnection(self, con):
        """Called by the database function to set the internal database connection for executing queries"""
        #logging.debug("__setConnection_ called for {} with {}".format(self._jobName, con));
        con.execute('select status from prepare_execution(\'{}\');'.format(self._jobName))
        self._isWaitingforExecution = False;
 
    def _preparePlpy(self,con):
        if self._isWaitingforExecution:
            self.__connection = con;
            self._executeEmpty.acquire();
            self._executeLock.acquire();
            (self._x,self._y)  = self._executeQry(self._sql);
            self._executeLock.release();
            self._executeFull.release();
            del self.__connection;
   
    def _getOutput(self):
        self._conMgr._currentJobName = self._jobName;
        self._isWaitingforExecution = True;
        self._executeFull.acquire();
        self._executeLock.acquire();
        (result,rows) = (self._x,self._y);
        self._executeLock.release();
        self._executeEmpty.release();
        self._conMgr._currentJobName = None;
        self._isWaitingforExecution = False;
        return (result,rows);

    
    def _executeQry(self, sql, resultFormat='column', sqlType=DBC.SQLTYPE.SELECT):
        """Execute a query and return results"""
        #TODO: either support row format results or throw an exception for not supported.
        #logging.debug("__executeQry called for {} with {}".format(self._jobName, sql));

        with self.__qryLock__:
            try:
                rv = self.__connection.execute(self._sql);
                result = {}
                for k in rv[0]:
                   result[k] = np.array([d[k] for d in rv]).astype(object)
                logging.info("__executeQry result {}".format(result));
                if(sqlType==DBC.SQLTYPE.SELECT):
                    if(resultFormat == 'column'):
                        #get some columns
                        c_tmp = result[list(result.keys())[0]]
                        #Find the length of the array (or masked array) that is the number of rows
                        rows = len(c_tmp.data if hasattr(c_tmp, 'data')  else c_tmp);
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
        pass;
           
                       

    def _saveTblrData(self, tblrData, tableName, dbName=None, drop=False):
        pass;


    def _dropTable(self, tableName, dbName=None):
        pass;


    def _dropTblUDF(self, tblrData, tableName=None):
        pass;

    def _describe(self, tblrData):
        pass;


    def _agg(self, agfn, tblrData, collist=None, valueOnly=True):
        pass;


    def __del__(self):
        pass;

class DBCPostgreSQLStub(DBCRemoteStub):
    pass;

copyreg.pickle(DBCPostgreSQL, DBCPostgreSQLStub.serializeObj);
copyreg.pickle(DBCPostgreSQLStub, DBCPostgreSQLStub.serializeObj);

