import sys;
import threading;

import collections;
import datetime;

import numpy as np;
import pandas as pd;

import psycopg2;

from aidacommon.aidaConfig import AConfig, UDFTYPE;

from aidacommon.dbAdapter import *;
from aidas.rdborm import *;
from aidas.dborm import DBTable, DataFrame;

from time import time;
import decimal;
from convert import convert;
import vtlib

DBC._dataFrameClass_ = DataFrame;

class DBCPostgreSQL(DBC):
    """Database adapter class for PostgreSQL"""

    # Map numpy data types to PostgreSQL compatible types.
    typeConverter = {np.int16:'smallint', np.int32:'integer', np.int64:'bigint', np.float32:'real'
    , np.float64:'double precision', np.object:'text', np.object_:'text', 'bytes':'bytea'
    , 'decimal':'double precision' ,'date':'DATE', 'time':'TIME', 'timestamp':'TIMESTAMP'};
    
    datetimeFormats = {'%Y-%m-%d':'date', '%H:%M:%S':'time', '%Y-%m-%d %H:%M:%S':'timestamp'};

    #Query to get the names of all tables in the database.
    __TABLE_LIST_QRY__ = \
        "SELECT table_name as tableName " \
        "FROM information_schema.tables " \
        "WHERE table_schema = '{}'     " \
        "AND table_type = 'BASE TABLE' " \
        ";"

    # Not enough information to find  columnSize

    #Query to fetch a table schema in PostgreSQL (does not support other database objects such as views)
    __TABLE_METADATA_QRY__ = \
        "SELECT table_schema as schemaName, table_name as tableName, column_name as columnName, data_type as columnType, " \
        "0 as columnSize, ordinal_position as columnPos, is_nullable as columnNullable " \
        "FROM information_schema.columns      " \
        "WHERE table_schema = '{}' AND table_name = '{}' " \
        "ORDER BY schemaName, tableName, columnPos " \
        ";"

    #Query to fetch a table schema in PostgreSQL (does not support other database objects such as views)
    __COLUMN_METADATA_QRY__ = \
        "SELECT column_name as columnName, data_type as columnType, " \
        "0 as columnSize, ordinal_position as columnPos, is_nullable  as columnNullable " \
        "FROM information_schema.columns " \
        "WHERE table_schema = '{}' AND table_name = '{}' " \
        "ORDER BY columnPos " \
        ";"

    __NUMERIC_COL_DESCRIBE__ = \
        "  RIGHT ('                    ' || CAST(CAST(COUNT({}) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS count_{}" \
        ", RIGHT ('                    ' || CAST(CAST(COUNT(DISTINCT {}) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS countd_{}" \
        ", RIGHT ('                    ' || CAST(CAST(SUM(CASE WHEN {} IS NULL THEN 1 ELSE 0 END) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS countn_{}" \
        ", RIGHT ('                    ' || CAST(CAST(MAX({}) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS max_{}" \
        ", RIGHT ('                    ' || CAST(CAST(MIN({}) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS min_{}" \
        ", RIGHT ('                    ' || CAST(CAST(AVG({}) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS avg_{}" \
        ", RIGHT ('                    ' || CAST(CAST(MEDIAN({}) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS median_{}" \
        ", RIGHT ('                    ' || CAST(CAST(PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {}) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS q25_{}" \
        ", RIGHT ('                    ' || CAST(CAST(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY {}) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS q50_{}" \
        ", RIGHT ('                    ' || CAST(CAST(PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {}) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS q75_{}" \
        ", RIGHT ('                    ' || CAST(CAST(STDDEV_POP({}) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS std_{}";

    __CHAR_COL_DESCRIBE__ = \
        "  RIGHT ('                    ' || CAST(CAST(COUNT({}) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS count_{}" \
        ", RIGHT ('                    ' || CAST(CAST(COUNT(DISTINCT {}) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS countd_{}" \
        ", RIGHT ('                    ' || CAST(CAST(SUM(CASE WHEN {} IS NULL THEN 1 ELSE 0 END) AS DECIMAL(20,2)) AS VARCHAR(20)), 20) AS countn_{}" \
        ", RIGHT ('                    ' || CAST(MAX({}) AS VARCHAR(20)), 20) AS max_{}" \
        ", RIGHT ('                    ' || CAST(MIN({}) AS VARCHAR(20)), 20) AS min_{}" \
        ", '                    ' AS avg_{}" \
        ", '                    ' AS median_{}" \
        ", '                    ' AS q25_{}" \
        ", '                    ' AS q50_{}" \
        ", '                    ' AS q75_{}" \
        ", '                    ' AS std_{}";

    __COL_EXP__ = "SELECT COUNT(*) FROM information_schema.columns WHERE table_name =\'{" "}\';"
    __STRING_TYPE_EXP__ = "SELECT COUNT(*) FROM information_schema.columns WHERE data_type = 'character varying' AND table_name =\'{" "}\';"    

    #TODO: Throw an error and abort object creation in case of failures.
    def __new__(cls, dbname, username, password, jobName, dbcRepoMgr, serverIPAddr):
        """See if the connection works, authentication fails etc. In which case we do not need to continue with the object creation"""
        #logging.debug("__new__ called for {}".format(jobName));
        con = psycopg2.connect(dbname=dbname,user=username,password=password,host='localhost');
        con.autocommit = True
        #logging.debug("connection test for {} result {}".format(jobName, con));
        con.close();
        return super().__new__(cls);

    def __init__(self, dbname, username, password, jobName, dbcRepoMgr, serverIPAddr):
        """Actual object creation"""
        #logging.debug("__init__ called for {}".format(jobName));
        self.__qryLock__ = threading.Lock();
        self._username = username; self._password = password;
        #To setup things at the repository
        super().__init__(dbcRepoMgr, jobName, dbname, serverIPAddr);

        import plpy
        self.__connection = plpy;
        self.__vtm = vtlib.VTManager(conn=self);

        import aidasys;
        self.__requestQueue = aidasys.requestQueue;
        self.__resultQueue = aidasys.resultQueue;

    def _tables(self):
        sql = DBCPostgreSQL.__TABLE_LIST_QRY__.format(self.dbName);
        (tables, rows) = self._executeQry(sql);
        return pd.DataFrame(tables);

    def _getDBTable(self, relName, dbName=None):
        #logging.debug(DBCPostgreSQL.__TABLE_METADATA_QRY__.format( dbName if(dbName) else self.dbName, relName));
        sql = DBCPostgreSQL.__TABLE_METADATA_QRY__.format( dbName if(dbName is not None) else self.dbName, relName);
        (metaData_, rows) = self._executeQry(sql);

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

    def _getResult(self):
        result = self.__resultQueue.get();
        self.__resultQueue.task_done();
        return result;

    def _executeRequest(self, conn, request):
        #self.__connection = conn;
        (result, rows) = self._execution(*request);
        self.__resultQueue.put((result, rows));
    
    def _executeQry(self, sql, resultFormat='column', sqlType=DBC.SQLTYPE.SELECT):
        self.__requestQueue.put( (self._jobName , (sql, resultFormat, sqlType) ) );
        result = self._getResult();
        return result;


    def _execution(self, sql, resultFormat='column', sqlType=DBC.SQLTYPE.SELECT):
        """Execute a query and return results"""
        #TODO: either support row format results or throw an exception for not supported.
        logging.info("_execution called for {} with {}".format(self._jobName, sql));
        with self.__qryLock__:
            try:
                # Naive conversion approach
                if(AConfig.CONVERSIONOPTION == 1):
                    rv = self.__connection.execute(sql);
                    if(rv.nrows() == 0 or sqlType!=DBC.SQLTYPE.SELECT):
                        try:
                            result  =  ({k: [] for k in rv.colnames()}, 0)
                            return result
                        except:
                            return ([], 0)


                    result = {col_names: [None] * rv.nrows() for col_names in rv.colnames()}

                    for index, row in enumerate(rv):
                        for key, value in row.items():
                            result[key][index] = value              

                    for key, col in result.items():
                        if type(col[0]).__name__ == 'NoneType':
                            result[key] = np.asarray( [] )
                        elif (type(col[0]).__name__) == 'int':
                            result[key] = np.asarray(col, dtype = np.int64)
                        elif (type(col[0]).__name__) == 'float':
                            result[key] = np.asarray(col, dtype = np.float64)
                        else:
                            result[key] = np.asarray(col, dtype = np.object_)

                # C Python extension conversion module:
                elif(AConfig.CONVERSIONOPTION == 2):
                    rv = self.__connection.execute(sql);
                    if(rv.nrows() == 0 or sqlType!=DBC.SQLTYPE.SELECT):
                        try:
                            result  =  ({k: [] for k in rv.colnames()}, 0)
                            return result
                        except:
                            return ([], 0)
                    row_list = list(rv)
                    columns = rv.colnames()
                    rowNums = len(row_list)
                    colNums = len(columns)
                    result = convert(row_list,columns,rowNums,colNums)

                # Conversion function added to the source code of PostgreSQL:
                elif(AConfig.CONVERSIONOPTION == 3):
                    result = self.__connection.execute_and_convert_to_columns(sql);

                    if not result:
                        return ([], 0)

                if(sqlType==DBC.SQLTYPE.SELECT):
                    if(resultFormat == 'column'):
                        #get some columns
                        c_tmp = result[list(result.keys())[0]]
                        #Find the length of the array (or masked array) that is the number of rows
                        rows = len(c_tmp.data if hasattr(c_tmp, 'data')  else c_tmp);
                        #for col in result:
                            #logging.debug("_execution col {} size {}  references {}".format(col, sys.getsizeof(result[col]), sys.getrefcount(result[col])));
                        #for c in result:
                        #    logging.debug("Result column {} type {}".format(c, result[c].dtype));
                        return (result, rows);
                    elif(resultFormat == 'row'):
                        pass;

            except Exception as e:
                the_type, the_value, the_traceback = sys.exc_info();
                logging.error("An exception occured while trying to execute query {}".format((the_type, the_value, the_traceback)));
                logging.exception("An exception occured while executing query {}".format(sql));
                #re-raise the exception.
                if (sqlType != DBC.SQLTYPE.DROP):
                    raise;

            #TODO: format....
    def _toUDF(self, tblrData):
        tblrData._toUDF_()

    def _toTable(self, tblrData, tableName=None):
        if(tableName is None):
            if(hasattr(tblrData, 'tableName')):
                tableName = tblrData.tableName;
        if(tableName is None):
            logging.warning("Error cannot deduce a tableName for the Tabular Data passed");
            raise AttributeError("Error cannot deduce a tableName for the Tabular Data passed")
       
        #Keep track of our UDFs/Virtual tables;
        #logging.debug("Created Table UDF/VT {}.{}".format(self.dbName, tableName));
        self._tableRepo_[tableName] = tblrData;

        # What should we use to expose the data to Postgres ?
        if(AConfig.UDFTYPE == UDFTYPE.TABLEUDF):
            self.__createTableUDF(tblrData, tableName);
        elif (AConfig.UDFTYPE == UDFTYPE.FOREIGNTABLE):
            self.__createForeignTable(tblrData, tableName);
        elif (AConfig.UDFTYPE == UDFTYPE.TEMPTABLE):
            self.__createTempTable(tblrData, tableName);

        

    def __createTableUDF(self, tblrData, tableName):
        data = tblrData.rows;
        numrows = tblrData.numRows;
        
        cudf = 'CREATE FUNCTION {}() RETURNS TABLE({})\nAS $$\nfrom aidacommon.dbAdapter import DBC;'
        cudf += '\ncols = [{}];\ndata = (DBC._getDBTable(\'{}\').rows);'
        cudf += '\nif len(cols) == 1:\n    result = data[cols[0]]'
        cudf += '\nelse:\n    result = [dict(zip(data,t)) for t in zip(*data.values())]'
        cudf += '\nreturn result \n$$ LANGUAGE plpython3u;';

        #The column list returned by this table udf.
        collist=None;
        colnames=None;
        for colname in data:
            collist = (collist + ',') if(collist is not None) else '';
            colnames = (colnames + ',') if(colnames is not None) else '';
            dataType = data[colname].dtype.type;
            if(dataType is np.object_):
                try:
                    cd = data[colname][0];
                    if(isinstance(cd, decimal.Decimal)):
                        dataType = "decimal";
                    elif(isinstance(cd, str)):
                        for f in self.datetimeFormats:
                            try:
                                datetime.datetime.strptime(cd, f);
                                dataType = self.datetimeFormats[f];
                                break;
                            except ValueError:
                                pass;
                    elif(isinstance(cd, bytes)):
                        dataType = "bytes";
                except IndexError:
                    pass;
            #logging.debug("UDF column {} type {}".format(colname, dataType));
            collist += colname + ' ' + ('text' if(dataType not in DBCPostgreSQL.typeConverter) else DBCPostgreSQL.typeConverter[dataType]) ;
            colnames += '\'' + colname + '\'';

        cudf = cudf.format(tableName, collist, colnames, tableName);            

        #logging.debug("Creating Table UDF {}".format(cudf));
        self._executeQry(cudf, sqlType=DBC.SQLTYPE.CREATE);
    
    def __createForeignTable(self, tblrData, tableName):
        #logging.debug("Creating Foreign Table {}".format(tableName));
        self.__vtm.regForeignTable(tblrData, tableName, foreignServer=AConfig.FDWVERSION);

    
    def __createTempTable(self, tblrData, tableName):
        # this function loads the data into this temporary table
        self.__vtm.regTempTable(tblrData, tableName, analyze=True);

           
                       

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
        elif(AConfig.UDFTYPE == UDFTYPE.FOREIGNTABLE):
            tblrData._toUDF_();
            ctbl = 'CREATE TABLE {} AS SELECT * FROM {};'.format(tableName, tblrData.tableName);
            self._executeQry(ctbl, sqlType=DBC.SQLTYPE.CREATE);
        else:
            self._toTable(tblrData, tableName);
            try:
                #logging.debug('DEBUG: _saveTblrData: persisting table {} in {}'.format(tableName, dbName if (dbName) else self.dbName));
                """self.__connection.persistTable(tableName, dbName if (dbName is not None) else self.dbName);"""
                #pass;
                #logging.debug('DEBUG: _saveTblrData: persisted table {}'.format(tableName));
            except:
                logging.exception("ERROR: _saveTblrData : while persisting");
                raise;

        setattr(self, tableName, self._getDBTable(tableName, dbName));

    def _dropTable(self, tableName, dbName=None):
        dtbl = 'DROP TABLE {}.{};'.format( dbName if(dbName is not None) else self.dbName, tableName );
        self._executeQry(dtbl, sqlType=DBC.SQLTYPE.DROP);

        try:
            del self._tableRepo_[tableName];
        except KeyError:
            pass;

    def _dropTblUDF(self, tblrData, tableName=None):
        if(tableName is None):
            if(hasattr(tblrData, 'tableName')):
                tableName = tblrData.tableName;
        if(tableName is None):
            #logging.warning("Error cannot deduce a tableName for the Tabular Data passed");
            raise AttributeError("Error cannot deduce a tableName for the Tabular Data passed");        

        #logging.info("Dropping Table UDF/VT {}".format(tableName));
        if (AConfig.UDFTYPE == UDFTYPE.TABLEUDF) :
            dropObjectType = 'FUNCTION';
            dobj = 'DROP {} {};'.format(dropObjectType, tableName);
            self._executeQry(dobj, sqlType=DBC.SQLTYPE.DROP);
        else:
            self.__vtm.unregTable(tableName);

        tblrData.__tableUDFExists__ = False;

    def _describe(self, tblrData):
        
        #logging.info("describing {}".format(type(tblrData)));
        if(isinstance(tblrData, DBTable)):
            #logging.info("describing DBTable ");
            #logging.info(DBCPostgreSQL.__COLUMN_METADATA_QRY__.format(tblrData.schemaName, tblrData.tableName));
            sql = DBCPostgreSQL.__COLUMN_METADATA_QRY__.format(tblrData.schemaName, tblrData.tableName);
            (colMeta, numcols) = self._executeQry(sql);
            sqlsel = None;
            for c in range(0, numcols):
                cname = colMeta["columnname"][c];
                ctype = colMeta["columntype"][c];
                if(ctype in ('character', 'character varying', 'timestamp', 'timestamp without time zone')):
                    colsel = DBCPostgreSQL.__CHAR_COL_DESCRIBE__.format(cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname);
                else:
                    colsel = DBCPostgreSQL.__NUMERIC_COL_DESCRIBE__.format(cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname);
                sqlsel = ((sqlsel + '\n,') if(sqlsel is not None) else '\n' ) + colsel;

            sqlsel = "SELECT {} \n FROM {};".format(sqlsel, tblrData.tableName);
            #logging.debug("Performing describe : {}".format(sqlsel));
            (data, rows) = self._executeQry(sqlsel);
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
                #.info("{} maps to {} {}".format(cname, tdata[cname].dtype.type, DBCPostgreSQL.typeConverter[tdata[cname].dtype.type]));
                if(DBCPostgreSQL.typeConverter[tdata[cname].dtype.type] in ['text']):
                    colsel = DBCPostgreSQL.__CHAR_COL_DESCRIBE__.format(cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname);
                else:
                    colsel = DBCPostgreSQL.__NUMERIC_COL_DESCRIBE__.format(cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname, cname);
                sqlsel = ((sqlsel + '\n,') if(sqlsel is not None) else '\n' ) + colsel;

            sqlsel = "SELECT {} \n FROM {}{};".format(sqlsel, tblrData.tableName, '()' if(AConfig.UDFTYPE == UDFTYPE.TABLEUDF) else '');
            #.debug("Performing describe : {}".format(sqlsel));
            (data, rows) = self._executeQry(sqlsel);
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
            sql = DBCPostgreSQL.__COLUMN_METADATA_QRY__.format(tblrData.schemaName, tblrData.tableName)
            (colMeta, numcols) = self._executeQry(sql);
            sqlsel = None;
            for c in range(0, numcols):
                cname = colMeta["columnname"][c];
                ctype = colMeta["columntype"][c];
                if(collist is None or cname in collist):
                    if(ctype in ('character', 'character varying') and agfn in (DBC.AGGTYPE.SUM, DBC.AGGTYPE.AVG)):
                        colsel = "'' AS agg_{}".format(cname);
                    else:
                        colsel = agfn.value.format(cname) + (' AS agg_{}').format(cname);
                    sqlsel = ((sqlsel + '\n,') if(sqlsel is not None) else '\n' ) + colsel;

            sqlsel = "SELECT {} \n FROM {};".format(sqlsel, tblrData.tableName);
            (data, rows) = self._executeQry(sqlsel);
            #.debug("Performing agg : {}".format(sqlsel));
            for c in range(0, numcols):
                cname = colMeta["columnname"][c];
                if(collist is None or cname in collist):
                    result[cname] = data['agg_{}'.format(cname)][0];
        else:
            tdata = tblrData.rows;
            sqlsel = None;
            for cname in tdata:
                #.info("{} maps to {} {}".format(cname, tdata[cname].dtype.type, DBCPostgreSQL.typeConverter[tdata[cname].dtype.type]));
                if(DBCPostgreSQL.typeConverter[tdata[cname].dtype.type] in ['text'] and agfn in (DBC.AGGTYPE.SUM, DBC.AGGTYPE.AVG)):
                    colsel = "'' AS agg_{}".format(cname);
                else:
                    colsel = agfn.value.format(cname) + (' AS agg_{}').format(cname);
                sqlsel = ((sqlsel + '\n,') if(sqlsel is not None) else '\n' ) + colsel;

            sqlsel = "SELECT {} \n FROM {}{};".format(sqlsel, tblrData.tableName, '()' if(AConfig.UDFTYPE == UDFTYPE.TABLEUDF) else '');
            #.info("Performing agg : {}".format(sqlsel));
            (data, rows) = self._executeQry(sqlsel);
            for cname in tdata:
                if(collist is None or cname in collist):
                    result[cname] = data['agg_{}'.format(cname)][0];

        #.debug("DEBUG: returning agg results {}".format(result));

        #return result[list(result.keys())[0]] if(len(result)==1 and collist is not None) else result;
        return result[list(result.keys())[0]] if(len(result)==1 and valueOnly) else result;


    def __del__(self):
        #.debug("__del__ called for {}".format(self._jobName));
         
        #Where we using regular Table UDFs or Virtuable Tables ?
        for obj in self._tableRepo_.keys():
            try:
                objval = self._tableRepo_[obj];
                if((not isinstance(objval, DBTable)) and objval.tableUDFExists):
                    #logging.debug("Dropping {} {}".format(dropObjectType, obj));
                    if (AConfig.UDFTYPE == UDFTYPE.TABLEUDF) :
                        dropObjectType = 'FUNCTION'; 
                        dobj = 'DROP {} {};'.format(dropObjectType, obj);
                        self._executeQry(dobj, sqlType=DBC.SQLTYPE.DROP);
                    elif (AConfig.UDFTYPE in (UDFTYPE.FOREIGNTABLE, UDFTYPE.TEMPTABLE) ):
                        self.__vtm.unregTable(tableName);

                    #.debug("dropped {} {}".format(dropObjectType, obj));
                    objval.__tableUDFExists__ = False;
            except:
                pass;
        
        #del self.__vtm;

class DBCPostgreSQLStub(DBCRemoteStub):
    pass;

copyreg.pickle(DBCPostgreSQL, DBCPostgreSQLStub.serializeObj);
copyreg.pickle(DBCPostgreSQLStub, DBCPostgreSQLStub.serializeObj);
