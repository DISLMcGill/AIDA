import sys;
import threading;

import collections;
import datetime;

from aidacommon.dbAdapter import *;
from aidas.rdborm import *;
from aidas.dborm import DBTable, DataFrame;
from aida.aida import *;
from aidaMonetDB.dbAdapter import DBCMonetDB;
from aidaMiddleware.serverConfig import ServerConfig;
from aidaMiddleware.distTabularData import DistTabularData;
from concurrent.futures import ThreadPoolExecutor, wait, as_completed;

DBC._dataFrameClass_ = DataFrame;

class DBCMiddleware(DBC):
    def __getstate__(self):
       return self.__dict__

    def __setstate__(self, d):
       self.__dict__ = d

    def _executeQry(self, sql, resultFormat='column'):
        futures = {self._executor.submit(lambda con: con._executeQry(sql, resultFormat), con) for con in self._extDBCon}
        result = []
        for f in as_completed(futures):
            result.append(f.result())
        return result

    def _toTable(self, tblrData, tableName=None):
        futures = {self._executor.submit(lambda con: con._toTable(tblrData, tableName), con) for con in self._extDBCon}
        result = []
        for f in as_completed(futures):
            result.append(f.result())
        return result

    def _saveTblrData(self, tblrData, tableName, dbName=None, drop=False):
        futures = {self._executor.submit(lambda con: con._saveTblrData(tblrData, tableName, dbName, drop), con) for con in self._extDBCon}
        result = []
        for f in as_completed(futures):
            result.append(f.result())
        return result

    def _dropTable(self, tableName, dbName=None):
        futures = {self._executor.submit(lambda con: con._dropTable(tableName, dbName), con) for con in self._extDBCon}
        result = []
        for f in as_completed(futures):
            result.append(f.result())
        return result

    def _dropTblUDF(self, tblrData, tableName=None):
        futures = {self._executor.submit(lambda con: con._dropTblUDF(tblrData, tableName), con) for con in self._extDBCon}
        result = []
        for f in as_completed(futures):
            result.append(f.result())
        return result

    def _describe(self, tblrData):
        futures = {self._executor.submit(lambda con: con._describe(tblrData), con) for con in self._extDBCon}
        result = []
        for f in as_completed(futures):
            result.append(f.result())
        return result

    def _agg(self, agfn, tblrData, collist=None, valueOnly=True):
        futures = {self._executor.submit(lambda con: con._agg(agfn, tblrData, collist, valueOnly), con) for con in self._extDBCon}
        result = []
        for f in as_completed(futures):
            result.append(f.result())
        return result

    def _tables(self):
        return self._extDBCcon[0]._tables()

    def __new__(cls, dbname, username, password, jobName, dbcRepoMgr, serverIPAddr):
        """See if the connection works, authentication fails etc. In which case we do not need to continue with the object creation"""
        #logging.debug("__new__ called for {}".format(jobName));
        #con = AIDA.connect('whe_server_1', dbname, username, password, jobName, 55660);
        #logging.debug("connection test for {} result {}".format(jobName, con));
        #con.close();
        return super().__new__(cls);

    def __init__(self, dbname, username, password, jobName, dbcRepoMgr, serverIPAddr):
        """Actual object creation"""
        #logging.debug("__init__ called for {}".format(jobName));
        self._username = username; self._password = password;
        self._serverConfig = ServerConfig();
        self._executor = ThreadPoolExecutor(max_workers=15);
        #To setup things at the repository
        super().__init__(dbcRepoMgr, jobName, dbname, serverIPAddr);
        #Setup the actual database connection to be used.
        self.__setDBC__();

    def __setDBC__(self):
        connections = []
        for host_name in self._serverConfig.get_server_names(): 
           con = AIDA.connect(host_name, self._dbName,self._username,self._password,self._jobName,55660);
           connections += [con]
        ##cursor = con.cursor();
        #This function call should set the internal database connection to MonetDB in THIS DBC object, using the jobName passed to it.
        #The database function basically calls back the _setConnection method.
        ##rows = cursor.execute('select status from aidas_setdbccon(\'{}\');'.format(self._jobName));
        ##data = cursor.fetchall();
        #logging.debug("rows = {} data = {} after setting dbc con.".format(rows, data));
        ##cursor.close();
        #con.execute('select status from aidas_setdbccon(\'{}\');'.format(self._jobName));
        self._extDBCcon = connections;

    def _getDBTable(self, relName, dbName=None):
        #logging.debug(DBCMonetDB.__TABLE_METADATA_QRY__.format( dbName if(dbName) else self.dbName, relName));
        #(metaData_, rows) = self._executeQry(DBCMonetDB.__TABLE_METADATA_QRY__.format( dbName if(dbName is not None) else self._dbName, relName));
        futures = {self._executor.submit(lambda con: con._getDBTable(relName, dbName), con) for con in self._extDBCon}
        result = []
        for f in as_completed(futures):
            try:
                result.append(f.result())
            except KeyError:
                logging.error("ERROR: cannot find table {} in {}".format(relName, dbName if(dbName is not None) else self._dbName ));
                raise KeyError("ERROR: cannot find table {} in {}".format(relName, dbName if(dbName is not None) else self._dbName ));
        #logging.debug("execute query returned for table metadata {}".format(metaData_));
        #metaData = _collections.OrderedDict();
        #for column in [ 'schemaname', 'tablename', 'columnname', 'columntype', 'columnsize', 'columnpos', 'columnnullable']:
        #    logging.debug("For column {} data is {}".format(column, metaData_[column].data if hasattr(metaData_[column], 'data') else metaData_[column]));
        #    metaData[column] = metaData_[column].data if hasattr(metaData_[column], 'data') else metaData_[column];

        #return DBTable(self, metaData_);
        d = DistTabularData(self._executor, self._extDBCcon, result)
        return d;

class DBCMiddlewareStub(DBCRemoteStub):
    pass;

copyreg.pickle(DBCMiddleware, DBCMiddlewareStub.serializeObj);
copyreg.pickle(DBCMiddlewareStub, DBCMiddlewareStub.serializeObj);
