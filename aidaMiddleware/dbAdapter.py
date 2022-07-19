import sys;
import sysconfig
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
from concurrent.futures import ThreadPoolExecutor, as_completed;
from aidaMiddleware.Model import *;

DBC._dataFrameClass_ = DataFrame;

class DBCMiddleware(DBC):
    def _LinearRegression(self, learning_rate):
        return LinearRegressionModel(self._executor, self.__monetConnection, learning_rate)

    def _toTable(self, tblrData, tableName=None):
        pass

    def _saveTblrData(self, tblrData, tableName, dbName=None, drop=False):
        pass

    def _dropTable(self, tableName, dbName=None):
        pass

    def _dropTblUDF(self, tblrData, tableName=None):
        pass

    def _describe(self, tblrData):
        pass

    def _agg(self, agfn, tblrData, collist=None, valueOnly=True):
        pass

    def __getstate__(self):
       return self.__dict__

    def __setstate__(self, d):
       self.__dict__ = d

    def _executeQry(self, sql, resultFormat='column', sqlType=DBC.SQLTYPE.SELECT):
        pass

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
        self._extDBCcon = None
        self._localDBCcon = None
        self.__qryLock__ = threading.Lock();
        self.__monetConnection = DBCMonetDB(dbname,username,password,jobName,dbcRepoMgr,serverIPAddr)
        #To setup things at the repository
        super().__init__(dbcRepoMgr, jobName + "_middleware", dbname, serverIPAddr);
        #Setup the actual database connection to be used.
        self.__setDBC__();

    def __setDBC__(self):
        connections = {}
        for host_name in self._serverConfig.get_server_names(): 
           con = AIDA.connect(host_name, self._dbName,self._username,self._password,self._jobName,55660)
           connections[host_name] = con
        self._extDBCcon = connections;

    def _getDBTable(self, relName, dbName=None):
        results = {}
        cons = [self._extDBCcon[c] for c in self._serverConfig.get_servers(relName)]
        futures = {self._executor.submit(con._getDBTable, relName, dbName): con for con in cons}
        for future in as_completed(futures):
            results[futures[future]] = future.result()
        d = DistTabularData(self._executor, results, self.__monetConnection)
        return d

class DBCMiddlewareStub(DBCRemoteStub):
    @aidacommon.rop.RObjStub.RemoteMethod()
    def _LinearRegression(self, learning_rate):
        pass;

copyreg.pickle(DBCMiddleware, DBCMiddlewareStub.serializeObj);
copyreg.pickle(DBCMiddlewareStub, DBCMiddlewareStub.serializeObj);
