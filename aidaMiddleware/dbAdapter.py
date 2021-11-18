import sys;
import threading;

import collections;
import datetime;

from aidacommon.dbAdapter import *;
from aidas.rdborm import *;
from aidas.dborm import DBTable, DataFrame;
from aida.aida import *;

DBC._dataFrameClass_ = DataFrame;

class DBCMiddleware(DBC):
    def _executeQry(self, sql, resultFormat='column'):
        return self.__extDBCcon._executeQry(sql, resultFormat)

    def _toTable(self, tblrData, tableName=None):
        return self.__extDBCcon._toTable(tblrData, tableName)

    def _saveTblrData(self, tblrData, tableName, dbName=None, drop=False):
        return self.__extDBCcon._saveTablrData(tblrData,tableName,dbName,drop)

    def _dropTable(self, tableName, dbName=None):
        return self.__extDBCcon._dropTable(tableName, dbName)

    def _dropTblUDF(self, tblrData, tableName=None):
        return self.__extDBCcon.dropTblUDF(tblrData, tableName)

    def _describe(self, tblrData):
        return self.__extDBCcon._describe(tblrData)

    def _agg(self, agfn, tblrData, collist=None, valueOnly=True):
        return self.__extDBCcon._add(agfn,tblrData,collist,valueOnly)

    def _tables(self):
        return self.__extDBCcon._tables()

    def __new__(cls, dbname, username, password, jobName, dbcRepoMgr, serverIPAddr):
        """See if the connection works, authentication fails etc. In which case we do not need to continue with the object creation"""
        #logging.debug("__new__ called for {}".format(jobName));
        con = AIDA.connection(dbname,hostname='whe_server_1',username=username,password=password,autocommit=True);
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
        #Setup the actual database connection to be used.
        self.__setDBC__();

    def __setDBC__(self):
        con = AIDA.connection(self.dbName,hostname='whe_server_1',username=self._username,password=self._password,port=55660);
        ##cursor = con.cursor();
        #This function call should set the internal database connection to MonetDB in THIS DBC object, using the jobName passed to it.
        #The database function basically calls back the _setConnection method.
        ##rows = cursor.execute('select status from aidas_setdbccon(\'{}\');'.format(self._jobName));
        ##data = cursor.fetchall();
        #logging.debug("rows = {} data = {} after setting dbc con.".format(rows, data));
        ##cursor.close();
        con.execute('select status from aidas_setdbccon(\'{}\');'.format(self._jobName));
        self.__extDBCcon = con;