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
from concurrent.futures import ThreadPoolExecutor, as_completed;
import pymonetdb.sql;

DBC._dataFrameClass_ = DataFrame;

class DBCMiddleware(DBC):
    def __getstate__(self):
       return self.__dict__

    def __setstate__(self, d):
       self.__dict__ = d

    def _executeQry(self, sql, resultFormat='column', sqlType=DBC.SQLTYPE.SELECT):
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

    def _setConnection(self, con):
        """Called by the database function to set the internal database connection for executing queries"""
        #logging.debug("__setConnection_ called for {} with {}".format(self._jobName, con));
        self.__connection= con;

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
        #To setup things at the repository
        super().__init__(dbcRepoMgr, jobName, dbname, serverIPAddr);
        #Setup the actual database connection to be used.
        self.__setDBC__();

    def __setDBC__(self):
        connections = []
        for host_name in self._serverConfig.get_server_names(): 
           con = AIDA.connect(host_name, self._dbName,self._username,self._password,self._jobName,55660);
           connections += [con]
        self._localDBCcon = pymonetdb.Connection(self.dbName,hostname='localhost',username=self._username,password=self._password,autocommit=True);
        self._extDBCcon = connections;

    def _getDBTable(self, relName, dbName=None):
        results = []
        for i in self._executor.map(lambda con: con._getDBTable(relName, dbName), self._extDBCcon):
            results.append(i)
        d = DistTabularData(self._executor, self._extDBCcon, results)
        return d;

class DBCMiddlewareStub(DBCRemoteStub):
    pass;

copyreg.pickle(DBCMiddleware, DBCMiddlewareStub.serializeObj);
copyreg.pickle(DBCMiddlewareStub, DBCMiddlewareStub.serializeObj);
