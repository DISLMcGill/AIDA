import sys;
import sysconfig
import threading;

import collections;
import datetime;

from aidacommon.dbAdapter import *;
from aidas.rdborm import *;
from aidas.dborm import DBTable, DataFrame, ModelService;
from aida.aida import *;
from aidaMonetDB.dbAdapter import DBCMonetDB;
from aidaMiddleware.serverConfig import ServerConfig;
from aidas.dborm import DistTabularData, CustomParameterServer;
from concurrent.futures import ThreadPoolExecutor, as_completed;
from aidaMiddleware.Model import *;

DBC._dataFrameClass_ = DataFrame;

class DBCMiddleware(DBC):
    def _RegisterModel(self, model):
        m = ModelService(model)
        m.server_init(self._executor, self.__monetConnection)
        return m

    def _RegisterPytorchModel(self, model):
        m = TorchModelService(model)
        m.server_init(self._executor, self.__monetConnection)
        return m

    def _RegisterPSModel(self, model):
        m = PSModelService(model)
        m.server_init(self._executor, self.__monetConnection)
        return m

    def _MakeParamServer(self, model, server, schedule=3):
        m = CustomParameterServer(model, server, schedule=schedule)
        m.server_init(self._executor)
        return m

    def _LinearRegression(self, learning_rate=0.0001, sync=True):
        m = LinearRegressionModel(learning_rate, sync)
        m.server_init(self._executor, self.__monetConnection)
        return ModelService(m)

    def _MatrixFactorizationPSModel(self, dim_1, dim_2, k, port="29500"):
        m = MatrixFactorization(dim_1, dim_2, k)
        s = TorchService(m)
        s.server_init(self._executor, self.__monetConnection, port, self._jobName)
        return s

    def _MatrixFactorizationTorchRMI(self, dim_1, dim_2, k):
        m = MatrixFactorization(dim_1, dim_2, k)
        s = TorchRMIService(m)
        s.server_init(self._executor, self.__monetConnection)
        return s

    def _workAggregateJob(self, job, data, ctx = {}, sync=True):
        def run_step_sync(s):
            #logging.info(f'Start work for step {s}')
            results = []
            futures = [self._executor.submit(con._X, s.work, data.tabular_datas[con], ctx) for con in
                       data.tabular_datas]
            for future in as_completed(futures):
                results.append(future.result())
            #logging.info(f'Start aggregate for step {sp}')
            r = s.aggregate(self, results, ctx)
            if r is not None:
                ctx['previous'] = r

        def run_steps_async(con, steps, context, lock):
            for s in steps:
                if isinstance(s, tuple):
                    start = time.perf_counter()
                    for i in range(s[1]):
                        r = con._X(s[0].work, data.tabular_datas[con], context)
                        with lock:
                            r = s[0].aggregate(self, r, context)
                        if r is not None:
                            context['previous'] = r
                    end = time.perf_counter()
                    logging.info(f"iterations time: {end-start}")
                else:
                    r = con._X(s.work, data.tabular_datas[con], context)
                    r = s.aggregate(self, r, context)
                    if r is not None:
                        context['previous'] = r

        logging.info('Starting work-agg job')
        if sync:
            for step in job:
                if isinstance(step, tuple):
                    for i in range(step[1]):
                        run_step_sync(step[0])
                else:
                    run_step_sync(step)
        else:
            lock = threading.Lock()
            results = []
            futures = [self._executor.submit(run_steps_async, con, job, ctx.copy(),lock) for con in
                 data.tabular_datas]
            for future in as_completed(futures):
                results.append(future)
        return

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
        super().__init__(dbcRepoMgr, jobName, dbname, serverIPAddr);
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

    def _close(self):
        if self._extDBCcon is not None:
            for server_name, connection in self._extDBCcon.items():
                connection._close()
        self.__monetConnection._requestQueue.put("close")

    def _getMonetConnection(self):
        return self.__monetConnection

    def _LoadDistTabularData(self, tabular_datas):
        tdict = {}
        for t in tabular_datas:
            for con in self._extDBCcon.values():
                if t._host == con._host:
                    tdict[con] = t
                    continue
        return DistTabularData(self._executor, tdict, self.__monetConnection)


class DBCMiddlewareStub(DBCRemoteStub):
    @aidacommon.rop.RObjStub.RemoteMethod()
    def _LinearRegression(self, learning_rate):
        pass;

    @aidacommon.rop.RObjStub.ModelCheck()
    @aidacommon.rop.RObjStub.RemoteMethod()
    def _RegisterModel(self, model):
        pass;

    @aidacommon.rop.RObjStub.RemoteMethod()
    def _MakeParamServer(self, model, server, schedule=3):
        pass;

    @aidacommon.rop.RObjStub.ModelCheck(isPS=True)
    @aidacommon.rop.RObjStub.RemoteMethod()
    def _RegisterPSModel(self, model):
        pass;

    @aidacommon.rop.RObjStub.RemoteMethod()
    def _MatrixFactorizationPSModel(self, dim_1, dim_2, k, port, *args):
        pass;

    @aidacommon.rop.RObjStub.RemoteMethod()
    def _RegisterPytorchModel(self, model):
        pass;

    @aidacommon.rop.RObjStub.RemoteMethod()
    def _workAggregateJob(self, job, data, ctx={}):
        pass;

    @aidacommon.rop.RObjStub.RemoteMethod()
    def _close(self):
        pass

    @aidacommon.rop.RObjStub.RemoteMethod()
    def _LoadDistTabularData(self, tabular_datas):
        pass

copyreg.pickle(DBCMiddleware, DBCMiddlewareStub.serializeObj);
copyreg.pickle(DBCMiddlewareStub, DBCMiddlewareStub.serializeObj);
