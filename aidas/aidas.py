from abc import ABCMeta;
import signal;
import weakref;

from tblib import pickling_support;
pickling_support.install();
import dill as custompickle;

import threading;
from socketserver import ThreadingTCPServer, StreamRequestHandler;

import logging;

from aidacommon.aidaConfig import AConfig;

class ConnectionManager(metaclass=ABCMeta):
    """Singleton class, there will be only one connection manager in the system"""
    __ClassLock = threading.RLock();
    __ConnectionManagerObj = None;

    @staticmethod
    def getConnectionManager(dbadapter=None):
        #logging.debug("getConnectionManager called.");
        class __ConnectionManager:
            """Class that manages database connection requests"""

            __RepoLock = threading.RLock();
            __DBCRepo = weakref.WeakValueDictionary();  #Keep track of database connections
            #__DBCRepo = dict();  #Keep track of database connections

            def __init__(self, dbadapter):
                CoMgrObj = self;
                class CoHandler(StreamRequestHandler):
                    """Class will handle a connection, request, authenticate it
                    One object per client connection request"""
                    disable_nagle_algorithm = True;

                    def handle(self):
                        "Called by TCPServer for each client connection request"
                        try:
                            (dbname, username, password, jobName) = custompickle.load(self.rfile);
                            #logging.debug("connection request received {}".format((dbname,username,jobName)));
                            #dbc = dborm.dbAdapter.DBCMonetDB(dbname, username, password, jobName, CoMgrObj);
                            dbc = dbadapter(dbname, username, password, jobName, CoMgrObj, self.request.getsockname()[0]);
                            #logging.debug("created dbc for {}, type {}".format((dbname,username,jobName), type(dbc)));
                            custompickle.dump(dbc, self.wfile); self.wfile.flush();
                            #Handshake to wait for the otherside to establish the stubs.
                            custompickle.load(self.rfile);
                        except Exception as e:
                            logging.error("error {} occured while creating dbc for {}".format(e, (dbname,username,jobName)));
                            logging.exception(e);

                    def finish(self):
                        self.wfile.close();
                        self.rfile.close();

                #Setup a TCP Server to listen to client stub connection requests.
                __srvr = ThreadingTCPServer(('', AConfig.CONNECTIONMANAGERPORT), CoHandler, False);
                __srvr.allow_reuse_address = True;
                __srvr.server_bind();
                __srvr.server_activate();
                self.__srvr = __srvr;

                #Handle signals to exit gracefully.
                if(threading.current_thread() == threading.main_thread()):
                    signal.signal(signal.SIGINT, self.terminate);
                    signal.signal(signal.SIGTERM, self.terminate);

                #Start the server polling as a daemon thread.
                self.__srvrThread = threading.Thread(target=self.__srvr.serve_forever);
                self.__srvrThread.daemon = True;
                self.__srvrThread.start();

            def get(self, jobName):
                dba = self.__class__.__DBCRepo[jobName]
                logging.debug(f'proxy dba obj is {weakref.proxy(dba)}')
                return weakref.proxy(dba);

            def add(self, jobName, dbc):
                self.__class__.__DBCRepo[jobName] = dbc;

            def remove(self, jobName):
                #TODO: how do you remove from weakvalue dictionary ? may be not relevant ?
                #logging.debug("Have to remove connection for job {}".format(jobName));
                #del self.__class__.__DBCRepo[jobName];
                pass;

            def close(self):
                self.__srvr.shutdown();
                self.__srvr.server_close();

            def terminate(self, signum, frame):
                self.close();

        with ConnectionManager.__ClassLock:
            if (dbadapter is not None and ConnectionManager.__ConnectionManagerObj is None):  # There is no connection manager object currently.
                #logging.debug("A connection manager does not exist.");
                cmgr = __ConnectionManager(dbadapter);
                ConnectionManager.__ConnectionManagerObj = cmgr;
                #logging.debug("A connection manager is created.");

            # Return the connection manager object.
            return ConnectionManager.__ConnectionManagerObj;

