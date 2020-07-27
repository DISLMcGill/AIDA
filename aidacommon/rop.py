import time;
import gc;
import signal;
import threading;
import socket;
import inspect;
import functools;

import weakref;
import uuid;

import logging;

import pickle;

from tblib import pickling_support;
pickling_support.install();

import sys;
import dill as custompickle;


from abc import ABCMeta, abstractmethod;
from six import  reraise;

from socketserver import ThreadingTCPServer, StreamRequestHandler;

from aidacommon.aidaConfig import AConfig, portMapper;

class ROMessages(metaclass=ABCMeta):
    _INIT_ = '__I'; _NOT_FOUND_ = '__N'; _OK_ = '__O'; _GET_ATTRIBUTE_='__A'; _COMPRESS_ = '__C'; _DUMMY_=None;

class ROMgr(metaclass=ABCMeta):

    __ClassLock = threading.RLock();
    __ROMgrObj = None;

    #TODO: Do we need this ?
#    @classmethod
#    def __delROMgr(cls, host, port):
#        with cls.__ClassLock:
#            if(cls.__ROMgrObjs.get((host, port)) is not None):
#                del cls.__ROMgrObjs[(host, port)];
#
    @staticmethod
    def getROMgr(host=None, port=None, create=False):
        """This function is called to obtain a remote manager object serving the application. It follows a Singleton pattern"""

        class __ROMgrObj:
            """Class that takes care of managing the objects that needs to be accessed remotely."""

            __RepoLock = threading.RLock();

            def __init__(self, host, port):

                ROMgrObj = self;
                class ROProxy(StreamRequestHandler):
                    """This class provides the proxy support for the objects.
                    Client stubs will invoke the proxy, which in turn will invoke the local object."""

                    #TODO .. we need to do this only once ? may be move it elsewhere.
                    disable_nagle_algorithm = True;

                    def handle(self):
                        "Called by TCPServer for each client connection request"
                        try:
                            while True:
                                msg = custompickle.load(self.rfile);
                                #logging.debug("ROProxy {}  {:0.20f}".format(msg, time.time()));

                                #First message from client stub, check if object exists or not.
                                if(msg == ROMessages._INIT_):
                                    robjName = custompickle.load(self.rfile);
                                    #logging.debug("_INIT_ message to look for object {}".format(robjName));
                                    if(ROMgrObj.has(robjName)):
                                        self.obj = ROMgrObj.get(robjName, self);
                                        #On success, send the id of the proxy.
                                        custompickle.dump(id(self), self.wfile); self.wfile.flush();
                                        self._robjName = robjName;
                                    else:
                                        logging.warning("_INIT_ message object {} not found".format(robjName));
                                        custompickle.dump(ROMessages._NOT_FOUND_, self.wfile); self.wfile.flush();
                                #Check if the return should be compressed or not.
                                elif(msg != ROMessages._COMPRESS_):
                                    #logging.debug("RemoteMethod: {} is not a compress directive.".format(msg));
                                    #Request for an attribute
                                    if(msg == ROMessages._GET_ATTRIBUTE_):
                                        item = custompickle.load(self.rfile);
                                        try:
                                            val = self.obj.__getattribute__(item);
                                            custompickle.dump(None,self.wfile); custompickle.dump(val, self.wfile); self.wfile.flush();
                                        except Exception as e:
                                            #An exception occured. send traceback info the client stub.
                                            custompickle.dump(sys.exc_info(), self.wfile);self.wfile.flush();
                                    #Regular client stub messages contain the name of the function to be invoked and any arguments.
                                    else:
                                        #logging.debug("ROProxy {} reading args time {:0.20f}".format(msg, time.time()));
                                        args   = custompickle.load(self.rfile); kwargs = custompickle.load(self.rfile);
                                        #logging.debug("ROProxy {} read args time {:0.20f}".format(msg, time.time()));

                                        #Execute the function locally and send back any results/exceptions.
                                        try:
                                            #Execute the local function, store the results.
                                            func = self.obj.__getattribute__(msg);
                                            if(inspect.ismethod(func)):
                                                result = func(*args, **kwargs);
                                                args = kwargs = None;
                                            else: #This is probably a property, in which case we already have the value, return it.
                                                result = func;
                                            #logging.debug("ROProxy {} local result time {:0.20f}".format(msg, time.time()));

                                            #No exception to report.
                                            custompickle.dump(None,self.wfile);#self.wfile.flush();
                                            #logging.debug("ROProxy {} exception send time {:0.20f}".format(msg, time.time()));
                                            #Return the results.
                                            custompickle.dump(result, self.wfile); self.wfile.flush();
                                            #logging.debug("ROProxy {} result send time {:0.20f}".format(msg, time.time()));
                                            #Hand shake to make sure this function scope is active till the other side has setup remote object stubs if any
                                            #the contents of this message is irrelevant to us.
                                            #NOT REQUIRED: this object reference (result) is alive in this space till next remote function call reaches it.
                                            #custompickle.load(self.rfile);
                                        except Exception as e:
                                            #An exception occured. send traceback info the client stub.
                                            custompickle.dump(sys.exc_info(), self.wfile);self.wfile.flush();
                                else:
                                    msg = custompickle.load(self.rfile);
                                    #logging.debug("RemoteMethod : request for compressing {}".format(msg));
                                    #Request for an attribute
                                    if(msg == ROMessages._GET_ATTRIBUTE_):
                                        item = custompickle.load(self.rfile);
                                        try:
                                            val = self.obj.__getattribute__(item);
                                            custompickle.dump(None, self.wfile); self.wfile.flush();
                                            AConfig.NTWKCHANNEL.transmit(val, self.wfile);
                                        except Exception as e:
                                            #An exception occured. send traceback info the client stub.
                                            custompickle.dump(sys.exc_info(), self.wfile);self.wfile.flush();
                                    #Regular client stub messages contain the name of the function to be invoked and any arguments.
                                    else:
                                        #logging.debug("ROProxy {} reading args time {:0.20f}".format(msg, time.time()));
                                        args   = custompickle.load(self.rfile); kwargs = custompickle.load(self.rfile);
                                        #logging.debug("ROProxy {} read args time {:0.20f}".format(msg, time.time()));

                                        #Execute the function locally and send back any results/exceptions.
                                        try:
                                            #Execute the local function, store the results.
                                            func = self.obj.__getattribute__(msg);
                                            if(inspect.ismethod(func)):
                                                result = func(*args, **kwargs);
                                                args = kwargs = None;
                                            else: #This is probably a property, in which case we already have the value, return it.
                                                result = func;
                                            #logging.debug("ROProxy {} local result time {:0.20f}".format(msg, time.time()));

                                            #No exception to report.
                                            custompickle.dump(None,self.wfile);self.wfile.flush();
                                            #logging.debug("ROProxy {} exception send time {:0.20f}".format(msg, time.time()));
                                            #Return the results.
                                            AConfig.NTWKCHANNEL.transmit(result, self.wfile);
                                            #logging.debug("ROProxy {} result send time {:0.20f}".format(msg, time.time()));
                                            #Hand shake to make sure this function scope is active till the other side has setup remote object stubs if any
                                            #the contents of this message is irrelevant to us.
                                            #NOT REQUIRED: this object reference (result) is alive in this space till next remote function call reaches it.
                                            #custompickle.load(self.rfile);
                                        except Exception as e:
                                            #An exception occured. send traceback info the client stub.
                                            custompickle.dump(sys.exc_info(), self.wfile);self.wfile.flush();
                                #logging.debug("ROProxy {} exit time {:0.20f}".format(msg, time.time()));

                        except EOFError:
                            pass;

                        #if(hasattr(self, 'obj')):
                            #gc.collect();
                            #logging.debug('ROProxy {} terminating ... object {} has currently {} references'.format(id(self),robjName, sys.getrefcount(self.obj)) );
                            #for obj in gc.get_referrers(self.obj):
                            #    logging.debug("Referred by {}-{}".format(type(obj), id(obj)));
                            #     if(hasattr(obj, 'f_code')):
                            #        logging.debug("Frame info {}-{}-{}".format(obj.f_code.co_filename, obj.f_code.co_name, obj.f_lineno));
                            #    if(hasattr(obj, '__func__')):
                            #        logging.debug("Function info {}".format(obj.__func__.__qualname__));

                    #TODO, we may need some locking to synchronize with the handle function.
                    def _swapObj(self, obj):
                        """Called by the remote object manager when it wants the proxy to start serving a different object"""
                        #logging.debug("Proxy _swapObj : swapping object {} with {}".format(self.obj, obj));
                        #logging.debug("Proxy _swapObj : old content {}".format(self.obj.rows));
                        #logging.info("Proxy _swapObj : new content {}".format(obj.rows));
                        self.obj = obj;

                    def finish(self):
                        self.wfile.close();
                        self.rfile.close();

                #self.__host = socket.gethostname();
                self.__host = host;
                self.__port = port;
                self.__RObjRepos = dict();  #Keep track of regular objects
                self.__RObjRepos_tmp = weakref.WeakValueDictionary(); #Keep track of temporary objects created as part of return values to remote calls.
                #self.__RObjReposIds = weakref.WeakValueDictionary(); #We will use the system ids to ensure that we keep an object just once.
                self.__proxyObjectRepo__ = weakref.WeakValueDictionary(); #Keep track of proxy objects for workspaces.
                #self.__RObjNames__ = weakref.WeakKeyDictionary();#For reverse mapping of objects to names.

                #Setup a TCP Server to listen to client stub connection requests.
                #self.__srvr = ThreadingTCPServer((host, port), ROProxy, True);
                __srvr = ThreadingTCPServer(("", port), ROProxy, False);
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

            def srvrInfo(self):
                return (self.__host, self.__port);

            def has(self, robj):
                with self.__RepoLock:
                    if(isinstance(robj, str)): #If the object's name was passed
                        return self.get(robj) is not None;
                    #else:   #Otherwise check if the object is present using it's id.
                        #return self.__RObjReposIds.get(id(robj)) is not None;

            #def __getObjName__(self, robjid):
            #    with self.__RepoLock:
            #        return self.__RObjReposIds.get(robjid);

            def get(self, robjName, proxy=None):
                with self.__RepoLock:
                    obj=None;
                    if(robjName is None):
                        obj = None;
                    if(robjName.startswith('__tmp__')):
                        obj = self.__RObjRepos_tmp.get(robjName);
                    else:
                        obj = self.__RObjRepos.get(robjName);

                    if(proxy):
                        self.__proxyObjectRepo__[id(proxy)] = proxy;

                    return obj;

            def add(self, robj, robjName=None):
                with self.__RepoLock:
                    if(robjName): #Is a name explicitly given ?
                        if(self.has(robjName)):
                            raise LookupError("Name {} is already associated with object id {}".format(robjName, id(self.get(robjName))));
                        else:
                            ##TODO Check for same object getting added twice (non-temporary) ?
                            #Add the object to the repository.
                            self.__RObjRepos[robjName] = robj;
                    else: #No explicit name given, make up a tempory name.
                        robjName = '__tmp__' + (str(uuid.uuid4())[:8]);
                        self.__RObjRepos_tmp[robjName] = robj;

                    #Update the list of object names associated with this object.
                    #objNames = self.__RObjNames__.get(robj);
                    #if(objNames): #There is already a list of objectnames, so update it.
                    #    objNames.append(robjName);
                    #else: #This is the only object name assigned to this object.
                    #    self.__RObjNames__[robj] = [robjName,];

                    return robjName;

            def replace(self, proxyList, obj):
                for proxy in proxyList:
                    try:
                        #proxy = self.__proxyObjectRepo__[proxy];
                        robjName = proxy._robjName;
                        if(robjName.startswith('__tmp__')):
                            self.__RObjRepos_tmp[robjName] = obj;
                        else:
                            self.__RObjRepos[robjName] = obj;
                        proxy._swapObj(obj);
                    except Exception as e:
                        the_type, the_value, the_traceback = sys.exc_info();
                        logging.error("An error occured while replacing remote object in proxies for object id {}".format(id(obj)));
                        logging.exception(e);
                        logging.error((the_type, the_value, the_traceback));
                        logging.error(dir(proxy));


            def rm(self, robjName):
                with self.__RepoLock:
                    if(robjName.startswith('__tmp__')):
                        del self.__RObjRepos_tmp[robjName];
                    else:
                        del self.__RObjRepos[robjName];

            def getProxy(self, proxyId):
                return self.__proxyObjectRepo__[proxyId];

            def close(self):
                self.__srvr.shutdown();
                self.__srvr.server_close();

            def wait(self):
                """Wait for the Remote Object Manager thread to terminate.
                Useful to ensure your main program just do not exit after registering some objects."""
                self.__srvrThread.join();

            def terminate(self, signum, frame):
                self.close();

            def __del__(self):
                #logging.info("Remote manager shuttind down...")
                self.close();

        with ROMgr.__ClassLock:
            if(ROMgr.__ROMgrObj is None): #There is no remote manager object currently.
                if(create): #We are asked to create.
                    if(not host):
                        host = socket.gethostname();
                    romgr = __ROMgrObj(host, port);
                    ROMgr.__ROMgrObj = romgr;
            elif(host and port): #We are requested for specific remote manager object.
                if(ROMgr.__ROMgrObj.srvrInfo() == (host, port)): #If found return it.
                    return ROMgr.__ROMgrObj;
                else:
                    return None;

            #Request is for the remote manager serving the current application.
            return ROMgr.__ROMgrObj;


class RObjStub (metaclass=ABCMeta):

    robjStubs = weakref.WeakValueDictionary();

    def __new__(cls,  objName, host, port):
        #logging.debug("ROBjStub {} new enter time {:0.20f}".format(objName, time.time()));
        #logging.debug('new {} {} {}'.format(objName, host, port));

        #If we already have this stub, return it.
        robjStub = cls.robjStubs.get((objName, host, port));
        if(robjStub):
            return robjStub;

        #Check if this is a local object, in which case return the actual object itself and DO NOT create a stub.
        romgr = ROMgr.getROMgr(host, port);
        if(romgr):
            #TODO Exception if object is not found.
            return romgr.get(objName);
        #Object is remote, proceed with stub generation.
        return super().__new__(cls);

    def __init__(self, objName, host, port):
        #logging.debug('init {} {} {}'.format(objName, host, port));
        (self._socket, self._rf, self._wf, self._proxyid) = ROStubMgr.getRO(objName, host, port);
        self._objName = objName; self._host = host; self._port = port;
        #Add this stub to our repository for later lookups.
        self.__class__.robjStubs[(objName, host, port)] = self;
        #logging.debug("ROBjStub {} init exit time {:0.20f}".format(objName, time.time()));

    @property
    def proxyid(self):
        return self._proxyid;

    def __getattribute__(self, item):
        """Augmented function to access attributes on remote objects.
        First it will check if the attribute belongs to the stub itself (local variable)
        If it cannot find an attribute requested locally, it will request it from the remote object."""

        ###logging.debug('__getattribute__ for {}'.format(item));

        if(item.startswith('__')):
            return super().__getattribute__(item);

        #Special attribute, to be always returned from the stub.
        if(item in ('_objName', '_host', '_port', '_wf', '_rf', '_socket', '_proxyid')):
           return super().__getattribute__(item);

        try:    #See if the stub itself has an attribute by the requested name and return it.
            #logging.debug("ROBjStub getattr {} enter time {:0.20f}".format(item, time.time()));
            return super().__getattribute__(item);
            #logging.debug("ROBjStub getattr {} exit time {:0.20f}".format(item, time.time()));
        except AttributeError as ae:
            #Last resort, this attribute does not exist locally, let us send it across to the remote Proxy.
            custompickle.dump(ROMessages._GET_ATTRIBUTE_, self._wf); #self._wf.flush();
            custompickle.dump(item, self._wf); self._wf.flush();

            #read back the exception or attribute value.
            exception = custompickle.load(self._rf);
            if(exception != None): #If there was an exception at the remote object, raise it locally.
                reraise(*exception);

            result = custompickle.load(self._rf);
            return result;

###    def __getstate__(self):
###        #print('serializing {} {} {}'.format(self._objName, self._host, self._port));
###        ###print('serializing {} '.format(self.__dict__));
###        state = self.__dict__.copy();
###        for var in ['_socket', '_rf', '_wf', '_localObj']:
###            try:
###                del state[var];
###            except KeyError:
###                pass
###        return state;
###
###    def __setstate__(self, state):
###        self.__dict__.update(state);
###        print('updating with state {}'.format(state));
###
###        romgr = ROMgr.getROMgr(self._host, self._port);
###        if(romgr is None):
###            (self._socket, self._rf, self._wf) = ROStubMgr.getRO(self._objName, self._host, self._port);
###        else:
###            ##TODO Remove this local obj assignment and instead update self to the local obj !!
###            ##TODO After testing those changes, remove all localobj logic from this class.
###            self._localObj = romgr.get(self._objName);
###        ###print('deserializing..');
###        #print('deserialized {} {} {}'.format(self._objName, self._host, self._port));
###

    @classmethod
    def serializeObj(cls, obj):
        """This function can be registered as the serialization function for every RObjStub inherited class.
        Instead of serializing the actual object it will return the stub class instance and the arguments needed to initialize the stub.
        """
        #logging.debug("serializeobj enter time {:0.20f}".format(time.time()));
        #We are asked to serialize the stub itself.
        if(isinstance(obj,RObjStub)):
            return cls, (obj._objName, obj._host, obj._port);

        #Get a handle to the remote manager.
        romgr = ROMgr.getROMgr();
        #Add the object to the remote manager if it does not have it already, get the name.
        tmpObjName = romgr.add(obj);
        # return the cls, and objname,host,port information.
        #logging.debug('custom serialization for {} object {}'.format(cls.__name__, tmpObjName));
        t = cls, ((tmpObjName, ) + romgr.srvrInfo());
        #logging.debug("serializeobj exit time {:0.20f}".format(time.time()));
        return t;
        #return cls, ((tmpObjName, ) + romgr.srvrInfo());

    #def __del__(self):
        #logging.debug("Removing remote obj stub ({},{},{})".format(self._objName, self._host, self._port));

    class RemoteMethod:
        """Decorator for all methods that needs to be executed remotely."""

        def __init__(self, compressResults=False):
            """Constructor optionally takes an argument which indicates whether the remote function's return results should be compressed before sending back"""
            self.__compressResults__ = compressResults;

        def __call__(self, rmfunc):
            if (not hasattr(rmfunc, '__call__')):
                raise TypeError("Argument rmfunc should be callable, {} does not satisfy this.".format(type(rmfunc)));

            if(not self.__compressResults__):
                @functools.wraps(rmfunc)
                def wrap(that, *args, **kwargs):
                    """Decorator function, sends the function name and arguments to the remote proxy object and returns the results."""

                    #logging.debug("RemoteMethod {} enter time {:0.20f}".format(rmfunc.__name__, time.time()));
                    custompickle.dump(rmfunc.__name__,that._wf); #that._wf.flush();
                    custompickle.dump(args, that._wf); #that._wf.flush();
                    custompickle.dump(kwargs, that._wf); that._wf.flush();
                    #logging.debug("RemoteMethod {} send time {:0.20f}".format(rmfunc.__name__, time.time()));

                    #read back the exception or result.
                    exception = custompickle.load(that._rf);
                    if(exception != None): #If there was an exception at the remote object, raise it locally.
                        reraise(*exception);
                    #logging.debug("RemoteMethod {} exception time {:0.20f}".format(rmfunc.__name__, time.time()));

                    result = custompickle.load(that._rf);
                    #logging.debug("RemoteMethod {} result time {:0.20f}".format(rmfunc.__name__, time.time()));
                    #Hand shake to tell the sending side that we got the result, and has setup remote object stubs.
                    #NOT REQUIRED: see note in ROProxy
                    #custompickle.dump(ROMessages._DUMMY_, that._wf); that._wf.flush();
                    #logging.debug('RemoteMethod for {} returning data type {}'.format(rmfunc.__name__, type(result)));
                    #logging.debug("RemoteMethod {} exit time {:0.20f}".format(rmfunc.__name__, time.time()));
                    return result;

                return wrap;
            else:
                @functools.wraps(rmfunc)
                def wrap(that, *args, **kwargs):
                    """Decorator function, sends the function name and arguments to the remote proxy object and returns the results."""

                    #logging.debug("RemoteMethod {} enter time {:0.20f}".format(rmfunc.__name__, time.time()));
                    custompickle.dump(ROMessages._COMPRESS_,that._wf); #that._wf.flush();
                    custompickle.dump(rmfunc.__name__,that._wf); #that._wf.flush();
                    custompickle.dump(args, that._wf); #that._wf.flush();
                    custompickle.dump(kwargs, that._wf); that._wf.flush();
                    #logging.debug("RemoteMethod {} send time {:0.20f}".format(rmfunc.__name__, time.time()));

                    #read back the exception or result.
                    exception = custompickle.load(that._rf);
                    if(exception != None): #If there was an exception at the remote object, raise it locally.
                        reraise(*exception);
                    #logging.debug("RemoteMethod {} exception time {:0.20f}".format(rmfunc.__name__, time.time()));

                    result = AConfig.NTWKCHANNEL.receive(that._rf);
                    #logging.debug("RemoteMethod {} result time {:0.20f}".format(rmfunc.__name__, time.time()));
                    #Hand shake to tell the sending side that we got the result, and has setup remote object stubs.
                    #NOT REQUIRED: see note in ROProxy
                    #custompickle.dump(ROMessages._DUMMY_, that._wf); that._wf.flush();
                    #logging.debug('RemoteMethod for {} returning data type {}'.format(rmfunc.__name__, type(result)));
                    #logging.debug("RemoteMethod {} exit time {:0.20f}".format(rmfunc.__name__, time.time()));
                    return result;

                return wrap;


class RObj(RObjStub):
    """Default class implementation for objects that need a remote object stub."""

    def __init__(self, objName, host, port):
        ###print("RObj __init__ called for {}".format(objName));
        super().__init__(objName, host, port);
        self.__myport__ = port;

    def __getstate__(self):
        ###print('serialization called...');
        return super().__getstate__();

    def __setstate__(self, state):
        ###print('de-serialization called...');
        super().__setstate__(state);

    def __getattribute__(self, item):
        attr  = super().__getattribute__(item);
        if(item.startswith('_') or (not hasattr(attr, '__call__'))):
            return attr;
        else:
            that=self;

            @functools.wraps(attr)
            def wrap(*args, **kwargs):
                """Decorator function, sends the function name and arguments to the remote proxy object and returns the results."""

                custompickle.dump(attr.__name__,that._wf); #that._wf.flush();
                custompickle.dump(args, that._wf); #that._wf.flush();
                custompickle.dump(kwargs, that._wf); that._wf.flush();

                #read back the exception or result.
                exception = custompickle.load(that._rf);
                if(exception != None): #If there was an exception at the remote object, raise it locally.
                    reraise(*exception);

                result = custompickle.load(that._rf);
                ###print('RObj RemoteMethod for {} returning data type {}'.format(attr.__name__, type(result)));
                return result;

            return wrap;

#    def __getattribute__(self, item):
#       print('RObj __getattribute__ {}'.format(item));
#
#       if(item.startswith('__')):
#           return super().__getattribute__(item);
#
#       if(item in ('_localObj', '_objname', '_host', '_port', '_wf', '_rf')):
#           return super().__getattribute__(item);
#
#       #Return the attribute from the remote object.
#       custompickle.dump('__GET_ATTRIBUTE__', self._wf); self._wf.flush();
#       custompickle.dump(item, self._wf); self._wf.flush();
#
#       #read back the exception or attribute value.
#       exception = custompickle.load(self._rf);
#       if(exception != None): #If there was an exception at the remote object, raise it locally.
#           reraise(*exception);
#
#       result = custompickle.load(self._rf);
#       return result;


class ROStubMgr (metaclass=ABCMeta):

    #TODO: Do we need to keep track of our stub objects ?
    __RepoLock = threading.RLock();


    @classmethod
    def getRO(cls, objname, host, port):
        """This method will connect to the remote object manager on the host. On success, there will be a dedicated Proxy object created
        at the other end of the connection for this stub object."""

        # Make network connection.
        #sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM);
        #sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1);
        sock = socket.socket();
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1);
        (host,port) = portMapper(host,port); #See if this port is tunnelled.
        sock.connect((host, port));

        #File handles to work directly with the custompickle functions.
        wf = sock.makefile('wb'); rf = sock.makefile('rb');

        #Check with the remote object manager server if the object exists.
        custompickle.dump(ROMessages._INIT_, wf); #wf.flush();
        custompickle.dump(objname, wf); wf.flush();

        #Get back the response from the remote object manager server.
        msg = custompickle.load(rf);
        if(msg == ROMessages._NOT_FOUND_):
            raise AttributeError("Object {} cannot be located at {},{}".format(objname, host, port));

        #On success, the server also sends back the id of the object (TBD: No use for this as of now).
        #objid = custompickle.load(rf);

        #msg is the remote proxy's id.
        return (socket, rf, wf, msg);


#TODO This class is NOW OBSELETE
##TODO This class to become obselete after the pickling changes are made.
##This class is now obselete as we are doing custom pickling via copyreg.
class RemoteReturn:
    """Decorator for all methods that wants to support returning a local data handle when called remotely"""

    __ClassLock = threading.RLock();
    __ROStubs = weakref.WeakKeyDictionary();

    @classmethod
    def regRemoteStub(cls, objClass, stubClass):
        """Register a stub to handle the remote class for a specific class type"""
        with cls.__ClassLock:
            cls.__ROStubs[objClass] = stubClass;

    @classmethod
    def getRemoteStub(cls, obj):
        "Find a remoteStub to handle the data type. Go down base classes if required. If no registered stubs, return the default stub."
        objClasses = inspect.getmro(type(obj));
        for c in objClasses:
            try:
                return cls.__ROStubs[c];
            except:
                pass;
        return RObj;


    def __call__(self, rmfunc):
        if (not hasattr(rmfunc, '__call__')):
            raise TypeError("Argument rmfunc should be callable, {} does not satisfy this.".format(type(rmfunc)));

        @functools.wraps(rmfunc)
        def wrap(that, *args, __rObjMgr__=None, **kwargs):
            """ This wrapper function will capture the return of the function, add it as a temporary object in the
             remote object manager and return a stub handle to it."""

            result = rmfunc(that, *args, **kwargs);

            if(__rObjMgr__ is None):
                return result;

            resultObjName = __rObjMgr__.add(result);
            resultStubClass = RemoteReturn.getRemoteStub(result);
            resultStub = resultStubClass( resultObjName, *__rObjMgr__.srvrInfo());
            #resultStub = RObj( resultObjName, *__rObjMgr__.srvrInfo());
            ###print('Remote return being executed for {} return type {}'.format(rmfunc.__name__, type(resultStub)));
            #s=custompickle.dumps(resultStub);
            #print(s);
            #rs=custompickle.loads(s);
            #print(type(rs));
            return resultStub;


        return wrap;

