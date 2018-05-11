import weakref;
import copyreg;
from enum import Enum;
from abc import ABCMeta, abstractmethod;

import collections;

import logging;

import numpy as np;

from aidacommon.rop import ROMgr;
from aidacommon.rdborm import *;

class DBC(metaclass=ABCMeta):
    _dataFrameClass_ = None;

    class SQLTYPE(Enum):
        SELECT=1; CREATE=2; DROP=3;

    class AGGTYPE(Enum):
        SUM='SUM({})'; AVG='AVG({})';
        COUNT='COUNT({})'; COUNTD='COUNT(DISTINCT {})'; COUNTN='SUM(CASE WHEN {} IS NULL THEN 1 ELSE 0 END)';
        MAX='MAX({})'; MIN='MIN({})';

    _tableRepo_ = weakref.WeakValueDictionary();

    def __init__(self, conMgr, jobName, dbName):
        self._conMgr = conMgr;
        self._jobName = jobName;
        self._conMgr.add(jobName, self);
        self._roMgrObj = ROMgr.getROMgr();
        self._dbName = dbName;
        self._workSpaceProxies_ = {};

    #@abstractmethod
    #def _getDBTable(self, relName, dbName=None): pass;

    @property
    def dbName(self):
        return self._dbName;

    #Give a list of tables.
    @abstractmethod
    def _tables(self): pass;

    def _getDBTable(self, relName, dbName=None):
        return self._tableRepo_[relName];

    @abstractmethod
    def _executeQry(self, sql, resultFormat='column'): pass;

    def _X(self, func, *args, **kwargs):
        """Function that is called from stub to execute a python function in this workspace"""
        #Execute the function with this workspace as the argument and return the results if any.
        return func(self, *args, **kwargs);

    def _XP(self, func, *args, **kwargs):
        """Function that is called from stub to execute a python function in this workspace"""
        #Execute the function with this workspace as the argument and return the results if any.
        #Wrap the DBC object to make sure that the DBC object returns only NumPy matrix representations of the TabularData objects.
        #TODO: Go over the args and kwargs and replace TabularData objects with NumPy matrix representations.
        return func(DBCWrap(self), *args, **kwargs);

    def _ones(self, shape, cols=None):
        return DBC._dataFrameClass_.ones(shape, cols, self);

    def _rand(self, shape, cols=None):
        return DBC._dataFrameClass_.rand(shape, cols, self);

    def _randn(self, shape, cols=None):
        return DBC._dataFrameClass_.randn(shape, cols, self);

    @abstractmethod
    def _toTable(self, tblrData, tableName=None): pass;

    @abstractmethod
    def _dropTblUDF(self, tblrData, tableName=None): pass;

    @abstractmethod
    def _describe(self, tblrData): pass;

    @abstractmethod
    def _agg(self, agfn, tblrData, collist=None, valueOnly=True): pass;

    def _close(self):
        self._conMgr.remove(self._jobName);

    def _registerProxy_(self, attrname, proxyid):
        """"Invoked by the remote stub of DBC to register the proxyid of a stub it has pointing to an object in this workspace """
        proxyObj = self._roMgrObj.getProxy(proxyid);
        try:
            self._workSpaceProxies_[attrname].add(proxyObj);
        except KeyError:
            self._workSpaceProxies_[attrname] = weakref.WeakSet([proxyObj,]);

    def __getattribute__(self, item):
        #Check if the object has this attribute.
        try:
            return super().__getattribute__(item);
        except:
            pass;
        #Maybe its a database object.
        #TODO: Need to ensure that databaseworkspace.var = tabularDataObject does not work if var is a valid tablename in db.
        try:
            #Check if we already have this table loaded up in the DB workspace.
            return super(self.__class__, self)._getDBTable(item);
        except KeyError:
            #We do not have it, so let us see if the adapter implementation class can load it from the database.
            tbl = self._getDBTable(item);
            #Store the table handle for any future use.
            self._tableRepo_[item] = tbl;
            return tbl;

    def _setattr_(self, key, value, returnAttr=False):
        """Invoked by the remote DBC to set an attribute in DBC for use in workspace"""
        #logging.debug("DBC workspace : to set {} to value of type {}".format(key, type(value)));
        try: #First see if we already have an attribute.
            cur =  self.__getattribute__(key);
        except:
            cur = None;

        if(cur):
            if(isinstance(cur, DBObject)):
                logging.error("Error: there is already an object {} in the database.".format(key));
                raise TypeError("Error: there is already an object {} in the database.".format(key));
            #If there is already an attribute, make sure that its stub serialization procedures match the new one.
            #Or current one has no special stub serialization. otherwise raise an exception.
            curtype = copyreg.dispatch_table.get(type(cur));
            valuetype = copyreg.dispatch_table.get(type(value));
            #logging.debug("_setattr_ called for an existing attribute {} curtype {} valuetype {}".format(key, curtype, valuetype));
            if(curtype and curtype != valuetype):
                logging.error("Error: unable to set {} remote stub for new type {} does not match that of the current type {}".format(key, type(key), type(cur)));
                raise TypeError("Error: unable to set {} remote stub for new type {} does not match that of the current type {}".format(key, type(key), type(cur)));
            #If current attributes's type is one with a remote stub assigned,
            # replace the reference of current proxies with the new obj.
            if(curtype):
                proxies = self._workSpaceProxies_.get(key);
                if(proxies):
                    #logging.debug("Replacing object in place for attribute {}".format(key));
                    self._roMgrObj.replace(proxies, value);

        #Do the actual setting of the attribute.
        super().__setattr__(key, value);
        if(returnAttr):
            return value;

    def __setattr__(self, key, value):
        if(key.startswith('_')):
            return super().__setattr__(key, value);
        self._setattr_(key, value, False);

copyreg.pickle(DBC, DBCRemoteStub.serializeObj);

class DBCRemoteStub(aidacommon.rop.RObjStub):
    @aidacommon.rop.RObjStub.RemoteMethod()
    def _getDBTable(self):
        pass;

    @aidacommon.rop.RObjStub.RemoteMethod()
    def _executeQry(self, sql, resultFormat='column'):
        pass;

    @aidacommon.rop.RObjStub.RemoteMethod()
    def _x(self, func, *args, **kwargs):
        pass;

    @aidacommon.rop.RObjStub.RemoteMethod()
    def _ones(self, shape, cols=None):
        pass;

    @aidacommon.rop.RObjStub.RemoteMethod()
    def _rand(self, shape, cols=None):
        pass;

    @aidacommon.rop.RObjStub.RemoteMethod()
    def _randn(self, shape, cols=None):
        pass;

    @aidacommon.rop.RObjStub.RemoteMethod()
    def _toTable(self, tblrData, tableName=None):
        pass;

    @aidacommon.rop.RObjStub.RemoteMethod()
    def _close(self):
        pass;

    @aidacommon.rop.RObjStub.RemoteMethod()
    def _registerProxy_(self, attrname, proxyid):
        pass;

    #TODO: write corresponding code in the DBC class
    @aidacommon.rop.RObjStub.RemoteMethod()
    def _setattr_(self, key, value, returnAttr=False):
        pass;

    def __getattribute__(self, item):
        try:
            #Check if we have the attribute locally.
            return object.__getattribute__(self, item);
        except:
            pass;

        #Find the object remotely ...
        result = super().__getattribute__(item);

        #If this a stub object, we are going to set it locally and also listen for updates on it.
        if(isinstance(result, aidacommon.rop.RObjStub)):
            self._registerProxy_(item, result.proxyid);
            super().__setattr__(item, result);
        #return the attribute.
        return result

    def __setattr__(self, key, value):
        if(key.startswith('_')):
            super().__setattr__(key, value);
        else:
            curval = None;
            try:
                #Find if there is an existing object for this key locally/remotely.
                curval = self.__getattribute__(key);
            except:
                pass;

            if(curval):
                #If there is currently an object which is a remote object stub,
                if(isinstance(curval, aidacommon.rop.RObjStub)):
                    # but the new one is not, we cannot allow this.
                    if(not isinstance(value, aidacommon.rop.RObjStub)):
                        raise AttributeError("Error: cannot replace a remote stub with a regular object")
                    #If we are replacing one remote obj stub with another, they need to have compatible stubs.
                    if(not (isinstance(value, curval.__class__))):
                        raise AttributeError("Error: the remote stubs are not compatible {} {}".format(value.__class__, curval.__class__))
                #TODO if curval and value are the same, do nothing.

            #Ask the remote object to set this attribute. it can also return a stub if this a stub and we do not have
            #currently a stub pointing to it.
            #TODO ... do we need to get stub back from do this ? why cannot we just use the existing stub ?
            robj = self._setattr_(key, value, not(curval) and isinstance(value, aidacommon.rop.RObjStub) );
            if(robj): #The remote object returned a stub.
                self._registerProxy_(key, robj.proxyid);
                super().__setattr__(key, robj);

    #TODO: trap __del__ and call _close ?


copyreg.pickle(DBCRemoteStub, DBCRemoteStub.serializeObj);

# Class to trap all calls to DBC from remote execution function if it only needs the NumPy objects.
# This class will trap any TabularData objects and pass out only their NumPy representation.
class DBCWrap:
    def __init__(self, dbcObj):
        self.__dbcObj__ = dbcObj; #This is the DBC workspace object we are wrapping
        self.__tDataColumns__ = {}; #Keep track of the column metadata of all TabularData variable names that we have seen so far.

    def __getattribute__(self, item):
        #Trap the calls to ALL my object variables here itself.
        if (item in ('__dbcObj__', '__tDataColumns__')):
            return super().__getattribute__(item);

        #Every other variable must come from the DBC object that we are wrapping.

        val = getattr(super().__getattribute__('__dbcObj__'), item);
        if(isinstance(val, TabularData)): #If the value returned from the DBC object is of type TabularData
            tDataCols = super().__getattribute__('__tDataColumns__');
            tDataCols[item] = val.columns; #We will keep track of that variable/objects metadata
            #Instead of returning the TabularData object, we will return only the NumPy matrix representation.
            #But since tabular data objects internally stored matrices in transposed format, we will have to transpose it
            # Back to regular format first.
            val = val.matrix.T;
            if(not val.flags['C_CONTIGUOUS']): #If the matrix is not C_CONTIGUOUS, make a copy in C_CONTGUOUS form.
                val = np.copy(val, order='C');
            if(len(val.shape) == 1):
                val = val.reshape(len(val), 1, order='C');
            #logging.debug("DBCWrap, getting : item {}, shape {}".format(item, val.shape));
        return val;

    def __setattr__(self, key, value):
        #Trap the calls to ALL my object variables here itself.
        if (key in ('__dbcObj__', '__tDataColumns__')):
            return super().__setattr__(key, value);

        #logging.debug("DBCWrap, setting called : item {}, value type {}".format(key, type(value)));
        #Every other variable is set inside the DBC object that we are wrapping.
        try:
            #Check if this was a TabularData object in the DBC object which we had reduced to just matrix representation.
            #logging.debug("DBCWrap: setattr : current known tabular data objects : {}".format(self.__tDataColumns__.keys()));
            tDataCols = self.__tDataColumns__[key];
            #If we got to this line, then it means it was a TabularData object.
            # So we need to build a new TabularData object using the original column metadata.
            #First step, transpose the matrix to fit the internal form of TabularData objects.
            value = value.T;
            if(not value.flags['C_CONTIGUOUS']): #If the matrix is not C_CONTIGUOUS, make a copy in C_CONTGUOUS form.
                value = np.copy(value, order='C');
            #Build a new TabularData object using virtual transformation.
            #logging.debug("DBCWrap, setting : item {}, shape {}".format(key, value.shape));
            valueDF = DBC._dataFrameClass_._virtualData_(lambda:value, cols=tuple(tDataCols.keys()), colmeta=tDataCols, dbc=self.__dbcObj__);
            setattr(self.__dbcObj__, key, valueDF);
            return;
        except :
            logging.exception("DBCWrap : Exception ");
            pass;
        setattr(self.__dbcObj__, key, value);