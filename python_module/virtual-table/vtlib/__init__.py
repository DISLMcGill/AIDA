import logging
import decimal
import datetime
import numpy as np
import pandas as pd
from weakref import WeakValueDictionary
from aidas.dborm import TabularData
from aidacommon.dbAdapter import DBC

# cache the registered data
foreign_tables = WeakValueDictionary()
temp_tables = WeakValueDictionary()

# This is a mapping from a postgrs data type oid to its size of the internal representation (number of bytes) for all fixed-size data types. 
postgres_type_len_lookup = {16: 1, 18: 1, 19: 64, 20: 8, 21: 2, 23: 4, 24: 4, 26: 4, 27: 6, 28: 4, 29: 4, 32: 8, 600: 16,
        601: 32, 603: 32, 628: 24, 700: 4, 701: 8, 718: 24, 790: 8, 829: 6, 774: 8, 1033: 12, 1082: 4, 1083: 8, 1114: 8, 
        1184: 8, 1186: 16, 1266: 12, 2202: 4, 2203: 4, 2204: 4, 2205: 4, 2206: 4, 4096: 4, 4089: 4, 2950: 16, 3220: 8, 
        3734: 4, 3769: 4, 2276: 4, 2278: 4, 2279: 4, 3838: 4, 2280: 4, 2281: 8, 2282: 4, 2283: 4, 2776: 4, 3500: 4, 
        3115: 4, 325: 4, 3310: 4, 269: 4, 12427: 4, 12432: 64, 12437: 8}

# Map numpy data types to PostgreSQL compatible types.
typeConverter = {int: 'integer', np.int16:'smallint', np.int32:'integer', np.int64:'bigint', np.float32:'real'
    , np.float64:'double precision', object:'text', np.object_:'text', 'bytes':'bytea'
    , 'decimal':'double precision' ,'date':'DATE', 'time':'TIME', 'timestamp':'TIMESTAMP'}
    
datetimeFormats = {'%Y-%m-%d':'date', '%H:%M:%S':'time', '%Y-%m-%d %H:%M:%S':'timestamp'}


class VTManager():
    
    def __init__(self, conn=None, aida=False):
        #logging.info("create the data registration manager.")
        
        # set up the connection to the database
        if conn:
            self._conn = conn
        else:
            import plpy
            self._conn = plpy
        

    

    def regTable(self, data, name, foreignTable=True):
        if foreignTable:
            self.regForeignTable(data, name)
        else:
            self.regTempTable(data, name)
    

    def regForeignTable(self, data, name, foreignServer=1):
        if name in foreign_tables:
            raise Exception("Another data set has already been registered with the name {}.".format(name))

        foreign_tables[name] = data
        try:
            colnames,collist = self._columnAnalyzer(data)

            ftb = "create foreign table {} ( \n"
            ftb += collist
            ftb += " ) server vt_server{} options (\n "
            ftb += " tblname  \'{}\'\n);"
            
            ftb = ftb.format(name, foreignServer, name)

            logging.info("Executing SQL request: \n{}".format(ftb))
            self.executeQry(ftb, sqlType=DBC.SQLTYPE.CREATE)
        except Exception as e:
            del foreign_tables[name]
            raise e
    
    def regTempTable(self, data, name, analyze=False):
        if name in temp_tables:
            raise Exception("Another data set has already been registered with the name {}.".format(name))

        temp_tables[name] = data
        try:
            colnames,collist = self._columnAnalyzer(data)

            # First create an intermediate TableUDF to expose this TabularData to the RDBMS
            # And then use the TableUDF to create a temporary table
            cudf =  'CREATE FUNCTION {}() RETURNS TABLE({})\nAS $$\nimport vtlib;'
            cudf += '\ndf = vtlib.temp_tables[\'{}\'];\ncols = [{}];'
            cudf += '\ntry:\n    data = df.rows\n    if len(cols) == 1:\n        result = data[cols[0]]'
            cudf += '\n    else:\n        result = [dict(zip(data,t)) for t in zip(*data.values())]'
            cudf += '\nexcept:\n    data = df;'
            cudf += '\n    if len(cols) == 1:\n        result = list(data.iloc[:,0])'
            cudf += '\n    else:\n        result = data.to_dict(\'records\')'
            cudf += '\nreturn result \n$$ LANGUAGE plpython3u;'

            cudf = cudf.format(name+"IntermUDF", collist, name, colnames)
            logging.info("Executing SQL request: \n{}".format(cudf))
            self.executeQry(cudf, sqlType=DBC.SQLTYPE.CREATE)

            tptb = "create TEMP table {} \n AS {};"
            sub_qry = "select * from " + name + "IntermUDF()"
            tptb  = tptb.format(name, sub_qry)
            
            logging.info("Executing SQL request: \n{}".format(tptb))
            self.executeQry(tptb, sqlType=DBC.SQLTYPE.CREATE)

            # Drop the intermediate UDF exposure
            dpie = 'DROP FUNCTION {};'.format(name+"IntermUDF")
            logging.info("Executing SQL request: \n{}".format(dpie))
            self.executeQry(dpie, sqlType=DBC.SQLTYPE.DROP)

            if analyze:
                # collects statistics about the contents of the temp table for optimization
                self.executeQry("ANALYZE {};".format(name), sqlType=DBC.SQLTYPE.CREATE)
        except Exception as e:
            del temp_tables[name]
            raise e
    
    def _columnAnalyzer(self, dataframe):
        if isinstance(dataframe,TabularData):
            data = dataframe.rows
            numrows = dataframe.numRows
        else:
            data = dataframe
            numrows = len(dataframe.index)

        collist=None
        colnames=None
        for colname in data:
            collist = (collist + ',' + '\n') if(collist is not None) else ''
            colnames = (colnames + ',') if(colnames is not None) else ''
            dataType = data[colname].dtype.type
            if(dataType is np.object_):
                try:
                    cd = data[colname][0]
                    if(isinstance(cd, decimal.Decimal)):
                        dataType = "decimal"
                    elif(isinstance(cd, str)):
                        for f in datetimeFormats:
                            try:
                                datetime.datetime.strptime(cd, f)
                                dataType = datetimeFormats[f]
                                break
                            except ValueError:
                                pass
                    elif(isinstance(cd, bytes)):
                        dataType = "bytes"
                except IndexError:
                    pass
            #logging.debug("UDF column {} type {}".format(colname, dataType));
            collist += colname + ' ' + ('text' if(dataType not in typeConverter) else typeConverter[dataType]) 
            colnames += '\'' + colname + '\''
        
        return (colnames,collist)

            
    def executeQry(self, *args, sqlType=DBC.SQLTYPE.SELECT):
        if not isinstance(self._conn, DBC):
            return self._conn.execute(*args)
        else:
            # using AIDA
            self._conn._executeQry(*args, sqlType=sqlType)

    def unregTable(self, tblname):
        if tblname in foreign_tables.keys():
            self.executeQry("drop foreign table {};".format(tblname), sqlType=DBC.SQLTYPE.DROP)
            del foreign_tables[tblname]
        
        elif tblname in temp_tables.keys():
            self.executeQry("drop table {};".format(tblname), sqlType=DBC.SQLTYPE.DROP)
            del temp_tables[tblname]

    def __del__(self):
        for tblname in foreign_tables.keys():
            self.executeQry("drop foreign table {};".format(tblname), sqlType=DBC.SQLTYPE.DROP)
        foreign_tables.clear()
        
        for tblname in temp_tables.keys():
            self.executeQry("drop table {};".format(tblname), sqlType=DBC.SQLTYPE.DROP)
        temp_tables.clear()
