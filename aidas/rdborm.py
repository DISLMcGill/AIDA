
from aidas.dborm import *;
from aidacommon.rdborm import *;

copyreg.pickle(DBTable, TabularDataRemoteStub.serializeObj);
copyreg.pickle(DataFrame, TabularDataRemoteStub.serializeObj);
