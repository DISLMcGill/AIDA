
from aidas.dborm import *;
from aidacommon.rdborm import *;

copyreg.pickle(DBTable, TabularDataRemoteStub.serializeObj);
copyreg.pickle(DataFrame, TabularDataRemoteStub.serializeObj);
copyreg.pickle(ModelService, ModelServiceRemoteStub.serializeObj);
copyreg.pickle(PSModelService, ModelServiceRemoteStub.serializeObj);
copyreg.pickle(TorchModelService, TorchModelServiceRemoteStub.serializeObj);
copyreg.pickle(TorchService, TorchServiceRemoteStub.serializeObj);
copyreg.pickle(TorchRMIService, TorchServiceRemoteStub.serializeObj);
copyreg.pickle(TorchRMIServer, ParameterServerRemoteStub.serializeObj);
copyreg.pickle(ParameterServer, ParameterServerRemoteStub.serializeObj);
copyreg.pickle(CustomParameterServer, ParameterServerRemoteStub.serializeObj)
copyreg.pickle(DistTabularData, DistTabularDataRemoteStub.serializeObj);
