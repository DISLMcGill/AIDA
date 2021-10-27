import copy

from aida.aida import *;
from aidacommon.dbAdapter import DataConversion

host = 'tfServer2'; dbname = 'bixi'; user = 'bixi'; passwd = 'bixi'; jobName = 'torchLinear'; port = 55660;
dw = AIDA.connect(host,dbname,user,passwd,jobName,port);
def trainingLoop(dw,input_size, output_size,nn,torch,datasets,F):
    learningrate = 0.01
    epoch_size = 100
    model = nn.Linear(input_size,output_size)
    model = model.cuda()
    criterion = nn.MSELoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=learningrate)
    distance = dw.gmdata2017[:,2]
    duration = dw.gmdata2017[:,3]
    X = DataConversion.extract_y(distance)
    y = DataConversion.extract_y(duration)
    X = np.copy(X)
    y = np.copy(y)
    X = X+0.01
    y = y+0.01
    X = torch.from_numpy(X.astype(np.float32))
    y = torch.from_numpy(y.astype(np.float32))
    X = F.normalize(X)
    y = F.normalize(y)
    y = y.view(y.shape[0],1)
    X = X.cuda()
    y = y.cuda()
    for epoch in range(epoch_size):
        y_predicted = model(X)
        loss = criterion(y_predicted, y)
        loss.backward()
        optimizer.step()
        optimizer.zero_grad()
    dw.linearModel = model
    return(model.weight)


weight = dw._X_torch(trainingLoop,1,1)
print(weight)