import copy

from aida.aida import *;
host = 'Server2'; dbname = 'bixi'; user = 'bixi'; passwd = 'bixi'; jobName = 'torchLinear'; port = 55660;
dw = AIDA.connect(host,dbname,user,passwd,jobName,port);
def trainingLoop(dw,input_size, output_size,nn,torch,datasets,F,np):
    import logging
    from aidacommon.dbAdapter import DataConversion
    learningrate = 0.01
    epoch_size = 1000
    logging.info("running on server")
    model = nn.Linear(input_size,output_size)
    model = model.cuda()
    criterion = nn.MSELoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=learningrate)
    distance = dw.gmdata2017[:,2]
    duration = dw.gmdata2017[:,3]
    X = DataConversion.extract_X(distance)
    y = DataConversion.extract_y(duration)
    X = np.copy(X)
    y = np.copy(y)
    X = torch.from_numpy(X.astype(np.float32))
    y = torch.from_numpy(y.astype(np.float32))
    y = y.view(y.shape[0],1)
    X = F.normalize(X, dim=0)
    X = X.cuda()
    y = y.cuda()
    for epoch in range(epoch_size):
        y_predicted = model(X)
        loss = criterion(y_predicted, y)
        loss.backward()
        optimizer.step()
        optimizer.zero_grad()
        print(model.weight)
    dw.linearModel = model
    return(model.weight)


weight = dw._X(trainingLoop,1,1)
print(weight)