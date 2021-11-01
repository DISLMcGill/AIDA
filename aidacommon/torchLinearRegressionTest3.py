import copy

from aida.aida import *;
host = 'Server2'; dbname = 'bixi'; user = 'bixi'; passwd = 'bixi'; jobName = 'torchLinear'; port = 55660;
dw = AIDA.connect(host,dbname,user,passwd,jobName,port);
def trainingLoop(dw,input_size, output_size,nn,torch,datasets,F,np):
    import time
    import logging
    from aidacommon.dbAdapter import DataConversion
    start_time = time.time()
    learningrate = 0.0000001
    epoch_size = 8000
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
    end_time = time.time()
    execution_time = start_time - end_time
    logging.info("execution time is",execution_time)
    return(model.weight)


weight = dw._X(trainingLoop,1,1)
print(weight)