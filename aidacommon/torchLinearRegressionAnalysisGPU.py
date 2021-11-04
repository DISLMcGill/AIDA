from aida.aida import *;
host = 'tfClient2608'; dbname = 'bixi'; user = 'bixi'; passwd = 'bixi'; jobName = 'torchLinear'; port = 55660;
dw = AIDA.connect(host,dbname,user,passwd,jobName,port);
def trainingLoop(dw,input_size, output_size,nn,torch,datasets,F,np):
    import psutil
    import time
    import logging
    start_time = time.time()
    start_cpu = psutil.cpu_percent()
    start_RAM = psutil.virtual_memory().percent  
    learningrate = 0.01
    epoch_size = 1000
    model = nn.Linear(input_size,output_size)
    model = model.cuda()
    criterion = nn.MSELoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=learningrate)
    X, y = datasets.make_regression(n_samples=10000,n_features=1,noise=20,random_state=1)
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
    end_time = time.time()
    execution_time = end_time - start_time
    end_cpu = psutil.cpu_percent() 
    logging.info("execution time for 10000 samples and 100000 iteration on GPU is " + str(execution_time))
    return(model.weight)

weight = dw._X(trainingLoop,1,1)
print(weight)