from aida.aida import *;
host = 'tfServer2608'; dbname = 'bixi'; user = 'bixi'; passwd = 'bixi'; jobName = 'torchLinear'; port = 55660;
dw = AIDA.connect(host,dbname,user,passwd,jobName,port);
def Analysis(dw,input_size, output_size,nn,torch,datasets,F,np):
    import time
    import logging
    import psutil
    import multiprocessing as mp
    start_time = time.time()
    def trainingLoop(input_size,output_size,nn,torch,datasets,F,np):
        learningrate = 0.01
        epoch_size = 1000
        model = nn.Linear(input_size,output_size)
        criterion = nn.MSELoss()
        optimizer = torch.optim.SGD(model.parameters(), lr=learningrate)
        X, y = datasets.make_regression(n_samples=1000000,n_features=1,noise=20,random_state=1)
        X = torch.from_numpy(X.astype(np.float32))
        y = torch.from_numpy(y.astype(np.float32))
        y = y.view(y.shape[0],1)
        time.sleep(1)
        for epoch in range(epoch_size):
            y_predicted = model(X)
            loss = criterion(y_predicted, y)
            loss.backward()
            optimizer.step()
            optimizer.zero_grad()
    worker_process = mp.Process(target = trainingLoop())
    worker_process.start()
    p = psutil.Process(worker_process.pid)
    cpu_percents = []
    while worker_process.is_alive():
        cpu_percents.append(p.cpu_percent())
        time.sleep(0.01)
    worker_process.join()
    return cpu_percents
cpu_percents = dw._X(Analysis,1,1)
print(cpu_percents)
