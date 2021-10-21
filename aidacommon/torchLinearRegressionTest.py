from aida.aida import *;
host = 'Server3'; dbname = 'bixi'; user = 'bixi'; passwd = 'bixi'; jobName = 'torchLinear'; port = 55660;
dw = AIDA.connect(host,dbname,user,passwd,jobName,port);
def trainingLoop(dw, input_size, output_size):
    import sys
    nn = sys.modules["torch.nn.modules"]
    torch = sys.modules["torch"]
    datasets = sys.modules["sklearn.datasets"]
    learningrate = 0.01
    epoch_size = 100
    model = nn.Linear(input_size,output_size)
    model = model.cuda()
    criterion = nn.MSELoss()
    optimizer = torch.optim.SGD(model.parameters(), lr=learningrate)
    X, y = datasets.make_regression(n_samples=100,n_features=1,noise=20,random_state=1)
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
    dw.linearModel = model
    return(model.weight)


weight = dw._X_torch(trainingLoop,1,1)
print(weight)
