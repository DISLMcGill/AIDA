from aida.aida import *;
host = 'Server'; dbname = 'bixi'; user = 'bixi'; passwd = 'bixi'; jobName = 'torchLinear'; port = 55660;
dw = AIDA.connect(host,dbname,user,passwd,jobName,port);
def trainingLoop(dw, input_size, output_size):
    model = nn.linear(input_size,output_size)
    model = model.cuda()
    criterion = nn.MSEloss()
    optimizer = torch.optim.SGD(self.model.parameters(), lr=learningrate)
    X, y = datasets.make_regression(n_samples=100,n_features=1,noise=20,random_state=1)
    X = torch.from_numpy(X_numpy.astype(np.float32))
    y = torch.from_numpy(y_numpy.astype(np.float32))
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
