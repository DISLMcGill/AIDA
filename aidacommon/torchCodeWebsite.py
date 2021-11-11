from aida.aida import *;
host = 'tfServer2608'; dbname = 'bixi'; user = 'bixi'; passwd = 'bixi'; jobName = 'torchLinear'; port = 55660;
dw = AIDA.connect(host,dbname,user,passwd,jobName,port);

def training(dw):
    def rmse(y, y_hat):
        """Compute root mean squared error"""
        return torch.sqrt(torch.mean((y - y_hat).pow(2).sum()))


    def forward(x, e):
        """Forward pass for our fuction"""
        return x.pow(e.repeat(x.size(0)))


    # Let's define some settings
    n = 100  # number of examples
    learning_rate = 5e-6
    target_exp = 2.0  # real value of the exponent will try to find

    # Model definition
    x = Variable(torch.rand(n) * 10, requires_grad=False)

    # Model parameter and it's true value
    exp = Variable(torch.FloatTensor([target_exp]), requires_grad=False)
    exp_hat = Variable(torch.FloatTensor([4]), requires_grad=True)  # just some starting value, could be random as well
    y = forward(x, exp)

    # a couple of buffers to hold parameter and loss history
    loss_history = []
    exp_history = []

    # Training loop
    for i in range(0, 200):
        print("Iteration %d" % i)

        # Compute current estimate
        y_hat = forward(x, exp_hat)

        # Calculate loss function
        loss = rmse(y, y_hat)

        # Do some recordings for plots
        loss_history.append(loss.item())
        exp_history.append(y_hat.data[0])

        # Compute gradients
        loss.backward()

        print("loss = %s" % loss.item())
        print("exp = %s" % exp_hat.item())

        # Update model parameters
        exp_hat.data -= learning_rate * exp_hat.grad.data
        exp_hat.grad.data.zero_()
    return exp_hat.data

exp = dw._X(training)
print(exp)