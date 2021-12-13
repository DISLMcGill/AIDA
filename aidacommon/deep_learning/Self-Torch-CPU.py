from aida.aida import *;
host = 'ht_Server'; dbname = 'bixi'; user = 'bixi'; passwd = 'bixi'; jobName = 'torchLinear'; port = 55660;
dw = AIDA.connect(host,dbname,user,passwd,jobName,port);
def trainingLoop(dw):
    script_start = time.time()
    logging.info('Script start time ' + str(script_start))
    n = 10000
    df = pd.DataFrame(randn(n))
    df.columns = ['A']
    df['B'] = randn(n)
    df['C'] = randn(n)
    df['D'] = randn(n)
    df['E'] = randn(n)
    df['Y'] = 5 + 3 * df.A + 6 * df.B ** 2 + 7 * df.C ** 3 + 2 * df.D ** 2 + 8 * df.E * df.D + randn(n)

    dataset = df.copy()



    # In[109]:

    train_dataset = dataset.sample(frac=0.8, random_state=0)
    test_dataset = dataset.drop(train_dataset.index)

    # In[110]:

    train_stats = train_dataset.describe()
    train_stats.pop("Y")
    train_stats = train_stats.transpose()

    # In[111]:

    train_labels = train_dataset.pop('Y')
    test_labels = test_dataset.pop('Y')


    # In[113]:

    train_target = torch.tensor(train_labels.values.astype(np.float32))

    # In[114]:

    train_target = train_target.view(train_target.shape[0], 1)

    # In[115]:

    test_target = torch.tensor(test_labels.values.astype(np.float32))

    # In[116]:

    test_target = test_target.view(test_target.shape[0], 1)

    # In[118]:

    def norm(x):
        return (x - train_stats['mean']) / train_stats['std']

    normed_train_data = norm(train_dataset)
    normed_test_data = norm(test_dataset)

    # In[119]:

    normed_train_data = torch.from_numpy(normed_train_data.values)
    normed_train_data = normed_train_data.float()

    # In[120]:

    normed_test_data = torch.from_numpy(normed_test_data.values)
    normed_test_data = normed_test_data.float()

    # In[121]:

    def get_training_model(inFeatures=len(train_dataset.keys()), hiddenDim=16, nbClasses=1):
        # construct a shallow, sequential neural network
        model = nn.Sequential(OrderedDict([
            ("hidden_layer_1", nn.Linear(inFeatures, hiddenDim)),
            ("activation_1", nn.ReLU()),
            ("hidden_layer_2", nn.Linear(hiddenDim, hiddenDim)),
            ("activation_2", nn.ReLU()),
            ("output_layer", nn.Linear(hiddenDim, nbClasses))
        ]))
        # return the sequential model
        return model

    # In[122]:

    model = get_training_model()

    # In[123]:

    optimizer = torch.optim.RMSprop(model.parameters(), lr=0.001)

    # In[124]:

    criterion = nn.MSELoss()
    epoch_size = 50000

    # In[125]:

    model(normed_train_data).size()

    start_time = time.time()
    logging.info('Training start time ' + str(start_time))
    for epoch in range(epoch_size):
        predicted = model(normed_train_data)
        loss = criterion(predicted, train_target)
        loss.backward()
        optimizer.step()
        optimizer.zero_grad()
    end_time = time.time()
    logging.info('Training end time ' + str(end_time))
    execution_time = end_time - start_time
    #200000
    logging.info('The execution time on CPU for a dataset of size 10000 and 50000 epochs using Pytorch is: '+str(execution_time))
    return_mesg = "The execution time on CPU for a dataset of size 10000 and 50000 epochs using Pytorch is:" + str(execution_time)
    # In[127]:
    normed_test_data = normed_test_data
    test_target = test_target
    predicted = model(normed_test_data)
    loss = criterion(predicted, test_target)
    return_mesg = return_mesg + " and the loss of the model is: " + str(loss)
    script_end = time.time()
    logging.info('The script end time is: '+str(script_end))
    return return_mesg


return_mesg = dw._X(trainingLoop)
print(return_mesg)