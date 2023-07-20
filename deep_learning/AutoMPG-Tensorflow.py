from aida.aida import *;
host = 'tfServer2608'; dbname = 'bixi'; user = 'bixi'; passwd = 'bixi'; jobName = 'torchLinear'; port = 55660;
dw = AIDA.connect(host,dbname,user,passwd,jobName,port);

def trainingLoop(dw):
    # config = tf.ConfigProto()
    # config.gpu_options.allow_growth = True

    column_names = ['MPG', 'Cylinders', 'Displacement', 'Horsepower', 'Weight',
                    'Acceleration', 'Model Year', 'Origin']
    raw_dataset = pd.read_csv('http://archive.ics.uci.edu/ml/machine-learning-databases/auto-mpg/auto-mpg.data', names=column_names,
                              na_values="?", comment='\t',
                              sep=" ", skipinitialspace=True, engine='python')

    # n = 5000
    # df = pd.DataFrame(randn(n))
    # df.columns = ['A']
    # df['B'] = randn(n)
    # df['C'] = randn(n)
    # df['D'] = randn(n)
    # df['E'] = randn(n)
    # df['Y'] = 5 + 3 * df.A + 6 * df.B ** 2 + 7 * df.C ** 3 + 2 * df.D ** 2 + 8 * df.E * df.D + randn(n)
    #
    dataset = raw_dataset.copy()
    dataset = dataset.dropna()
    origin = dataset.pop('Origin')
    dataset['USA'] = (origin == 1) * 1.0
    dataset['Europe'] = (origin == 2) * 1.0
    dataset['Japan'] = (origin == 3) * 1.0
    train_dataset = dataset.sample(frac=0.8, random_state=0)
    test_dataset = dataset.drop(train_dataset.index)
    train_stats = train_dataset.describe()
    train_stats.pop("MPG")
    train_stats = train_stats.transpose()
    train_labels = train_dataset.pop('MPG')
    test_labels = test_dataset.pop('MPG')



    def norm(x):
        return (x - train_stats['mean']) / train_stats['std']

    normed_train_data = norm(train_dataset)
    normed_test_data = norm(test_dataset)

    def build_model():
        model = keras.Sequential([
            layers.Dense(16, activation='relu', input_shape=[len(train_dataset.keys())]),
            layers.Dense(16, activation='relu'),
            layers.Dense(1)
        ])

        optimizer = tf.keras.optimizers.RMSprop(0.001)

        model.compile(loss='mse',
                      optimizer=optimizer,
                      metrics=['mae', 'mse'])
        return model


    model = build_model()


    class PrintDot(keras.callbacks.Callback):
        def on_epoch_end(self, epoch, logs):
            if epoch % 100 == 0: print('')
            print('.', end='')

    EPOCHS = 1000

    history = model.fit(
        normed_train_data, train_labels,
        epochs=EPOCHS, validation_split=0.2, verbose=0,
        callbacks=[PrintDot()])

    # loss, mae, mse = model.evaluate(normed_test_data, test_labels, verbose=2)
    # return [loss, mae, mse]
    # weights = model.layers[2].get_weights()[0]
    # example_batch = normed_train_data[:10]
    # example_result = model.predict(example_batch)
    # example_result
    loss, mae, mse = model.evaluate(normed_test_data, test_labels, verbose=2)
    return [loss, mae, mse]


data = dw._X(trainingLoop)
print(data)
