from aida.aida import *;
host = 'tfServer2608'; dbname = 'bixi'; user = 'bixi'; passwd = 'bixi'; jobName = 'torchLinear'; port = 55660;
dw = AIDA.connect(host,dbname,user,passwd,jobName,port);

def trainingLoop(dw):
    n = 100000
    df = pd.DataFrame(randn(n))
    df.columns = ['A']
    df['B'] = randn(n)
    df['C'] = randn(n)
    df['D'] = randn(n)
    df['E'] = randn(n)
    df['Y'] = 5 + 3 * df.A + 6 * df.B ** 2 + 7 * df.C ** 3 + 2 * df.D ** 2 + 8 * df.E * df.D + randn(n)

    dataset = df.copy()


    train_dataset = dataset.sample(frac=0.8, random_state=0)
    test_dataset = dataset.drop(train_dataset.index)
    train_stats = train_dataset.describe()
    train_stats.pop("Y")
    train_stats = train_stats.transpose()
    train_labels = train_dataset.pop('Y')
    test_labels = test_dataset.pop('Y')



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

    EPOCHS = 10
    start_time = time.time()
    history = model.fit(
        normed_train_data, train_labels,
        epochs=EPOCHS, validation_split=0.2, verbose=0)
    end_time = time.time()
    execution_time = end_time - start_time
    logging.info("The execution time on GPU for a dataset of size 100000 and 10 epochs using TensorFlow is:",execution_time)
    print("The execution time on GPU for a dataset of size 100000 and 10 epochs using TensorFlow is:",execution_time)
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