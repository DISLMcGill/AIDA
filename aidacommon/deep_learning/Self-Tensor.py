from aida.aida import *;
host = 'tf_cpu_server'; dbname = 'bixi'; user = 'bixi'; passwd = 'bixi'; jobName = 'torchLinear'; port = 55660;
dw = AIDA.connect(host,dbname,user,passwd,jobName,port);
import time
def trainingLoop(dw):
    script_start = time.time()
    print("Script start time ", script_start)
    logging.info('Script start time ' + str(script_start))
    max_usage = 2000 # example for using up to 95%

    # gpus = tf.config.experimental.list_physical_devices('GPU')
    # tf.config.experimental.set_virtual_device_configuration(
    #     gpus[0],
    #     [tf.config.experimental.VirtualDeviceConfiguration(memory_limit=max_usage)])
    n = 10000
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
    transfer_start = time.time()
    with tf.device('/gpu:0'):
        train_set = tf.constant(normed_train_data, dtype=tf.float32, shape=[8000, 5])
        label = tf.constant(train_labels, 'float32', shape=[8000, 1])
    transfer_end = time.time()
    print(train_set.device)
    print(label.device)
    transfer_time = transfer_end - transfer_start
    logging.info('The data transfer time on GPU for a dataset of 10000 and 100 epochs using TensorFlow is:'+str(transfer_time))
    print("The data transfer time on GPU for a dataset of 10000 and 100 epochs using TensorFlow is:",transfer_time)
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

    EPOCHS = 100
    start_time = time.time()
    print("ML tranining start time ", start_time)
    logging.info('ML tranining start time ' + str(start_time))
    history = model.fit(
        train_set,label,epochs=EPOCHS,validation_split=0.2, verbose=0)
    end_time = time.time()
    execution_time = end_time - start_time
    print("ML tranining end time ",end_time)
    logging.info('ML tranining end time ' + str(end_time))
    logging.info('The execution time on GPU for a dataset of size 10000 and 100 epochs using TensorFlow is:'+str(execution_time))
    print("The execution time on GPU for a dataset of size 10000 and 100 epochs using TensorFlow is:",execution_time)
    # loss, mae, mse = model.evaluate(normed_test_data, test_labels, verbose=2)
    # return [loss, mae, mse]
    # weights = model.layers[2].get_weights()[0]
    # example_batch = normed_train_data[:10]
    # example_result = model.predict(example_batch)
    # example_result
    loss, mae, mse = model.evaluate(normed_test_data, test_labels, verbose=2)
    end_time = time.time()
    print("Script end time ", end_time)
    logging.info('Script end time ' + str(end_time))
    return [loss, mae, mse]


data = dw._X(trainingLoop)
print(data)
