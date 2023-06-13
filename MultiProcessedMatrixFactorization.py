import numpy as np
import socket
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, wait
import threading
import dill

HOST = 'localhost'
PORT = 6000
DIMENSION = 5
NUM_WORKERS = 3
DATA_SIZE = 2500


def function(x):
    coefficients = np.asarray([3, 5.1, -6, -1.5, 0.2, 2.33])
    return x.T @ coefficients + np.random.normal()


def generate_data(size):
    data = np.random.randint(-40, 40, size=(DIMENSION, size))
    data_with_bias = np.vstack((data, np.ones(size)))
    return data_with_bias, function(data_with_bias)


class Worker:
    def __init__(self, id, data_size):
        self.id = id
        self.socket = None
        self.wf = None
        self.rf = None
        self.dataset = generate_data(data_size)

    def run(self, iterations, batch_size):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.connect((HOST, PORT))
        self.wf = self.socket.makefile('wb')
        self.rf = self.socket.makefile('rb')

        print(f"Starting training on worker {self.id}")
        test_size = int(len(self.dataset) * 0.8)
        test_x = self.dataset[0][0:test_size]
        test_y = self.dataset[1][0:test_size]
        validate_x = self.dataset[0][int(len(self.dataset) * 0.8):len(self.dataset)]
        validate_y = self.dataset[1][int(len(self.dataset) * 0.8):len(self.dataset)]

        for i in range(iterations):
            batch = np.random.randint(test_size, size=(batch_size,))
            batch_x = test_x[batch]
            batch_y = test_y[batch]
            dill.dump("get", self.wf)

            # do forward and backwards pass
            model = dill.load(self.rf)
            preds = batch_x @ model.T
            grad_desc_weights = 2 * (((preds - batch_y).T @ batch_x) / batch_size)
            dill.dump(grad_desc_weights, self.wf)

            if i % 100 == 0:
                preds = validate_x @ model.T
                loss = np.sum((validate_y - preds) ** 2)

                print(f"Loss for process {self.id} at iteration {i} is {loss}.")

        dill.dump("close", self.wf)


class Server:
    def __init__(self, number_connections, learning_rate=0.0001):
        self.num_connections = number_connections
        self.lr = learning_rate

        self.socket = None
        self.weights = None
        self.server_thread = None
        self._executor = ThreadPoolExecutor(number_connections)
        self._lock = threading.Lock()
        self.setup_server()

    def setup_server(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.bind((HOST, PORT))
        self.socket.listen()
        self.weights = np.random.uniform(size=(1, DIMENSION))
        self.server_thread = threading.Thread(target=self.run_server)
        self.server_thread.start()

    def manage_worker(self, con):
        wf = con.makefile('wb')
        rf = con.makefile('rb')
        while True:
            request = dill.load(rf)
            match request:
                case "get":
                    dill.dump(self.weights, wf)
                case "close":
                    wf.close()
                    rf.close()
                    return
                case _:  # Worker sent gradient update
                    with self._lock:
                        self.weights -= self.lr * request

    def run_server(self):
        futures = []
        while len(futures) < self.num_connections:
            con, _ = self.socket.accept()
            futures.append(self._executor.submit(self.manage_worker, con))


if __name__ == "__main__":
    server = Server(NUM_WORKERS)
    workers = [Worker(i, DATA_SIZE) for i in range(NUM_WORKERS)]
    iterations = 2000
    batch_size = 32

    print(f'Weights at start of training: {server.weights}')
    process_executor = ProcessPoolExecutor()
    futures = []
    for i in range(NUM_WORKERS):
        futures.append(process_executor.submit(workers[i].run, iterations, batch_size))

    wait(futures)
    for f in futures:
        try:
            print(f.result())
        except Exception as e:
            print(e)
    print(f'Weights at end of training: {server.weights}')
    server.socket.close()
