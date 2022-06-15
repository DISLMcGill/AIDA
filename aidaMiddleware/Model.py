from aidacommon.dborm import Model
import numpy as np
import threading

class LinearRegressionModel(Model):
    def __init__(self, executor, db, learning_rate):
        super().__init__()
        self.executor = executor
        self.db = db
        self.lr = learning_rate
        self.weights = None
        self.bias = 0
        self.lock = threading.Lock()

    def fit(self, x, y, iterations, batch_size=1):
        if self.weights is None:
            self.weights = np.random.rand(x.shape[1])
        else:
            if self.weights.shape[0] != x.shape[1]:
                raise ValueError("Model weights are not the same dimension as input.")

        def iterate(db, table, weights, bias, batch_size):
            batch = np.random.choice(table.size[0], batch_size, replace=False)
            preds = table[batch] @ weights.T + bias


        pass

    def predict(self, x):
        pass

    def update_params(self, delta_params):
        self.lock.acquire()


    def get_params(self):
        return self.params
