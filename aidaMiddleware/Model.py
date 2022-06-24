from aidacommon.dborm import Model
import numpy as np
import threading
from aidaMiddleware.distTabularData import DistTabularData
from concurrent.futures import as_completed


class LinearRegressionModel(Model):
    def __init__(self, executor, db, learning_rate):
        super().__init__()
        self.executor = executor
        self.db = db
        self.lr = learning_rate
        self.weights = None
        self.lock = threading.Lock()

    def fit(self, x, y, iterations, batch_size=1):
        # send gradient descent function to remote
        def grad_desc(actual_y, predicted_y, batch_x):
            return 2 * (((predicted_y - actual_y).T @ batch_x) / predicted_y.shape[0])

        for con in x.tabular_datas:
            con.grad_desc = grad_desc

        # initialize weights if not already initialized
        if self.weights is None:
            self.weights = np.random.rand(x.shape[1] + 1)
        else:
            if self.weights.shape[0] + 1 != x.shape[1]:
                raise ValueError("Model weights are not the same dimension as input.")

        # add 1s column to x for bias value
        results = {}
        futures = {self.executor.submit(lambda con, table: con._ones(table.shape[0]).hstack(table),
                                        c, x.tabular_datas[c]): c for c in x.tabular_datas}
        for future in as_completed(futures):
            results[futures[future]] = future.result()

        x_ones = DistTabularData(self.executor, results, x.dbc)

        def iterate(db, x, y, weights, batch_size):
            batch = np.random.choice(x.size[0], batch_size, replace=False)
            preds = x[batch] @ weights.T
            grad_desc_weights = db.grad_desc(y, preds, x)
            return grad_desc_weights

        for i in range(iterations):
            futures = [self.executor.submit(lambda con: con._XP(iterate, x_ones[con], y[con], self.weights, batch_size), c) for c in x_ones.tabular_datas]
            for future in as_completed(futures):
                result = future.result()
                self.update_params(result)

    def predict(self, x):
        return x @ self.weights.T

    def update_params(self, delta_params):
        self.lock.acquire()
        self.weights = self.weights - (self.lr * delta_params)
        self.lock.release()

    @staticmethod
    def score(y_pred, y):
        rss = np.sum((y_pred - y) ** 2)
        tss = np.sum((y - y.mean()) ** 2)

        r2 = 1 - (rss / tss)
        return r2

    def get_params(self):
        return self.weights
