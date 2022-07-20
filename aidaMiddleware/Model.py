from aidacommon.dborm import Model
from aidacommon.rdborm import ModelRemoteStub
import numpy as np
import threading
from aidaMiddleware.distTabularData import DistTabularData
from concurrent.futures import as_completed
import copyreg
import logging

class LinearRegressionModel(Model):
    def __init__(self, executor, db, learning_rate, sync=True):
        super().__init__(executor, db, learning_rate, sync)

    def fit(self, x, y, iterations, batch_size=1):
        # initialize weights if not already initialized
        if self.weights is None:
            self.weights = self.db._ones((1,x.shape[1]+1))
        else:
            if self.weights.shape[0] + 1 != x.shape[1]:
                raise ValueError("Model weights are not the same dimension as input.")

        # add 1s column to x for bias value
        logging.warning("made weights")
        results = {}
        futures = {self.executor.submit(lambda con, table: con._ones(table.shape[0]).hstack(table),
                                        c, x.tabular_datas[c]): c for c in x.tabular_datas}
        for future in as_completed(futures):
            results[futures[future]] = future.result()

        x_ones = DistTabularData(self.executor, results, x.dbc)
        logging.warning("added bias")
        def iterate(db, x, y, weights, batch_size):
            db.weights = DataFrame._loadExtData_(lambda: weights, db)
            batch = np.random.choice(x.shape[0], batch_size, replace=False)
            batch_x = x[batch,:]
            batch_y = y[batch,:]
            preds = batch_x @ db.weights.T
            grad_desc_weights = 2 * (((preds - batch_y).T @ batch_x) / preds.shape[0])
            return grad_desc_weights

        for i in range(iterations):
            futures = [self.executor.submit(lambda con: con._XP(iterate, x_ones.tabular_datas[con],
                                            y.tabular_datas[con], self.weights.cdata, batch_size),
                                            c) for c in x_ones.tabular_datas]
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

copyreg.pickle(LinearRegressionModel, ModelRemoteStub.serializeObj)
<<<<<<< Updated upstream
=======


>>>>>>> Stashed changes
