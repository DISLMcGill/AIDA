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

    @staticmethod
    def iterate(db, x, y, weights, batch_size):
        db.weights = DataFrame._loadExtData_(lambda: weights, db)
        batch = np.random.choice(x.shape[0], batch_size, replace=False)
        batch_x = x[batch, :]
        batch_y = y[batch, :]
        preds = batch_x @ db.weights.T
        grad_desc_weights = 2 * (((preds - batch_y).T @ batch_x) / preds.shape[0])
        return grad_desc_weights

    @staticmethod
    def preprocess(db, x, y):
        x_bias = db._ones(x.shape[0]).hstack(x)
        return (x_bias, y)

    def initialize(self, x, y):
        # initialize weights if not already initialized
        if self.weights is None:
            self.weights = self.db._ones((1,x.shape[1]+1))
        else:
            if self.weights.shape[0] + 1 != x.shape[1]:
                raise ValueError("Model weights are not the same dimension as input.")

    def aggregate(self, results):
        if (self.sync):
            n = len(results)
            for i in range(n):
                self.weights = self.weights - (self.lr * delta_params / n)
        else:
            self.weights = self.weights - (self.lr * delta_params)

    def predict(self, x):
        return x @ self.weights.T

    @staticmethod
    def score(y_pred, y):
        rss = np.sum((y_pred - y) ** 2)
        tss = np.sum((y - y.mean()) ** 2)

        r2 = 1 - (rss / tss)
        return r2

    def get_params(self):
        return self.weights

copyreg.pickle(LinearRegressionModel, ModelRemoteStub.serializeObj)
