from aida.aida import *

con = AIDA.connect('whe_middleware', 'bixi', 'bixi', 'bixi')

class LogisticRegressionModel(Model):
    @staticmethod
    def preprocess(db, x, y):
        x_bias = db._ones(x.shape[0]).hstack(x)
        return (x_bias, y)

    def initialize(self, x, y):
        if self.weights is None:
            self.weights = self.db._ones((1,x.shape[1]+1))
        else:
            if self.weights.shape[0] + 1 != x.shape[1]:
                raise ValueError("Model weights are not the same dimension as input.")

    @staticmethod
    def iterate(db, x, y, weights, batch_size):
        import numpy as np
        db.weights = DataFrame._loadExtData_(lambda: weights, db)
        batch = np.random.choice(x.shape[0], batch_size, replace=False)
        batch_x = x[batch, :]
        batch_y = y[batch, :]
        def sigmoid(vector):
            cn = vector.columns[0]
            c = vector.cdata[cn]
            fn = np.asarray([(1 / 1 + np.exp(n)) for n in c])
            return {cn: fn}
        preds = batch_x @ db.weights.T
        grad_desc_weights = (1 / preds.shape[0]) * (batch_x.T @ (preds._U(sigmoid) - batch_y))
        return grad_desc_weights

    def agg(self, results):
        if self.sync:
            n = len(results)
            for i in range(n):
                self.weights = self.weights - (self.lr * delta_params / n)
        else:
            self.weights = self.weights - (self.lr * delta_params)

    def predict(self, x):
        def sigmoid(vector):
            cn = vector.columns[0]
            c = vector.cdata[cn]
            fn = np.asarray([(1 / 1 + np.exp(n)) for n in c])
            return {cn: fn}
        preds = batch_x @ db.weights.T
        return preds._U(sigmoid)

    @staticmethod
    def score(y_pred, y):
        rss = np.sum((y_pred - y) ** 2)
        tss = np.sum((y - y.mean()) ** 2)

        r2 = 1 - (rss / tss)
        return r2

    def get_params(self):
        return self.weights
