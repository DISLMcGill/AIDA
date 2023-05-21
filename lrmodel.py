from aida.aida import *

class LinearRegressionModel:
    def __init__(self):
        self.weights = None
        self.lr = 0.0005

    @staticmethod
    def iterate(db, data, weights):
        import numpy as np

        x = data[0].matrix.T
        y = data[1].matrix
        batch_size = 64
        batch = np.random.choice(x.shape[0], batch_size, replace=False)
        batch_x = x[batch, :]
        batch_y = y[batch]
        preds = batch_x @ weights.T
        grad_desc_weights = 2 * (((preds - batch_y).T @ batch_x) / preds.shape[0])
        return grad_desc_weights

    @staticmethod
    def preprocess(db, data):
        x = data.project(('x1','x2','x3','x4','x5'))
        y = data.project(('y'))
        x_bias = db._ones(x.shape[0]).hstack(x)
        return (x_bias, y)

    def initialize(self, data):
        import numpy as np
        x = data[0]
        # initialize weights if not already initialized
        if self.weights is None:
            self.weights = np.ones((1,x.shape[1]))
        else:
            if self.weights.shape[0] + 1 != x.shape[1]:
                raise ValueError("Model weights are not the same dimension as input.")

    def aggregate(self, results):
        if self.sync:
            n = len(results)
            for i in range(n):
                self.weights = self.weights - (self.lr * results[i] / n)
        else:
            self.weights = self.weights - (self.lr * results)

dw = AIDA.connect('nwhe_middleware', 'bixi', 'bixi', 'bixi', 'lr')

model = LinearRegressionModel
data = dw.lr_data

print('Register Model')
service = dw._RegisterModel(model)

print('fit model')
service.fit(data, 50000)

print('model finished fitting')



