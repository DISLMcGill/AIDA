from aida.aida import *
import time
import torch

class LRModel:
    class LinearRegression(torch.nn.Module):
        def __init__(self):
            import torch
            super().__init__()
            self.linear = torch.nn.Linear(5, 1)

        def forward(self, data):
            return self.linear(data)
    def __init__(self):
        import torch
        self.weights = None
        self.optimizer = None
        self.model = self.LinearRegression

    @staticmethod
    def iterate(db, data, weights):
        import torch
        import time
        import logging

        db.num += 1

        model = weights

        s = time.perf_counter()
        try:
            batch, target = next(db.iterator)
        except StopIteration:
            db.iterator = iter(data.getLoader())
            batch, target = next(db.iterator)
        db.batch_time += time.perf_counter() - s
        start = time.perf_counter()
        preds = model(torch.squeeze(batch).float())
        loss = db.loss(torch.squeeze(preds), target.float())
        if db.num % 100 == 0:
            logging.info(f"iteration {db.num} has loss {loss.item()}")
        loss.backward()
        grads = []
        for param in model.parameters():
            grads.append(param.grad)
        db.calc_time += time.perf_counter() - start
        if db.num == 5000:
            logging.info(f"total calc time: {db.calc_time} {db.batch_time=}")
        return grads

    @staticmethod
    def preprocess(db, data):
        import torch
        data.makeLoader([('x1', 'x2', 'x3', 'x4', 'x5'), 'y'], 1000)
        db.iterator = iter(data.getLoader())
        db.loss = torch.nn.MSELoss()
        db.num = 0
        db.calc_time = 0
        db.batch_time = 0
        return data

    def initialize(self, data):
        import torch
        self.weights = self.model()
        self.optimizer = torch.optim.SGD(self.weights.parameters(), lr=0.0003)

    def aggregate(self, results):
        if not self.sync:
            for grad, param in zip(results, self.weights.parameters()):
                param.grad = grad
            self.optimizer.step()
            self.optimizer.zero_grad()
        else:
            for r in results:
                for grad, param in zip(r, self.weights.parameters()):
                    param.grad = grad
                self.optimizer.step()
                self.optimizer.zero_grad()

dw = AIDA.connect('localhost', 'bixi', 'bixi', 'bixi', 'lr')
print('Registering model')
service = dw._RegisterModel(LRModel)
print('Fitting model')
start = time.perf_counter()
service.fit(dw.lr_data, 5000, sync=False)
stop = time.perf_counter()
print(f'Central Model finished in {stop-start}')
dw._close()
