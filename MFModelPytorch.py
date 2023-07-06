import torch
import time
from aida.aida import *
class MatrixFactorization:
    class MatrixFactorization(torch.nn.Module):
        def __init__(self):
            import torch
            super().__init__()
            self.user_factors = torch.nn.Embedding(1500, 3, sparse=True)
            self.item_factors = torch.nn.Embedding(2000, 3, sparse=True)

        def forward(self, data):
            user = data[0]
            item = data[1]
            return (self.user_factors(user) * self.item_factors(item)).sum(1)
    def __init__(self):
        import torch
        self.weights = self.MatrixFactorization()
        self.optimizer = torch.optim.SGD(self.weights.parameters(), lr=0.00001, weight_decay=0.002)

    def aggregate(self, update):
        self.optimizer.zero_grad()
        self.weights.user_factors.weight.grad = update[0]
        self.weights.item_factors.weight.grad = update[1]
        self.optimizer.step()

    def initialize(self, data):
        pass

    @staticmethod
    def preprocess(db, data):
        import torch
        data.makeLoader([('user_id', 'movie_id'), 'rating'], 1000)
        db.x = iter(data.getLoader())
        db.loss_fun = torch.nn.MSELoss()
        return data

    @staticmethod
    def iterate(db, data, weights):
        import torch
        import logging
        import time

        try:
            db.num += 1
        except KeyError:
            db.num = 0
            db.calc_time = 0

        start = time.perf_counter()
        try:
            x, rating = next(db.x)
        except StopIteration:
            db.x = iter(data.getLoader())
            x, rating = next(db.x)

        ids = []
        x = torch.squeeze(x.T)
        for d in x:
            ids.append(torch.squeeze(d))
        preds = weights(ids)
        loss = db.loss_fun(preds, torch.squeeze(rating))
        if db.num % 100 == 0:
            logging.info(f"iteration {db.num} has loss {loss.item()}")
        loss.backward()
        grads = [weights.user_factors.weight.grad,
                 weights.item_factors.weight.grad]
        db.calc_time = time.perf_counter() - start
        if db.num == 4999:
            logging.info(f"calc time: {db.calc_time}")
        return grads

dw = AIDA.connect('nwhe_middleware', 'bixi', 'bixi', 'bixi', 'mf')
print('Registering model')
service = dw._RegisterModel(MatrixFactorization)
print('Fitting model')
start = time.perf_counter()
service.fit(dw.mf_data, 5000, sync=False)
stop = time.perf_counter()
print(f'Central Model finished in {stop-start}')
dw._close()
