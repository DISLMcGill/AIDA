import torch
import time
from aida.aida import *


class MatrixFactorization(torch.nn.Module):
    def __init__(self):
        import torch
        super().__init__()
        self.user_factors = torch.nn.Embedding(1500, 3)
        self.item_factors = torch.nn.Embedding(2000, 3)

    def forward(self, data):
        user = data[0]
        item = data[1]
        return (self.user_factors(user) * self.item_factors(item)).sum(1)

class Preprocess:
    @staticmethod
    def work(dw, data, cxt):
        import torch
        data.makeLoader([('user_id', 'movie_id'), 'rating'], 1000)
        dw.x = iter(data.getLoader())
        dw.loss_fun = torch.nn.MSELoss()

    @staticmethod
    def aggregate(dw, data, cxt):
        import torch
        dw.weights = dw.MatrixFactorization()
        dw.optimizer = torch.optim.SGD(dw.weights.parameters(), lr=0.00001, weight_decay=0.002)
        return dw.weights

class Iterate:
    @staticmethod
    def work(db, data, cxt):
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
        weights = cxt['previous']
        preds = weights(ids)
        loss = db.loss_fun(preds, torch.squeeze(rating))
        if db.num % 100 == 0:
            logging.info(f"iteration {i} with loss {loss.item()}")
        loss.backward()
        grads = [weights.user_factors.weight.grad,
                 weights.item_factors.weight.grad]
        db.calc_time += time.perf_counter() - start
        if db.num == 4999:
            print(f"calc time: {db.calc_time}")
        return grads

    @staticmethod
    def aggregate(db, results, cxt):
        import time

        if not hasattr(dw, "agg_time"):
            dw.agg_time = 0
        start = time.pref_counter()
        db.optimizer.zero_grad()
        db.weights.user_factors.weight.grad = results[0]
        db.weights.item_factors.weight.grad = results[1]
        db.optimizer.step()
        db.agg_time = time.pref_counter() - start
        return db.weights

dw = AIDA.connect('localhost', 'bixi', 'bixi','bixi', 'mf')
dw.MatrixFactorization = MatrixFactorization
job = [Preprocess(), (Iterate(), 5000)]
print('start work aggregate job')
start = time.perf_counter()
dw._workAggregateJob(job, dw.mf_data, sync=False)
stop = time.perf_counter()
print(f'Work-aggregate MF completed in {stop-start}')
print(f"Aggregation time: {dw.agg_time}")
dw._close()
