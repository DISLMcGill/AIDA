import torch
import time
from aida.aida import *


class MatrixFactorization(torch.nn.Module):
    def __init__(self):
        import torch
        super().__init__()
        self.user_factors = torch.nn.Embedding(73517, 3)
        self.item_factors = torch.nn.Embedding(34476, 3)

    def forward(self, data):
        user = torch.squeeze(data[,:[0]])
        item = torch.squeeze(data[,:[1]])
        return (self.user_factors(user) * self.item_factors(item)).sum(1)

class Preprocess:
    @staticmethod
    def work(dw, data, cxt):
        import torch
        data.makeLoader([('user_id', 'movie_id'), 'rating'], 64)
        dw.x = iter(data.getLoader())
        dw.loss_fun = torch.nn.MSELoss()
        dw.num = 0
        dw.calc_time = 0

    @staticmethod
    def aggregate(db, data, cxt):
        import torch
        db.weights = db.MatrixFactorization()
        db.optimizer = torch.optim.SGD(db.weights.parameters(), lr=0.1)
        db.agg_time = 0
        return db.weights

class Iterate:
    @staticmethod
    def work(db, data, cxt):
        import torch
        import logging
        import time

        db.num += 1
        try:
            batch, rating = next(db.x)
        except StopIteration:
            db.x = iter(data.getLoader())
            batch, rating = next(db.x)

        start = time.perf_counter()
        weights = cxt['previous']
        preds = weights(batch)
        loss = db.loss_fun(preds, torch.squeeze(rating))
        if db.num % 5000 == 0:
            logging.info(f"iteration {db.num} with loss {loss.item()}")
        loss.backward()
        grads = [weights.user_factors.weight.grad,
                 weights.item_factors.weight.grad]
        db.calc_time += time.perf_counter() - start
        if db.num == 40000:
            logging.info(f"calc time: {db.calc_time}")
        return grads

    @staticmethod
    def aggregate(db, results, cxt):
        import time
        start = time.perf_counter()
        db.weights.user_factors.weight.grad = results[0]
        db.weights.item_factors.weight.grad = results[1]
        db.optimizer.step()
        db.optimizer.zero_grad()
        db.agg_time = time.perf_counter() - start
        return db.weights

dw = AIDA.connect('localhost', 'bixi', 'bixi','bixi', 'mf')
dw.MatrixFactorization = MatrixFactorization
job = [Preprocess(), (Iterate(), 40000)]
print('start work aggregate job')
start = time.perf_counter()
dw._workAggregateJob(job, dw.mf_data, sync=False)
stop = time.perf_counter()
print(f'Work-aggregate MF completed in {stop-start}')
print(f"Aggregation time: {dw.agg_time}")
dw._close()
