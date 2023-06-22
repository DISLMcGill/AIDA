import torch
import time
from aida.aida import *


class MatrixFactorization(torch.nn.Module):
    def __init__(self):
        super().__init__()
        self.user_factors = torch.nn.Embedding(1500, 3, sparse=True)
        self.item_factors = torch.nn.Embedding(2000, 3, sparse=True)

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
        dw.optimizer = torch.optim.SGD(dw.weights.parameters(), lr=0.0002, weight_decay=0.02)
        return dw.weights

class Iterate:
    @staticmethod
    def work(db, data, cxt):
        import torch
        try:
            data, rating = next(db.x)
        except StopIteration:
            db.x = iter(data.getLoader())
            data, rating = next(db.x)

        ids = []
        data = torch.squeeze(data.T)
        for d in data:
            ids.append(torch.squeeze(d))
        weights = cxt['previous']
        preds = weights(ids)
        loss = db.loss_fun(preds, torch.squeeze(rating))
        loss.backward()
        grads = [torch.sparse_coo_tensor(torch.unsqueeze(ids[0], dim=0), weights.user_factors.grad, (1500, 3)),
                 torch.sparse_coo_tensor(torch.unsqueeze(ids[1], dim=0), weights.item_factors.grad, (2000, 3))]
        return grads

dw = AIDA.connect('nwhe_middleware', 'bixi', 'bixi','bixi', 'lr')
dw.MatrixFactorization = MatrixFactorization
job = [Preprocess(), (Iterate(), 50000)]
print('start work aggregate job')
start = time.perf_counter()
dw._workAggregateJob(job, dw.mf_data, sync=False)
stop = time.perf_counter()
print(f'Work-aggregate MF completed in {stop-start}')
dw._close()
