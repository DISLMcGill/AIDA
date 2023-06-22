import torch
import time
from aida.aida import *
class MatrixFactorization:
    class MatrixFactorization(torch.nn.Module):
        def __init__(self):
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
        self.optimizer = torch.optim.SGD(self.weights.parameters(), lr=0.0002, weight_decay=0.02)

    def aggregate(self, update):
        self.optimizer.zero_grad()
        self.weights.user_factors.grad = update[0]
        self.weights.item_factors.grad = update[1]
        self.optimizer.step()

    def initialize(self, data):
        pass

    @staticmethod
    def preprocess(db, data):
        import torch
        data.makeLoader([('user_id', 'movie_id'), 'rating'], 1000)
        db.x = iter(data.getLoader())
        db.loss_fun = torch.nn.MSELoss()

    @staticmethod
    def iterate(db, data, weights):
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
        preds = weights(ids)
        loss = db.loss_fun(preds, torch.squeeze(rating))
        loss.backward()
        grads = [torch.sparse_coo_tensor(torch.unsqueeze(ids[0], dim=0), weights.user_factors.grad.grad, (1500, 3)),
                 torch.sparse_coo_tensor(torch.unsqueeze(ids[1], dim=0), weights.item_factors.grad, (2000, 3))]
        return grads

dw = AIDA.connect('nwhe_middleware', 'bixi', 'bixi', 'bixi')
print('Registering model')
service = dw._RegisterModel(MatrixFactorization)
print('Fitting model')
start = time.perf_counter()
service.fit(dw.mf_data, 15000, sync=False)
stop = time.perf_counter()
print(f'Central Model finished in {stop-start}')
dw._close()
