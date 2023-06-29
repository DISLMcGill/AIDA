from aida.aida import *
import torch
import time

class MatrixFactorization(torch.nn.Module):
    def __init__(self):
        super().__init__()
        self.user_factors = torch.nn.Embedding(1500, 3, sparse=True)
        self.item_factors = torch.nn.Embedding(2000, 3, sparse=True)

    def forward(self, data):
        user = data[0]
        item = data[1]
        return (self.user_factors(user) * self.item_factors(item)).sum(1)

class CustomMF:
    def __init__(self, model):
        import torch
        self.model = model()
        self.optimizer = torch.optim.SGD(self.model.parameters(), lr=0.0002, weight_decay=0.02)

    def pull(self, param_ids):
        return (self.model.user_factors(param_ids[0]), self.model.item_factors(param_ids[1]))

    def update(self, update):
        self.optimizer.zero_grad()
        self.model.user_factors.grad = update[0]
        self.model.item_factors.grad = update[1]
        self.optimizer.step()

    @staticmethod
    def run_training(con, ps, data):
        import torch
        data.makeLoader([('user_id', 'movie_id'), 'rating'], 1000)
        x = iter(data.getLoader())
        epochs = 5000
        loss_fun = torch.nn.MSELoss()
        for i in range(epochs):
            try:
                data, rating = next(x)
            except StopIteration:
                x = iter(data.getLoader())
                data, rating = next(x)

            ids = []
            data = torch.squeeze(data.T)
            for d in data:
                ids.append(torch.squeeze(d))
            factors = ps.pull(ids)
            preds = (factors[0] * factors[1]).sum(1)
            loss = loss_fun(preds, torch.squeeze(rating))
            loss.backward()
            grads = []
            grads.append(torch.sparse_coo_tensor(torch.unsqueeze(ids[0], dim=0), factors[0].grad, (1500,3)))
            grads.append(torch.sparse_coo_tensor(torch.unsqueeze(ids[1], dim=0), factors[1].grad, (2000,3)))
            ps.push(grads)

dw = AIDA.connect('nwhe_middleware', 'bixi', 'bixi', 'bixi', 'mf')
print('making parameter server')
server = dw._MakeParamServer(MatrixFactorization, CustomMF)
print('fitting mf')
data = dw.mf_data
start = time.perf_counter()
server.start_training(data)
stop = time.perf_counter()
print(f'execution time for custom param server: {stop - start}')
dw._close()
