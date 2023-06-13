import torch
import time
from aida.aida import *

class LinearRegression(torch.nn.Module):
    def __init__(self):
        super().__init__()
        self.linear = torch.nn.Linear(5, 1)

    def forward(self, data):
        return self.linear(data)


class LRPS:
    def __init__(self, model):
        import torch
        self.model = model
        self.optimizer = torch.optim.SGD(self.model.parameters(), lr=1e-3)

    def pull(self, param_ids):
        return self.model

    def update(self, update):
        self.optimizer.zero_grad()
        for weight, up in zip(self.model.parameters(), update):
            weight.grad = up
        self.optimizer.step()

    @staticmethod
    def run_training(con, ps, data):
        import torch
        data.makeLoader([('x1', 'x2', 'x3', 'x4', 'x5'), 'y'], 1000)
        iterator = iter(data.getLoader())
        loss_fn = torch.nn.MSELoss()

        for i in range(50000):
            try:
                batch, target = next(iterator)
            except StopIteration:
                iterator = iter(data.getLoader())
                batch, target = next(iterator)

            model = ps.pull(None)
            preds = model(torch.squeeze(batch).float())
            loss = loss_fn(torch.squeeze(preds), target)
            loss.backward()
            grads = []
            for param in model.parameters():
                grads.append(param.grad)
            ps.push(grads)

dw = AIDA.connect('nwhe_middleware', 'bixi', 'bixi', 'bixi', 'mf')
print('making parameter server')
server = dw._MakeParamServer(LinearRegression, LRPS)
print('fitting mf')
data = dw.lr_data
start = time.perf_counter()
server.start_training(data)
stop = time.perf_counter()
print(f'execution time for custom param server: {stop - start}')