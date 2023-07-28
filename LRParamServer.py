import torch
import time
from aida.aida import *

class LinearRegression(torch.nn.Module):
    def __init__(self):
        import torch
        super().__init__()
        self.linear = torch.nn.Linear(5, 1)

    def forward(self, data):
        return self.linear(data)

class LRPS:
    def __init__(self, model):
        import torch
        self.model = model()
        self.optimizer = torch.optim.SGD(self.model.parameters(), lr=0.0003)

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
        import time
        import logging
        data.makeLoader([('x1', 'x2', 'x3', 'x4', 'x5'), 'y'], 1000)
        iterator = iter(data.getLoader())
        loss_fn = torch.nn.MSELoss()

        calc_time = 0
        batch_time = 0
        start = time.perf_counter()
        for i in range(5000):
            s = time.perf_counter()
            try:
                batch, target = next(iterator)
            except StopIteration:
                iterator = iter(data.getLoader())
                batch, target = next(iterator)
            batch_time += time.perf_counter() - s
            model = ps.pull(None)
            it_start = time.perf_counter()
            preds = model(torch.squeeze(batch).float())
            loss = loss_fn(preds, target)
            loss.backward()
            if i % 100 == 0:
                logging.info(f"iteration {i} loss: {loss.item()}")
            grads = []
            for param in model.parameters():
                grads.append(param.grad)
            it_end = time.perf_counter()
            calc_time += it_end-it_start
            ps.push(grads)
        end = time.perf_counter()
        logging.info(f"Finished iterations in {end-start} {calc_time=} {batch_time=}")

dw = AIDA.connect('localhost', 'bixi', 'bixi', 'bixi', 'lr')
print('making parameter server')
server = dw._MakeParamServer(LinearRegression, LRPS)
print('fitting mf')
data = dw.lr_data
start = time.perf_counter()
server.start_training(data)
stop = time.perf_counter()
print(f'execution time for custom param server: {stop - start}')
dw._close()
