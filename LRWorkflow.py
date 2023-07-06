from aida.aida import *
import time
import torch

class LinearRegression(torch.nn.Module):
    def __init__(self, input_size, output_size):
        import torch
        self.linear = torch.nn.Linear(input_size, output_size)

    def forward(self, input):
        return self.linear(input)

class FirstStep():
    @staticmethod
    def work(dw, data, context=None):
        import logging
        import torch

        data.makeLoader([('x1', 'x2', 'x3', 'x4', 'x5'), 'y'], 1000)
        dw.iterator = iter(data.getLoader())
        dw.loss = torch.nn.MSELoss()
        return

    @staticmethod
    def aggregate(dw, results, cxt):
        import logging
        import torch
        dw.optimizer = torch.optim.SGD(dw.lr_model.parameters(), lr=1e-3)
        return dw.lr_model

class Iterate():
    @staticmethod
    def work(dw, data, context):
        import logging
        import torch
        import time

        try:
            dw.num += 1
        except KeyError:
            dw.num = 0
            dw.calc_time = 0
        start = time.perf_counter()
        model = context['previous']
        try:
            batch, target = next(dw.iterator)
        except StopIteration:
            dw.iterator = iter(data.getLoader())
            batch, target = next(dw.iterator)

        preds = model(torch.squeeze(batch).float())
        loss = dw.loss(torch.squeeze(preds), target)
        if dw.num % 100 == 0:
            logging.info(f"iteration {dw.num} has loss {loss.item()}")
        loss.backward()
        grads = []
        for param in model.parameters():
            grads.append(param.grad)
        dw.calc_time = time.perf_counter() - start
        if dw.num == 4999:
            logging.info(f"total calc time {dw.calc_time}")
        return grads

    @staticmethod
    def aggregate(dw, results, cxt):
        import time

        if not hasattr(dw, "agg_time"):
            dw.agg_time = 0
        start = time.pref_counter()
        dw.optimizer.zero_grad()
        for r in results:
            for grad, param in zip(r, dw.lr_model.parameters()):
                param.grad = grad
        dw.optimizer.step()
        dw.agg_time += time.perf_counter() - start
        return dw.lr_model

dw = AIDA.connect('localhost', 'bixi', 'bixi','bixi', 'lr')
dw.lr_model = LinearRegression(5, 1)
job = [FirstStep(), (Iterate(), 5000)]
print('start work aggregate job')
start = time.perf_counter()
dw._workAggregateJob(job, dw.lr_data, sync=False)
stop = time.perf_counter()
print(f'Work-aggregate LR completed in {stop-start}')
print(f"Aggregation time: {dw.agg_time}")
dw._close()
