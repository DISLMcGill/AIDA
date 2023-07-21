from aida.aida import *
import time
import torch

class LinearRegression(torch.nn.Module):
    def __init__(self, input_size, output_size):
        import torch
        super().__init__()
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
        dw.num = 0
        dw.calc_time = 0
        return

    @staticmethod
    def aggregate(dw, results, cxt):
        import logging
        import torch
        dw.optimizer = torch.optim.SGD(dw.lr_model.parameters(), lr=1e-3)
        dw.agg_time = 0
        return dw.lr_model

class Iterate():
    @staticmethod
    def work(dw, data, context):
        import logging
        import torch
        import time

        model = context['previous']
        dw.num += 1
        try:
            batch, target = next(dw.iterator)
        except StopIteration:
            dw.iterator = iter(data.getLoader())
            batch, target = next(dw.iterator)
        start = time.perf_counter()
        preds = model(torch.squeeze(batch).float())
        loss = dw.loss(preds, target)
        if dw.num % 100 == 0:
            logging.info(f"iteration {dw.num} has loss {loss.item()}")
        loss.backward()
        grads = [p.grad for p in model.parameters()]
        dw.calc_time = time.perf_counter() - start
        if dw.num == 5000:
            logging.info(f"total calc time {dw.calc_time}")
        return grads

    @staticmethod
    def aggregate(dw, results, cxt):
        import time
        start = time.perf_counter()
        dw.lr_model.linear.weight.grad = results[0]
        dw.lr_model.linear.bias.grad = results[1]
        dw.optimizer.step()
        dw.optimizer.zero_grad()
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
