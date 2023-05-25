from aida.aida import *
import torch

class LinearRegression(torch.nn.Module):
    def __init__(self, input_size, output_size):
        self.linear = torch.nn.Linear(input_size, output_size)

    def forward(self, input):
        return self.linear(input)

class FirstStep():
    @staticmethod
    def work(dw, data, context=None):
        import logging
        import torch

        logging.info('Worker started preprocessing')
        data.makeLoader([('x1', 'x2', 'x3', 'x4', 'x5'), 'y'], 1000)
        dw.iterator = iter(data.getLoader())
        dw.loss = torch.nn.MSELoss()
        return

    @staticmethod
    def aggregate(dw, results):
        import logging
        import torch
        logging.info('set up optimizer')
        dw.optimizer = torch.optim.SGD(dw.lr_model.parameters(), lr=1e-3)
        return dw.lr_model

class Iterate():
    @staticmethod
    def work(dw, data, context):
        import logging
        import torch

        logging.info('running iteration')
        model = context['previous']
        try:
            batch, target = next(dw.iterator)
        except StopIteration:
            dw.iterator = iter(data.getLoader())
            batch, target = next(dw.iterator)

        preds = model(torch.squeeze(batch).float())
        loss = dw.loss(torch.squeeze(preds), target)
        loss.backward()
        grads = []
        for param in model.parameters():
            grads.append(param.grad)
        return grads

    @staticmethod
    def aggregate(dw, results):
        import logging

        logging.info('running aggregation')
        dw.optimizer.zero_grad()
        for r in results:
            for grad, param in zip(r, dw.lr_model.parameters()):
                param.grad = grad
        dw.optimizer.step()
        return dw.lr_model

dw = AIDA.connect('nwhe_middleware', 'bixi', 'bixi','bixi', 'lr')
dw.lr_model = LinearRegression(5, 1)
job = [FirstStep(), (Iterate(), 50000)]
print('start work aggregate job')
dw._workAggregateJob(job, dw.lr_data)
