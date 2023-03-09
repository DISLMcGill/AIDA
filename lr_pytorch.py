from aida.aida import *
import torch
import time

dw = AIDA.connect('whe_middleware', 'bixi', 'bixi', 'bixi', 'mf')

class LinearRegression(torch.nn.Module):
    def __init__(self, input_size, output_size):
        self.linear = torch.nn.Linear(input_size, output_size)

    def forward(self, input):
        return self.linear(input)

data = dw.lr_data
split = [('x1', 'x2', 'x3', 'x4', 'x5'), 'y']
model = LinearRegression(5, 1)

print('Sending model')
service = dw._RegisterPytorchModel(model)

print('training model')
service.train((data, split), 10000, 25)
