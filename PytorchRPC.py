import torch
import torch.distributed.autograd as dist_autograd
import torch.distributed.rpc as rpc
from torch.distributed.optim import DistributedOptimizer
from torch.utils.data import Dataset, DataLoader
import numpy as np
import os

from threading import Lock
import time
import csv
from collections import OrderedDict
import argparse

class MatrixFactorization(torch.nn.Module):
    def __init__(self):
        super().__init__()
        self.user_factors = torch.nn.Embedding(73517, 3)
        self.item_factors = torch.nn.Embedding(34476, 3)

    def forward(self, data):
        user = torch.squeeze(data[:, [0]])
        item = torch.squeeze(data[:, [1]])
        return (self.user_factors(user) * self.item_factors(item)).sum(1)

class LinearRegression(torch.nn.Module):
    def __init__(self):
        super().__init__()
        self.linear = torch.nn.Linear(5, 1)

    def forward(self, input):
        return self.linear(input)

def call_method(method, rref, *args, **kwargs):
    return method(rref.local_value(), *args, **kwargs)

def remote_method(method, rref, *args, **kwargs):
    args = [method, rref] + list(args)
    return rpc.rpc_sync(rref.owner(), call_method, args=args, kwargs=kwargs)

class ParameterServer(torch.nn.Module):
    def __init__(self, model):
        super().__init__()
        model = model()
        self.model = model

    def forward(self, inp):
        out = self.model(inp)
        return out

    def get_dist_gradients(self, cid):
        grads = dist_autograd.get_gradients(cid)
        return grads

    def get_param_rrefs(self):
        param_rrefs = [rpc.RRef(param) for param in self.model.parameters()]
        return param_rrefs

param_server = None
global_lock = Lock()

def get_parameter_server(m):
    """
    Returns a singleton parameter server to all trainer processes
    """
    global param_server
    with global_lock:
        if not param_server:
            param_server = ParameterServer(m)
        return param_server

def run_parameter_server(rank, world_size):
    print("PS master initializing RPC")
    rpc.init_rpc(name="parameter_server", rank=rank, world_size=world_size)
    print("RPC initialized! Running parameter server...")
    start = time.perf_counter()
    rpc.shutdown()
    stop = time.perf_counter()
    print("RPC shutdown on parameter server.")
    print(f"Runtime = {stop-start}")

class TrainerNet(torch.nn.Module):
    def __init__(self, m):
        super().__init__()
        self.model = m
        self.param_server_rref = rpc.remote(
            "parameter_server", get_parameter_server, args=(m,))

    def get_global_param_rrefs(self):
        remote_params = remote_method(
            ParameterServer.get_param_rrefs,
            self.param_server_rref)
        return remote_params

    def forward(self, x):
        model_output = remote_method(
            ParameterServer.forward, self.param_server_rref, x)
        return model_output

class LRDataset(Dataset):
    def __init__(self, filename):
        with open(filename, "rb") as f:
            data = np.loadtxt(f, delimiter=",")
        self.data = torch.IntTensor(data[:,[0,1,2,3,4]])
        self.targets = torch.squeeze(torch.DoubleTensor(data[:,[5]]))

    def __len__(self):
        return self.data.shape[0]

    def __getitem__(self, item):
        return self.data[item], self.targets[item]

class MFDataset(Dataset):
    def __init__(self, filename):
        with open(filename, "rb") as f:
            data = np.loadtxt(f, delimiter=",")
        self.data = torch.IntTensor(data[:, [0, 1]])
        self.targets = torch.squeeze(torch.DoubleTensor(data[:, [2]]))

    def __len__(self):
        return self.data.shape[0]

    def __getitem__(self, item):
        return self.data[item], self.targets[item]

def run_training_loop(rank, model, iterations, train_loader):
    net = TrainerNet(model)
    param_rrefs = net.get_global_param_rrefs()
    if model == MatrixFactorization:
        opt = DistributedOptimizer(torch.optim.SGD, param_rrefs, lr=0.1)
    else:
        opt = DistributedOptimizer(torch.optim.SGD, param_rrefs, lr=0.0003)

    x = iter(train_loader)
    loss_fun = torch.nn.MSELoss()
    batch_time = 0
    for i in range(iterations):
        s = time.perf_counter()
        try:
            data, target = next(x)
        except StopIteration:
            x = iter(train_loader)
            data, target = next(x)
        batch_time += time.perf_counter() - s

        with dist_autograd.context() as cid:
            model_output = net(torch.squeeze(data).float())
            loss = loss_fun(torch.squeeze(model_output), target.float())
            if i % 100 == 0:
                print(f"Rank {rank} training batch {i} loss {loss.item()}")
            dist_autograd.backward(cid, [loss])
            opt.step(cid)

    print(f"Training complete! loss = {loss.item()} {batch_time=}")

def run_worker(rank, world_size, model, iterations, train_loader):
    print(f"Worker rank {rank} initializing RPC")
    rpc.init_rpc(
        name=f"trainer_{rank}",
        rank=rank,
        world_size=world_size)

    print(f"Worker {rank} done initializing RPC")
    start = time.perf_counter()

    run_training_loop(rank, model, iterations, train_loader)
    rpc.shutdown()
    stop = time.perf_counter()
    print(f"Worker {rank} runtime = {stop-start}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Parameter-Server RPC based training")
    parser.add_argument(
        "--world_size",
        type=int,
        default=3,
        help="""Total number of participating processes. Should be the sum of
        master node and all training nodes.""")
    parser.add_argument(
        "rank",
        type=int,
        default=None,
        help="Global rank of this process. Pass in 0 for master.")
    parser.add_argument(
        "--master_addr",
        type=str,
        default="localhost",
        help="""Address of master, will default to localhost if not provided.
        Master must be able to accept network traffic on the address + port.""")
    parser.add_argument(
        "--master_port",
        type=str,
        default="29500",
        help="""Port that master is listening on, will default to 29500 if not
        provided. Master must be able to accept network traffic on the host and port.""")
    parser.add_argument(
        "--dataset",
        type=str,
        default="lr",
        help="""Dataset being used""")
    parser.add_argument(
        "--filename",
        type=str,
        default="lr_data.csv",
        help="""Path to dataset file being used""")
    parser.add_argument(
        "--batch_size",
        type=int,
        default=1000,
        help="""Batch size""")
    parser.add_argument(
        "--iterations",
        type=int,
        default=5000,
        help="""Number of iterations per worker""")

    args = parser.parse_args()
    assert args.rank is not None, "must provide rank argument."
    os.environ['MASTER_ADDR'] = args.master_addr
    os.environ["MASTER_PORT"] = args.master_port


    if args.rank == 0:
        run_parameter_server(args.rank, args.world_size)
    else:
        if args.dataset == "lr":
            dataloader = DataLoader(LRDataset(args.filename), batch_size=args.batch_size, shuffle=True)
            model = LinearRegression
        else:
            dataloader = DataLoader(MFDataset(args.filename), batch_size=args.batch_size, shuffle=True)
            model = MatrixFactorization

        run_worker(args.rank, args.world_size, model, args.iterations, dataloader)
