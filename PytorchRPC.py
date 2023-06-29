import torch
import torch.distributed.autograd as dist_autograd
import torch.distributed.rpc as rpc
from torch.distributed.optim import DistributedOptimizer
from torch.utils.data import Dataset, DataLoader
import os

from threading import Lock
import time
import csv
from collections import OrderedDict
import argparse

class MatrixFactorization(torch.nn.Module):
    def __init__(self):
        super().__init__()
        self.user_factors = torch.nn.Embedding(1500, 3)
        self.item_factors = torch.nn.Embedding(2000, 3)

    def forward(self, data):
        user = data[0]
        item = data[1]
        return (self.user_factors(user) * self.item_factors(item)).sum(1)

class LinearRegression(torch.nn.Module):
    def __init__(self):
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
    rpc.shutdown()
    print("RPC shutdown on parameter server.")

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
        with open(filename) as f:
            reader = csv.reader(f)
            self.data = OrderedDict()
            for r in reader:
                self.data.setdefault('x1', []).append(float(r[0]))
                self.data.setdefault('x2', []).append(float(r[1]))
                self.data.setdefault('x3', []).append(float(r[2]))
                self.data.setdefault('x4', []).append(float(r[3]))
                self.data.setdefault('x5', []).append(float(r[4]))
                self.data.setdefault('y', []).append(float(r[5]))
            for x in self.data:
                self.data[x] = torch.FloatTensor(self.data[x])

    def __len__(self):
        return len(self.data['x1'])

    def __getitem__(self, item):
        x = [self.data['x1'][item], self.data['x2'][item], self.data['x3'][item],
             self.data['x4'][item], self.data['x5'][item]]
        x = torch.cat(x, dim=0)
        y = self.data['y'][item]
        return x, y

class MFDataset(Dataset):
    def __init__(self, filename):
        with open(filename) as f:
            reader = csv.reader(f)
            self.data = OrderedDict()
            for r in reader:
                self.data.setdefault('user_id', []).append(float(r[0]))
                self.data.setdefault('movie_id', []).append(float(r[1]))
                self.data.setdefault('rating', []).append(float(r[2]))
            for x in self.data:
                self.data[x] = torch.FloatTensor(self.data[x])

    def __len__(self):
        return len(self.data['user_id'])

    def __getitem__(self, item):
        x = [self.data['user_id'][item], self.data['movie_id'][item]]
        x = torch.cat(x, dim=0)
        y = self.data['rating'][item]
        return x, y

def run_training_loop(rank, model, iterations, train_loader):
    net = TrainerNet(model)
    param_rrefs = net.get_global_param_rrefs()
    if model == MatrixFactorization:
        opt = DistributedOptimizer(torch.optim.SGD, param_rrefs, lr=0.0002, weight_decay=0.02)
    else:
        opt = DistributedOptimizer(torch.optim.SGD, param_rrefs, lr=0.03)

    x = iter(train_loader)
    loss_fun = torch.nn.MSELoss()
    for i in range(iterations):
        try:
            data, target = next(x)
        except StopIteration:
            x = iter(train_loader)
            data, target = next(x)

        with dist_autograd.context() as cid:
            model_output = net(torch.squeeze(data))
            loss = loss_fun(torch.squeeze(model_output), target)
            if i % 100 == 0:
                print(f"Rank {rank} training batch {i} loss {loss.item()}")
            dist_autograd.backward(cid, [loss])
            # Ensure that dist autograd ran successfully and gradients were
            # returned.
            assert remote_method(
                ParameterServer.get_dist_gradients,
                net.param_server_rref,
                cid) != {}
            opt.step(cid)

    print("Training complete!")

def run_worker(rank, world_size, model, iterations, train_loader):
    print(f"Worker rank {rank} initializing RPC")
    rpc.init_rpc(
        name=f"trainer_{rank}",
        rank=rank,
        world_size=world_size)

    print(f"Worker {rank} done initializing RPC")

    run_training_loop(rank, model, iterations, train_loader)
    rpc.shutdown()

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

    args = parser.parse_args()
    assert args.rank is not None, "must provide rank argument."
    os.environ['MASTER_ADDR'] = args.master_addr
    os.environ["MASTER_PORT"] = args.master_port


    if args.rank == 0:
        start = time.perf_counter()
        run_parameter_server(args.rank, args.world_size)
    else:
        if args.dataset == "lr":
            dataloader = DataLoader(LRDataset(args.filename), batch_size=1000)
            model = LinearRegression
        else:
            dataloader = DataLoader(MFDataset(args.filename), batch_size=1000)
            model = MatrixFactorization

        start = time.perf_counter()
        run_worker(args.rank, args.world_size, model, 5000, dataloader)

    print(f'Rank {args.rank} finished in {time.perf_counter() - start}')
    