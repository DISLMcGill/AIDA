import os
import sys
import tempfile
import time
import argparse
import torch
import torch.distributed as dist
import torch.nn as nn
import torch.optim as optim

from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data import Dataset, DataLoader
from PytorchRPC import MatrixFactorization, LinearRegression, MFDataset, LRDataset

def run_training_loop(rank, model, iterations, train_loader):
    net = DDP(model())
    if model == MatrixFactorization:
        opt = torch.optim.SGD(net.params(), lr=0.0002, weight_decay=0.02)
    else:
        opt = torch.optim.SGD(net.params(), lr=0.0003)

    x = iter(train_loader)
    loss_fun = torch.nn.MSELoss()
    for i in range(iterations):
        try:
            data, target = next(x)
        except StopIteration:
            x = iter(train_loader)
            data, target = next(x)

        model_output = net(torch.squeeze(data))
        loss = loss_fun(torch.squeeze(model_output), target)
        if i % 100 == 0:
            print(f"Rank {rank} training batch {i} loss {loss.item()}")
        loss.backward()
        opt.step()

    print("Training complete!")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Distributed Data Parallel based training")
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

    dist.init_process_group("gloo", rank=args.rank, world_size=args.world_size)

    if args.dataset == "lr":
        dataloader = DataLoader(LRDataset(args.filename), batch_size=args.batch_size)
        model = LinearRegression
    else:
        dataloader = DataLoader(MFDataset(args.filename), batch_size=args.batch_size)
        model = MatrixFactorization

    start = time.perf_counter()
    run_training_loop(args.rank, model, args.iterations, dataloader)
    dist.destroy_process_group()

    print(f'Rank {args.rank} finished in {time.perf_counter() - start}')
