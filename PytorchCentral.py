import torch
import time
import argparse

from torch.utils.data import Dataset, DataLoader

from PytorchRPC import MFDataset, MatrixFactorization, LRDataset, LinearRegression

def run_training_loop(model, iterations, train_loader):
    net = model()
    if model == MatrixFactorization:
        opt = torch.optim.SGD(net.parameters(), lr=0.1)
    else:
        opt = torch.optim.SGD(net.parameters(), lr=0.0003)

    x = iter(train_loader)
    loss_fun = torch.nn.MSELoss()
    calc_time = 0
    batch_time = 0
    for i in range(iterations):
        opt.zero_grad()
        s = time.perf_counter()
        try:
            data, target = next(x)
        except StopIteration:
            x = iter(train_loader)
            data, target = next(x)
        batch_time += time.perf_counter() - s

        start = time.perf_counter()
        model_output = net(torch.squeeze(data).float())
        loss = loss_fun(torch.squeeze(model_output), target.float())
        if i % 5000 == 0:
            print(f"training batch {i} loss {loss.item()}")
        loss.backward()
        opt.step()
        calc_time += time.perf_counter() - start

    print(f"Training complete! {calc_time=} {batch_time=}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Pytorch Central training for timing purposes")
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

    if args.dataset == "lr":
        dataloader = DataLoader(LRDataset(args.filename), batch_size=args.batch_size, shuffle=True)
        model = LinearRegression
    else:
        dataloader = DataLoader(MFDataset(args.filename), batch_size=args.batch_size, shuffle=True)
        model = MatrixFactorization

    start = time.perf_counter()
    run_training_loop(model, args.iterations, dataloader)
    print(f'finished in {time.perf_counter() - start}')