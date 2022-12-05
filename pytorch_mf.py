import pandas as pd
import numpy as np
import torch
import time

num_epochs = 10000

dfr = pd.read_csv('movie_data_unified.csv')
dfr.columns = ['user_id', 'movie_id', 'rating']
n_users = dfr['user_id'].nunique()
users = dfr['user_id'].unique()
n_movies = dfr['movie_id'].nunique()
movies = dfr['movie_id'].unique()

u_map = dict(zip(users, range(n_users)))
m_map = dict(zip(movies, range(n_movies)))

class MatrixFactorization(torch.nn.Module):
    def __init__(self, n_users, n_items, n_factors=3):
        super().__init__()
        self.user_factors = torch.nn.Embedding(n_users, n_factors, sparse=True)
        self.item_factors = torch.nn.Embedding(n_items, n_factors, sparse=True)

    def forward(self, user, item):
        return (self.user_factors(user) * self.item_factors(item)).sum(1)

model = MatrixFactorization(n_users, n_movies)
loss_func = torch.nn.MSELoss()
optimizer = torch.optim.SGD(model.parameters(), lr=1e-6)

def iter(row, col, rating):
    # Set gradients to zero
    optimizer.zero_grad()

    # Turn data into tensors
    rating = torch.FloatTensor([rating])
    row = torch.LongTensor([row])
    col = torch.LongTensor([col])

    # Predict and calculate loss
    prediction = model(row, col)
    loss = loss_func(prediction, rating)

    # Backpropagate
    loss.backward()

    # Update the parameters
    optimizer.step()

for epoch in range(num_epochs):
    start = time.perf_counter()
    batch = dfr.sample(25)
    [iter(u_map(row[0]), m_map(row[1]), row[2]) for row in batch[['user_id', 'movie_id', 'rating']].to_numpy()]
    print(f'iteration {epoch} time: {time.perf_counter() - start}')