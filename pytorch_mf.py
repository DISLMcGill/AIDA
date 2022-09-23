import pandas as pd
import numpy as np
import torch
import time

num_epochs = 10000

dfr = pd.read_csv('movie_data_unified.csv')
dfr.columns = ['user_id', 'movie_id', 'Rating']
rating_matrix = df.pivot(index='user_id', columns='movie_id', values=Rating)
n_users, n_movies = rating_matrix.shape

print((n_users, n_movies))

class MatrixFactorization(torch.nn.Module):
    def __init__(self, n_users, n_items, n_factors=3):
        super().__init__()
        self.user_factors = torch.nn.Embedding(n_users, n_factors, sparse=True)
        self.item_factors = torch.nn.Embedding(n_items, n_factors, sparse=True)

    def forward(self, user, item):
        return (self.user_factors(user) * self.item_factors(item)).sum(1)

rating_matrix[rating_matrix.isna()] = 0
rating_matrix = torch.from_numpy(rating_matrix.values)

model = MatrixFactorization(n_users, n_movies)
loss_func = torch.nn.MSELoss()
optimizer = torch.optim.SGD(model.parameters(), lr=1e-6)

users, movies = rating_matrix.nonzero()

for epoch in range(num_epochs):
    start = time.perf_counter()
    p = np.random.permutation(25)
    rows, cols = users[p], movies[p]
    for row, col in zip(*(rows, cols)):
        # Set gradients to zero
        optimizer.zero_grad()

        # Turn data into tensors
        rating = torch.FloatTensor([rating_matrix[row, col]])
        row = torch.LongTensor([row])
        col = torch.LongTensor([col])

        # Predict and calculate loss
        prediction = model(row, col)
        loss = loss_func(prediction, rating)

        # Backpropagate
        loss.backward()

        # Update the parameters
        optimizer.step()
    print(f'iteration {epoch} time: {time.perf_counter() - start}')