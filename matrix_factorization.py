from aida.aida import *
from aida.model import Model
from collections import Counter

dw = AIDA.connect('whe_middleware', 'bixi', 'bixi', 'bixi', 'mf')

class MatrixFactorization:
    def __init__(self):
        self.info = {}
        self.weights = None

    @staticmethod
    def preprocess(db, x):
        return x

    def initialize(self, x):
        import numpy as np
        if self.weights is None:
            self.info['users'] = list(x.project('user_id').distinct().order('user_id').cdata['user_id'])
            self.info['movies'] = list(x.project('movie_id').distinct().order('movie_id').cdata['movie_id'])
            users_matrix = dict.fromkeys(self.info['users'], np.random.rand(3))
            movies_matrix = dict.fromkeys(self.info['movies'], np.random.rand(3))
            self.weights = (users_matrix, movies_matrix)

    @staticmethod
    def iterate(db, x, weights, batch_size):
        import numpy as np
        batch = np.random.choice(x.shape[0], batch_size, replace=False)
        batch_x = x[batch, :].cdata
        users_update = {}
        movies_update = {}
        for p in range(batch_x['movie_id'].shape[0]):
            user = batch_x['user_id'][p]
            movie = batch_x['movie_id'][p]
            e = batch_x['rating'][p] - np.dot(weights[0][user], weights[1][movie])
            users_update[user] = users_update.get(user, 0) + 0.0002 * (2 * e * weights[1][movie] - 0.02 * weights[0][user])
            movies_update[movie] = movies_update.get(movie, 0) + 0.0002 * (2 * e * weights[0][user] - 0.02 * weights[1][movie])
        return (users_update, movies_update)

    def aggregate(self, results):
        from collections import Counter
        if self.sync:
            user_sums = Counter()
            user_counters = Counter()
            movie_sums = Counter()
            movie_counters = Counter()
            for update in results:
                user_sums.update(update[0])
                user_counters.update(update[0].keys())
                movie_sums.update(update[1])
                movie_counters.update(update[1].keys())
            user_update = {x: float(user_sums[x]) / user_counters[x] for x in user_sums.keys()}
            movie_update = {x: float(movie_sums[x] / movie_counters[x]) for x in movie_sms.keys()}
            for u in user_update:
                self.weights[0][u] = self.weights[0][u] + user_update[u]
            for m in movie_update:
                self.weights[1][m] = self.weights[1][m] + movie_update[m]
        else:
            for u in results[0]:
                self.weights[0][u] = self.weights[0][u] + results[0][u]
            for m in results[1]:
                self.weights[1][m] = self.weights[1][m] + results[1][m]

    def get_params(self):
        return (self.weights[0], self.weights[1].T)

    def score(self, x):
        y = x.cdata
        e = 0
        for p in range(y['movie_id'].shape[0]):
            user = y['user_id'][p]
            movie = y['movie_id'][p]
            e = e + pow(y['rating'][p] - np.dot(self.weights[0][user], self.weights[1][movie]), 2)
            e = e + 0.01 * (np.sum(np.power(self.weights[0][user], 2)) + np.sum(np.power(self.weights[1][movie], 2)))
        return e


print("Sync Central model")

print(f'iteration {i}')
print("Sending model...")
m = (dw._RegisterModel(MatrixFactorization()))

print("Fitting model...")
x = dw.mf_data

start = time.perf_counter()
m.fit(x, 10000, batch_size=25)
end = time.perf_counter()

print(f"Fitting time: {end - start}")
score = m.score(x)
print(f"Model score: {score}")


