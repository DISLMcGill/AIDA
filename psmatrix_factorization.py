from aida.aida import *
import time
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
            self.info['users'] = list(x[0].project('user_id').distinct().cdata['user_id'])
            self.info['movies'] = list(x[0].project('movie_id').distinct().cdata['movie_id'])
            users_matrix = dict.fromkeys([f'user_{u}' for u in self.info['users']], np.zeros(3))
            movies_matrix = dict.fromkeys([f'movie_{m}' for m in self.info['movies']], np.zeros(3))
            self.weights = {**users_matrix, **movies_matrix}

    @staticmethod
    def iterate(db, ps, x, batch_size):
        import numpy as np
        # movie keys local -- updated all at the end
        movies = x.project('movie_id').distinct().cdata['movie_id']
        movies_params = dict.fromkeys([f'movie_{m}' for m in movies], np.random.rand(3))
        for i in range(10000):
            batch = np.random.choice(x.shape[0], batch_size, replace=False)
            batch_x = x[batch, :].cdata

            # pull user keys from ps, user params updated every iteration
            keys_to_pull = set()
            for user in batch_x['user_id']:
                keys_to_pull.add(f'user_{user}')
            update_keys = ps.pull(keys_to_pull)
            users_update = {}
            for p in range(batch_x['movie_id'].shape[0]):
                user = 'user_{}'.format(batch_x['user_id'][p])
                movie = 'movie_{}'.format(batch_x['movie_id'][p])
                e = batch_x['rating'][p] - np.dot(update_keys[user], movies_params[movie])
                for i in range(3):
                    users_update[user] = users_update.get(user, 0) + 0.0002 * (2 * e * movies_params[movie] - 0.02 * update_keys[user])
                    # movie updates are not batched -- to be fixed
                    movies_params[movie] = movies_params.get(movie, 0) + 0.0002 * (2 * e * update_keys[user] - 0.02 * movies_params[movie])
            ps.push(users_update)
        ps.push(movies_params)

    def score(self, ps, x):
        import numpy as np
        y = x.cdata
        e = 0
        weights = ps.pull(self.weights.keys())
        for p in range(y['movie_id'].shape[0]):
            user = 'user_{}'.format(y['user_id'][p])
            movie = 'movie_{}'.format(y['movie_id'][p])
            e = e + pow(y['rating'][p] - np.dot(weights[user], weights[movie]), 2)
            e = e + 0.01 * (np.sum(np.power(weights[user], 2)) + np.sum(np.power(weights[movie], 2)))
        return e

print("PS model")

print("Sending model...")
m = dw._RegisterPSModel(MatrixFactorization())

print("Fitting model...")
x = dw.mf_data

start = time.perf_counter()
m.fit([x], batch_size=25)
end = time.perf_counter()

print(f"Fitting time: {end-start}")
score = m.score(x)
print(f"Model score: {score}")



