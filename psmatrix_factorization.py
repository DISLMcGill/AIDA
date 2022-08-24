from aida.aida import *
from collections import Counter

dw = AIDA.connect('whe_middleware', 'bixi', 'bixi', 'bixi', 'mf')

class MatrixFactorization:
    def __init__(self, learning_rate, sync):
        self.info = {}
        self.learning_rate = learning_rate
        self.sync = sync
        self.weights = None

    @staticmethod
    def preprocess(db, x, y):
        return (x, y)

    def initialize(self, x, y):
        import numpy as np
        if self.weights is None:
            self.info['users'] = list(x.project('user_id').distinct().cdata['user_id'])
            self.info['movies'] = list(x.project('movie_id').distinct().cdata['movie_id'])
            users_matrix = dict.fromkeys([f'user_{u}' for u in self.info['users']], np.zeros(3))
            movies_matrix = dict.fromkeys([f'movie_{m}' for m in self.info['movies']], np.zeros(3))
            self.weights = {**users_matrix, **movies_matrix}

    @staticmethod
    def iterate(db, ps, x, y, batch_size):
        import numpy as np
        # movie keys local -- updated all at the end
        movies = x.project('movie_id').distinct().cdata['movie_id']
        movie_params = dict.fromkeys([f'movie_{m}' for m in movies], np.random.rand(3))
        for i in range(1000):
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
                e = batch_x['rating'][p] - np.dot(update_keys[user], movie_params[movie])
                for i in range(3):
                    users_update[user] = users_update.get(user, 0) + 0.0002 * (2 * e * movies_params[movie] - 0.02 * update_keys[user])
                    # movie updates are not batched -- to be fixed
                    movies_params[movie] = movies_params.get(movie, 0) + 0.0002 * (2 * e * update_keys[user] - 0.02 * movies_params[movie])
            ps.push(users_update)
        ps.push(movie_params)

    def predict(self, x):
        return (self.weights[0], self.weights[1].T)

    def get_params(self):
        return (self.weights[0], self.weights[1].T)

    @staticmethod
    def score(y_preds, y):
        return 0

print("Sending model...")
m = dw._RegisterModel(MatrixFactorization(0.0001, True))

print("Fitting model")
x = dw.mf_data
m.fit(x, x, 1000, batch_size=25)

print("Model parameters: ")
p = m.get_params()
print(p)


