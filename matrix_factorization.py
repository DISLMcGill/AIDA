from aida.aida import *
from collections import Counter

dw = AIDA.connect('whe_middleware', 'bixi', 'bixi', 'bixi', 'mf')

class MatrixFactorization:
    def __init__(self, learning_rate, sync):
        self.info = []
        self.learning_rate = learning_rate
        self.sync = sync
        self.weights = None

    @staticmethod
    def preprocess(db, x, y):
        return (x, y)

    def initialize(self, x, y):
        if self.weights is None:
            self.info['users'] = list(x.project('user_id').distinct().order('user_id').cdata['user_id'])
            self.info['movies'] = list(x.project('movie_id').distinct().order('movie_id').cdata['movie_id'])
            users_matrix = dict.from_keys(self.info['users'], np.random.rand(3))
            movies_matrix = dict.from_keys(self.info['movies'], np.random.rand(3))
            self.weights = (users_matrix, movies_matrix)
            for db in x:
                db.users = self.info['users']
                db.movies = self.info['movies']

    @staticmethod
    def iterate(db, x, y, weights, batch_size):
        import numpy as np
        batch = np.random.choice(x.shape[0], batch_size, replace=False)
        batch_x = x[batch, :].cdata
        users_update = {}
        movies_update = {}
        for p in range(batch_x['movie_id'].shape[0]):
            user = db.users.index(batch_x['user_id'][p])
            movie = db.movies.index(batch_x['movie_id'][p])
            e = batch_x['rating'][p] - np.dot(weights[0][user], weights[1][movie])
            for i in range(3):
                users_update[user] = users_update[user] + (2 * e * weights[1][movie] - 0.02 * weights[0][user])
                movies_update[movie] = movies_update[movie] + (2 * e * weights[0][user] - 0.02 * weights[1][movie])
        return (users_update, movies_update)

    def aggregate(self, results):
        if self.sync:
            user_sums = Counter()
            user_counters = Counter()
            movie_sums = Counter()
            movie_counters = Counter
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
            for m in results[m]:
                self.weights[1][m] = self.weights[1][m] + results[1][m]

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
print(m.get_params())

